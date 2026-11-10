from __future__ import annotations

import csv
import hashlib
import html
import json
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Iterable
from xml.etree import ElementTree

import pandas as pd

from models import (
    ReceitaAnexo10Arquivo,
    ReceitaAnexo10Registro,
    ReceitaAnexo10Upload,
    db,
)

INPUT_DIR = Path("upload/receita_anexo10")
OUTPUT_DIR = Path("outputs/receita_anexo10")

ProgressFn = Callable[[str, int, str], None]
CancelFn = Callable[[], None]

MONEY_RE = re.compile(r"^-?\d{1,3}(?:\.\d{3})*,\d{2}$|^-?\d+,\d{2}$")
CODE_RE = re.compile(r"^\d(?:\.\d+){2,}(?:\.\d+)*$")
FONTE_RE = re.compile(r"Fonte\s+de\s+Recurso\s*:\s*([\d.]+)", re.IGNORECASE)
ORGAO_RE = re.compile(r"\b(\d{4,6})\s*-\s*([A-ZÁÀÂÃÉÊÍÓÔÕÚÜÇ][^\n\r]+)")
NO_MOVEMENT_RE = re.compile(r"N[aã]o\s+houve\s+movimenta[cç][aã]o\s+no\s+per[ií]odo\.?", re.IGNORECASE)
NO_MOVEMENT_MESSAGE = "Não houve movimentação no período."
MONTHS = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "março": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}


@dataclass
class ParsedRecord:
    codigo_receita: str
    descricao_receita: str
    orcado_atualizado: Decimal | None
    arrecadada: Decimal | None
    diferenca_para_mais: Decimal | None
    diferenca_para_menos: Decimal | None
    linha_origem: int | None = None
    pagina_origem: int | None = None
    raw_payload: dict | None = None


@dataclass
class ParsedFile:
    metadata: dict
    records: list[ParsedRecord]
    warnings: list[str]


class ReceitaParseError(ValueError):
    pass


class _TableHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs) -> None:
        tag = tag.lower()
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"td", "th"} and self._row is not None and self._cell_parts is not None:
            text = " ".join("".join(self._cell_parts).split())
            self._row.append(html.unescape(text))
            self._cell_parts = None
        elif tag == "tr" and self._row is not None:
            if any(cell.strip() for cell in self._row):
                self.rows.append(self._row)
            self._row = None

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def detect_format(path: Path) -> str:
    head = path.read_bytes()[:4096]
    stripped = head.lstrip()
    lower = stripped[:256].lower()
    if head.startswith(b"%PDF"):
        return "pdf"
    if head.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return "xls_biff"
    if head.startswith(b"PK\x03\x04"):
        try:
            with zipfile.ZipFile(path) as zf:
                names = set(zf.namelist())
                if "[Content_Types].xml" in names and any(n.startswith("xl/") for n in names):
                    return "xlsx"
        except Exception:
            return "zip"
        return "xlsx_falso_zip"
    if lower.startswith(b"<html") or lower.startswith(b"<!doctype html"):
        return "html_excel"
    if lower.startswith(b"<?xml") or b"<workbook" in lower:
        return "xml_excel"
    text = head.decode("utf-8", errors="ignore")
    if ";" in text and "\n" in text:
        return "csv_br"
    return "desconhecido"


def normalize_money_br(value) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    text = text.replace("\xa0", " ").strip()
    text = re.sub(r"\s+", "", text)
    if text in {"-", "--"}:
        return None
    if not MONEY_RE.match(text):
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
    try:
        return Decimal(text.replace(".", "").replace(",", "."))
    except InvalidOperation:
        return None


def _clean(value) -> str:
    return " ".join(str(value or "").replace("\xa0", " ").split()).strip()


def _upper_clean(value) -> str | None:
    text = _clean(value)
    return text.upper() if text else None


def _strip_period_suffix(value: str | None) -> str | None:
    text = _clean(value)
    if not text:
        return None
    month_names = "|".join(re.escape(name) for name in MONTHS)
    text = re.sub(rf"\s+(?:{month_names})/\d{{4}}\s*$", "", text, flags=re.IGNORECASE)
    return _clean(text) or None


def _make_key(competencia: str | None, fonte: str | None, cod_uo: str | None) -> str | None:
    if not competencia or not fonte or not cod_uo:
        return None
    return f"{competencia}|{fonte}|{cod_uo}"


def _competencia_tuple(competencia: str | None) -> tuple[int, int] | None:
    if not competencia:
        return None
    match = re.match(r"^(\d{4})\.(\d{2})$", competencia)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _extract_metadata(text: str, filename: str) -> dict:
    metadata: dict = {
        "relatorio_detectado": "Anexo 10" if "Anexo 10" in text else None,
        "fonte_recurso": None,
        "exercicio": None,
        "mes": None,
        "competencia": None,
        "orgao_codigo": None,
        "orgao_nome": None,
        "escopo_relatorio": None,
        "cod_uo": None,
        "uo": None,
    }
    fonte = FONTE_RE.search(text)
    if fonte:
        metadata["fonte_recurso"] = fonte.group(1).strip().rstrip("._")

    for name, month in MONTHS.items():
        match = re.search(rf"\b{name}/(\d{{4}})\b", text, re.IGNORECASE)
        if match:
            metadata["mes"] = month
            metadata["exercicio"] = int(match.group(1))
            break

    if metadata["exercicio"] and metadata["mes"]:
        metadata["competencia"] = f"{int(metadata['exercicio']):04d}.{int(metadata['mes']):02d}"

    orgao = ORGAO_RE.search(text)
    if orgao:
        metadata["orgao_codigo"] = orgao.group(1).strip()
        metadata["orgao_nome"] = (_strip_period_suffix(orgao.group(2)) or "")[:255]

    escopo = re.search(r"\b(CONSOLIDADO DO ESTADO)\b", text, re.IGNORECASE)
    if escopo:
        metadata["escopo_relatorio"] = escopo.group(1).upper()
        metadata["cod_uo"] = "9900"
        metadata["uo"] = "ESTADO"
    elif metadata["orgao_codigo"]:
        metadata["cod_uo"] = metadata["orgao_codigo"]
        metadata["uo"] = _upper_clean(metadata["orgao_nome"])

    return metadata


def _row_to_record(row: Iterable, line_no: int | None, page_no: int | None = None) -> ParsedRecord | None:
    cells = [_clean(cell) for cell in list(row)]
    cells = [cell for cell in cells if cell != ""]
    if len(cells) < 6:
        return None
    code_index = next((idx for idx, cell in enumerate(cells) if CODE_RE.match(cell)), None)
    if code_index is None or len(cells) - code_index < 6:
        return None
    code = cells[code_index]
    desc_parts = cells[code_index + 1 : len(cells) - 4]
    if not desc_parts:
        return None
    money_cells = cells[-4:]
    values = [normalize_money_br(cell) for cell in money_cells]
    if all(value is None for value in values):
        return None
    return ParsedRecord(
        codigo_receita=code,
        descricao_receita=" ".join(desc_parts)[:1000],
        orcado_atualizado=values[0],
        arrecadada=values[1],
        diferenca_para_mais=values[2],
        diferenca_para_menos=values[3],
        linha_origem=line_no,
        pagina_origem=page_no,
        raw_payload={"cells": cells},
    )


def _records_from_table(rows: Iterable[Iterable], page_no: int | None = None) -> list[ParsedRecord]:
    records = []
    for idx, row in enumerate(rows, start=1):
        record = _row_to_record(row, idx, page_no)
        if record:
            records.append(record)
    return records


def parse_pdf(path: Path) -> ParsedFile:
    try:
        import pdfplumber
    except ImportError as exc:
        raise ReceitaParseError("Dependência pdfplumber não instalada.") from exc

    warnings: list[str] = []
    all_text_parts: list[str] = []
    records: list[ParsedRecord] = []
    with pdfplumber.open(path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            all_text_parts.append(text)
            tables = page.extract_tables() or []
            for table in tables:
                records.extend(_records_from_table(table, page_number))
    text = "\n".join(all_text_parts)
    metadata = _extract_metadata(text, path.name)
    if not records:
        warnings.append(NO_MOVEMENT_MESSAGE if NO_MOVEMENT_RE.search(text) else "Nenhuma linha de receita foi extraida do PDF.")
    return ParsedFile(metadata=metadata, records=records, warnings=warnings)


def parse_html_excel(path: Path) -> ParsedFile:
    raw = path.read_bytes()
    for encoding in ("utf-8", "latin1", "cp1252"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw.decode("latin1", errors="replace")
    parser = _TableHtmlParser()
    parser.feed(text)
    metadata = _extract_metadata(" ".join(" ".join(row) for row in parser.rows[:30]), path.name)
    records = _records_from_table(parser.rows)
    warnings = [] if records else [
        NO_MOVEMENT_MESSAGE if NO_MOVEMENT_RE.search(text) else "Nenhuma linha de receita foi extraida do HTML Excel."
    ]
    return ParsedFile(metadata=metadata, records=records, warnings=warnings)


def parse_csv_br(path: Path) -> ParsedFile:
    raw = path.read_bytes()
    for encoding in ("utf-8-sig", "latin1", "cp1252"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw.decode("latin1", errors="replace")
    rows = list(csv.reader(text.splitlines(), delimiter=";"))
    metadata = _extract_metadata(text[:5000], path.name)
    records = _records_from_table(rows)
    warnings = [] if records else [
        NO_MOVEMENT_MESSAGE if NO_MOVEMENT_RE.search(text) else "Nenhuma linha de receita foi extraida do CSV."
    ]
    return ParsedFile(metadata=metadata, records=records, warnings=warnings)


def parse_xlsx(path: Path) -> ParsedFile:
    df = pd.read_excel(path, sheet_name=0, header=None, dtype=str)
    rows = df.fillna("").values.tolist()
    text = "\n".join(" ".join(_clean(cell) for cell in row) for row in rows[:40])
    metadata = _extract_metadata(text, path.name)
    records = _records_from_table(rows)
    warnings = [] if records else [
        NO_MOVEMENT_MESSAGE if NO_MOVEMENT_RE.search(text) else "Nenhuma linha de receita foi extraida do XLSX."
    ]
    return ParsedFile(metadata=metadata, records=records, warnings=warnings)


def parse_xml_excel(path: Path) -> ParsedFile:
    raw = path.read_bytes()
    text = raw.decode("utf-8", errors="replace")
    rows: list[list[str]] = []
    try:
        root = ElementTree.fromstring(text)
        for row_el in root.iter():
            if row_el.tag.lower().endswith("row"):
                cells = []
                for cell_el in list(row_el):
                    values = [node.text or "" for node in cell_el.iter() if node.text]
                    cells.append(_clean(" ".join(values)))
                if any(cells):
                    rows.append(cells)
    except ElementTree.ParseError as exc:
        raise ReceitaParseError(f"XML Excel invalido: {exc}") from exc
    metadata = _extract_metadata(text[:5000], path.name)
    records = _records_from_table(rows)
    warnings = [] if records else [
        NO_MOVEMENT_MESSAGE if NO_MOVEMENT_RE.search(text) else "Nenhuma linha de receita foi extraida do XML Excel."
    ]
    return ParsedFile(metadata=metadata, records=records, warnings=warnings)


def parse_biff(path: Path) -> ParsedFile:
    try:
        import xlrd  # type: ignore
    except ImportError as exc:
        raise ReceitaParseError("Arquivo XLS BIFF detectado, mas a dependência xlrd não está instalada.") from exc
    book = xlrd.open_workbook(str(path))
    sheet = book.sheet_by_index(0)
    rows = [[sheet.cell_value(r, c) for c in range(sheet.ncols)] for r in range(sheet.nrows)]
    text = "\n".join(" ".join(_clean(cell) for cell in row) for row in rows[:40])
    metadata = _extract_metadata(text, path.name)
    records = _records_from_table(rows)
    warnings = [] if records else [
        NO_MOVEMENT_MESSAGE if NO_MOVEMENT_RE.search(text) else "Nenhuma linha de receita foi extraida do XLS BIFF."
    ]
    return ParsedFile(metadata=metadata, records=records, warnings=warnings)


def parse_file(path: Path, detected_format: str) -> ParsedFile:
    if detected_format == "pdf":
        return parse_pdf(path)
    if detected_format == "html_excel":
        return parse_html_excel(path)
    if detected_format == "csv_br":
        return parse_csv_br(path)
    if detected_format == "xlsx":
        return parse_xlsx(path)
    if detected_format == "xml_excel":
        return parse_xml_excel(path)
    if detected_format == "xls_biff":
        return parse_biff(path)
    raise ReceitaParseError(f"Formato não suportado: {detected_format}.")


def _apply_file_metadata(file_row: ReceitaAnexo10Arquivo, parsed: ParsedFile) -> None:
    metadata = parsed.metadata
    for field in (
        "relatorio_detectado",
        "fonte_recurso",
        "exercicio",
        "mes",
        "competencia",
        "orgao_codigo",
        "orgao_nome",
        "escopo_relatorio",
        "cod_uo",
        "uo",
    ):
        setattr(file_row, field, metadata.get(field))
    file_row.chave_competencia_fonte_uo = _make_key(
        file_row.competencia,
        file_row.fonte_recurso,
        file_row.cod_uo,
    )


def _validate_policy(upload: ReceitaAnexo10Upload, file_row: ReceitaAnexo10Arquivo) -> None:
    missing = []
    if not file_row.competencia:
        missing.append("competência")
    if not file_row.fonte_recurso:
        missing.append("fonte de recurso")
    if not file_row.cod_uo:
        missing.append("Cod.UO")
    if missing:
        raise ReceitaParseError("Arquivo rejeitado: metadados obrigatórios ausentes (" + ", ".join(missing) + ").")

    competencia = _competencia_tuple(file_row.competencia)
    if not competencia:
        raise ReceitaParseError("Arquivo rejeitado: competência inválida no conteúdo.")

    current = (datetime.now().year, datetime.now().month)
    tipo_carga = "fechada" if bool(upload.mes_fechado) else "aberta"
    if tipo_carga == "fechada" and competencia >= current:
        raise ReceitaParseError("Arquivo rejeitado: mês fechado aceita somente competências anteriores ao mês atual.")
    if tipo_carga == "aberta" and competencia != current:
        raise ReceitaParseError("Arquivo rejeitado: mês não fechado aceita somente a competência atual.")

    closed_exists = ReceitaAnexo10Registro.query.filter_by(
        chave_competencia_fonte_uo=file_row.chave_competencia_fonte_uo,
        tipo_carga="fechada",
        ativo=True,
    ).first()
    if closed_exists and tipo_carga == "aberta":
        raise ReceitaParseError("Arquivo rejeitado: já existe carga fechada ativa para esta competência, fonte e Cod.UO.")
    if closed_exists and tipo_carga == "fechada" and not bool(upload.permite_corrigir_fechada):
        raise ReceitaParseError(
            "Arquivo rejeitado: já existe carga fechada ativa para esta competência, fonte e Cod.UO. "
            "Somente administrador pode corrigir carga fechada."
        )


def _deactivate_previous_for_key(upload: ReceitaAnexo10Upload, file_row: ReceitaAnexo10Arquivo) -> list[int]:
    key = file_row.chave_competencia_fonte_uo
    if not key:
        return []
    query = ReceitaAnexo10Registro.query.filter(
        ReceitaAnexo10Registro.chave_competencia_fonte_uo == key,
        ReceitaAnexo10Registro.ativo.is_(True),
    )
    if not bool(upload.permite_corrigir_fechada):
        query = query.filter(ReceitaAnexo10Registro.tipo_carga == "aberta")
    rows = query.all()
    upload_ids = sorted({row.upload_id for row in rows if row.upload_id and row.upload_id != upload.id})
    for row in rows:
        row.ativo = False
        row.substituido_por_upload_id = upload.id
    ReceitaAnexo10Arquivo.query.filter(
        ReceitaAnexo10Arquivo.chave_competencia_fonte_uo == key,
        ReceitaAnexo10Arquivo.status.in_(("processado", "alerta")),
        ReceitaAnexo10Arquivo.upload_id != upload.id,
    ).update(
        {
            "status": "substituido",
            "substituido_por_upload_id": upload.id,
        },
        synchronize_session=False,
    )
    return upload_ids


def _record_model(upload, file_row, parsed_record: ParsedRecord) -> ReceitaAnexo10Registro:
    return ReceitaAnexo10Registro(
        upload_id=upload.id,
        arquivo_id=file_row.id,
        fonte_recurso=file_row.fonte_recurso,
        exercicio=file_row.exercicio,
        mes=file_row.mes,
        competencia=file_row.competencia,
        orgao_codigo=file_row.orgao_codigo,
        orgao_nome=file_row.orgao_nome,
        escopo_relatorio=file_row.escopo_relatorio,
        cod_uo=file_row.cod_uo,
        uo=file_row.uo,
        mes_fechado=bool(upload.mes_fechado),
        tipo_carga=upload.tipo_carga or ("fechada" if bool(upload.mes_fechado) else "aberta"),
        chave_competencia_fonte_uo=file_row.chave_competencia_fonte_uo,
        codigo_receita=parsed_record.codigo_receita,
        descricao_receita=parsed_record.descricao_receita,
        orcado_atualizado=parsed_record.orcado_atualizado,
        arrecadada=parsed_record.arrecadada,
        diferenca_para_mais=parsed_record.diferenca_para_mais,
        diferenca_para_menos=parsed_record.diferenca_para_menos,
        linha_origem=parsed_record.linha_origem,
        pagina_origem=parsed_record.pagina_origem,
        raw_payload=json.dumps(parsed_record.raw_payload or {}, ensure_ascii=False),
        data_arquivo=file_row.data_arquivo or upload.data_arquivo,
        user_email=file_row.user_email,
        ativo=True,
    )


def run_receita_anexo10(
    input_dir: Path,
    upload_id: int,
    progress: ProgressFn | None = None,
    cancel_check: CancelFn | None = None,
) -> tuple[int, Path | None, dict]:
    progress = progress or (lambda etapa, percentual, mensagem: None)
    cancel_check = cancel_check or (lambda: None)
    upload = db.session.get(ReceitaAnexo10Upload, upload_id)
    if not upload:
        raise RuntimeError(f"Upload de receita não encontrado: {upload_id}")
    files = (
        ReceitaAnexo10Arquivo.query.filter_by(upload_id=upload.id)
        .order_by(ReceitaAnexo10Arquivo.id.asc())
        .all()
    )
    if not files:
        raise RuntimeError("Nenhum arquivo encontrado para processar.")

    upload.tipo_carga = "fechada" if bool(upload.mes_fechado) else "aberta"
    upload.status_validacao = "processando"
    for file_row in files:
        file_row.mes_fechado = bool(upload.mes_fechado)
        file_row.tipo_carga = upload.tipo_carga
    db.session.query(ReceitaAnexo10Registro).filter_by(upload_id=upload.id, ativo=False).update(
        {"ativo": False},
        synchronize_session=False,
    )
    db.session.commit()

    total_records = 0
    files_ok = 0
    files_warn = 0
    files_error = 0
    replaced_upload_ids: set[int] = set()
    seen_keys: set[str] = set()
    alerts: dict[str, list[str]] = {}
    total = len(files)

    for index, file_row in enumerate(files, start=1):
        cancel_check()
        percent_base = int(((index - 1) / total) * 90)
        progress("arquivo", percent_base, f"Processando {index}/{total}: {file_row.original_filename}")
        file_path = input_dir / file_row.stored_filename
        try:
            if not file_path.exists():
                raise ReceitaParseError("Arquivo físico não encontrado.")
            detected_format = detect_format(file_path)
            file_row.formato_detectado = detected_format
            parsed = parse_file(file_path, detected_format)
            if parsed.metadata.get("relatorio_detectado") != "Anexo 10":
                raise ReceitaParseError("Arquivo rejeitado: relatório não identificado como Anexo 10.")
            _apply_file_metadata(file_row, parsed)
            file_row.mes_fechado = bool(upload.mes_fechado)
            file_row.tipo_carga = upload.tipo_carga
            file_row.total_linhas_detectadas = len(parsed.records)
            _validate_policy(upload, file_row)
            if file_row.chave_competencia_fonte_uo in seen_keys:
                raise ReceitaParseError(
                    "Arquivo rejeitado: chave competência, fonte e Cod.UO duplicada no mesmo upload."
                )
            db.session.flush()
            replaced_upload_ids.update(_deactivate_previous_for_key(upload, file_row))
            batch = []
            if parsed.records:
                batch = [_record_model(upload, file_row, record) for record in parsed.records]
                db.session.add_all(batch)
            file_row.total_linhas_importadas = len(batch)
            file_alerts = list(parsed.warnings)
            if not parsed.records and not file_alerts:
                file_alerts.append("Arquivo valido sem linhas de receita para importar.")
            file_row.total_alertas = len(file_alerts)
            file_row.alertas_json = json.dumps(file_alerts, ensure_ascii=False) if file_alerts else None
            file_row.status = "alerta" if file_alerts else "processado"
            file_row.mensagem = NO_MOVEMENT_MESSAGE if not parsed.records and file_alerts else f"{len(batch)} linha(s) importada(s)."
            total_records += len(batch)
            seen_keys.add(file_row.chave_competencia_fonte_uo)
            if file_alerts:
                files_warn += 1
                alerts[file_row.original_filename] = file_alerts
            else:
                files_ok += 1
            db.session.commit()
        except Exception as exc:
            db.session.rollback()
            file_row = db.session.get(ReceitaAnexo10Arquivo, file_row.id)
            if file_row:
                file_row.status = "erro"
                file_row.total_erros = 1
                file_row.erro_tecnico = f"{type(exc).__name__}: {exc}"
                file_row.mensagem = str(exc)[:1000]
                try:
                    file_row.formato_detectado = detect_format(file_path) if file_path.exists() else file_row.formato_detectado
                except Exception:
                    pass
                db.session.commit()
            files_error += 1
            alerts[file_row.original_filename if file_row else f"arquivo_{index}"] = [str(exc)]
        finally:
            upload = db.session.get(ReceitaAnexo10Upload, upload_id)
            if upload:
                upload.arquivos_processados = index
                upload.arquivos_sucesso = files_ok
                upload.arquivos_alerta = files_warn
                upload.arquivos_erro = files_error
                upload.total_registros = total_records
                upload.total_alertas = sum(len(v) for v in alerts.values())
                upload.total_erros = files_error
                upload.detalhes_alertas = json.dumps(alerts, ensure_ascii=False) if alerts else None
                upload.substitui_upload_ids = (
                    json.dumps(sorted(replaced_upload_ids), ensure_ascii=False) if replaced_upload_ids else None
                )
                db.session.commit()

    cancel_check()
    progress("finalizando", 95, "Consolidando resultado do processamento.")
    upload = db.session.get(ReceitaAnexo10Upload, upload_id)
    if upload:
        upload.status_validacao = "validado" if total_records else "bloqueado"
        if files_error:
            upload.mensagem_validacao = (
                f"{files_error} arquivo(s) não processado(s). "
                "Consulte os detalhes do processamento para ver os motivos."
            )
        else:
            upload.mensagem_validacao = "Todos os arquivos validos foram processados."
    db.session.commit()
    progress("finalizado", 100, f"Processamento concluido. {total_records} registros importados.")
    return total_records, None, alerts
