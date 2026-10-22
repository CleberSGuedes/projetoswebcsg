from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from models import db
from openpyxl import load_workbook
from openpyxl.styles import Font
from services.execucao_uo import desativar_versao_anterior
from services.uo import validar_uo_execucao

BATCH_SIZE = 200
UPLOAD_DIR = Path("upload") / "fip_613"
OUTPUT_DIR = Path("outputs") / "fip_613"

# "Total UO 26201" - fim da tabela de qualquer UO (era fixo em 14101).
TOTAL_UO_RE = re.compile(r"^\s*Total\s+UO\s+(\d+)", re.IGNORECASE)
# "Código da Unidade Orçamentária igual a 26201" - filtro impresso no cabeçalho.
FILTRO_UO_RE = re.compile(r"Unidade\s+Or\S+\s+igual\s+a\s+(\d+)", re.IGNORECASE)

# Colunas de valor do relatório -> colunas do banco.
NUMERIC_HEADERS = {
    "Dotação Inicial": "dotacao_inicial",
    "Créd. Suplementar": "cred_suplementar",
    "Créd. Especial": "cred_especial",
    "Créd. Extraordinário": "cred_extraordinario",
    "Redução": "reducao",
    "Créd. Autorizado": "cred_autorizado",
    "Bloqueado/Conting.": "bloqueado_conting",
    "Reserva Empenho": "reserva_empenho",
    "Saldo de Destaque": "saldo_destaque",
    "Saldo Dotação": "saldo_dotacao",
    "Empenhado": "empenhado",
    "Liquidado": "liquidado",
    "A liquidar": "a_liquidar",
    "Valor Pago": "valor_pago",
    "Valor a Pagar": "valor_a_pagar",
}


def _num_ptbr(valor) -> float:
    s = str(valor).replace(".", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except ValueError:
        return 0.0


def _brl(valor: float) -> str:
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _uos_do_filtro(raw_data: pd.DataFrame) -> list[str]:
    achados = []
    for linha in raw_data.head(40).itertuples(index=False):
        for cel in linha:
            m = FILTRO_UO_RE.search(str(cel))
            if m:
                achados.append(m.group(1))
    return achados


def ensure_dirs():
    for base in (UPLOAD_DIR, OUTPUT_DIR, UPLOAD_DIR / "tmp", OUTPUT_DIR / "tmp"):
        base.mkdir(parents=True, exist_ok=True)


def move_existing_to_tmp(base_dir: Path):
    tmp = base_dir / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    for f in base_dir.glob("*.xlsx"):
        dest = tmp / f"{f.stem}_{datetime.now().strftime('%Y%m%d%H%M%S')}{f.suffix}"
        try:
            f.rename(dest)
        except OSError:
            pass


def get_active_sheet_name(file_path: Path) -> str | None:
    try:
        wb = load_workbook(file_path, read_only=True, data_only=True)
        return wb.active.title if wb.active else None
    except Exception:
        return None


def get_year_from_file(file_path, sheet_name: str | None = None):
    if sheet_name is None:
        sheet_name = get_active_sheet_name(file_path)
    if not sheet_name:
        return None
    try:
        raw_data = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        for _, row in raw_data.iterrows():
            for cell in row:
                if isinstance(cell, str) and "Exercício igual a" in cell:
                    year = int(cell.split()[-1])
                    return year
    except Exception:
        return None
    return None


def load_clean_data(file_path, sheet_name: str | None = None):
    if sheet_name is None:
        sheet_name = get_active_sheet_name(file_path)
    if not sheet_name:
        return None
    raw_data = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    header_row_index = None
    for i, row in raw_data.iterrows():
        if "UO" in row.values and "UG" in row.values:
            header_row_index = i
            break
    if header_row_index is None:
        return None

    # UO e UG como texto: a UG vem como "0011" no relatório e virava 11.0.
    data = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=header_row_index,
        dtype={"UO": str, "UG": str},
    )
    data = data.dropna(how="all").reset_index(drop=True)

    # Linha "Total UO <código>" impressa pelo FIPLAN: marca o fim da tabela
    # (para qualquer UO) e traz os totais para conferência.
    eh_total = data.apply(
        lambda row: row.astype(str).str.match(TOTAL_UO_RE.pattern).any(), axis=1
    )
    linhas_total = data[eh_total]
    totais_codigos: list[str] = []
    total_impresso: dict[str, float] = {}
    for _, linha in linhas_total.iterrows():
        for cel in linha.astype(str):
            m = TOTAL_UO_RE.match(cel)
            if m:
                totais_codigos.append(m.group(1))
                break
    if not linhas_total.empty:
        total_impresso = {
            NUMERIC_HEADERS[col]: _num_ptbr(v)
            for col, v in linhas_total.iloc[0].items()
            if col in NUMERIC_HEADERS
        }
        # A tabela termina na primeira linha de total; o resto são os resumos.
        data = data.iloc[: linhas_total.index[0]]
    data = data.dropna(subset=["UO", "UG", "Função", "Subfunção", "Programa", "Projeto/Atividade"])

    # A UO vem do CONTEÚDO (linhas, total impresso e filtro do cabeçalho),
    # nunca do nome do arquivo. Recusa mais de uma UO ou UO não liberada.
    uo = validar_uo_execucao(
        list(data["UO"].astype(str).unique())
        + totais_codigos
        + _uos_do_filtro(raw_data)
    )

    data.rename(
        columns={
            "UO": "uo",
            "UG": "ug",
            "Função": "funcao",
            "Subfunção": "subfuncao",
            "Programa": "programa",
            "Projeto/Atividade": "projeto_atividade",
            "Regional": "regional",
            "Natureza de Despesa": "natureza_despesa",
            "Fonte de Recurso": "fonte_recurso",
            "Iduso": "iduso",
            "Tipo de Recurso": "tipo_recurso",
            "Dotação Inicial": "dotacao_inicial",
            "Créd. Suplementar": "cred_suplementar",
            "Créd. Especial": "cred_especial",
            "Créd. Extraordinário": "cred_extraordinario",
            "Redução": "reducao",
            "Créd. Autorizado": "cred_autorizado",
            "Bloqueado/Conting.": "bloqueado_conting",
            "Reserva Empenho": "reserva_empenho",
            "Saldo de Destaque": "saldo_destaque",
            "Saldo Dotação": "saldo_dotacao",
            "Empenhado": "empenhado",
            "Liquidado": "liquidado",
            "A liquidar": "a_liquidar",
            "Valor Pago": "valor_pago",
            "Valor a Pagar": "valor_a_pagar",
        },
        inplace=True,
    )

    numeric_columns = [
        "dotacao_inicial",
        "cred_suplementar",
        "cred_especial",
        "cred_extraordinario",
        "reducao",
        "cred_autorizado",
        "bloqueado_conting",
        "reserva_empenho",
        "saldo_destaque",
        "saldo_dotacao",
        "empenhado",
        "liquidado",
        "a_liquidar",
        "valor_pago",
        "valor_a_pagar",
    ]

    for col in numeric_columns:
        data[col] = data[col].astype(str).str.replace(".", "", regex=False)
        data[col] = data[col].str.replace(",", ".", regex=False)
        data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0.0)

    data["iduso"] = pd.to_numeric(data["iduso"], errors="coerce").fillna(0).astype(int)
    # manter natureza/fonte como texto (evita notação científica)
    for col in ["natureza_despesa", "fonte_recurso"]:
        data[col] = data[col].apply(lambda v: str(v).split(".")[0] if pd.notna(v) else "")
    data["uo"] = data["uo"].astype(str).str.strip()
    data["ug"] = data["ug"].astype(str).str.strip()
    data.attrs["uo"] = uo
    data.attrs["conferencia"] = conferir_total(data, total_impresso)
    return data


def conferir_total(data: pd.DataFrame, total_impresso: dict[str, float]) -> str:
    """Compara a soma das linhas lidas com a linha "Total UO" do FIPLAN.
    Só informa (não bloqueia): um rodapé com outro formato não pode barrar a
    carga diária automática."""
    if not total_impresso:
        return "Total do relatório não encontrado para conferência."
    divergentes = []
    for col, esperado in total_impresso.items():
        if col not in data.columns:
            continue
        lido = round(float(data[col].sum()), 2)
        if abs(lido - esperado) > 0.01:
            divergentes.append(f"{col}: lido {_brl(lido)} x relatório {_brl(esperado)}")
    if divergentes:
        return "ATENÇÃO: soma diferente do total do relatório (" + "; ".join(divergentes[:3]) + ")."
    return "Confere com o total do relatório."


def save_clean_data(data, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    move_existing_to_tmp(output_dir)
    filename = f"fip613_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    output_path = output_dir / filename
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        data.to_excel(writer, index=False, sheet_name="FIP613")
        ws = writer.sheets["FIP613"]
        # definir largura básica das colunas
        for col_cells in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col_cells)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max(max_len + 2, 12), 40)
        # formato numérico
        num_fmt = "#,##0.00"
        for col in range(12, ws.max_column + 1):
            for row in range(2, ws.max_row + 1):
                ws.cell(row=row, column=col).number_format = num_fmt
        # fonte e autofiltro
        for row in ws.iter_rows():
            for cell in row:
                cell.font = Font(name="Helvetica", size=8)
        ws.auto_filter.ref = ws.dimensions
    return output_path


def update_database(data, ano, data_arquivo, user_email, upload_id):
    insert_sql = text(
        """
        INSERT INTO fip613 (
            upload_id, uo, ug, funcao, subfuncao, programa, projeto_atividade, regional, natureza_despesa,
            fonte_recurso, iduso, tipo_recurso, dotacao_inicial, cred_suplementar, cred_especial,
            cred_extraordinario, reducao, cred_autorizado, bloqueado_conting, reserva_empenho,
            saldo_destaque, saldo_dotacao, empenhado, liquidado, a_liquidar, valor_pago,
            valor_a_pagar, data_atualizacao, ano, data_arquivo, user_email, ativo
        )
        VALUES (
            :upload_id, :uo, :ug, :funcao, :subfuncao, :programa, :projeto_atividade, :regional, :natureza_despesa,
            :fonte_recurso, :iduso, :tipo_recurso, :dotacao_inicial, :cred_suplementar, :cred_especial,
            :cred_extraordinario, :reducao, :cred_autorizado, :bloqueado_conting, :reserva_empenho,
            :saldo_destaque, :saldo_dotacao, :empenhado, :liquidado, :a_liquidar, :valor_pago,
            :valor_a_pagar, :data_atualizacao, :ano, :data_arquivo, :user_email, :ativo
        )
        """
    )

    # Desativa só a versão anterior da MESMA UO e do MESMO ano (antes era a
    # tabela inteira: o arquivo de uma UO apagaria as outras).
    desativar_versao_anterior("fip613", data.attrs["uo"], [ano], coluna_exercicio="ano")

    rows = data.to_dict(orient="records")
    total = 0
    for start in range(0, len(rows), BATCH_SIZE):
        chunk = rows[start : start + BATCH_SIZE]
        for r in chunk:
            r["data_atualizacao"] = datetime.utcnow()
            r["ano"] = ano
            r["data_arquivo"] = data_arquivo
            r["user_email"] = user_email
            r["upload_id"] = upload_id
            r["ativo"] = True
        try:
            db.session.execute(insert_sql, chunk)
            db.session.commit()
            total += len(chunk)
        except SQLAlchemyError as exc:
            db.session.rollback()
            raise exc
    return total


def run_fip613(
    file_path: Path, data_arquivo: datetime, user_email: str, upload_id: int
) -> tuple[int, Path, str]:
    ensure_dirs()
    ano = get_year_from_file(file_path)
    # Valida a UO (UORecusada) antes de gerar a saída ou gravar qualquer coisa.
    data = load_clean_data(file_path)
    if data is None or ano is None:
        raise RuntimeError("Não foi possível ler o arquivo FIP 613 (cabeçalho ou ano ausente).")

    output_path = save_clean_data(data, OUTPUT_DIR)
    total = update_database(data, ano, data_arquivo, user_email, upload_id)
    return total, output_path, data.attrs.get("conferencia", "")
