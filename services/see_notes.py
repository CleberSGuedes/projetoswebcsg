from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable

import pandas as pd
import pdfplumber


def _ascii_upper(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text).strip().upper()


def _decimal_pt(value: str) -> Decimal | None:
    raw = re.sub(r"[^0-9,.-]", "", value or "").strip()
    if not raw:
        return None
    if "," in raw and "." in raw:
        if raw.rfind(",") > raw.rfind("."):
            raw = raw.replace(".", "").replace(",", ".")
        else:
            raw = raw.replace(",", "")
    elif "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _near_label(text: str, labels: str, stop_labels: str, value_pattern: str = r"[^\n]{1,180}") -> str | None:
    pattern = rf"(?:{labels})\s*[:.\-]?\s*({value_pattern})(?=\s+(?:{stop_labels})\s*[:.\-]?|\n|$)"
    match = re.search(pattern, text, re.IGNORECASE)
    return re.sub(r"\s+", " ", match.group(1)).strip(" :-") if match else None


def _destinatario_escola(lines: list[str]) -> str | None:
    """Nome do destinatário quando ele é uma escola (linha após 'NOME/RAZAO SOCIAL CNPJ/CPF')."""
    for idx, line in enumerate(lines[:-1]):
        if not re.match(r"NOME\s*/\s*RAZAO SOCIAL\s+CNPJ", line):
            continue
        match = re.match(r"(.+?)\s+\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b", lines[idx + 1])
        if match and "SECRETARIA" not in match.group(1):
            return match.group(1).strip()
        return None
    return None


def _municipio(lines: list[str], normalized: str) -> str | None:
    """Município do destinatário (linha após 'MUNICIPIO FONE/FAX') ou 'CIDADE: X - MT'."""
    for idx, line in enumerate(lines[:-1]):
        if re.match(r"MUNICIPIO\s+FONE", line):
            value = re.sub(r"\s+(?:\d.*|MT\b.*)$", "", lines[idx + 1]).strip()
            return value or None
    match = re.search(r"CIDADE\s*:\s*([A-Z' ]+?)\s*-\s*MT\b", normalized)
    return match.group(1).strip() if match else None


def _metadata(text: str, filename: str) -> dict:
    normalized = _ascii_upper(text).replace("\r", "")
    filename_digits = re.sub(r"\D", "", Path(filename).stem)
    chave = None
    for candidate in re.findall(r"(?:\d[\s.\-]*){44}", text):
        digits = re.sub(r"\D", "", candidate)
        if len(digits) == 44:
            chave = digits
            break
    if not chave and len(filename_digits) == 44:
        chave = filename_digits

    numero = None
    for pattern in (
        r"(?:N(?:º|°|O)?|NUMERO)\s*[:.\-]?\s*(\d{1,12})\s*(?:SERIE|SÉRIE)",
        r"DANFE[^\n]{0,100}?N(?:º|°|O)?\s*[:.\-]?\s*(\d{1,12})",
    ):
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            numero = match.group(1).lstrip("0") or "0"
            break
    if not numero and chave:
        numero = chave[25:34].lstrip("0") or "0"

    combined = re.search(
        r"(?:ESCOLA|ESC\.?|E\.E\.)\s*:\s*(.*?)\s+"
        r"D\.?R\.?E\.?\s*:\s*((?:D\.?R\.?E\.?\s+)?.*?)\s+"
        r"(?:COD(?:IGO)?|CD)\s*:\s*(\d+)",
        normalized,
        re.IGNORECASE,
    )
    lines = [_ascii_upper(line) for line in text.splitlines()]
    destinatario_escola = _destinatario_escola(lines)
    dre_line = next((line for line in lines if re.fullmatch(r"DRE\s+[A-Z][A-Z ]{2,60}", line)), None)
    codigo_fora_do_layout = False
    if combined:
        escola, dre, codigo = (combined.group(1).strip(), combined.group(2).strip(), combined.group(3).strip())
    elif destinatario_escola and dre_line:
        # Layout em que a escola é o destinatário e a DRE vem numa linha própria (ex.: Athos);
        # esse modelo de nota não traz o código da escola.
        escola, dre, codigo = destinatario_escola, dre_line, None
        codigo_fora_do_layout = True
    else:
        dre = _near_label(
            normalized,
            r"D\.?R\.?E\.?",
            r"COD(?:IGO)?|CD|PEDIDO(?:\(S\))?|ESC(?:OLA)?|CNPJ|DANFE|CHAVE",
        )
        codigo = _near_label(
            normalized,
            r"COD(?:IGO)?(?:\s+DA\s+ESCOLA)?|CD",
            r"D\.?R\.?E\.?|PEDIDO(?:\(S\))?|SEM\s+VENCIMENTO|ESC(?:OLA)?|CNPJ|DANFE|CHAVE|ENTREGA|END",
            r"\d{1,30}",
        )
        escola = _near_label(
            normalized,
            r"ESCOLA(?:\s+ESTADUAL)?|ESC\.?|E\.E\.",
            r"D\.?R\.?E\.?|COD(?:IGO)?|CNPJ|DANFE|CHAVE",
        )
    if not dre:
        dre_match = re.search(
            r"D\.?R\.?E\.?\s*:\s*((?:D\.?R\.?E\.?\s+)?.{2,100}?)"
            r"(?=\s+(?:PEDIDO(?:\(S\))?|COD(?:IGO)?|CD|ENTREGA|SEM\s+VENCIMENTO|VIAGEM|$))",
            normalized,
            re.IGNORECASE,
        )
        if dre_match:
            dre = dre_match.group(1).strip(" :-")
    if dre:
        dre = re.split(
            r"\s+(?:COD(?:IGO)?|CD|PEDIDO(?:\(S\))?|ENTREGA|SEM\s+VENCIMENTO|VIAGEM)\s*:",
            dre,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0].strip(" :-")
    if escola and (
        len(escola) > 180
        or any(marker in escola for marker in ("VALOR TOTAL DA NOTA", "FRETE POR CONTA", "DESPESAS ACESSORIAS"))
    ):
        escola = None
    has_school_block = bool(re.search(r"(?:ESCOLA|ESC\.?|E\.E\.)\s*:", normalized, re.IGNORECASE))
    entrega_direta_dre = bool(dre and not has_school_block and not codigo_fora_do_layout)
    if codigo_fora_do_layout:
        expected_values = (dre, escola, numero)
    elif entrega_direta_dre:
        expected_values = (dre, numero)
    else:
        expected_values = (dre, codigo, escola, numero)
    found = sum(bool(value) for value in expected_values)
    return {
        "chave_acesso": chave,
        "numero_danfe": numero,
        "dre": dre,
        "codigo_escola": codigo,
        "nome_escola": escola,
        "municipio": _municipio(lines, normalized),
        "entrega_direta_dre": entrega_direta_dre,
        "codigo_fora_do_layout": codigo_fora_do_layout,
        "confianca": round(found / len(expected_values), 4),
    }


def _row_around(words: list[dict], anchor: dict, tolerance: float = 1.5) -> list[dict]:
    """Palavras na mesma linha visual da palavra-âncora (o código do produto).

    As colunas de uma DANFE podem ter alturas levemente diferentes (ex.: quantidade 0,75pt
    abaixo do código), e há letras soltas de textos girados na margem; por isso a linha é
    montada pela distância ao código, e não por faixas fixas de altura.
    """
    top = float(anchor.get("top", 0))
    row = [word for word in words if abs(float(word.get("top", 0)) - top) <= tolerance]
    return sorted(row, key=lambda item: float(item.get("x0", 0)))


def _extract_row_quantity(words: list[dict], code: str) -> tuple[Decimal, str, str, bool] | None:
    """Retorna quantidade, unidade, texto da linha e se quantidade x unitário confere com o total."""
    code_indexes = [idx for idx, word in enumerate(words) if re.sub(r"\D", "", word.get("text", "")) == code]
    units = {"UN", "UND", "UNID", "UNIDADE", "PC", "PCT", "CX"}
    for code_idx in code_indexes:
        # A descrição do produto pode ter muitas palavras; procura a unidade em toda a linha.
        for idx in range(code_idx + 1, len(words)):
            unit = _ascii_upper(words[idx].get("text", "")).rstrip(".")
            if unit not in units:
                continue
            numbers = [
                value
                for value in (_decimal_pt(word.get("text", "")) for word in words[idx + 1 : idx + 6])
                if value is not None
            ]
            if not numbers:
                continue
            qty = numbers[0]
            consistent = True
            if len(numbers) >= 3:
                unit_price, total = numbers[1], numbers[2]
                consistent = bool(total) and abs(qty * unit_price - total) <= max(Decimal("0.05"), total * Decimal("0.01"))
            source = " ".join(word.get("text", "") for word in words)
            return qty, unit, source, consistent
    return None


def extract_pdf(
    path: Path,
    products: list[dict],
    page_callback: Callable[[int, int], None] | None = None,
) -> dict:
    product_by_code = {str(item["codigo"]).strip(): item for item in products}
    occurrences: list[dict] = []
    seen: set[tuple] = set()
    texts: list[str] = []
    inconsistent_codes: list[str] = []

    with pdfplumber.open(path) as pdf:
        total_pages = len(pdf.pages)
        for page_number, page in enumerate(pdf.pages, start=1):
            page_text = page.extract_text() or ""
            texts.append(page_text)
            words = page.extract_words(use_text_flow=True, keep_blank_chars=False)
            for anchor in words:
                code = re.sub(r"\D", "", anchor.get("text", ""))
                if code not in product_by_code:
                    continue
                extracted = _extract_row_quantity(_row_around(words, anchor), code)
                if not extracted:
                    continue
                quantity, unit, source, consistent = extracted
                marker = (page_number, code, quantity, round(float(anchor.get("top", 0))))
                if marker in seen:
                    continue
                seen.add(marker)
                if not consistent:
                    inconsistent_codes.append(f"{code} (página {page_number})")
                product = product_by_code[code]
                occurrences.append(
                    {
                        "catalogo_produto_id": product.get("id"),
                        "codigo": code,
                        "nome": product.get("nome") or code,
                        "unidade": unit,
                        "quantidade": quantity,
                        "pagina": page_number,
                        "estrategia": "coordenadas_linha",
                        "confianca": Decimal("0.9500") if consistent else Decimal("0.5000"),
                        "texto_origem": source[:2000],
                    }
                )
            if page_callback:
                page_callback(page_number, total_pages)

    full_text = "\n".join(texts)
    metadata = _metadata(full_text, path.name)
    warnings = []
    required_metadata = [("dre", "DRE"), ("numero_danfe", "número da DANFE")]
    if metadata.get("codigo_fora_do_layout"):
        required_metadata[1:1] = [("nome_escola", "nome da escola")]
        warnings.append({
            "codigo": "CAMPO_CODIGO_ESCOLA_AUSENTE",
            "mensagem": "O modelo desta nota não informa o código da escola (escola identificada pelo destinatário).",
        })
    elif not metadata.get("entrega_direta_dre"):
        required_metadata[1:1] = [("codigo_escola", "código da escola"), ("nome_escola", "nome da escola")]
    for field, label in required_metadata:
        if not metadata.get(field):
            warnings.append({"codigo": f"CAMPO_{field.upper()}_AUSENTE", "mensagem": f"Não foi possível identificar {label}."})
    if not occurrences:
        warnings.append({"codigo": "PRODUTOS_NAO_ENCONTRADOS", "mensagem": "Nenhum produto do catálogo foi encontrado no PDF."})
    if inconsistent_codes:
        warnings.append({
            "codigo": "QUANTIDADE_INCONSISTENTE",
            "mensagem": "Quantidade x valor unitário não confere com o valor total: " + ", ".join(inconsistent_codes) + ".",
        })
    return {
        "metadata": metadata,
        "items": occurrences,
        "warnings": warnings,
        "total_pages": len(texts),
        "method": "pdfplumber_coordenadas",
    }


# Cores das linhas que precisam de conferência na Planilha Consolidada.
DESTAQUE_CORES = {
    "sugestao": "FFF2CC",        # amarelo: escola sugerida pelo cadastro, conferir o código
    "divergente": "FCE4D6",      # laranja: nota diverge do cadastro (DRE/município)
    "nao_encontrada": "FCE4D6",  # laranja: escola/código não encontrado no cadastro
}
INDEX_COLUMNS = ["DRE", "COD", "Escola", "Número DANFE"]
LEGENDA = [
    ("FFF2CC", "Amarelo", "Escola não encontrada com o mesmo nome; o cadastro sugere uma escola. Confira e informe o código (ver coluna Conferência)."),
    ("FCE4D6", "Laranja", "Escola ou código não encontrado no cadastro, ou DRE/município da nota diverge do cadastro (ver coluna Conferência)."),
    (None, "Sem cor", "Escola e DRE conferidas com o cadastro DRE/Escola."),
]


def _write_legend(sheet, column: int) -> None:
    """Legenda das cores ao lado da tabela, sem deslocar o cabeçalho dos dados."""
    from openpyxl.styles import Border, Font, PatternFill, Side

    thin = Side(style="thin", color="BFBFBF")
    title = sheet.cell(row=1, column=column, value="Legenda")
    title.font = Font(bold=True)
    for offset, (color, label, description) in enumerate(LEGENDA, start=2):
        swatch = sheet.cell(row=offset, column=column, value=label)
        swatch.border = Border(top=thin, bottom=thin, left=thin, right=thin)
        if color:
            swatch.fill = PatternFill(start_color=color, end_color=color, fill_type="solid")
        sheet.cell(row=offset, column=column + 1, value=description)
    sheet.column_dimensions[swatch.column_letter].width = 12


def generate_xlsx(path: Path, files: list[dict], items: list[dict], occurrences: list[dict], products: list[dict]) -> None:
    from openpyxl.styles import PatternFill

    path.parent.mkdir(parents=True, exist_ok=True)
    base_rows = []
    for item in items:
        base_rows.append(
            {
                "DRE": item.get("dre"),
                "COD": item.get("codigo_escola"),
                "Escola": item.get("nome_escola"),
                "Número DANFE": item.get("numero_danfe"),
                "Código Produto": item.get("codigo"),
                "Nome Produto": item.get("nome"),
                "Quantidade": float(item.get("quantidade") or 0),
                "Arquivo PDF": item.get("arquivo"),
                "Escola (nota)": item.get("escola_nota"),
                "Origem do código": item.get("origem_codigo"),
                "Conferência": item.get("conferencia"),
                "_destaque": item.get("destaque"),
            }
        )
    base = pd.DataFrame(base_rows)
    name_counts = defaultdict(int)
    for product in products:
        name_counts[str(product["nome"])] += 1
    product_names = {
        str(product["codigo"]): (
            f"{product['nome']} [{product['codigo']}]"
            if name_counts[str(product["nome"])] > 1
            else str(product["nome"])
        )
        for product in products
    }
    destaques: list[str | None] = []
    if base.empty:
        summary = pd.DataFrame(columns=[*INDEX_COLUMNS, *product_names.values(), "Conferência"])
    else:
        for column in INDEX_COLUMNS:
            base[column] = base[column].fillna("NÃO IDENTIFICADO")
        summary = base.pivot_table(
            index=INDEX_COLUMNS,
            columns="Código Produto",
            values="Quantidade",
            aggfunc="sum",
            fill_value=0,
        ).reset_index().rename(columns=product_names)
        for name in product_names.values():
            if name not in summary.columns:
                summary[name] = 0
        notes = base.groupby(INDEX_COLUMNS, dropna=False).agg(
            conferencia=("Conferência", lambda values: next((v for v in values if v), None)),
            destaque=("_destaque", lambda values: next((v for v in values if v), None)),
        ).reset_index()
        summary = summary.merge(notes, on=INDEX_COLUMNS, how="left").rename(columns={"conferencia": "Conferência"})
        destaques = list(summary["destaque"])
        summary = summary.reindex(columns=[*INDEX_COLUMNS, *product_names.values(), "Conferência"])
    base = base.drop(columns=["_destaque"], errors="ignore")
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="Planilha Consolidada", index=False)
        sheet = writer.sheets["Planilha Consolidada"]
        for offset, destaque in enumerate(destaques, start=2):
            color = DESTAQUE_CORES.get(destaque or "")
            if not color:
                continue
            fill = PatternFill(start_color=color, end_color=color, fill_type="solid")
            for cell in sheet[offset]:
                cell.fill = fill
        _write_legend(sheet, column=len(summary.columns) + 2)
        base.to_excel(writer, sheet_name="Dados_Extraídos", index=False)
        pd.DataFrame(files).to_excel(writer, sheet_name="Auditoria_Arquivos", index=False)
        pd.DataFrame(occurrences).to_excel(writer, sheet_name="Ocorrências", index=False)
        pd.DataFrame(products).to_excel(writer, sheet_name="Catálogo_Produtos", index=False)
