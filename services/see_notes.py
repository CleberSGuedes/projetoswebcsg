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
    if combined:
        escola, dre, codigo = (combined.group(1).strip(), combined.group(2).strip(), combined.group(3).strip())
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
    entrega_direta_dre = bool(dre and not has_school_block)
    expected_values = (dre, numero) if entrega_direta_dre else (dre, codigo, escola, numero)
    found = sum(bool(value) for value in expected_values)
    return {
        "chave_acesso": chave,
        "numero_danfe": numero,
        "dre": dre,
        "codigo_escola": codigo,
        "nome_escola": escola,
        "entrega_direta_dre": entrega_direta_dre,
        "confianca": round(found / len(expected_values), 4),
    }


def _extract_row_quantity(words: list[dict], code: str) -> tuple[Decimal, str, str] | None:
    code_indexes = [idx for idx, word in enumerate(words) if re.sub(r"\D", "", word.get("text", "")) == code]
    units = {"UN", "UND", "UNID", "UNIDADE", "PC", "PCT", "CX"}
    for code_idx in code_indexes:
        for idx in range(code_idx + 1, min(len(words), code_idx + 18)):
            unit = _ascii_upper(words[idx].get("text", "")).rstrip(".")
            if unit not in units:
                continue
            for qty_word in words[idx + 1 : min(len(words), idx + 5)]:
                qty = _decimal_pt(qty_word.get("text", ""))
                if qty is not None:
                    source = " ".join(word.get("text", "") for word in words)
                    return qty, unit, source
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

    with pdfplumber.open(path) as pdf:
        total_pages = len(pdf.pages)
        for page_number, page in enumerate(pdf.pages, start=1):
            page_text = page.extract_text() or ""
            texts.append(page_text)
            words = page.extract_words(use_text_flow=True, keep_blank_chars=False)
            rows: dict[int, list[dict]] = defaultdict(list)
            for word in words:
                rows[round(float(word.get("top", 0)) / 3)].append(word)
            for row_words in rows.values():
                row_words.sort(key=lambda word: float(word.get("x0", 0)))
                row_digits = {re.sub(r"\D", "", word.get("text", "")) for word in row_words}
                for code in product_by_code.keys() & row_digits:
                    extracted = _extract_row_quantity(row_words, code)
                    if not extracted:
                        continue
                    quantity, unit, source = extracted
                    marker = (page_number, code, quantity, round(float(row_words[0].get("top", 0))))
                    if marker in seen:
                        continue
                    seen.add(marker)
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
                            "confianca": Decimal("0.9500"),
                            "texto_origem": source[:2000],
                        }
                    )
            if page_callback:
                page_callback(page_number, total_pages)

    full_text = "\n".join(texts)
    metadata = _metadata(full_text, path.name)
    warnings = []
    required_metadata = [("dre", "DRE"), ("numero_danfe", "número da DANFE")]
    if not metadata.get("entrega_direta_dre"):
        required_metadata[1:1] = [("codigo_escola", "código da escola"), ("nome_escola", "nome da escola")]
    for field, label in required_metadata:
        if not metadata.get(field):
            warnings.append({"codigo": f"CAMPO_{field.upper()}_AUSENTE", "mensagem": f"Não foi possível identificar {label}."})
    if not occurrences:
        warnings.append({"codigo": "PRODUTOS_NAO_ENCONTRADOS", "mensagem": "Nenhum produto do catálogo foi encontrado no PDF."})
    return {
        "metadata": metadata,
        "items": occurrences,
        "warnings": warnings,
        "total_pages": len(texts),
        "method": "pdfplumber_coordenadas",
    }


def generate_xlsx(path: Path, files: list[dict], items: list[dict], occurrences: list[dict], products: list[dict]) -> None:
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
    if base.empty:
        summary = pd.DataFrame(columns=["DRE", "COD", "Escola", "Número DANFE", *product_names.values()])
    else:
        for column in ("DRE", "COD", "Escola", "Número DANFE"):
            base[column] = base[column].fillna("NÃO IDENTIFICADO")
        summary = base.pivot_table(
            index=["DRE", "COD", "Escola", "Número DANFE"],
            columns="Código Produto",
            values="Quantidade",
            aggfunc="sum",
            fill_value=0,
        ).reset_index().rename(columns=product_names)
        for name in product_names.values():
            if name not in summary.columns:
                summary[name] = 0
        summary = summary.reindex(columns=["DRE", "COD", "Escola", "Número DANFE", *product_names.values()])
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="Planilha Consolidada", index=False)
        base.to_excel(writer, sheet_name="Dados_Extraídos", index=False)
        pd.DataFrame(files).to_excel(writer, sheet_name="Auditoria_Arquivos", index=False)
        pd.DataFrame(occurrences).to_excel(writer, sheet_name="Ocorrências", index=False)
        pd.DataFrame(products).to_excel(writer, sheet_name="Catálogo_Produtos", index=False)
