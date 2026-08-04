from __future__ import annotations

import csv
import html
import re
import zipfile
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree

import pandas as pd


SUPPORTED_FORMATS = {"xlsx", "xls_biff", "html_excel", "xml_excel", "csv_br"}


@dataclass(frozen=True)
class FiplanPreparedFile:
    original_path: Path
    processing_path: Path
    formato_detectado: str
    normalized: bool


class FiplanFormatError(ValueError):
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
            if any(str(cell).strip() for cell in self._row):
                self.rows.append(self._row)
            self._row = None

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)


def _decode_bytes(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin1", errors="replace")


def detect_fiplan_format(path: Path) -> str:
    head = path.read_bytes()[:8192]
    stripped = head.lstrip()
    lower = stripped[:512].lower()
    if head.startswith(b"%PDF"):
        return "pdf"
    if head.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return "xls_biff"
    if head.startswith(b"PK\x03\x04"):
        try:
            with zipfile.ZipFile(path) as zf:
                names = set(zf.namelist())
                if "[Content_Types].xml" in names and any(name.startswith("xl/") for name in names):
                    return "xlsx"
        except Exception:
            return "zip"
        return "xlsx_falso_zip"
    if lower.startswith(b"<?xml") or b"<workbook" in lower:
        return "xml_excel"
    if lower.startswith(b"<html") or lower.startswith(b"<!doctype html") or b"<table" in lower:
        return "html_excel"
    text = _decode_bytes(head)
    if ";" in text and ("\n" in text or "\r" in text):
        return "csv_br"
    return "desconhecido"


def _rows_to_df(rows: list[list[object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    width = max(len(row) for row in rows)
    padded = [list(row) + [""] * (width - len(row)) for row in rows]
    return pd.DataFrame(padded)


def _read_html_table(path: Path) -> pd.DataFrame:
    parser = _TableHtmlParser()
    parser.feed(_decode_bytes(path.read_bytes()))
    return _rows_to_df(parser.rows)


def _xml_text(elem: ElementTree.Element) -> str:
    return " ".join("".join(elem.itertext()).split())


def _read_xml_tables(path: Path) -> list[pd.DataFrame]:
    text = _decode_bytes(path.read_bytes())
    root = ElementTree.fromstring(text)
    tables: list[pd.DataFrame] = []
    worksheets = [el for el in root.iter() if el.tag.lower().endswith("worksheet")]
    if not worksheets:
        worksheets = [root]
    for worksheet in worksheets:
        rows: list[list[str]] = []
        for row in worksheet.iter():
            if not row.tag.lower().endswith("row"):
                continue
            values: list[str] = []
            cells = [cell for cell in row.iter() if cell.tag.lower().endswith("cell")]
            for cell in cells:
                values.append(_xml_text(cell))
            if any(v.strip() for v in values):
                rows.append(values)
        df = _rows_to_df(rows)
        if not df.empty:
            tables.append(df)
    return tables


def _read_csv_br(path: Path) -> pd.DataFrame:
    text = _decode_bytes(path.read_bytes())
    sample = text[:4096]
    delimiter = ";"
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";\t,")
        delimiter = dialect.delimiter
    except Exception:
        pass
    rows = list(csv.reader(text.splitlines(), delimiter=delimiter))
    return _rows_to_df(rows)


def read_fiplan_tables(path: Path) -> list[pd.DataFrame]:
    formato = detect_fiplan_format(path)
    if formato == "xlsx":
        xls = pd.ExcelFile(path, engine="openpyxl")
        return [pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str) for sheet in xls.sheet_names]
    if formato == "xls_biff":
        try:
            xls = pd.ExcelFile(path, engine="xlrd")
        except ImportError as exc:
            raise FiplanFormatError("Arquivo XLS BIFF antigo requer a dependência xlrd instalada.") from exc
        return [pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str) for sheet in xls.sheet_names]
    if formato == "html_excel":
        return [_read_html_table(path)]
    if formato == "xml_excel":
        return _read_xml_tables(path)
    if formato == "csv_br":
        return [_read_csv_br(path)]
    if formato == "pdf":
        raise FiplanFormatError("PDF não está habilitado para esta funcionalidade.")
    raise FiplanFormatError(f"Formato de arquivo não suportado ou não identificado: {formato}.")


def iter_preview_rows(path: Path, max_rows: int = 260) -> Iterable[list[object]]:
    formato = detect_fiplan_format(path)
    if formato == "xlsx":
        xls = pd.ExcelFile(path, engine="openpyxl")
        sheet = xls.sheet_names[0] if xls.sheet_names else 0
        df = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str, nrows=max_rows)
        for _, row in df.iterrows():
            yield row.tolist()
        return
    if formato == "xls_biff":
        try:
            xls = pd.ExcelFile(path, engine="xlrd")
        except ImportError as exc:
            raise FiplanFormatError("Arquivo XLS BIFF antigo requer a dependência xlrd instalada.") from exc
        sheet = xls.sheet_names[0] if xls.sheet_names else 0
        df = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str, nrows=max_rows)
        for _, row in df.iterrows():
            yield row.tolist()
        return
    if formato == "csv_br":
        text = _decode_bytes(path.read_bytes())
        sample = text[:4096]
        delimiter = ";"
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";\t,")
            delimiter = dialect.delimiter
        except Exception:
            pass
        for index, row in enumerate(csv.reader(text.splitlines(), delimiter=delimiter)):
            if index >= max_rows:
                break
            yield row
        return
    for df in read_fiplan_tables(path):
        if df is None or df.empty:
            continue
        for _, row in df.head(max_rows).iterrows():
            yield row.tolist()


def normalize_to_xlsx(source_path: Path, output_path: Path) -> Path:
    tables = read_fiplan_tables(source_path)
    valid_tables = [df for df in tables if df is not None and not df.empty]
    if not valid_tables:
        raise FiplanFormatError("Nenhuma tabela foi encontrada no arquivo.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for index, df in enumerate(valid_tables, start=1):
            sheet_name = f"Planilha{index}"
            df.to_excel(writer, index=False, header=False, sheet_name=sheet_name)
    return output_path


def prepare_fiplan_file(source_path: Path, output_dir: Path, base_stem: str) -> FiplanPreparedFile:
    formato = detect_fiplan_format(source_path)
    if formato not in SUPPORTED_FORMATS:
        if formato == "pdf":
            raise FiplanFormatError("PDF não está habilitado para esta funcionalidade.")
        raise FiplanFormatError(f"Formato de arquivo não suportado ou não identificado: {formato}.")
    if formato == "xlsx" and source_path.suffix.lower() == ".xlsx":
        return FiplanPreparedFile(source_path, source_path, formato, False)
    normalized_path = output_dir / f"{base_stem}_normalizado.xlsx"
    normalize_to_xlsx(source_path, normalized_path)
    return FiplanPreparedFile(source_path, normalized_path, formato, True)
