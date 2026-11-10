"""Atualizar > Execução > PLAN 51 (Relação Detalhada de Créditos Adicionais).

Tratamento portado do script `td_plan51.py` (macro VBA -> Python): as funções
`_norm_text` ... `processar_plan51` e `salvar_saida` são o script original, com
um único ajuste (`processar_plan51` aceita o bruto já lido). O que é do
sistema fica no fim do arquivo: validação do cabeçalho, conferência com os
totais impressos pelo FIPLAN, gravação no banco e `run_plan51`.

Diferente do PED/FIP 613, o relatório é do ESTADO inteiro (todas as UOs, e o
remanejamento entre UOs é justamente o que ele mostra): não há UO única nem
validação de UO. A carga é uma "foto" completa do exercício, então a versão
anterior desativada é a do MESMO exercício.
"""

from __future__ import annotations

import os
import re
import unicodedata
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from models import db

UPLOAD_DIR = Path("upload") / "plan51"
OUTPUT_DIR = Path("outputs") / "plan51"
BATCH_SIZE = 100


# ---------------------------------------------------------------------------
# Script original (td_plan51.py)
# ---------------------------------------------------------------------------

def _norm_text(value: object) -> str:
    txt = str(value or "").strip()
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = txt.upper()
    txt = txt.replace("°", " ").replace("º", " ").replace("ª", " ")
    txt = re.sub(r"[^A-Z0-9]+", " ", txt)
    txt = re.sub(r"\s+", " ", txt)
    return txt


def _is_blank(value: object) -> bool:
    return str(value or "").strip() == "" or str(value).strip().upper() == "NAN"


def _is_total_marker(value: object) -> bool:
    t = _norm_text(value)
    return "TOTAL SUPLEMENTADO" in t or "TOTAL ANULADO" in t


def _find_token(df_raw: pd.DataFrame, token: str) -> tuple[int, int] | None:
    key = _norm_text(token)
    for r in range(len(df_raw)):
        for c in range(df_raw.shape[1]):
            if key in _norm_text(df_raw.iat[r, c]):
                return r, c
    return None


def _extract_struct_parts(text: object) -> dict[str, str]:
    raw = str(text or "").strip()
    # Replica das formulas VBA:
    # LEFT(5), MID(7,2), MID(10,3), MID(14,3), MID(18,4), IF(LEN=26,RIGHT(4),RIGHT(3))
    def _slice(start_1: int, size: int) -> str:
        i = start_1 - 1
        if len(raw) < i + 1:
            return ""
        return raw[i : i + size]

    uo = _slice(1, 5)
    funcao = _slice(7, 2)
    subfuncao = _slice(10, 3)
    programa = _slice(14, 3)
    paoe = _slice(18, 4)
    regiao = raw[-4:] if len(raw) == 26 else (raw[-3:] if len(raw) >= 3 else "")

    # No resultado da macro esses campos acabam sem zero a esquerda em varias colunas.
    def _num_text(s: str) -> str:
        digs = re.sub(r"\D", "", s or "")
        if digs == "":
            return ""
        return str(int(digs))

    return {
        "uo": re.sub(r"\D", "", uo),
        "funcao": _num_text(funcao),
        "subfuncao": _num_text(subfuncao),
        "programa": _num_text(programa),
        "paoe": _num_text(paoe),
        "regiao": _num_text(regiao),
    }


def _extract_despesa_parts(text: object) -> dict[str, str]:
    raw = str(text or "").strip()
    # Replica das formulas VBA sobre o campo despesa:
    # LEFT(1), MID(3,1), MID(5,2), MID(7,3)
    cat = raw[0:1] if len(raw) >= 1 else ""
    gru = raw[2:3] if len(raw) >= 3 else ""
    mod = raw[4:6] if len(raw) >= 6 else ""
    elem = raw[6:9] if len(raw) >= 7 else ""

    def _num_text(s: str) -> str:
        digs = re.sub(r"\D", "", s or "")
        if digs == "":
            return ""
        return str(int(digs))

    return {
        "cat_e": _num_text(cat),
        "gru": _num_text(gru),
        "mod": _num_text(mod),
        "elem": _num_text(elem),
    }


def _first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    keys = {_norm_text(col): col for col in df.columns}
    keys_compact = {re.sub(r"\s+", "", k): v for k, v in keys.items()}
    for cand in candidates:
        hit = keys.get(_norm_text(cand))
        if hit:
            return hit
    for cand in candidates:
        kc_compact = re.sub(r"\s+", "", _norm_text(cand))
        hit = keys_compact.get(kc_compact)
        if hit:
            return hit
    for cand in candidates:
        kc = _norm_text(cand)
        for k, col in keys.items():
            if kc in k:
                return col
    for cand in candidates:
        kc_compact = re.sub(r"\s+", "", _norm_text(cand))
        for k, col in keys.items():
            if kc_compact in re.sub(r"\s+", "", k):
                return col
    return None


def _make_unique_columns(cols: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for c in cols:
        base = str(c).strip()
        n = seen.get(base, 0) + 1
        seen[base] = n
        out.append(base if n == 1 else f"{base}__{n}")
    return out


def _as_money(value: object) -> float | None:
    txt = str(value or "").strip()
    if txt == "":
        return None
    txt = txt.replace(".", "").replace(",", ".")
    try:
        return float(txt)
    except Exception:
        return None


def _br_date_to_excel_serial(value: object) -> str:
    txt = str(value or "").strip()
    if txt == "":
        return ""
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", txt)
    if not m:
        return txt
    dd, mm, yyyy = map(int, m.groups())
    try:
        dt = datetime(yyyy, mm, dd)
    except ValueError:
        return txt
    base = datetime(1899, 12, 30)
    return str((dt - base).days)


def _resolver_arquivo_plan51(base_dir: str) -> str:
    if not os.path.isdir(base_dir):
        raise RuntimeError(f"Pasta base do PLAN51 nao encontrada: {base_dir}")

    candidatos = []
    for nome in os.listdir(base_dir):
        caminho = os.path.join(base_dir, nome)
        if not os.path.isfile(caminho):
            continue
        if not nome.lower().endswith(".xlsx"):
            continue
        if nome.startswith("~$"):
            continue
        candidatos.append(caminho)

    if not candidatos:
        raise RuntimeError(f"Nenhum arquivo .xlsx encontrado em: {base_dir}")
    return max(candidatos, key=os.path.getmtime)


def _fill_block_down(
    df: pd.DataFrame,
    block_cols: list[str],
    start_col: str,
    value_col: str,
    flag_col: str | None = None,
) -> None:
    for i in range(1, len(df)):
        if _is_blank(df.at[i, start_col]) and not _is_blank(df.at[i, value_col]):
            for col in block_cols:
                df.at[i, col] = df.at[i - 1, col]
            if flag_col:
                df.at[i, flag_col] = True


def _compute_calc_value(row: pd.Series, source_col: str, kind: str) -> str:
    raw = str(row.get(source_col, "") or "").strip()
    if kind == "CAT_E":
        return _extract_despesa_parts(raw)["cat_e"]
    if kind == "GRU":
        return _extract_despesa_parts(raw)["gru"]
    if kind == "MOD":
        return _extract_despesa_parts(raw)["mod"]
    if kind == "ELEM":
        return _extract_despesa_parts(raw)["elem"]
    return raw


def _split_dif_rows(
    df: pd.DataFrame,
    col_s: str,
    col_a: str,
    col_def: str,
    sup_block_cols: list[str],
    anul_block_cols: list[str],
    split_sup_flag_col: str | None = None,
    split_anul_flag_col: str | None = None,
    red_def_flag_col: str | None = None,
) -> pd.DataFrame:
    out_rows: list[pd.Series] = []
    for _, row in df.iterrows():
        s_val = str(row.get(col_s, "") or "").strip()
        a_val = str(row.get(col_a, "") or "").strip()
        if s_val != "(*)" and a_val != "(*)" and s_val != a_val:
            row_sup = row.copy()
            row_anul = row.copy()

            for col in anul_block_cols:
                row_sup[col] = ""
            for col in sup_block_cols:
                row_anul[col] = ""

            row_sup[col_a] = "(*)"
            row_sup[col_def] = s_val

            row_anul[col_s] = "(*)"
            row_anul[col_def] = a_val
            row_sup["__FLAG_SPLIT_SUPL"] = True
            row_anul["__FLAG_SPLIT_ANUL"] = True
            if split_sup_flag_col:
                row_sup[split_sup_flag_col] = True
            if split_anul_flag_col:
                row_anul[split_anul_flag_col] = True
            if red_def_flag_col:
                row_sup[red_def_flag_col] = True
                row_anul[red_def_flag_col] = True

            out_rows.append(row_sup)
            out_rows.append(row_anul)
        else:
            out_rows.append(row.copy())
    return pd.DataFrame(out_rows).reset_index(drop=True)




def processar_plan51(
    caminho_entrada: str, modo: str = "TODOS", df_raw: pd.DataFrame | None = None
) -> tuple[pd.DataFrame, dict[str, float], dict]:
    if df_raw is None:
        df_raw = pd.read_excel(caminho_entrada, sheet_name=0, header=None, dtype=str, engine="openpyxl")
    df_raw = df_raw.fillna("")

    tipo_pos = _find_token(df_raw, "TIPO CREDITO")
    if not tipo_pos:
        raise RuntimeError("Nao foi encontrado o marcador 'TIPO CREDITO' no arquivo.")
    hdr_row = tipo_pos[0]

    hdr = df_raw.iloc[hdr_row].astype(str).tolist()
    df = df_raw.iloc[hdr_row + 1 :].copy().reset_index(drop=True)
    df.columns = _make_unique_columns([str(x).strip() for x in hdr])
    df = df.loc[:, [str(c).strip() != "" for c in df.columns]]
    # Remove linhas de totais do relatorio bruto (Total Suplementado/Anulado),
    # que nao fazem parte da malha de dados detalhados.
    df = df[~df.apply(lambda r: any(_is_total_marker(v) for v in r.tolist()), axis=1)]
    df = df[df.apply(lambda r: any(str(v).strip() != "" for v in r), axis=1)].reset_index(drop=True)

    cols_norm = [_norm_text(c) for c in df.columns]
    for idx, cn in enumerate(cols_norm):
        if "ELEMENTO SUPLEMENTADO" in cn:
            df = df.rename(columns={df.columns[idx]: "DESPESA SUPL"})
        if "ELEMENTO ANULADO" in cn:
            df = df.rename(columns={df.columns[idx]: "DESPESA ANUL"})

    c_uoproj_supl = _first_col(df, ["UO/PROJETO ATIV.SUPL", "UO/PROJETO ATIV. SUPL"])
    c_fonte_supl = _first_col(df, ["FONTE SUPL"])
    c_iduso_supl = _first_col(df, ["IDUSO SUPL"])
    c_valor_supl = _first_col(df, ["VALOR SUPL"])

    c_uoproj_anul = _first_col(df, ["UO/PROJETO ATIV.ANUL", "UO/PROJETO ATIV. ANUL"])
    c_fonte_anul = _first_col(df, ["FONTE ANUL"])
    c_iduso_anul = _first_col(df, ["IDUSO ANUL"])
    c_valor_anul = _first_col(df, ["VALOR ANUL"])

    c_nfiplan = _first_col(df, ["N. FIPLAN", "N FIPLAN", "NUMERO FIPLAN"])
    c_decreto = _first_col(df, ["DECRETO"])
    c_data = _first_col(df, ["DATA CONTABIL", "DATA CONTABIL"])
    c_doe = _first_col(df, ["N. DOE", "N DOE"])
    c_tipo = _first_col(df, ["TIPO DE CREDITO", "TIPO CREDITO"])

    c_despesa_s = _first_col(df, ["DESPESA SUPL", "ELEMENTO SUPLEMENTADO"])
    c_despesa_a = _first_col(df, ["DESPESA ANUL", "ELEMENTO ANULADO"])

    # PLAN51 geralmente vem com cabecalhos duplicados (UO/PROJETO ATIV., FONTE, IDUSO, VALOR).
    # Quando isso ocorre, localizamos por posicao relativa aos campos ELEMENTO SUPLEMENTADO/ANULADO.
    cols = list(df.columns)
    if c_despesa_s in cols:
        i = cols.index(c_despesa_s)
        if i - 1 >= 0 and c_uoproj_supl is None:
            c_uoproj_supl = cols[i - 1]
        if i + 1 < len(cols) and c_fonte_supl is None:
            c_fonte_supl = cols[i + 1]
        if i + 2 < len(cols) and c_iduso_supl is None:
            c_iduso_supl = cols[i + 2]
        if i + 3 < len(cols) and c_valor_supl is None:
            c_valor_supl = cols[i + 3]

    if c_despesa_a in cols:
        i = cols.index(c_despesa_a)
        if i - 1 >= 0 and c_uoproj_anul is None:
            c_uoproj_anul = cols[i - 1]
        if i + 1 < len(cols) and c_fonte_anul is None:
            c_fonte_anul = cols[i + 1]
        if i + 2 < len(cols) and c_iduso_anul is None:
            c_iduso_anul = cols[i + 2]
        if i + 3 < len(cols) and c_valor_anul is None:
            c_valor_anul = cols[i + 3]

    # Padroniza nomes para o mesmo padrao visual do VBA (evita sufixos "__2").
    rename_map = {}
    if c_nfiplan:
        rename_map[c_nfiplan] = "N. FIPLAN"
        c_nfiplan = "N. FIPLAN"
    if c_decreto:
        rename_map[c_decreto] = "DECRETO"
        c_decreto = "DECRETO"
    if c_data:
        rename_map[c_data] = "DATA CONTÁBIL"
        c_data = "DATA CONTÁBIL"
    if c_doe:
        rename_map[c_doe] = "N. DOE"
        c_doe = "N. DOE"
    if c_tipo:
        rename_map[c_tipo] = "TIPO CRÉDITO"
        c_tipo = "TIPO CRÉDITO"
    if c_uoproj_supl:
        rename_map[c_uoproj_supl] = "UO/PROJETO ATIV.SUPL"
        c_uoproj_supl = "UO/PROJETO ATIV.SUPL"
    if c_despesa_s:
        rename_map[c_despesa_s] = "ELEMENTO SUPLEMENTADO"
        c_despesa_s = "ELEMENTO SUPLEMENTADO"
    if c_fonte_supl:
        rename_map[c_fonte_supl] = "FONTE SUPL"
        c_fonte_supl = "FONTE SUPL"
    if c_iduso_supl:
        rename_map[c_iduso_supl] = "IDUSO SUPL"
        c_iduso_supl = "IDUSO SUPL"
    if c_valor_supl:
        rename_map[c_valor_supl] = "VALOR SUPL"
        c_valor_supl = "VALOR SUPL"
    if c_uoproj_anul:
        rename_map[c_uoproj_anul] = "UO/PROJETO ATIV.ANUL"
        c_uoproj_anul = "UO/PROJETO ATIV.ANUL"
    if c_despesa_a:
        rename_map[c_despesa_a] = "ELEMENTO ANULADO"
        c_despesa_a = "ELEMENTO ANULADO"
    if c_fonte_anul:
        rename_map[c_fonte_anul] = "FONTE ANUL"
        c_fonte_anul = "FONTE ANUL"
    if c_iduso_anul:
        rename_map[c_iduso_anul] = "IDUSO ANUL"
        c_iduso_anul = "IDUSO ANUL"
    if c_valor_anul:
        rename_map[c_valor_anul] = "VALOR ANUL"
        c_valor_anul = "VALOR ANUL"
    if rename_map:
        df = df.rename(columns=rename_map)

    obrig = [
        c_uoproj_supl,
        c_fonte_supl,
        c_iduso_supl,
        c_valor_supl,
        c_uoproj_anul,
        c_fonte_anul,
        c_iduso_anul,
        c_valor_anul,
        c_nfiplan,
        c_decreto,
        c_data,
        c_doe,
        c_tipo,
        c_despesa_s,
        c_despesa_a,
    ]
    if any(col is None for col in obrig):
        faltando = [
            nome
            for nome, col in [
                ("UO/PROJETO ATIV.SUPL", c_uoproj_supl),
                ("FONTE SUPL", c_fonte_supl),
                ("IDUSO SUPL", c_iduso_supl),
                ("VALOR SUPL", c_valor_supl),
                ("UO/PROJETO ATIV.ANUL", c_uoproj_anul),
                ("FONTE ANUL", c_fonte_anul),
                ("IDUSO ANUL", c_iduso_anul),
                ("VALOR ANUL", c_valor_anul),
                ("N FIPLAN", c_nfiplan),
                ("DECRETO", c_decreto),
                ("DATA CONTABIL", c_data),
                ("N DOE", c_doe),
                ("TIPO CREDITO", c_tipo),
                ("ELEMENTO SUPLEMENTADO", c_despesa_s),
                ("ELEMENTO ANULADO", c_despesa_a),
            ]
            if col is None
        ]
        raise RuntimeError(
            "Cabecalhos obrigatorios nao encontrados no PLAN 51. "
            f"Faltando: {', '.join(faltando)}. "
            f"Cabecalhos detectados: {list(df.columns)}"
        )

    for c in [c_nfiplan, c_decreto, c_data, c_doe, c_tipo]:
        df[c] = df[c].astype(str)
    df["__FLAG_15"] = False
    for i in range(1, len(df)):
        if _is_blank(df.at[i, c_nfiplan]):
            for c in [c_nfiplan, c_decreto, c_data, c_doe, c_tipo]:
                df.at[i, c] = df.at[i - 1, c]
            df.at[i, "__FLAG_15"] = True

    parsed_s = df[c_uoproj_supl].map(_extract_struct_parts)
    parsed_a = df[c_uoproj_anul].map(_extract_struct_parts)
    df["UO SUPL"] = parsed_s.map(lambda x: x["uo"])
    df["FUNÇÃO SUPL"] = parsed_s.map(lambda x: x["funcao"])
    df["SUBFUNÇÃO SUPL"] = parsed_s.map(lambda x: x["subfuncao"])
    df["PROGRAMA SUPL"] = parsed_s.map(lambda x: x["programa"])
    df["PAOE SUPL"] = parsed_s.map(lambda x: x["paoe"])
    df["REGIÃO SUPL"] = parsed_s.map(lambda x: x["regiao"])

    df["UO ANUL"] = parsed_a.map(lambda x: x["uo"])
    df["FUNÇÃO ANUL"] = parsed_a.map(lambda x: x["funcao"])
    df["SUBFUNÇÃO ANUL"] = parsed_a.map(lambda x: x["subfuncao"])
    df["PROGRAMA ANUL"] = parsed_a.map(lambda x: x["programa"])
    df["PAOE ANUL"] = parsed_a.map(lambda x: x["paoe"])
    df["REGIÃO ANUL"] = parsed_a.map(lambda x: x["regiao"])

    c_uo_s = "UO SUPL"
    c_func_s = "FUNÇÃO SUPL"
    c_sfunc_s = "SUBFUNÇÃO SUPL"
    c_prog_s = "PROGRAMA SUPL"
    c_paoe_s = "PAOE SUPL"
    c_reg_s = "REGIÃO SUPL"

    c_uo_a = "UO ANUL"
    c_func_a = "FUNÇÃO ANUL"
    c_sfunc_a = "SUBFUNÇÃO ANUL"
    c_prog_a = "PROGRAMA ANUL"
    c_paoe_a = "PAOE ANUL"
    c_reg_a = "REGIÃO ANUL"

    sup_block_cols = [c_uoproj_supl, c_uo_s, c_func_s, c_sfunc_s, c_prog_s, c_paoe_s, c_reg_s]
    sup_block_full = sup_block_cols + [c_despesa_s, c_fonte_supl, c_iduso_supl, c_valor_supl]
    anul_block_cols = [c_uoproj_anul, c_uo_a, c_func_a, c_sfunc_a, c_prog_a, c_paoe_a, c_reg_a]
    anul_block_full = anul_block_cols + [c_despesa_a, c_fonte_anul, c_iduso_anul, c_valor_anul]

    dims_todos = [
        ("__UO", c_uo_s, c_uo_a, "RAW"),
        ("__FUNÇÃO", c_func_s, c_func_a, "RAW"),
        ("__s_FUN", c_sfunc_s, c_sfunc_a, "RAW"),
        ("__PROGR", c_prog_s, c_prog_a, "RAW"),
        ("__PAOE", c_paoe_s, c_paoe_a, "RAW"),
        ("__REGIÃO", c_reg_s, c_reg_a, "RAW"),
        ("__DESPESA", c_despesa_s, c_despesa_a, "RAW"),
        ("__CAT_E", c_despesa_s, c_despesa_a, "CAT_E"),
        ("__GRU", c_despesa_s, c_despesa_a, "GRU"),
        ("__MOD", c_despesa_s, c_despesa_a, "MOD"),
        ("__ELEM", c_despesa_s, c_despesa_a, "ELEM"),
        ("__FONTE", c_fonte_supl, c_fonte_anul, "RAW"),
        ("__IDUSO", c_iduso_supl, c_iduso_anul, "RAW"),
    ]
    dims_paoe = [d for d in dims_todos if d[0] in {"__UO", "__FUNÇÃO", "__s_FUN", "__PROGR", "__PAOE", "__REGIÃO", "__DESPESA", "__FONTE", "__IDUSO"}]
    dims = dims_todos if modo.upper() == "TODOS" else dims_paoe

    add_cols: dict[str, pd.Series] = {}
    for base, _, _, _ in dims:
        add_cols[f"{base} (S)"] = pd.Series([""] * len(df), index=df.index, dtype=object)
        add_cols[f"{base} (A)"] = pd.Series([""] * len(df), index=df.index, dtype=object)
        add_cols[f"{base} (DEF)"] = pd.Series([""] * len(df), index=df.index, dtype=object)
        add_cols[f"__FLAG_SPLIT_SUPL_{base}"] = pd.Series([False] * len(df), index=df.index, dtype=bool)
        add_cols[f"__FLAG_SPLIT_ANUL_{base}"] = pd.Series([False] * len(df), index=df.index, dtype=bool)
        add_cols[f"__FLAG_RED_DEF_{base}"] = pd.Series([False] * len(df), index=df.index, dtype=bool)
    add_cols["__FLAG_SUPL"] = pd.Series([False] * len(df), index=df.index, dtype=bool)
    add_cols["__FLAG_ANUL"] = pd.Series([False] * len(df), index=df.index, dtype=bool)
    add_cols["__FLAG_SPLIT_SUPL"] = pd.Series([False] * len(df), index=df.index, dtype=bool)
    add_cols["__FLAG_SPLIT_ANUL"] = pd.Series([False] * len(df), index=df.index, dtype=bool)
    df = pd.concat([df, pd.DataFrame(add_cols, index=df.index)], axis=1)

    for base, src_s, src_a, kind in dims:
        col_s = f"{base} (S)"
        col_a = f"{base} (A)"
        col_d = f"{base} (DEF)"

        _fill_block_down(df, sup_block_cols, c_uoproj_supl, c_valor_supl, "__FLAG_SUPL")
        _fill_block_down(df, anul_block_cols, c_uoproj_anul, c_valor_anul, "__FLAG_ANUL")

        s_vals: list[str] = []
        a_vals: list[str] = []
        d_vals: list[str] = []
        for _, row in df.iterrows():
            if _is_blank(row.get(c_uoproj_supl, "")) and _is_blank(row.get(c_valor_supl, "")):
                s_val = "(*)"
            else:
                s_val = _compute_calc_value(row, src_s, kind).strip()
            if _is_blank(row.get(c_uoproj_anul, "")) and _is_blank(row.get(c_valor_anul, "")):
                a_val = "(*)"
            else:
                a_val = _compute_calc_value(row, src_a, kind).strip()

            if s_val == a_val:
                d_val = s_val
            elif s_val == "(*)":
                d_val = a_val
            elif a_val == "(*)":
                d_val = s_val
            else:
                d_val = "(*) DIF"
            s_vals.append(s_val)
            a_vals.append(a_val)
            d_vals.append(d_val)

        df[col_s] = s_vals
        df[col_a] = a_vals
        df[col_d] = d_vals

        df = _split_dif_rows(
            df,
            col_s,
            col_a,
            col_d,
            sup_block_full,
            anul_block_full,
            split_sup_flag_col=f"__FLAG_SPLIT_SUPL_{base}",
            split_anul_flag_col=f"__FLAG_SPLIT_ANUL_{base}",
            red_def_flag_col=f"__FLAG_RED_DEF_{base}",
        )

    colunas_calc: list[str] = []
    for base, _, _, _ in dims:
        colunas_calc.extend([f"{base} (S)", f"{base} (A)", f"{base} (DEF)"])

    colunas_fixas = [
        c_nfiplan,
        c_decreto,
        c_data,
        c_doe,
        c_tipo,
        c_uoproj_supl,
        c_uo_s,
        c_func_s,
        c_sfunc_s,
        c_prog_s,
        c_paoe_s,
        c_reg_s,
        c_despesa_s,
        c_fonte_supl,
        c_iduso_supl,
        c_valor_supl,
        c_uoproj_anul,
        c_uo_a,
        c_func_a,
        c_sfunc_a,
        c_prog_a,
        c_paoe_a,
        c_reg_a,
        c_despesa_a,
        c_fonte_anul,
        c_iduso_anul,
        c_valor_anul,
    ]
    helper_cols = ["__FLAG_15", "__FLAG_SUPL", "__FLAG_ANUL", "__FLAG_SPLIT_SUPL", "__FLAG_SPLIT_ANUL"]
    helper_cols.extend([f"__FLAG_SPLIT_SUPL_{base}" for base, _, _, _ in dims])
    helper_cols.extend([f"__FLAG_SPLIT_ANUL_{base}" for base, _, _, _ in dims])
    helper_cols.extend([f"__FLAG_RED_DEF_{base}" for base, _, _, _ in dims])

    df_aux = df[colunas_calc + colunas_fixas + helper_cols].copy()
    df_aux = df_aux[~df_aux.apply(lambda r: any(_is_total_marker(v) for v in r.tolist()), axis=1)].reset_index(drop=True)

    # Remove linhas fantasma no fim (geradas por expansoes de fluxo),
    # preservando apenas linhas com algum dado operacional em SUPL/ANUL.
    cols_check = [c_uoproj_supl, c_valor_supl, c_despesa_s, c_uoproj_anul, c_valor_anul, c_despesa_a]
    def _tem_dado_operacional(row: pd.Series) -> bool:
        for c in cols_check:
            v = str(row.get(c, "") or "").strip()
            if v != "":
                return True
        return False
    df_aux = df_aux[df_aux.apply(_tem_dado_operacional, axis=1)].reset_index(drop=True)

    # Mantem data em formato textual dd/mm/aaaa para facilitar conferencia do usuario.
    df_aux[c_data] = df_aux[c_data].astype(str).str.strip()

    def _to_num_or_blank(v: object) -> object:
        n = _as_money(v)
        if n is None:
            return ""
        return int(n) if float(n).is_integer() else n

    for c in [c_valor_supl, c_valor_anul]:
        df_aux[c] = df_aux[c].map(_to_num_or_blank)

    def _digits_or_blank(v: object) -> str:
        d = re.sub(r"\D", "", str(v or ""))
        if d == "":
            return ""
        return str(int(d))

    for c in [c_nfiplan, c_decreto, c_doe, c_fonte_supl, c_fonte_anul, c_iduso_supl, c_iduso_anul]:
        df_aux[c] = df_aux[c].map(_digits_or_blank)

    df_out = df_aux[colunas_calc + colunas_fixas].copy()

    style_info = {
        "rows_flag_15": [i + 2 for i, v in enumerate(df_aux["__FLAG_15"].tolist()) if bool(v)],
        "rows_flag_supl": [i + 2 for i, v in enumerate(df_aux["__FLAG_SUPL"].tolist()) if bool(v)],
        "rows_flag_anul": [i + 2 for i, v in enumerate(df_aux["__FLAG_ANUL"].tolist()) if bool(v)],
        "rows_split_supl": [i + 2 for i, v in enumerate(df_aux["__FLAG_SPLIT_SUPL"].tolist()) if bool(v)],
        "rows_split_anul": [i + 2 for i, v in enumerate(df_aux["__FLAG_SPLIT_ANUL"].tolist()) if bool(v)],
        "split_anul_by_base": {
            base: [i + 2 for i, v in enumerate(df_aux[f"__FLAG_SPLIT_ANUL_{base}"].tolist()) if bool(v)]
            for base, _, _, _ in dims
        },
        "red_def_by_base": {
            base: [i + 2 for i, v in enumerate(df_aux[f"__FLAG_RED_DEF_{base}"].tolist()) if bool(v)]
            for base, _, _, _ in dims
        },
    }

    tot_sup = sum(_as_money(v) or 0.0 for v in df_out[c_valor_supl].tolist())
    tot_anu = sum(_as_money(v) or 0.0 for v in df_out[c_valor_anul].tolist())
    resumo = {
        "subtotal_suplementacao": round(tot_sup, 2),
        "subtotal_anulacao": round(tot_anu, 2),
        "subtotal_liquido": round(tot_sup - tot_anu, 2),
    }
    return df_out, resumo, style_info


def salvar_saida(
    df_out: pd.DataFrame,
    resumo: dict[str, float],
    style_info: dict,
    caminho_saida: str,
    sheet_name: str,
    modo: str,
    arquivo_processado: str,
    inicio_proc: str,
    fim_proc: str,
    duracao_proc: str,
) -> None:
    os.makedirs(os.path.dirname(caminho_saida) or ".", exist_ok=True)
    with pd.ExcelWriter(caminho_saida, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name=sheet_name)
        pd.DataFrame([resumo]).to_excel(writer, index=False, sheet_name="RESUMO")

    wb = load_workbook(caminho_saida)
    ws = wb[sheet_name]
    ws2 = wb["RESUMO"]

    fonte = Font(name="Arial", size=8)
    fill_yellow = PatternFill(fill_type="solid", start_color="FFFF00", end_color="FFFF00")
    fill_blue_def = PatternFill(fill_type="solid", start_color="0070C0", end_color="0070C0")
    fill_gray = PatternFill(fill_type="solid", start_color="F2F2F2", end_color="F2F2F2")
    fill_sup = PatternFill(fill_type="solid", start_color="E3F6FD", end_color="E3F6FD")
    fill_anul = PatternFill(fill_type="solid", start_color="EEE2FA", end_color="EEE2FA")
    fill_hdr_15 = PatternFill(fill_type="solid", start_color="E8E8E8", end_color="E8E8E8")
    font_magenta = Font(name="Arial", size=8, bold=True, color="A02B93")
    font_red = Font(name="Arial", size=8, color="FF0000")

    for plan in [ws, ws2]:
        for row in plan.iter_rows(min_row=1, max_row=plan.max_row, min_col=1, max_col=plan.max_column):
            for cell in row:
                cell.font = fonte
        for col_cells in plan.columns:
            max_len = 0
            letra = col_cells[0].column_letter
            for cell in col_cells:
                val = "" if cell.value is None else str(cell.value)
                max_len = max(max_len, len(val))
            plan.column_dimensions[letra].width = min(max_len + 2, 60)

    # AJUSTE DE DESEMPENHO (único desvio do script): `ws.max_column` do openpyxl
    # varre todas as células a cada chamada, e o script o chamava dentro do laço
    # de cada linha (custo quadrático: ~7 min para 12 mil linhas). Calculado uma vez.
    max_col = ws.max_column
    header = [ws.cell(row=1, column=c).value for c in range(1, max_col + 1)]
    hmap = {_norm_text(v): i for i, v in enumerate(header, start=1)}

    def _col(name: str) -> int | None:
        return hmap.get(_norm_text(name))

    def _cols(names: list[str]) -> list[int]:
        out: list[int] = []
        for n in names:
            c = _col(n)
            if c:
                out.append(c)
        return out

    yellow_headers = {
        "N. FIPLAN",
        "UO SUPL",
        "FUNÇÃO SUPL",
        "SUBFUNÇÃO SUPL",
        "PROGRAMA SUPL",
        "PAOE SUPL",
        "REGIÃO SUPL",
        "UO ANUL",
        "FUNÇÃO ANUL",
        "SUBFUNÇÃO ANUL",
        "PROGRAMA ANUL",
        "PAOE ANUL",
        "REGIÃO ANUL",
    }
    for c, name in enumerate(header, start=1):
        txt = str(name or "")
        cell = ws.cell(row=1, column=c)
        if txt.endswith("(DEF)"):
            cell.fill = fill_blue_def
            cell.font = Font(name="Arial", size=8, bold=True, color="FFFFFF")
        elif txt.endswith("(S)") or txt.endswith("(A)"):
            cell.font = Font(name="Arial", size=8, bold=True)
        if txt in yellow_headers:
            cell.fill = fill_yellow
            cell.font = Font(name="Arial", size=8, bold=True)

    cols_15 = _cols(["N. FIPLAN", "DECRETO", "DATA CONTÁBIL", "N. DOE", "TIPO CRÉDITO"])
    cols_sup = _cols(["UO/PROJETO ATIV.SUPL", "UO SUPL", "FUNÇÃO SUPL", "SUBFUNÇÃO SUPL", "PROGRAMA SUPL", "PAOE SUPL", "REGIÃO SUPL"])
    cols_an = _cols(["UO/PROJETO ATIV.ANUL", "UO ANUL", "FUNÇÃO ANUL", "SUBFUNÇÃO ANUL", "PROGRAMA ANUL", "PAOE ANUL", "REGIÃO ANUL"])
    c_valor_an = _col("VALOR ANUL")
    if c_valor_an and cols_an:
        cols_split_an = [c for c in range(min(cols_an), c_valor_an + 1)]
    else:
        cols_split_an = cols_an

    for r in style_info.get("rows_flag_15", []):
        for c in cols_15:
            cc = ws.cell(row=r, column=c)
            cc.fill = fill_hdr_15
            cc.font = font_magenta
            if r > 2:
                ws.cell(row=r - 1, column=c).font = font_magenta

    for r in style_info.get("rows_flag_supl", []):
        for c in cols_sup:
            ws.cell(row=r, column=c).fill = fill_sup

    for r in style_info.get("rows_flag_anul", []):
        for c in cols_an:
            ws.cell(row=r, column=c).fill = fill_anul

    for r in style_info.get("rows_split_anul", []):
        for c in cols_split_an:
            ws.cell(row=r, column=c).fill = fill_gray

    split_anul_by_base = style_info.get("split_anul_by_base", {}) or {}
    red_def_by_base = style_info.get("red_def_by_base", {}) or {}
    for base, rows in split_anul_by_base.items():
        c_s = _col(f"{base} (S)")
        if c_s and c_s > 1:
            for r in rows:
                for c in range(1, c_s):
                    ws.cell(row=r, column=c).fill = fill_gray

    for base, rows in red_def_by_base.items():
        c_d = _col(f"{base} (DEF)")
        if not c_d:
            continue
        for r in rows:
            ws.cell(row=r, column=c_d).font = font_red

    cols_center_limit = 39 if modo.upper() == "TODOS" else 27
    for r in range(1, len(df_out) + 2):
        for c in range(1, min(cols_center_limit, max_col) + 1):
            ws.cell(row=r, column=c).alignment = Alignment(horizontal="center")

    final_data_row = len(df_out) + 1
    ws.auto_filter.ref = f"A1:{get_column_letter(max_col)}{final_data_row}"
    ws.freeze_panes = "A2"
    ws.sheet_view.showGridLines = False

    c_vsup = _col("VALOR SUPL")
    c_van = _col("VALOR ANUL")
    if not c_vsup or not c_van:
        c_vsup = 55 if modo.upper() == "TODOS" else 43
        c_van = 66 if modo.upper() == "TODOS" else 54
    c_lbl_sup = max(1, c_vsup - 2)
    c_lbl_an = max(1, c_van - 2)

    row_sub = final_data_row + 2
    row_liq = final_data_row + 4

    ws.cell(row=row_sub, column=c_lbl_sup).value = "SUBTOTAL (SUPLEMENT)"
    ws.cell(row=row_sub, column=c_lbl_an).value = "SUBTOTAL (ANULAÇÕES)"
    ws.cell(row=row_liq, column=c_lbl_an).value = "SUBTOTAL (LÍQUIDO)"
    ws.cell(row=row_sub, column=c_lbl_sup).font = Font(name="Arial", size=8, bold=True)
    ws.cell(row=row_sub, column=c_lbl_an).font = Font(name="Arial", size=8, bold=True)
    ws.cell(row=row_liq, column=c_lbl_an).font = Font(name="Arial", size=8, bold=True)

    col_vsup = get_column_letter(c_vsup)
    col_van = get_column_letter(c_van)
    ws.cell(row=row_sub, column=c_vsup).value = f"=SUBTOTAL(9,{col_vsup}1:{col_vsup}{final_data_row})"
    ws.cell(row=row_sub, column=c_van).value = f"=SUBTOTAL(9,{col_van}1:{col_van}{final_data_row})"
    ws.cell(row=row_liq, column=c_van).value = f"={col_vsup}{row_sub}-{col_van}{row_sub}"
    for rr, cc in [(row_sub, c_vsup), (row_sub, c_van), (row_liq, c_van)]:
        ws.cell(row=rr, column=cc).fill = fill_yellow
        ws.cell(row=rr, column=cc).font = Font(name="Arial", size=8, bold=True)

    # AJUSTE DO SISTEMA (pedido do usuário): formato Contábil do Excel, sem "R$",
    # nas colunas de valor e nos subtotais.
    fmt_contabil = '_-* #,##0.00_-;-* #,##0.00_-;_-* "-"??_-;_-@_-'
    for cc in (c_vsup, c_van):
        for rr in range(2, final_data_row + 1):
            ws.cell(row=rr, column=cc).number_format = fmt_contabil
    for rr, cc in [(row_sub, c_vsup), (row_sub, c_van), (row_liq, c_van)]:
        ws.cell(row=rr, column=cc).number_format = fmt_contabil

    for c in range(1, max_col + 1):
        ws.cell(row=row_sub, column=c).border = Border(top=Side(style="thin"))

    for rr in [row_sub, row_sub + 1, row_sub + 2, row_sub + 3, row_sub + 4, row_sub + 5]:
        ws.merge_cells(start_row=rr, start_column=1, end_row=rr, end_column=min(21, max_col))

    ws.cell(row=row_sub, column=1).value = (
        "Fonte: Relatório FIPLAN PLAN 51 - Relação Detalhada de Créditos Adicionais, "
        f"arquivo processado: {arquivo_processado}"
    )
    ws.cell(row=row_sub + 2, column=1).value = f"Data e hora início do processamento: {inicio_proc}"
    ws.cell(row=row_sub + 3, column=1).value = f"Data e hora final do processamento: {fim_proc}"
    ws.cell(row=row_sub + 4, column=1).value = f"Tempo de duração do processamento: {duracao_proc}"
    ws.cell(row=row_sub + 5, column=1).value = ""
    for rr in [row_sub + 2, row_sub + 3, row_sub + 4, row_sub + 5]:
        ws.cell(row=rr, column=1).font = Font(name="Arial", size=8, italic=True)

    def _group(start_idx: int, end_idx: int) -> None:
        if start_idx <= max_col:
            ws.column_dimensions.group(
                get_column_letter(start_idx),
                get_column_letter(min(end_idx, max_col)),
                outline_level=1,
                hidden=False,
            )

    if modo.upper() == "TODOS":
        for a, b in [(1, 2), (4, 5), (7, 8), (10, 11), (13, 14), (16, 17), (19, 20), (22, 23), (25, 26), (28, 29), (31, 32), (34, 35), (37, 38), (46, 51), (57, 62)]:
            _group(a, b)
    else:
        for a, b in [(1, 2), (4, 5), (7, 8), (10, 11), (13, 14), (16, 17), (19, 20), (22, 23), (25, 26), (34, 39), (45, 50)]:
            _group(a, b)

    wb.save(caminho_saida)


# ---------------------------------------------------------------------------
# Parte do sistema
# ---------------------------------------------------------------------------

class Plan51Recusado(ValueError):
    """Arquivo recusado antes de gravar qualquer linha (cabeçalho inválido)."""


EXERCICIO_RE = re.compile(r"Exerc\S*\s+igual\s+a\s+(\d{4})", re.IGNORECASE)
STATUS_RE = re.compile(r"FLAG_STATUS\s+igual\s+a\s+(.+)", re.IGNORECASE)
# O FIPLAN imprime o filtro com o nome ("Efetivado", arquivos antigos) ou com o
# código do status ("5", arquivo de 03/11/2026); os dois são o mesmo filtro.
STATUS_EFETIVADO = {"EFETIVADO", "5"}

# Dimensões calculadas (S/A/DEF) na ordem do script (modo TODOS) -> prefixo da
# coluna no banco. Só a tradução de nome; a lógica continua no script original.
_DIMENSOES = [
    ("__UO", "uo"),
    ("__FUNÇÃO", "funcao"),
    ("__s_FUN", "subfuncao"),
    ("__PROGR", "programa"),
    ("__PAOE", "paoe"),
    ("__REGIÃO", "regiao"),
    ("__DESPESA", "despesa"),
    ("__CAT_E", "cat_e"),
    ("__GRU", "gru"),
    ("__MOD", "modalidade"),
    ("__ELEM", "elem"),
    ("__FONTE", "fonte"),
    ("__IDUSO", "iduso"),
]
_FIXAS = [
    ("N. FIPLAN", "n_fiplan", "text"),
    ("DECRETO", "decreto", "text"),
    ("DATA CONTÁBIL", "data_contabil", "text"),
    ("N. DOE", "n_doe", "text"),
    ("TIPO CRÉDITO", "tipo_credito", "text"),
    ("UO/PROJETO ATIV.SUPL", "uo_projeto_ativ_supl", "text"),
    ("UO SUPL", "uo_supl", "text"),
    ("FUNÇÃO SUPL", "funcao_supl", "text"),
    ("SUBFUNÇÃO SUPL", "subfuncao_supl", "text"),
    ("PROGRAMA SUPL", "programa_supl", "text"),
    ("PAOE SUPL", "paoe_supl", "text"),
    ("REGIÃO SUPL", "regiao_supl", "text"),
    ("ELEMENTO SUPLEMENTADO", "elemento_suplementado", "text"),
    ("FONTE SUPL", "fonte_supl", "text"),
    ("IDUSO SUPL", "iduso_supl", "text"),
    ("VALOR SUPL", "valor_supl", "num"),
    ("UO/PROJETO ATIV.ANUL", "uo_projeto_ativ_anul", "text"),
    ("UO ANUL", "uo_anul", "text"),
    ("FUNÇÃO ANUL", "funcao_anul", "text"),
    ("SUBFUNÇÃO ANUL", "subfuncao_anul", "text"),
    ("PROGRAMA ANUL", "programa_anul", "text"),
    ("PAOE ANUL", "paoe_anul", "text"),
    ("REGIÃO ANUL", "regiao_anul", "text"),
    ("ELEMENTO ANULADO", "elemento_anulado", "text"),
    ("FONTE ANUL", "fonte_anul", "text"),
    ("IDUSO ANUL", "iduso_anul", "text"),
    ("VALOR ANUL", "valor_anul", "num"),
]

# Fonte única das 66 colunas: (rótulo = nome da coluna do script/Excel,
# coluna no banco, tipo "text"/"num"), na ordem da saída do script. O model,
# a gravação, o relatório e o Excel baixado saem desta lista.
PLAN51_COLUNAS: list[tuple[str, str, str]] = [
    (f"{base} ({suf})", f"{prefixo}_{suf.lower()}", "text")
    for base, prefixo in _DIMENSOES
    for suf in ("S", "A", "DEF")
] + _FIXAS


# ---------------------------------------------------------------------------
# Cores do script (salvar_saida) guardadas por linha
# ---------------------------------------------------------------------------
# O script pinta a planilha conforme marcações calculadas no tratamento
# (style_info). Para o relatório e o Excel baixado reproduzirem a planilha
# tratada, cada linha guarda essas marcações numa máscara de bits (coluna
# `estilo`). Ordem das linhas = ordem do banco (id) = ordem da planilha.
BIT_FLAG15 = 0        # N. FIPLAN/DECRETO/DATA/DOE/TIPO repetidos da linha de cima (cinza + magenta)
BIT_SUPL = 1          # bloco suplementado repetido (azul claro)
BIT_ANUL = 2          # bloco anulado repetido (lilás)
BIT_SPLIT_ANUL = 3    # linha de anulação criada pela divisão (cinza no bloco anulado)
BIT_PROX_FLAG15 = 4   # a linha de baixo é FLAG15: as 5 colunas desta ficam em magenta
BIT_SPLIT_BASE = 5    # + índice da dimensão (13): cinza nas colunas à esquerda de "<dim> (S)"
BIT_RED_DEF = 18      # + índice da dimensão (13): "<dim> (DEF)" em vermelho

_BASES = [base for base, _ in _DIMENSOES]


def calcular_estilos(n_linhas: int, style_info: dict) -> list[int]:
    """style_info do script (linhas numeradas a partir de 2) -> uma máscara por linha."""
    masks = [0] * n_linhas

    def marca(linhas, bit):
        for r in linhas or []:
            i = r - 2
            if 0 <= i < n_linhas:
                masks[i] |= 1 << bit

    marca(style_info.get("rows_flag_15"), BIT_FLAG15)
    marca(style_info.get("rows_flag_supl"), BIT_SUPL)
    marca(style_info.get("rows_flag_anul"), BIT_ANUL)
    marca(style_info.get("rows_split_anul"), BIT_SPLIT_ANUL)
    marca([r - 1 for r in style_info.get("rows_flag_15", []) if r > 2], BIT_PROX_FLAG15)
    for k, base in enumerate(_BASES):
        marca((style_info.get("split_anul_by_base") or {}).get(base), BIT_SPLIT_BASE + k)
        marca((style_info.get("red_def_by_base") or {}).get(base), BIT_RED_DEF + k)
    return masks


def style_info_das_mascaras(masks: list[int]) -> dict:
    """Operação inversa: refaz o style_info que o `salvar_saida` do script espera."""

    def linhas(bit):
        return [i + 2 for i, m in enumerate(masks) if (m or 0) >> bit & 1]

    return {
        "rows_flag_15": linhas(BIT_FLAG15),
        "rows_flag_supl": linhas(BIT_SUPL),
        "rows_flag_anul": linhas(BIT_ANUL),
        "rows_split_supl": [],
        "rows_split_anul": linhas(BIT_SPLIT_ANUL),
        "split_anul_by_base": {base: linhas(BIT_SPLIT_BASE + k) for k, base in enumerate(_BASES)},
        "red_def_by_base": {base: linhas(BIT_RED_DEF + k) for k, base in enumerate(_BASES)},
    }



def ensure_dirs() -> None:
    for base in (UPLOAD_DIR, OUTPUT_DIR, UPLOAD_DIR / "tmp", OUTPUT_DIR / "tmp"):
        base.mkdir(parents=True, exist_ok=True)


def move_existing_to_tmp(base_dir: Path) -> None:
    tmp = base_dir / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    for f in base_dir.glob("*.xlsx"):
        dest = tmp / f"{f.stem}_{datetime.now().strftime('%Y%m%d%H%M%S')}{f.suffix}"
        try:
            f.rename(dest)
        except OSError:
            pass


def _num_ptbr(valor: object) -> float | None:
    """"17.378.126.545,30" -> 17378126545.3 (valor impresso pelo FIPLAN)."""
    txt = str(valor or "").strip().replace(".", "").replace(",", ".")
    try:
        return round(float(txt), 2)
    except ValueError:
        return None


def _brl(valor: float) -> str:
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _somar(serie) -> float:
    """Soma a coluna de valores já tratada (int/float, "" = vazio). Não usar o
    `resumo` do script: ele reaplica _as_money em número que já é decimal."""
    return round(sum(float(v) for v in serie if v != "" and v is not None), 2)


def ler_bruto(file_path: Path) -> pd.DataFrame:
    return pd.read_excel(file_path, sheet_name=0, header=None, dtype=str, engine="openpyxl").fillna("")


def ler_cabecalho(df_raw: pd.DataFrame) -> dict:
    """Exercício e status do filtro impresso no cabeçalho, mais os totais
    "Total Suplementado"/"Total Anulado" do rodapé (para conferência)."""
    exercicio = None
    status = None
    totais: dict[str, float] = {}
    for r in range(len(df_raw)):
        linha = [str(v).strip() for v in df_raw.iloc[r].tolist()]
        for i, cel in enumerate(linha):
            if not cel:
                continue
            if exercicio is None:
                m = EXERCICIO_RE.search(cel)
                if m:
                    exercicio = int(m.group(1))
            if status is None:
                m = STATUS_RE.search(cel)
                if m:
                    status = m.group(1).strip()
            chave = _norm_text(cel)
            if chave in ("TOTAL SUPLEMENTADO", "TOTAL ANULADO"):
                for prox in linha[i + 1 :]:
                    valor = _num_ptbr(prox) if prox else None
                    if valor is not None:
                        totais["supl" if chave == "TOTAL SUPLEMENTADO" else "anul"] = valor
                        break
    return {"exercicio": exercicio, "status": status, "totais": totais}


def validar_cabecalho(cab: dict) -> int:
    """Recusa (antes de gravar) arquivo sem exercício ou que não seja só de
    créditos efetivados. Devolve o exercício."""
    if not cab.get("exercicio"):
        raise Plan51Recusado('Arquivo recusado: não foi encontrado o filtro "Exercício igual a" no cabeçalho do PLAN 51.')
    if _norm_text(cab.get("status") or "") not in STATUS_EFETIVADO:
        achado = cab.get("status") or "não informado"
        raise Plan51Recusado(
            f'Arquivo recusado: o PLAN 51 deve ser emitido com "FLAG_STATUS igual a Efetivado" (encontrado: {achado}).'
        )
    return int(cab["exercicio"])


def conferir_totais(df_out: pd.DataFrame, totais: dict[str, float]) -> str:
    """Compara a soma lida com os totais impressos. Só informa (não bloqueia):
    um rodapé com outro formato não pode barrar a carga diária automática."""
    if not totais:
        return "Totais do relatório não encontrados para conferência."
    pares = [("supl", "Suplementado", "VALOR SUPL"), ("anul", "Anulado", "VALOR ANUL")]
    divergentes = []
    for chave, nome, col in pares:
        esperado = totais.get(chave)
        if esperado is None:
            continue
        lido = _somar(df_out[col])
        if abs(lido - esperado) > 0.01:
            divergentes.append(f"{nome}: lido {_brl(lido)} x relatório {_brl(esperado)}")
    if divergentes:
        return "ATENÇÃO: soma diferente do total do relatório (" + "; ".join(divergentes) + ")."
    return "Confere com o total do relatório."


def _registros(df_out: pd.DataFrame) -> list[dict]:
    """Linhas da saída do script -> dicts com os nomes das colunas do banco."""
    faltando = [r for r, _, _ in PLAN51_COLUNAS if r not in df_out.columns]
    if faltando:
        raise RuntimeError(f"Colunas esperadas ausentes na saída do tratamento do PLAN 51: {faltando}")
    registros = []
    for linha in df_out[[r for r, _, _ in PLAN51_COLUNAS]].itertuples(index=False, name=None):
        item = {}
        for (_, db_col, tipo), valor in zip(PLAN51_COLUNAS, linha):
            if tipo == "num":
                item[db_col] = None if valor == "" or valor is None else round(float(valor), 2)
            else:
                texto = "" if valor is None else str(valor).strip()
                item[db_col] = texto if texto != "" else None
        registros.append(item)
    return registros


def update_database(
    df_out: pd.DataFrame,
    exercicio: int,
    data_arquivo,
    user_email: str,
    upload_id: int,
    estilos: list[int] | None = None,
) -> int:
    """Grava a carga inativa e, no fim, troca a versão anterior do mesmo
    exercício pela nova numa única transação (padrão do PED): uma falha no meio
    nunca deixa o relatório sem dados nem com dados pela metade."""
    cols = [db_col for _, db_col, _ in PLAN51_COLUNAS]
    meta = ["upload_id", "exercicio", "data_arquivo", "user_email", "ativo", "estilo"]
    insert_sql = text(
        f"INSERT INTO plan51 ({', '.join(cols + meta)}) "
        f"VALUES ({', '.join(':' + c for c in cols + meta)})"
    )
    registros = _registros(df_out)
    total = 0
    # O tratamento anterior leva dezenas de segundos; a transação aberta ao ler
    # `upload_id` seguraria uma conexão parada e o servidor do banco a derrubaria
    # ("MySQL server has gone away"). Devolve a conexão antes de gravar.
    db.session.rollback()
    try:
        for start in range(0, len(registros), BATCH_SIZE):
            chunk = registros[start : start + BATCH_SIZE]
            for j, r in enumerate(chunk):
                r.update(
                    upload_id=upload_id,
                    exercicio=exercicio,
                    data_arquivo=data_arquivo,
                    user_email=user_email,
                    ativo=0,
                    estilo=estilos[start + j] if estilos else None,
                )
            db.session.execute(insert_sql, chunk)
            db.session.commit()
            total += len(chunk)
        db.session.execute(
            text("UPDATE plan51 SET ativo = 0 WHERE ativo = 1 AND exercicio = :ex AND upload_id <> :up"),
            {"ex": exercicio, "up": upload_id},
        )
        db.session.execute(text("UPDATE plan51 SET ativo = 1 WHERE upload_id = :up"), {"up": upload_id})
        db.session.commit()
    except Exception:
        db.session.rollback()
        # Apaga o que esta carga já tinha inserido (inativo): a versão anterior segue ativa.
        try:
            db.session.execute(text("DELETE FROM plan51 WHERE upload_id = :up AND ativo = 0"), {"up": upload_id})
            db.session.commit()
        except Exception:
            db.session.rollback()
        raise
    return total


@contextmanager
def _trava_exercicio(exercicio: int):
    """Uma gravação do PLAN 51 por exercício por vez (dois envios seguidos
    disputavam a troca de versão). O GET_LOCK pertence à CONEXÃO: usa uma
    dedicada, não a da sessão (que o commit devolve ao pool - seção 24)."""
    nome = f"plan51:{exercicio}"
    conn = db.engine.connect()
    try:
        if conn.execute(text("SELECT GET_LOCK(:n, 180)"), {"n": nome}).scalar() != 1:
            raise RuntimeError("Já existe um processamento do PLAN 51 em andamento para este exercício. Tente novamente.")
        yield
    finally:
        try:
            conn.execute(text("SELECT RELEASE_LOCK(:n)"), {"n": nome})
        finally:
            conn.close()


def reconstruir_saida(linhas: list, estilos: list[int]) -> tuple[pd.DataFrame, dict]:
    """Linhas do banco (na ordem de PLAN51_COLUNAS) -> o df_out e o style_info
    do script. Serve para regerar a planilha tratada quando o arquivo de saída
    gravado no upload não existe mais."""
    numericas = [tipo == "num" for _, _, tipo in PLAN51_COLUNAS]

    def valor(v, num):
        if v is None:
            return ""
        if num:
            n = float(v)
            return int(n) if n.is_integer() else n
        return str(v)

    dados = [[valor(v, num) for v, num in zip(linha, numericas)] for linha in linhas]
    df_out = pd.DataFrame(dados, columns=[r for r, _, _ in PLAN51_COLUNAS])
    return df_out, style_info_das_mascaras(estilos)


def gerar_excel_do_banco(caminho_saida: str, linhas: list, estilos: list[int], arquivo: str, quando: str) -> None:
    """Mesma planilha do upload (cores, subtotais e rodapé), refeita do banco."""
    df_out, style_info = reconstruir_saida(linhas, estilos)
    sup, anu = _somar(df_out["VALOR SUPL"]), _somar(df_out["VALOR ANUL"])
    salvar_saida(
        df_out=df_out,
        resumo={"subtotal_suplementacao": sup, "subtotal_anulacao": anu, "subtotal_liquido": round(sup - anu, 2)},
        style_info=style_info,
        caminho_saida=caminho_saida,
        sheet_name=f"PLAN_51_TODOS_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        modo="TODOS",
        arquivo_processado=arquivo,
        inicio_proc=quando,
        fim_proc=datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        duracao_proc="regerada a partir do banco",
    )


def run_plan51(file_path: Path, data_arquivo: datetime, user_email: str, upload_id: int) -> tuple[int, Path, str]:
    ensure_dirs()
    t0 = datetime.now()
    df_raw = ler_bruto(file_path)
    cab = ler_cabecalho(df_raw)
    exercicio = validar_cabecalho(cab)  # Plan51Recusado antes de gerar saída ou gravar

    df_out, _resumo_script, style_info = processar_plan51(str(file_path), "TODOS", df_raw=df_raw)
    if df_out.empty:
        raise Plan51Recusado("Arquivo recusado: nenhuma linha de dados encontrada no PLAN 51.")
    t1 = datetime.now()

    sup, anu = _somar(df_out["VALOR SUPL"]), _somar(df_out["VALOR ANUL"])
    resumo = {"subtotal_suplementacao": sup, "subtotal_anulacao": anu, "subtotal_liquido": round(sup - anu, 2)}

    move_existing_to_tmp(OUTPUT_DIR)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = OUTPUT_DIR / f"plan51_{stamp}.xlsx"
    salvar_saida(
        df_out=df_out,
        resumo=resumo,
        style_info=style_info,
        caminho_saida=str(output_path),
        sheet_name=f"PLAN_51_TODOS_{stamp}",
        modo="TODOS",
        arquivo_processado=os.path.basename(str(file_path)),
        inicio_proc=t0.strftime("%d/%m/%Y %H:%M:%S"),
        fim_proc=t1.strftime("%d/%m/%Y %H:%M:%S"),
        duracao_proc=f"{(t1 - t0).total_seconds():.2f} segundos",
    )

    estilos = calcular_estilos(len(df_out), style_info)
    with _trava_exercicio(exercicio):
        total = update_database(df_out, exercicio, data_arquivo, user_email, upload_id, estilos)
    return total, output_path, conferir_totais(df_out, cab["totais"])
