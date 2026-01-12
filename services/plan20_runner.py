#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import csv
import re
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from openpyxl.styles import Font

# ----------------------------
# CONFIG / CONSTANTES
# ----------------------------
EXTR_HEADERS = [
    "Exercício",
    "Programa",
    "Função",
    "Unidade Orçamentária",
    "Ação (P/A/OE)",
    "Subfunção",
    "Objetivo Específico",
    "Esfera",
    "Responsável pela Ação",
    "Produto(s) da Ação",
    "Unidade de Medida do Produto",
    "Região do Produto",
    "Meta do Produto",
    "Saldo Meta do Produto",
    "Público Transversal",
    # Campos de G (Subação)
    "Subação/entrega",
    "Responsável",
    "Prazo",
    "Unid. Gestora",
    "Unidade Setorial de Planejamento",
    "Produto da Subação",
    "Unidade de Medida",
    "Região da Subação",
    "Código",
    "Município(s) da entrega",
    "Meta da Subação",
    "Detalhamento do produto",
    # Campos de H/I (Etapa / Região da Etapa / Itens de Despesa)
    "Etapa",
    "Responsável da Etapa",
    "Prazo da Etapa",
    "Região da Etapa",
    "Natureza",
    "Fonte",
    "IDU",
    "Descrição do Item de Despesa",
    "Unid. Medida",
    "Quantidade",
    "Valor Unitário",
    "Valor Total",
]

KEYS = {
    "A_exercicio": r"\bexercici?o\s*igual\s*a\b",
    "Programa": r"^programa\b",
    "Acao": r"\bacao\b.*\bp\s*a\s*o\s*e\b",
    "Produto": r"\bproduto\s*s?\s*da\s*acao\b|\bprodutos\s+da\s*acao\b",
    "PublicoTransversal": r"\bpublico\s*transversal\b",
    "PlanoPorProduto": r"\bplano\s*de\s*acao\s*por\s*produto\b",
    # G/H/I
    "SubacaoEntrega": r"^suba[cç][aã]o(?:\s*[/ ]?entrega)?\b",
    "Etapa": r"^etapa\b",
    "RegiaoPlanejamento": r"^regiao\s*de\s*planejamento\b|^regiao\s*planejamento\b",
}

NORMALIZA_MAP = {
    "á": "a",
    "â": "a",
    "ã": "a",
    "à": "a",
    "é": "e",
    "ê": "e",
    "í": "i",
    "ó": "o",
    "ô": "o",
    "õ": "o",
    "ú": "u",
    "ü": "u",
    "ç": "c",
}

DEBUG_ROWS: list[tuple[str, str, str]] = []


def dbg(local: str, msg: Any) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    DEBUG_ROWS.append((ts, local, str(msg)))


def normaliza(texto: Any) -> str:
    try:
        if pd.isna(texto):
            return ""
        s = str(texto).strip().lower()
        for k, v in NORMALIZA_MAP.items():
            s = s.replace(k, v)
        s = re.sub(r"[\[\]\(\)\{\}\,;:\"]", " ", s)
        s = re.sub(r"[-–—|/]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s
    except Exception:
        return ""


def linha_vazia(row: list[Any]) -> bool:
    for x in row:
        if not (pd.isna(x) or str(x).strip() == ""):
            return False
    return True


def acha(regex: str, row_norm: str) -> bool:
    return bool(re.search(regex, row_norm))


def is_linha_filtro_b(row_norm: str) -> bool:
    return row_norm.startswith("emitir relatorio")


def extrai_paoe(row_norm: str) -> str | None:
    m = re.search(r"p\s*a\s*o\s*e\s*[:\- ]+(\d+)", row_norm)
    if m:
        return m.group(1)
    if "acao" in row_norm and "p a o e" in row_norm:
        nums = re.findall(r"(\d+)", row_norm)
        if nums:
            return nums[-1]
    return None


def join_with_pipes_posicional(row: list[Any]) -> tuple[str, str, list[str]]:
    vals = [("" if pd.isna(x) else str(x)).strip() for x in row]
    return "||".join(vals), " ".join([v for v in vals if v]), vals


def processar_arquivo(caminho_arquivo: Path, a_contador_inicial: int = 1) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    dbg("processar_arquivo", f"inicio: {caminho_arquivo}")
    xls = pd.ExcelFile(caminho_arquivo)
    sheets_out: dict[str, pd.DataFrame] = {}

    # A é único por arquivo
    A_id = f"A{a_contador_inicial}"

    raw_rows: list[list[Any]] = []
    max_cols_raw = 0

    def extrai_chave_apos_doispontos(row_vals: list[Any]) -> str:
        for cel in row_vals:
            if pd.isna(cel):
                continue
            s = str(cel)
            if ":" in s:
                return s.split(":", 1)[1].strip()
        return " ".join(str(c).strip() for c in row_vals if not pd.isna(c) and str(c).strip()).strip()

    contador_B = 0  # B por aba

    for sheet_name in xls.sheet_names:
        contador_B += 1
        dbg("sheet", f"{sheet_name} (B{contador_B})")

        df = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=object)
        n, mcols = df.shape
        max_cols_raw = max(max_cols_raw, mcols)

        ident_col = [""] * n
        subid_col = [""] * n

        B_puro = f"B{contador_B}"
        B_id = f"{A_id}.{B_puro}"
        b_ativo = False

        # ---- Controle do "C base" (por programa) e do "C final" (por PAOE) ----
        contador_C = 0
        programas_vistos: dict[str, int] = {}

        C_base = None  # Ex.: A1.B1.C1 (base por programa)
        C_id = None  # Ex.: A1.B1.C1.2009 (por ação/PAOE)
        c_encerrado = True
        PAOE_num = None

        # Buffer para linhas C antes de aparecer a "Ação (P/A/OE)"
        c_pend_indices: list[int] = []
        c_pend_base = None
        c_pend_ativo = False

        cont_D = cont_E = cont_F = cont_G = cont_H = cont_I = 0
        cont_N = 0
        D_id = E_id = F_id = G_id = H_id = I_id = None
        N_id = None
        n_ativo = False
        sub_count: dict[str, int] = defaultdict(int)
        chave_G_atual = None
        chave_H_atual = None

        i = 0
        while i < n:
            row = df.iloc[i, :].tolist()
            _, _, vals = join_with_pipes_posicional(row)
            row_norm = normaliza(" ".join([("" if pd.isna(x) else str(x)) for x in row]))

            if linha_vazia(row):
                I_id = None
                H_id = None
                chave_H_atual = None
                n_ativo = False
                N_id = None
                b_ativo = False
                # encerra pendência de C (se houver)
                c_pend_indices = []
                c_pend_base = None
                c_pend_ativo = False
                i += 1
                continue

            if acha(KEYS["A_exercicio"], row_norm):
                b_ativo = True
                sub = sub_count.get(B_id, 0) + 1
                sub_count[B_id] = sub
                ident_col[i] = B_id
                subid_col[i] = str(sub)
                i += 1
                continue

            if b_ativo:
                if is_linha_filtro_b(row_norm):
                    i += 1
                    continue
                sub = sub_count.get(B_id, 0) + 1
                sub_count[B_id] = sub
                ident_col[i] = B_id
                subid_col[i] = str(sub)
                i += 1
                continue

            eh_programa = acha(KEYS["Programa"], row_norm)
            eh_acao = acha(KEYS["Acao"], row_norm)
            eh_produto = acha(KEYS["Produto"], row_norm)
            eh_publico = acha(KEYS["PublicoTransversal"], row_norm)
            eh_plano = acha(KEYS["PlanoPorProduto"], row_norm)
            eh_subacao = acha(KEYS["SubacaoEntrega"], row_norm)
            eh_etapa = acha(KEYS["Etapa"], row_norm)
            eh_regiao = acha(KEYS["RegiaoPlanejamento"], row_norm)

            if n_ativo and (eh_publico or eh_plano):
                n_ativo = False
                N_id = None

            # -------------------------
            # INÍCIO DO BLOCO C (Programa)
            # -------------------------
            if eh_programa:
                m_prog = re.search(r"^programa\s+(\d+)", row_norm)
                if m_prog:
                    prog = m_prog.group(1)
                    if prog not in programas_vistos:
                        contador_C += 1
                        programas_vistos[prog] = contador_C
                    cidx = programas_vistos[prog]
                    C_base = f"{A_id}.{B_puro}.C{cidx}"

                    # ativa pendência: vamos segurar as linhas do bloco C até achar a Ação
                    c_pend_ativo = True
                    c_pend_base = C_base
                    c_pend_indices = [i]

                    # reseta escopos abaixo de C
                    C_id = None
                    c_encerrado = False
                    D_id = E_id = F_id = G_id = H_id = I_id = None
                    N_id = None
                    n_ativo = False
                    cont_D = cont_E = cont_F = cont_G = cont_H = cont_I = 0
                    cont_N = 0
                    PAOE_num = None
                    chave_G_atual = None
                    chave_H_atual = None

                    i += 1
                    continue

            # -------------------------
            # SE ESTAMOS NO C PENDENTE e ainda NÃO achamos a AÇÃO,
            # vamos continuar coletando as linhas até chegar na "Ação (P/A/OE)".
            # -------------------------
            if c_pend_ativo and C_id is None:
                if eh_acao:
                    # cria o C_id definitivo com PAOE
                    paoe = extrai_paoe(row_norm)
                    if paoe:
                        PAOE_num = paoe
                    else:
                        nums = re.findall(r"(\d+)", row_norm)
                        PAOE_num = nums[-1] if nums else "0"

                    base = c_pend_base or C_base
                    if base is None:
                        contador_C += 1
                        base = f"{A_id}.{B_puro}.C{contador_C}"
                        C_base = base

                    C_id = f"{base}.{PAOE_num}"
                    c_encerrado = False

                    # atribui IDs/Sub-IDs em sequência para todas as linhas pendentes + a linha atual (Ação)
                    sub_count[C_id] = 0
                    for idx_p in c_pend_indices + [i]:
                        sub_count[C_id] += 1
                        ident_col[idx_p] = C_id
                        subid_col[idx_p] = str(sub_count[C_id])

                    # encerra pendência
                    c_pend_indices = []
                    c_pend_base = None
                    c_pend_ativo = False

                    # reseta escopos abaixo de C (mas mantém C_id)
                    D_id = E_id = F_id = G_id = H_id = I_id = None
                    N_id = None
                    n_ativo = False
                    cont_D = cont_E = cont_F = cont_G = cont_H = cont_I = 0
                    cont_N = 0
                    chave_G_atual = None
                    chave_H_atual = None

                    i += 1
                    continue
                else:
                    # continua coletando linhas do cabeçalho C (Função, UO, etc.)
                    c_pend_indices.append(i)
                    i += 1
                    continue

            # -------------------------
            # AÇÃO fora de pendência (fallback)
            # -------------------------
            if eh_acao:
                paoe = extrai_paoe(row_norm)
                if paoe:
                    PAOE_num = paoe
                else:
                    nums = re.findall(r"(\d+)", row_norm)
                    PAOE_num = nums[-1] if nums else "0"

                base = C_base
                if base is None:
                    contador_C += 1
                    base = f"{A_id}.{B_puro}.C{contador_C}"
                    C_base = base

                C_id = f"{base}.{PAOE_num}"
                c_encerrado = False

                # começa sub-id em 1 para este C_id
                sub = sub_count.get(C_id, 0) + 1
                sub_count[C_id] = sub
                ident_col[i] = C_id
                subid_col[i] = str(sub)

                # reseta escopos abaixo de C
                D_id = E_id = F_id = G_id = H_id = I_id = None
                N_id = None
                n_ativo = False
                cont_D = cont_E = cont_F = cont_G = cont_H = cont_I = 0
                cont_N = 0
                chave_G_atual = None
                chave_H_atual = None

                i += 1
                continue

            if eh_produto:
                if C_id is None:
                    # fallback: cria um C_id "genérico" se necessário
                    if C_base is None:
                        contador_C += 1
                        C_base = f"{A_id}.{B_puro}.C{contador_C}"
                    if PAOE_num is None:
                        PAOE_num = "0"
                    C_id = f"{C_base}.{PAOE_num}"
                    c_encerrado = False

                F_id = G_id = H_id = I_id = None
                cont_F = cont_G = cont_H = cont_I = 0
                chave_G_atual = None
                chave_H_atual = None
                cont_N = 0
                cont_D += 1
                # D agora não repete PAOE (pois o C já tem PAOE)
                D_id = f"{C_id}.D{cont_D}"
                c_encerrado = True
                sub = 1
                sub_count[D_id] = sub
                ident_col[i] = D_id
                subid_col[i] = str(sub)
                i += 1
                continue

            if "total por produto" in row_norm:
                n_ativo = True
                if C_id is None:
                    if C_base is None:
                        contador_C += 1
                        C_base = f"{A_id}.{B_puro}.C{contador_C}"
                    if PAOE_num is None:
                        PAOE_num = "0"
                    C_id = f"{C_base}.{PAOE_num}"
                    c_encerrado = False

                if D_id is None:
                    cont_D += 1
                    D_id = f"{C_id}.D{cont_D}"
                    c_encerrado = True
                    cont_N = 0
                cont_N += 1
                N_id = f"{D_id}.N{cont_N}"
                G_id = H_id = I_id = None
                cont_G = cont_H = cont_I = 0
                chave_G_atual = None
                chave_H_atual = None
                sub = 1
                sub_count[N_id] = sub
                ident_col[i] = N_id
                subid_col[i] = str(sub)
                i += 1
                continue

            if eh_publico:
                n_ativo = False
                N_id = None
                if D_id is None:
                    if C_id is None:
                        if C_base is None:
                            contador_C += 1
                            C_base = f"{A_id}.{B_puro}.C{contador_C}"
                        if PAOE_num is None:
                            PAOE_num = "0"
                        C_id = f"{C_base}.{PAOE_num}"
                        c_encerrado = False
                    cont_D += 1
                    D_id = f"{C_id}.D{cont_D}"
                    c_encerrado = True
                cont_E += 1
                E_id = f"{D_id}.E{cont_E}"
                F_id = G_id = H_id = I_id = None
                cont_F = cont_G = cont_H = cont_I = 0
                chave_G_atual = None
                chave_H_atual = None
                sub = 1
                sub_count[E_id] = sub
                ident_col[i] = E_id
                subid_col[i] = str(sub)
                i += 1
                continue

            if eh_plano:
                if D_id is None:
                    if C_id is None:
                        if C_base is None:
                            contador_C += 1
                            C_base = f"{A_id}.{B_puro}.C{contador_C}"
                        if PAOE_num is None:
                            PAOE_num = "0"
                        C_id = f"{C_base}.{PAOE_num}"
                        c_encerrado = False
                    cont_D += 1
                    D_id = f"{C_id}.D{cont_D}"
                    c_encerrado = True
                base_parent = E_id if E_id else D_id
                cont_F += 1
                F_id = f"{base_parent}.F{cont_F}"
                G_id = H_id = I_id = None
                cont_G = cont_H = cont_I = 0
                chave_G_atual = None
                chave_H_atual = None
                sub = 1
                sub_count[F_id] = sub
                ident_col[i] = F_id
                subid_col[i] = str(sub)
                i += 1
                continue

            if eh_subacao and F_id is not None:
                chave = extrai_chave_apos_doispontos(vals)
                if chave_G_atual is None or chave != chave_G_atual:
                    cont_G += 1
                    G_id = f"{F_id}.G{cont_G}"
                    chave_G_atual = chave
                    H_id = I_id = None
                    cont_H = cont_I = 0
                    chave_H_atual = None
                    sub = 1
                    sub_count[G_id] = sub
                else:
                    sub = sub_count.get(G_id, 0) + 1
                    sub_count[G_id] = sub
                ident_col[i] = G_id
                subid_col[i] = str(sub)
                i += 1
                continue

            if eh_etapa and (G_id is not None or F_id is not None):
                if G_id is None and F_id is not None:
                    cont_G += 1
                    G_id = f"{F_id}.G{cont_G}"
                    chave_G_atual = "<IMPLICITO>"
                    H_id = I_id = None
                    cont_H = cont_I = 0
                    chave_H_atual = None
                    sub_count[G_id] = 1
                chave = extrai_chave_apos_doispontos(vals)
                if chave_H_atual is None or chave != chave_H_atual:
                    cont_H += 1
                    H_id = f"{G_id}.H{cont_H}"
                    chave_H_atual = chave
                    I_id = None
                    cont_I = 0
                    sub = 1
                    sub_count[H_id] = sub
                else:
                    sub = sub_count.get(H_id, 0) + 1
                    sub_count[H_id] = sub
                ident_col[i] = H_id
                subid_col[i] = str(sub)
                i += 1
                continue

            if eh_regiao and (G_id is not None or F_id is not None):
                if H_id is None:
                    if G_id is None and F_id is not None:
                        cont_G += 1
                        G_id = f"{F_id}.G{cont_G}"
                        chave_G_atual = "<IMPLICITO>"
                        sub_count[G_id] = 1
                    if cont_H == 0:
                        cont_H = 1
                    H_id = f"{G_id}.H{cont_H}"
                    if chave_H_atual is None:
                        chave_H_atual = "<IMPLICITO>"
                    if H_id not in sub_count:
                        sub_count[H_id] = 1
                cont_I += 1
                I_id = f"{H_id}.I{cont_I}"
                sub = 1
                sub_count[I_id] = sub
                ident_col[i] = I_id
                subid_col[i] = str(sub)
                i += 1
                continue

            if n_ativo and N_id is not None:
                sub = sub_count.get(N_id, 0) + 1
                sub_count[N_id] = sub
                ident_col[i] = N_id
                subid_col[i] = str(sub)
                i += 1
                continue

            destino = None
            for cand in (I_id, H_id, G_id, F_id, E_id, D_id):
                if cand:
                    destino = cand
                    break
            if destino is None and C_id is not None and not c_encerrado:
                destino = C_id
            if destino:
                sub = sub_count.get(destino, 0) + 1
                sub_count[destino] = sub
                ident_col[i] = destino
                subid_col[i] = str(sub)
                i += 1
                continue

            i += 1

        for j in range(n):
            if ident_col[j]:
                row_vals = df.iloc[j, :].tolist()
                vals_str = [("" if pd.isna(x) else str(x)) for x in row_vals]
                raw_rows.append([ident_col[j], subid_col[j]] + vals_str)

        df_out = df.copy()
        df_out.insert(0, "Sub-Identificador", subid_col)
        df_out.insert(0, "Identificador", ident_col)
        sheets_out[sheet_name] = df_out

    cols_raw = ["id", "sub-id"] + [f"col_{i}" for i in range(1, max_cols_raw + 1)]
    ids_df_raw = pd.DataFrame(raw_rows, columns=cols_raw)
    dbg("processar_arquivo", "fim ok")
    return sheets_out, ids_df_raw

def _ab_from_id(c_id: str) -> str | None:
    m = re.match(r"(A\d+\.B\d+)", c_id)
    return m.group(1) if m else None


def _split_produto_unidade(texto: str) -> tuple[str, str]:
    s = (texto or "").strip()
    if not s:
        return "", ""

    if s.endswith("))"):
        first = s.find("(")
        last = s.rfind(")")
        if first != -1 and last != -1 and last > first:
            produto = s[:first].strip()
            unidade = s[first + 1 : last].strip()
            return produto, unidade

    start = s.rfind("(")
    end = s.rfind(")")
    if start != -1 and end != -1 and end > start:
        produto = s[:start].strip()
        unidade = s[start + 1 : end].strip()
        return produto, unidade

    return s, ""


def extrair_dados(ids_raw: pd.DataFrame) -> pd.DataFrame:
    if ids_raw.empty:
        return pd.DataFrame(columns=EXTR_HEADERS)

    # 1) Exercício por AB
    b_mask = ids_raw["id"].str.match(r"A\d+\.B\d+$")
    b_rows = ids_raw[b_mask].copy()
    b_rows["sub-id"] = b_rows["sub-id"].astype(str)
    exercicio_por_ab: dict[str, str] = {}
    for _, row in b_rows.iterrows():
        ab = row["id"]
        subid = row["sub-id"]
        if subid != "1":
            continue
        val = str(row.get("col_1", "")).strip()
        if not val:
            continue
        m = re.search(r"(\d{4})", val)
        if m:
            exercicio_por_ab[ab] = m.group(1)

    # 2) Campos por C (agora C tem PAOE no ID)
    c_mask = ids_raw["id"].str.match(r"A\d+\.B\d+\.C\d+\.\d+$")
    c_rows = ids_raw[c_mask].copy()
    c_rows["sub-id"] = c_rows["sub-id"].astype(str)
    c_rows = c_rows.reset_index().sort_values(["id", "index"])
    c_info: dict[str, dict[str, Any]] = {}
    current_c_id = None
    campos: dict[str, str] = {}
    acoes: dict[str, list[str]] = defaultdict(list)

    def flush_current_c() -> None:
        nonlocal campos, acoes, current_c_id
        if not current_c_id:
            return
        ab = _ab_from_id(current_c_id)
        c_info[current_c_id] = {
            "ab": ab,
            "campos": dict(campos),
            "acoes": {k: list(v) for k, v in acoes.items()},
        }
        campos.clear()
        acoes.clear()

    for _, row in c_rows.iterrows():
        cid = row["id"]
        if current_c_id is None:
            current_c_id = cid
        elif cid != current_c_id:
            flush_current_c()
            current_c_id = cid
        subid = row["sub-id"]
        if subid not in {"1", "2", "3", "4", "5", "6", "7", "8"}:
            continue
        col1 = str(row.get("col_1", "")).strip()
        col4 = str(row.get("col_4", "")).strip()
        val_or_rot = col4 if col4 else col1
        if subid == "1":
            campos["Programa"] = val_or_rot
        elif subid == "2":
            campos["Função"] = val_or_rot
        elif subid == "3":
            campos["Unidade Orçamentária"] = val_or_rot
        elif subid == "4":
            if val_or_rot:
                digits = re.findall(r"(\d+)", val_or_rot)
                paoe = None
                for d in digits:
                    if len(d) >= 3:
                        paoe = d
                        break
                if paoe is None and digits:
                    paoe = digits[-1]
                if paoe is None:
                    paoe = val_or_rot.strip()
                acoes[paoe].append(val_or_rot)
        elif subid == "5":
            campos["Subfunção"] = val_or_rot
        elif subid == "6":
            campos["Objetivo Específico"] = val_or_rot
        elif subid == "7":
            campos["Esfera"] = val_or_rot
        elif subid == "8":
            campos["Responsável pela Ação"] = val_or_rot
    flush_current_c()

    # 3) Produtos D (agora D está em A.B.Cx.PAOE.Dn)
    d_mask = ids_raw["id"].str.match(r"A\d+\.B\d+\.C\d+\.\d+\.D\d+$")
    d_rows = ids_raw[d_mask].copy()
    d_rows["sub-id"] = d_rows["sub-id"].astype(str)
    produtos_por_cid: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for _, row in d_rows.iterrows():
        if row["sub-id"] == "1":
            continue
        id_str = row["id"]
        m = re.match(r"(A\d+\.B\d+\.C\d+\.\d+)\.D(\d+)$", id_str)
        if not m:
            continue
        cid, d_idx_str = m.group(1), m.group(2)
        try:
            d_idx = int(d_idx_str)
        except Exception:
            d_idx = None
        col4 = str(row.get("col_4", "")).strip()
        col6 = str(row.get("col_6", "")).strip()
        col7 = str(row.get("col_7", "")).strip()
        col8 = str(row.get("col_8", "")).strip()
        prod, unidade = _split_produto_unidade(col4)
        if not (prod or unidade or col6 or col7 or col8):
            continue
        produtos_por_cid[cid].append(
            {
                "D_idx": d_idx,
                "Produto(s) da Ação": prod,
                "Unidade de Medida do Produto": unidade,
                "Região do Produto": col6,
                "Meta do Produto": col7,
                "Saldo Meta do Produto": col8,
                "_usado": False,
            }
        )

    # 4) Público Transversal E (chave por CID)
    e_mask = ids_raw["id"].str.match(r"A\d+\.B\d+\.C\d+\.\d+\.D\d+\.E\d+$")
    e_rows = ids_raw[e_mask].copy()
    e_rows["sub-id"] = e_rows["sub-id"].astype(str)
    publicos_por_cid: dict[str, list[str]] = defaultdict(list)
    for _, row in e_rows.iterrows():
        id_str = row["id"]
        m = re.match(r"(A\d+\.B\d+\.C\d+\.\d+)\.D\d+\.E\d+$", id_str)
        if not m:
            continue
        cid = m.group(1)
        val = str(row.get("col_4", "")).strip()
        if not val:
            continue
        lista = publicos_por_cid[cid]
        if val not in lista:
            lista.append(val)

    # 4.1) Produto do F
    f_mask = ids_raw["id"].str.match(r"A\d+\.B\d+\.C\d+\.\d+\.D\d+(?:\.E\d+)?\.F\d+$")
    f_rows = ids_raw[f_mask].copy()
    f_rows["sub-id"] = f_rows["sub-id"].astype(str)
    f_rows = f_rows.reset_index().sort_values(["id", "index"])
    produto_por_fid: dict[str, str] = {}

    def _produto_limpo(s: str) -> str:
        s = (s or "").strip()
        if not s:
            return ""
        s = re.sub(r'^\s*produto\(s\)?:\s*', '', s, flags=re.IGNORECASE)
        return s

    for _, row in f_rows.iterrows():
        fid = row["id"]
        if fid in produto_por_fid:
            continue
        c5 = str(row.get("col_5", "")).strip()
        if c5:
            produto_por_fid[fid] = _produto_limpo(c5)
    for fid in f_rows["id"].unique():
        if fid not in produto_por_fid:
            produto_por_fid[fid] = "Produto exclusivo para ação padronizada"

    # 5) Subações G (chave por CID = A.B.Cx.PAOE)
    g_mask = ids_raw["id"].str.match(r"A\d+\.B\d+\.C\d+\.\d+\.D\d+(?:\.E\d+)?\.F\d+\.G\d+$")
    g_rows = ids_raw[g_mask].copy()
    g_rows["sub-id"] = g_rows["sub-id"].astype(str)
    g_rows = g_rows.reset_index().sort_values(["id", "index"])

    subacoes_por_cid: dict[str, list[dict[str, Any]]] = defaultdict(list)
    current_gid = None
    subacao_info: dict[str, Any] | None = None
    linhas_regiao: list[dict[str, Any]] = []

    def fechar_subacao() -> None:
        nonlocal subacao_info, linhas_regiao
        if not subacao_info:
            return
        cid_local = subacao_info["_cid"]
        if linhas_regiao:
            for reg in linhas_regiao:
                d = dict(subacao_info)
                d.update(reg)
                subacoes_por_cid[cid_local].append(d)
        else:
            d = dict(subacao_info)
            subacoes_por_cid[cid_local].append(d)
        subacao_info = None
        linhas_regiao = []

    def nova_subacao(gid: str, fid: str, cid: str, d_idx: int | None, produto_F: str) -> dict[str, Any]:
        paoe_local = cid.split(".")[-1] if cid else ""
        return {
            "_gid": gid,
            "_fid": fid,
            "_cid": cid,
            "_paoe": paoe_local,
            "_d_idx": d_idx,
            "_produto_F": produto_F,
            "Subação/entrega": "",
            "Responsável": "",
            "Prazo": "",
            "Unid. Gestora": "",
            "Unidade Setorial de Planejamento": "",
            "Produto da Subação": "",
            "Unidade de Medida": "",
            "Detalhamento do produto": "",
        }

    for _, row in g_rows.iterrows():
        gid = row["id"]
        subid = row["sub-id"]

        m = re.match(r"((A\d+\.B\d+\.C\d+\.\d+)\.(D\d+)(?:\.E\d+)?\.F\d+)\.G\d+$", gid)
        if not m:
            continue
        fid = m.group(1)
        cid = m.group(2)
        d_token = m.group(3)
        try:
            d_idx = int(d_token[1:])
        except Exception:
            d_idx = None

        produto_F = produto_por_fid.get(fid, "Produto exclusivo para ação padronizada")

        if current_gid is None or gid != current_gid:
            if current_gid is not None:
                fechar_subacao()
            current_gid = gid
            subacao_info = nova_subacao(gid, fid, cid, d_idx, produto_F)

        if subacao_info is None:
            subacao_info = nova_subacao(gid, fid, cid, d_idx, produto_F)

        c1 = str(row.get("col_1", "")).strip()
        c2 = str(row.get("col_2", "")).strip()
        c4 = str(row.get("col_4", "")).strip()
        c5 = str(row.get("col_5", "")).strip()
        c7 = str(row.get("col_7", "")).strip()

        if subid == "1":
            subacao_info["Subação/entrega"] = c1.split(":", 1)[1].strip() if ":" in c1 else c1.strip()

        elif subid == "2":
            subacao_info["Responsável"] = c1.split(":", 1)[1].strip() if ":" in c1 else c1.strip()
            if "Prazo" in c5:
                pr = c5.split("Prazo", 1)[1].strip(": ").strip()
                subacao_info["Prazo"] = pr

        elif subid == "3":
            subacao_info["Unid. Gestora"] = c1.split(":", 1)[1].strip() if ":" in c1 else c1.strip()
            subacao_info["Unidade Setorial de Planejamento"] = c4.split(":", 1)[1].strip() if ":" in c4 else c4.strip()
            if ":" in c5:
                subacao_info["Produto da Subação"] = c5.split(":", 1)[1].strip()
            if ":" in c7:
                subacao_info["Unidade de Medida"] = c7.split(":", 1)[1].strip()

        elif subid == "4":
            pass

        elif subid.isdigit() and int(subid) >= 5:
            if c1.lower().startswith("detalhamento do produto"):
                det = c1.split(":", 1)[1].strip() if ":" in c1 else c1.strip()
                subacao_info["Detalhamento do produto"] = det
                fechar_subacao()
            else:
                if any([c2, c4, c5, c7]):
                    linhas_regiao.append(
                        {
                            "Região da Subação": c2,
                            "Código": c4,
                            "Município(s) da entrega": c5,
                            "Meta da Subação": c7,
                        }
                    )

    fechar_subacao()

    # H: Etapa
    h_mask = ids_raw["id"].str.match(r".*\.F\d+\.G\d+\.H\d+$")
    h_rows = ids_raw[h_mask].copy()
    h_rows["sub-id"] = h_rows["sub-id"].astype(str)
    h_rows = h_rows.reset_index().sort_values(["id", "index"])

    etapas_por_gid: dict[str, list[dict[str, Any]]] = defaultdict(list)
    current_hid = None
    h_info: dict[str, Any] | None = None

    def _coletar_texto_h(row: pd.Series) -> str:
        parts = []
        for k in range(1, 9):
            parts.append(str(row.get(f"col_{k}", "")).strip())
        return " ".join([p for p in parts if p])

    for _, row in h_rows.iterrows():
        hid = row["id"]
        subid = row["sub-id"]
        m = re.match(r"(.*\.F\d+\.G\d+)\.H\d+$", hid)
        if not m:
            continue
        gid = m.group(1)

        if current_hid is None or hid != current_hid:
            if h_info:
                etapas_por_gid[h_info["_gid"]].append(h_info)
            current_hid = hid
            h_info = {
                "_hid": hid,
                "_gid": gid,
                "Etapa": "",
                "Responsável da Etapa": "",
                "Prazo da Etapa": "",
                "_texto_busca": "",
            }

        c_all = {f"col_{k}": str(row.get(f"col_{k}", "")).strip() for k in range(1, 9)}

        if subid == "1":
            h_info["Etapa"] = c_all.get("col_4", "").strip()
            h_info["_texto_busca"] += " " + _coletar_texto_h(row)
        elif subid == "2":
            h_info["Responsável da Etapa"] = c_all.get("col_3", "").strip()
            prazo = c_all.get("col_6", "").strip()
            if ":" in prazo:
                prazo = prazo.split(":", 1)[1].strip()
            h_info["Prazo da Etapa"] = prazo
            h_info["_texto_busca"] += " " + _coletar_texto_h(row)
        else:
            h_info["_texto_busca"] += " " + _coletar_texto_h(row)

    if h_info:
        etapas_por_gid[h_info["_gid"]].append(h_info)

    # I: Região da Etapa + Itens
    i_mask = ids_raw["id"].str.match(r".*\.F\d+\.G\d+\.H\d+\.I\d+$")
    i_rows = ids_raw[i_mask].copy()
    i_rows["sub-id"] = i_rows["sub-id"].astype(str)
    i_rows = i_rows.reset_index().sort_values(["id", "index"])

    itens_por_hid: dict[str, list[dict[str, Any]]] = defaultdict(list)
    regiao_por_hid: dict[str, list[str]] = defaultdict(list)

    current_iid = None
    regiao_etapa_atual = ""

    for _, row in i_rows.iterrows():
        iid = row["id"]
        subid = row["sub-id"]
        m = re.match(r"(.*\.H\d+)\.I\d+$", iid)
        if not m:
            continue
        hid = m.group(1)
        if current_iid is None or iid != current_iid:
            current_iid = iid
            regiao_etapa_atual = ""

        c = {f"col_{k}": str(row.get(f"col_{k}", "")).strip() for k in range(1, 9)}

        if subid == "1":
            regiao_etapa_atual = c.get("col_4", "").strip()
            regiao_por_hid[hid].append(regiao_etapa_atual)
        elif subid == "2":
            pass
        else:
            if any([c.get(f"col_{k}", "") for k in range(1, 9)]):
                itens_por_hid[hid].append(
                    {
                        "Região da Etapa": regiao_etapa_atual,
                        "Natureza": c.get("col_1", ""),
                        "Fonte": c.get("col_2", ""),
                        "IDU": c.get("col_3", ""),
                        "Descrição do Item de Despesa": c.get("col_4", ""),
                        "Unid. Medida": c.get("col_5", ""),
                        "Quantidade": c.get("col_6", ""),
                        "Valor Unitário": c.get("col_7", ""),
                        "Valor Total": c.get("col_8", ""),
                    }
                )

    # --------- MONTAGEM BASE (produtos + G) ---------

    resultados_base: list[dict[str, Any]] = []

    def _meta_nao_zero(v: str) -> bool:
        s = (v or "").strip()
        if s in {"", "0", "0,0", "0,00", "0.0", "0.00"}:
            return False
        try:
            return float(s.replace(".", "").replace(",", ".")) != 0.0
        except Exception:
            return True

    def _reg_num(s: str) -> str:
        s_str = str(s or "")
        m = re.search(r"(\d{4})", s_str)
        return m.group(1) if m else ""

    def _concat_campos_g(lista: list[dict[str, Any]]) -> tuple[str, str, str]:
        cods, munis, metas = [], [], []
        for r in lista:
            c = (r.get("Código", "") or "").strip()
            m = (r.get("Município(s) da entrega", "") or "").strip()
            mt = (r.get("Meta da Subação", "") or "").strip()
            if c:
                cods.append(c)
            if m:
                munis.append(m)
            if mt:
                metas.append(mt)
        return " * ".join(cods), " * ".join(munis), " * ".join(metas)

    for cid, info in c_info.items():
        ab = info["ab"]
        campos = info["campos"]
        acoes_dict = info["acoes"]
        exercicio = exercicio_por_ab.get(ab, "")

        for paoe, lista_textos in acoes_dict.items():
            ac_texto = lista_textos[0] if lista_textos else ""
            produtos = produtos_por_cid.get(cid, [])
            publicos = publicos_por_cid.get(cid, [])
            publico_str = " * ".join(publicos) if publicos else ""

            def _base_from_d(d_escolhido: dict[str, Any] | None) -> dict[str, Any]:
                base = {
                    "Exercício": exercicio,
                    "Programa": campos.get("Programa", ""),
                    "Função": campos.get("Função", ""),
                    "Unidade Orçamentária": campos.get("Unidade Orçamentária", ""),
                    "Ação (P/A/OE)": ac_texto,
                    "Subfunção": campos.get("Subfunção", ""),
                    "Objetivo Específico": campos.get("Objetivo Específico", ""),
                    "Esfera": campos.get("Esfera", ""),
                    "Responsável pela Ação": campos.get("Responsável pela Ação", ""),
                    "Público Transversal": publico_str,
                }
                if d_escolhido is None:
                    base.update(
                        {
                            "Produto(s) da Ação": "",
                            "Unidade de Medida do Produto": "",
                            "Região do Produto": "",
                            "Meta do Produto": "",
                            "Saldo Meta do Produto": "",
                        }
                    )
                else:
                    base.update(
                        {
                            "Produto(s) da Ação": d_escolhido["Produto(s) da Ação"],
                            "Unidade de Medida do Produto": d_escolhido["Unidade de Medida do Produto"],
                            "Região do Produto": d_escolhido["Região do Produto"],
                            "Meta do Produto": d_escolhido["Meta do Produto"],
                            "Saldo Meta do Produto": d_escolhido["Saldo Meta do Produto"],
                        }
                    )
                return base

            subacoes = subacoes_por_cid.get(cid, [])
            if subacoes:
                for sa in subacoes:
                    prod_F_norm = normaliza(sa.get("_produto_F", ""))
                    reg_sub = (sa.get("Região da Subação", "") or "").strip()

                    d_candidatos = produtos

                    d_idx_sa = sa.get("_d_idx")
                    if d_idx_sa is not None and d_candidatos:
                        d_filtrados = [p for p in d_candidatos if p.get("D_idx") == d_idx_sa]
                        if d_filtrados:
                            d_candidatos = d_filtrados

                    if prod_F_norm and d_candidatos:
                        d_filtrados = [p for p in d_candidatos if normaliza(p["Produto(s) da Ação"]) == prod_F_norm]
                        if d_filtrados:
                            d_candidatos = d_filtrados

                    d_escolhido = None
                    if reg_sub and d_candidatos:
                        for p in d_candidatos:
                            if (p.get("Região do Produto", "") or "").strip() == reg_sub:
                                d_escolhido = p
                                break
                    if d_escolhido is None and d_candidatos:
                        for p in d_candidatos:
                            if _meta_nao_zero(p.get("Meta do Produto", "")):
                                d_escolhido = p
                                break
                    if d_escolhido is None and d_candidatos:
                        d_escolhido = d_candidatos[0]

                    if d_escolhido is not None:
                        d_escolhido["_usado"] = True

                    linha = _base_from_d(d_escolhido)
                    if d_escolhido is not None and reg_sub:
                        reg_d = (d_escolhido.get("Região do Produto", "") or "").strip()
                        if reg_d != reg_sub:
                            linha["Região do Produto"] = f"{reg_d} (Região da Subação divergente: {reg_sub})"

                    linha.update(
                        {
                            "Subação/entrega": sa.get("Subação/entrega", ""),
                            "Responsável": sa.get("Responsável", ""),
                            "Prazo": sa.get("Prazo", ""),
                            "Unid. Gestora": sa.get("Unid. Gestora", ""),
                            "Unidade Setorial de Planejamento": sa.get("Unidade Setorial de Planejamento", ""),
                            "Produto da Subação": sa.get("Produto da Subação", ""),
                            "Unidade de Medida": sa.get("Unidade de Medida", ""),
                            "Região da Subação": sa.get("Região da Subação", ""),
                            "Código": sa.get("Código", ""),
                            "Município(s) da entrega": sa.get("Município(s) da entrega", ""),
                            "Meta da Subação": sa.get("Meta da Subação", ""),
                            "Detalhamento do produto": sa.get("Detalhamento do produto", ""),
                        }
                    )
                    linha["_cid"] = cid
                    linha["_paoe"] = paoe
                    linha["_gid"] = sa.get("_gid")
                    resultados_base.append(linha)

                if produtos:
                    for p in produtos:
                        if not p.get("_usado"):
                            linha = _base_from_d(p)
                            linha["_cid"] = cid
                            linha["_paoe"] = paoe
                            linha["_gid"] = None
                            resultados_base.append(linha)

            else:
                if produtos:
                    for p in produtos:
                        p["_usado"] = True
                        linha = _base_from_d(p)
                        linha["_cid"] = cid
                        linha["_paoe"] = paoe
                        linha["_gid"] = None
                        resultados_base.append(linha)
                else:
                    linha = _base_from_d(None)
                    linha["_cid"] = cid
                    linha["_paoe"] = paoe
                    linha["_gid"] = None
                    resultados_base.append(linha)

    if not resultados_base:
        return pd.DataFrame(columns=EXTR_HEADERS)

    # --------- ENRIQUECIMENTO COM H/I ---------

    indices_por_gid: dict[str, list[int]] = defaultdict(list)
    for idx, r in enumerate(resultados_base):
        gid = r.get("_gid")
        if gid:
            indices_por_gid[gid].append(idx)

    finais: list[dict[str, Any]] = []

    def _split_municipios(s: str) -> list[str]:
        s = s or ""
        parts = re.split(r"[;,*]+", s)
        return [p.strip() for p in parts if p.strip()]

    def _codes_from_str(s: str) -> set[str]:
        s = s or ""
        return set(re.findall(r"\b\d{6,8}\b", s))

    for gid, idx_list in indices_por_gid.items():
        linhas_gid = [resultados_base[i] for i in idx_list]

        grupos_por_reg: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in linhas_gid:
            reg_sub_raw = (r.get("Região da Subação", "") or "").strip()
            reg_sub_key = _reg_num(reg_sub_raw)
            grupos_por_reg[reg_sub_key].append(r)

        etapas = etapas_por_gid.get(gid, [])
        if not etapas:
            for r in linhas_gid:
                finais.append(dict(r))
            continue

        for h in etapas:
            hid = h["_hid"]
            texto_h_raw = h.get("_texto_busca", "") or ""
            texto_h_norm = normaliza(texto_h_raw)
            codes_h = _codes_from_str(texto_h_raw)

            itens_i = itens_por_hid.get(hid, [])

            if not itens_i:
                regs_h = regiao_por_hid.get(hid, []) or [""]
                for reg_h in regs_h:
                    reg_h_str = (reg_h or "").strip()
                    reg_h_key = _reg_num(reg_h_str)
                    alvo = grupos_por_reg.get(reg_h_key, [])

                    if not alvo and grupos_por_reg:
                        first_reg_key, alvo = next(iter(grupos_por_reg.items()))
                        base = dict(alvo[0])
                        cods_str, munis_str, metas_str = _concat_campos_g(alvo)
                        base["Código"] = cods_str
                        base["Município(s) da entrega"] = munis_str
                        base["Meta da Subação"] = metas_str

                        if _reg_num(base.get("Região da Subação", "")) != reg_h_key and reg_h_key:
                            base["Região da Subação"] = (
                                f"{(base.get('Região da Subação', '') or '').strip()} "
                                f"(Região da Etapa divergente: {reg_h_key})"
                            )

                        base["Etapa"] = h.get("Etapa", "")
                        base["Responsável da Etapa"] = h.get("Responsável da Etapa", "")
                        base["Prazo da Etapa"] = h.get("Prazo da Etapa", "")
                        base["Região da Etapa"] = reg_h_str
                        finais.append(base)
                        continue

                    if alvo:
                        base = dict(alvo[0])
                        cods_str, munis_str, metas_str = _concat_campos_g(alvo)
                        base["Código"] = cods_str
                        base["Município(s) da entrega"] = munis_str
                        base["Meta da Subação"] = metas_str

                        base["Etapa"] = h.get("Etapa", "")
                        base["Responsável da Etapa"] = h.get("Responsável da Etapa", "")
                        base["Prazo da Etapa"] = h.get("Prazo da Etapa", "")
                        base["Região da Etapa"] = reg_h_str
                        finais.append(base)
                        continue

            for item in itens_i:
                reg_h = (item.get("Região da Etapa", "") or "").strip()
                reg_h_key = _reg_num(reg_h)
                candidatos = grupos_por_reg.get(reg_h_key, [])

                if candidatos:
                    cand_by_muni = []
                    for r in candidatos:
                        munis_g = _split_municipios(r.get("Município(s) da entrega", ""))
                        hit = False
                        for mg in munis_g:
                            mg_norm = normaliza(mg)
                            if mg_norm and mg_norm in texto_h_norm:
                                hit = True
                                break
                        if hit:
                            cand_by_muni.append(r)
                    if cand_by_muni:
                        candidatos = cand_by_muni
                    else:
                        if codes_h:
                            cand_by_code = []
                            for r in candidatos:
                                codes_g = _codes_from_str(r.get("Código", ""))
                                if codes_h.intersection(codes_g):
                                    cand_by_code.append(r)
                            if cand_by_code:
                                candidatos = cand_by_code

                if candidatos:
                    base = dict(candidatos[0])
                    cods_str, munis_str, metas_str = _concat_campos_g(candidatos)
                    base["Código"] = cods_str
                    base["Município(s) da entrega"] = munis_str
                    base["Meta da Subação"] = metas_str
                else:
                    if grupos_por_reg:
                        reg_escolhida_key, lst = next(iter(grupos_por_reg.items()))
                        base = dict(lst[0])
                        cods_str, munis_str, metas_str = _concat_campos_g(lst)
                        base["Código"] = cods_str
                        base["Município(s) da entrega"] = munis_str
                        base["Meta da Subação"] = metas_str

                        if _reg_num(base.get("Região da Subação", "")) != reg_h_key and reg_h_key:
                            base["Região da Subação"] = (
                                f"{(base.get('Região da Subação', '') or '').strip()} "
                                f"(Região da Etapa divergente: {reg_h_key})"
                            )
                    else:
                        base = dict(linhas_gid[0])

                base["Etapa"] = h.get("Etapa", "")
                base["Responsável da Etapa"] = h.get("Responsável da Etapa", "")
                base["Prazo da Etapa"] = h.get("Prazo da Etapa", "")
                base["Região da Etapa"] = reg_h
                base["Natureza"] = item.get("Natureza", "")
                base["Fonte"] = item.get("Fonte", "")
                base["IDU"] = item.get("IDU", "")
                base["Descrição do Item de Despesa"] = item.get("Descrição do Item de Despesa", "")
                base["Unid. Medida"] = item.get("Unid. Medida", "")
                base["Quantidade"] = item.get("Quantidade", "")
                base["Valor Unitário"] = item.get("Valor Unitário", "")
                base["Valor Total"] = item.get("Valor Total", "")
                finais.append(base)

    for r in resultados_base:
        if r.get("_gid") is None:
            finais.append(dict(r))

    for r in finais:
        for k in list(r.keys()):
            if k.startswith("_"):
                del r[k]

    extr_df = pd.DataFrame(finais, columns=EXTR_HEADERS)

    # ----------------------------------------
    # PÓS-REGRA: preencher vazios padrão
    # ----------------------------------------
    cols_to_clean = [
        "Produto(s) da Ação",
        "Unidade de Medida do Produto",
        "Meta do Produto",
        "Saldo Meta do Produto",
        "Público Transversal",
        "Código",
        "Município(s) da entrega",
        "Detalhamento do produto",
        "Meta da Subação",
        "Etapa",
        "Responsável da Etapa",
        "Prazo da Etapa",
        "Região da Etapa",
        "Natureza",
        "Fonte",
        "IDU",
        "Descrição do Item de Despesa",
        "Unid. Medida",
        "Quantidade",
        "Valor Unitário",
        "Valor Total",
    ]
    for col in cols_to_clean:
        if col not in extr_df.columns:
            extr_df[col] = pd.NA
        extr_df[col] = extr_df[col].replace({"nan": pd.NA, "<NA>": pd.NA}).replace(r"^\s*$", pd.NA, regex=True)

    defaults_text = {
        "Produto(s) da Ação": "Produto exclusivo para ação padronizada",
        "Unidade de Medida do Produto": "Percentual",
        "Meta do Produto": "100,00",
        "Saldo Meta do Produto": "0.0",
        "Público Transversal": "-",
        "Código": "-",
        "Município(s) da entrega": "-",
        "Detalhamento do produto": "-",
        "Meta da Subação": "-",
        "Etapa": "-",
        "Responsável da Etapa": "-",
        "Prazo da Etapa": "-",
        "Região da Etapa": "-",
        "Natureza": "0.0.00.00.000",
        "Fonte": "-",
        "IDU": "-",
        "Descrição do Item de Despesa": "-",
        "Unid. Medida": "-",
    }

    for col, default_val in defaults_text.items():
        if col in extr_df.columns:
            extr_df[col] = extr_df[col].fillna(default_val)

    for col in ["Quantidade", "Valor Unitário", "Valor Total"]:
        extr_df[col] = extr_df[col].fillna("0,00")

    g_text_cols = [
        "Subação/entrega",
        "Responsável",
        "Prazo",
        "Unid. Gestora",
        "Unidade Setorial de Planejamento",
        "Produto da Subação",
        "Unidade de Medida",
        "Região da Subação",
    ]
    for col in g_text_cols:
        if col in extr_df.columns:
            extr_df[col] = (
                extr_df[col]
                .astype(str)
                .replace({"nan": pd.NA})
                .replace(r"^\s*$", pd.NA, regex=True)
                .fillna("-")
            )

    return extr_df

def salvar_debug_csv(caminho: Path) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["timestamp", "local", "mensagem"])
        for ts, loc, msg in DEBUG_ROWS:
            writer.writerow([ts, loc, msg])


def debug_df() -> pd.DataFrame:
    return pd.DataFrame(DEBUG_ROWS, columns=["timestamp", "local", "mensagem"])


# -------------------------
# ABA Plan20_SEDUC helpers
# -------------------------

def extrai_chave_planejamento(subacao: Any) -> str:
    if pd.isna(subacao):
        return "-"
    s = str(subacao).replace("\xa0", " ")
    m = re.search(r"-\s*(\*.*\*)", s)
    if not m:
        return "-"
    chave = m.group(1)
    chave = re.sub(r"\s+", " ", chave)
    return chave.strip()


def _explode_chave(ch: Any) -> pd.Series:
    if pd.isna(ch):
        partes: list[str] = []
    else:
        s = str(ch).replace("\xa0", " ")
        partes = [p.strip() for p in s.split("*") if p.strip()]
    while len(partes) < 8:
        partes.append("-")
    return pd.Series(
        {
            "Região": partes[0],
            "Subfunção + UG": partes[1],
            "ADJ": partes[2],
            "Macropolitica": partes[3],
            "Pilar": partes[4],
            "Eixo": partes[5],
            "Politica_Decreto": partes[6],
            "Público Transversal (chave)": partes[7],
        }
    )


def _explode_natureza(s: Any) -> pd.Series:
    s_str = str(s or "").strip()
    raw = s_str.split(".") if s_str else []
    raw = raw + [""] * 5
    parts = raw[:5]
    return pd.Series(
        {
            "Cat.Econ": parts[0],
            "Grupo": parts[1],
            "Modalidade": parts[2],
            "Elemento": parts[3],
            "Subelemento": parts[4],
        }
    )


def run_plan20(input_file: Path, output_dir: Path) -> Path:
    """
    Processa um único arquivo .xlsx do Plan20 com as mesmas regras do script legado,
    gerando as abas Identificadores_Raw, Extrair_dados, Plan20_SEDUC e Debug_Log.
    """
    DEBUG_ROWS.clear()
    output_dir.mkdir(parents=True, exist_ok=True)

    arquivos = [Path(input_file)]

    todos_ids_raw: list[pd.DataFrame] = []
    todos_extr_df: list[pd.DataFrame] = []

    contador_A_global = 1  # BLOCO A é único por arquivo

    for arquivo in arquivos:
        if not arquivo.is_file():
            continue
        dbg("main", f"Arquivo de entrada: {arquivo}")
        t0 = time.perf_counter()

        _, ids_df_raw = processar_arquivo(arquivo, a_contador_inicial=contador_A_global)
        dbg("main", f"processar_arquivo concluído para: {arquivo.name}")
        dbg("main", f"ids_df_raw linhas ({arquivo.name}): {len(ids_df_raw)}")

        extr_df = extrair_dados(ids_df_raw)
        dbg("main", f"Extrair_dados linhas ({arquivo.name}): {len(extr_df)}")

        if not ids_df_raw.empty:
            todos_ids_raw.append(ids_df_raw)
        if not extr_df.empty:
            todos_extr_df.append(extr_df)

        t1 = time.perf_counter()
        dbg("main", f"Tempo total ({arquivo.name}): {t1 - t0:.3f}s")
        contador_A_global += 1

    ids_df_all = (
        pd.concat(todos_ids_raw, ignore_index=True)
        if todos_ids_raw
        else pd.DataFrame(columns=["id", "sub-id"])
    )
    extr_df_all = (
        pd.concat(todos_extr_df, ignore_index=True)
        if todos_extr_df
        else pd.DataFrame(columns=EXTR_HEADERS)
    )

    # -------- Plan20_SEDUC --------
    if not extr_df_all.empty:
        df_tmp = extr_df_all.copy()
        exercicio_num = pd.to_numeric(df_tmp["Exercício"], errors="coerce")

        mask_uo = (
            df_tmp["Unidade Orçamentária"]
            .astype(str)
            .str.strip()
            == "14.101 - SECRETARIA DE ESTADO DE EDUCAÇÃO"
        )
        mask_exercicio = exercicio_num >= 2025
        plan20_seduc_df = df_tmp[mask_uo & mask_exercicio].copy()
        dbg("Plan20_SEDUC", f"Linhas filtradas (UO+Exercício): {len(plan20_seduc_df)}")

        if not plan20_seduc_df.empty:
            plan20_seduc_df["Chave de Planejamento"] = plan20_seduc_df["Subação/entrega"].apply(
                extrai_chave_planejamento
            )

            chave_cols = plan20_seduc_df["Chave de Planejamento"].apply(_explode_chave)
            plan20_seduc_df = pd.concat([plan20_seduc_df, chave_cols], axis=1)

            bloco_chave = [
                "Chave de Planejamento",
                "Região",
                "Subfunção + UG",
                "ADJ",
                "Macropolitica",
                "Pilar",
                "Eixo",
                "Politica_Decreto",
                "Público Transversal (chave)",
            ]

            cols = [c for c in plan20_seduc_df.columns if c not in bloco_chave]
            if "Exercício" in cols:
                idx = cols.index("Exercício")
                cols = cols[: idx + 1] + bloco_chave + cols[idx + 1 :]
            else:
                cols = bloco_chave + cols
            plan20_seduc_df = plan20_seduc_df[cols]

            if "Natureza" in plan20_seduc_df.columns:
                nat_cols = plan20_seduc_df["Natureza"].apply(_explode_natureza)
                plan20_seduc_df = pd.concat([plan20_seduc_df, nat_cols], axis=1)

                cols2 = list(plan20_seduc_df.columns)
                idx_nat = cols2.index("Natureza")
                novas = ["Cat.Econ", "Grupo", "Modalidade", "Elemento", "Subelemento"]
                for c in novas:
                    if c in cols2:
                        cols2.remove(c)
                cols2 = cols2[: idx_nat + 1] + novas + cols2[idx_nat + 1 :]
                plan20_seduc_df = plan20_seduc_df[cols2]
        else:
            plan20_seduc_df = pd.DataFrame(columns=EXTR_HEADERS)
    else:
        plan20_seduc_df = pd.DataFrame(columns=EXTR_HEADERS)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"plan20_seduc_{ts}.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        ids_df_all.to_excel(writer, sheet_name="Identificadores_Raw", index=False)
        extr_df_all.to_excel(writer, sheet_name="Extrair_dados", index=False)
        plan20_seduc_df.to_excel(writer, sheet_name="Plan20_SEDUC", index=False)
        debug_df().to_excel(writer, sheet_name="Debug_Log", index=False)

        wb = writer.book
        fonte_padrao = Font(name="Helvetica", size=8)
        for ws in wb.worksheets:
            for row in ws.iter_rows():
                for cell in row:
                    cell.font = fonte_padrao

    try:
        salvar_debug_csv(output_dir / "plan20_debug.csv")
    except Exception:
        pass

    return out_path
