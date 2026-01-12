from __future__ import annotations

import json
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text, event
from sqlalchemy.exc import SQLAlchemyError

from models import db

BATCH_SIZE = 1000
INPUT_DIR = Path("upload/est_emp")
OUTPUT_DIR = Path("outputs/td_est_emp")
HEADER_INICIO = ["exercicio", "n_est", "n_emp", "n_ped", "historico"]

_FAST_EXEC_ENABLED = False

COL_MAP = {
    "exercicio": "exercicio",
    "n_est": "numero_est",
    "no_est": "numero_est",
    "numero_est": "numero_est",
    "n_emp": "numero_emp",
    "no_emp": "numero_emp",
    "numero_emp": "numero_emp",
    "n_ped": "numero_ped",
    "no_ped": "numero_ped",
    "numero_ped": "numero_ped",
    "rp": "rp",
    "situacao": "situacao",
    "historico": "historico",
    "valor_emp": "valor_emp",
    "valor_est_emp_a_liq_em_liq_sem_aqs": "valor_est_emp_sem_aqs",
    "valor_est_emp_em_liq_com_aqs": "valor_est_emp_com_aqs",
    "valor_emp_a_liq_em_liq_sem_aqs_em_liq_com_aqs": "valor_emp_liquido",
    "empenho_atual": "empenho_atual",
    "empenho_rp": "empenho_rp",
    "ug": "ug",
    "uo": "uo",
    "nome_da_unidade_orcamentaria": "nome_unidade_orcamentaria",
    "nome_unidade_orcamentaria": "nome_unidade_orcamentaria",
    "nome_da_unidade_gestora": "nome_unidade_gestora",
    "nome_unidade_gestora": "nome_unidade_gestora",
    "dotacao_orcamentaria": "dotacao_orcamentaria",
    "credor": "credor",
    "nome_do_credor": "nome_credor",
    "nome_credor": "nome_credor",
    "cpf_cnpj_do_credor": "cpf_cnpj_credor",
    "cpf_cnpj_credor": "cpf_cnpj_credor",
    "data_emissao": "data_emissao",
    "data_criacao": "data_criacao",
}

INSERT_COLS = [
    "upload_id",
    "exercicio",
    "numero_est",
    "numero_emp",
    "empenho_atual",
    "empenho_rp",
    "numero_ped",
    "valor_emp",
    "valor_est_emp_sem_aqs",
    "valor_est_emp_com_aqs",
    "valor_emp_liquido",
    "uo",
    "nome_unidade_orcamentaria",
    "ug",
    "nome_unidade_gestora",
    "dotacao_orcamentaria",
    "historico",
    "credor",
    "nome_credor",
    "cpf_cnpj_credor",
    "data_criacao",
    "data_emissao",
    "situacao",
    "rp",
    "raw_payload",
    "data_atualizacao",
    "data_arquivo",
    "user_email",
    "ativo",
]


def ensure_dirs() -> None:
    for base in (INPUT_DIR, OUTPUT_DIR, INPUT_DIR / "tmp", OUTPUT_DIR / "tmp"):
        base.mkdir(parents=True, exist_ok=True)


def move_existing_to_tmp(base_dir: Path) -> None:
    tmp = base_dir / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    for f in base_dir.glob("*.xlsx"):
        if f.name.startswith("~$"):
            continue
        dest = tmp / f"{f.stem}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}{f.suffix}"
        try:
            shutil.move(str(f), dest)
        except PermissionError:
            print(f"Aviso: nao foi possivel mover {f} para tmp (arquivo em uso).")


def _normalize_text(texto: str) -> str:
    texto = re.sub(r"\s+", " ", str(texto)).strip().upper()
    texto = re.sub(r"[ÁÀÂÃ]", "A", texto)
    texto = re.sub(r"[ÉÈÊ]", "E", texto)
    texto = re.sub(r"[ÍÌÎ]", "I", texto)
    texto = re.sub(r"[ÓÒÔÕ]", "O", texto)
    texto = re.sub(r"[ÚÙÛ]", "U", texto)
    texto = texto.replace("Ç", "C")
    return texto


def _normalize_col(name: Any) -> str:
    texto = str(name or "")
    texto = texto.replace("¶§", "o").replace("¶¦", "a")
    texto = re.sub(r"[^\w\s]", " ", texto)
    texto = _normalize_text(texto)
    texto = re.sub(r"[^A-Z0-9]+", "_", texto).strip("_").lower()
    return texto


def encontrar_linha_cabecalho(df_raw: pd.DataFrame) -> int:
    for idx in range(min(30, len(df_raw))):
        linha = df_raw.iloc[idx].tolist()
        norm = [_normalize_col(val) for val in linha[: len(HEADER_INICIO)]]
        if norm == HEADER_INICIO:
            return idx
    raise ValueError("Cabecalho nao encontrado nas 30 primeiras linhas.")


def extrair_df_est(xls: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
    header_idx = encontrar_linha_cabecalho(df_raw)
    df_est = pd.read_excel(xls, sheet_name=sheet_name, header=header_idx)
    df_est.columns = df_est.columns.str.strip()
    return df_est


def remover_colunas(df: pd.DataFrame) -> pd.DataFrame:
    columns_to_drop = [
        "Nº Processo Orçamentário de Pagamento",
        "Nº NOBLIST",
        "Nº DOTLIST",
        "Nº OS",
        "Nº Emenda (EP)",
        "Autor da Emenda (EP)",
        "Nº Convênio",
        "Tipo Empenho",
        "UO Extinta",
        "Nº RPV",
        "RPV Vencido",
        "Ordenador",
        "Nome do Ordenador de Despesa",
    ]
    return df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors="ignore")


def tratar_colunas_texto(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str)
        df[col] = df[col].apply(lambda x: re.sub(r"_x000D_", "", x))
        df[col] = df[col].apply(lambda x: re.sub(r"\s+", " ", x).strip())
        df[col] = df[col].apply(lambda x: x.replace("*", "|"))
        df[col] = df[col].replace("", "NÃO INFORMADO").replace("nan", "NÃO INFORMADO")
    return df


def tratar_colunas_numericas(df: pd.DataFrame) -> pd.DataFrame:
    col_monetarias = [
        "Valor EMP",
        "Valor Est EMP (A LIQ/Em LIQ sem AQS)",
        "Valor Est EMP (Em LIQ com AQS)",
    ]
    valores_numericos: dict[str, pd.Series] = {}

    def _parse_ptbr(valor: Any) -> float | None:
        return pd.to_numeric(str(valor).replace(".", "").replace(",", "."), errors="coerce")

    def _format_ptbr(valor: float) -> str:
        return f"{valor:.2f}".replace(".", ",")

    for col in col_monetarias:
        if col in df.columns:
            numericos = df[col].apply(_parse_ptbr)
            valores_numericos[col] = numericos.fillna(0)
            df[col] = numericos.apply(lambda x: _format_ptbr(x) if pd.notnull(x) else "NÃO INFORMADO")

    df["Valor EMP - (A LIQ/Em LIQ sem AQS) - (Em LIQ com AQS)"] = (
        valores_numericos.get("Valor EMP", 0)
        - valores_numericos.get("Valor Est EMP (A LIQ/Em LIQ sem AQS)", 0)
        - valores_numericos.get("Valor Est EMP (Em LIQ com AQS)", 0)
    ).apply(_format_ptbr)

    col_datas = ["Data Emissão", "Data Criação"]
    for col in col_datas:
        if col in df.columns:
            serie_str = df[col].astype(str).str.strip()
            if serie_str.str.match(r"\d{4}-\d{2}-\d{2}").all():
                serie_dt = pd.to_datetime(serie_str, errors="coerce", dayfirst=False)
            else:
                serie_dt = pd.to_datetime(serie_str, errors="coerce", dayfirst=True)
            df[col] = serie_dt.dt.strftime("%d/%m/%Y").fillna("NÃO INFORMADO")

    col_numericas = ["Exercício", "UG", "UO"]
    for col in col_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna("NÃO INFORMADO").astype(str)

    return df


def adicionar_colunas_empenho(df: pd.DataFrame) -> pd.DataFrame:
    if "Nº EMP" not in df.columns or "Exercício" not in df.columns:
        print("Erro: Coluna 'Nº EMP' ou 'Exercício' nao encontrada!")
        return df

    def extrair_ano(n_emp: str) -> str | None:
        partes = n_emp.split(".")
        if len(partes) >= 3 and partes[2].isdigit():
            return partes[2][-2:]
        return None

    df["Ano Nº EMP"] = df["Nº EMP"].astype(str).apply(extrair_ano)
    df["Exercício"] = df["Exercício"].astype(str).str[-2:]

    df["Empenho Atual"] = df.apply(
        lambda x: x["Nº EMP"] if x["Ano Nº EMP"] == x["Exercício"] else "", axis=1
    )
    df["Empenho RP"] = df.apply(
        lambda x: x["Nº EMP"] if x["Ano Nº EMP"] != x["Exercício"] else "", axis=1
    )

    df["Empenho Atual"] = df["Empenho Atual"].replace("", "NÃO INFORMADO")
    df["Empenho RP"] = df["Empenho RP"].replace("", "NÃO INFORMADO")
    df.drop(columns=["Ano Nº EMP"], inplace=True)
    return df


def reorganizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    colunas = list(df.columns)

    for col in ["Data Emissão", "Data Criação"]:
        if col in colunas and "CPF/CNPJ do Credor" in colunas:
            colunas.remove(col)
            index = colunas.index("CPF/CNPJ do Credor") + 1
            colunas.insert(index, col)

    coluna_esperada = "Valor EMP - (A LIQ/Em LIQ sem AQS) - (Em LIQ com AQS)"
    if coluna_esperada not in df.columns:
        print(f"Erro: Coluna '{coluna_esperada}' nao encontrada.")
        return df

    if "Valor Est EMP (Em LIQ com AQS)" in colunas:
        index = colunas.index("Valor Est EMP (Em LIQ com AQS)") + 1
    elif "Valor Est EMP (A LIQ/Em LIQ sem AQS)" in colunas:
        index = colunas.index("Valor Est EMP (A LIQ/Em LIQ sem AQS)") + 1
    else:
        index = len(colunas)

    colunas.remove(coluna_esperada)
    colunas.insert(index, coluna_esperada)

    monetarias = [
        "Valor EMP",
        "Valor Est EMP (A LIQ/Em LIQ sem AQS)",
        "Valor Est EMP (Em LIQ com AQS)",
        coluna_esperada,
    ]

    for col in reversed(monetarias):
        if col in colunas:
            colunas.remove(col)
    if "Nº PED" in colunas:
        index = colunas.index("Nº PED") + 1
        colunas[index:index] = monetarias

    if "Histórico" in colunas and "Credor" in colunas:
        colunas.remove("Histórico")
        index = colunas.index("Credor")
        colunas.insert(index, "Histórico")

    if "Empenho Atual" in colunas and "Empenho RP" in colunas and "Nº EMP" in colunas:
        colunas.remove("Empenho Atual")
        colunas.remove("Empenho RP")
        index = colunas.index("Nº EMP") + 1
        colunas.insert(index, "Empenho Atual")
        colunas.insert(index + 1, "Empenho RP")

    return df[colunas]


def criar_writer_seguro(output_path: Path) -> tuple[pd.ExcelWriter, Path]:
    try:
        return pd.ExcelWriter(output_path, engine="xlsxwriter"), output_path
    except PermissionError:
        fallback = output_path.with_name(f"{output_path.stem}_{int(time.time())}_novo{output_path.suffix}")
        print(f"Aviso: {output_path} esta em uso. Salvando como {fallback}.")
        return pd.ExcelWriter(fallback, engine="xlsxwriter"), fallback


def processar_est_emp(file_path: Path) -> Path:
    xls = pd.ExcelFile(file_path)
    df_est = extrair_df_est(xls, sheet_name=xls.sheet_names[0])

    df_limpo = remover_colunas(df_est)
    df_tratado = tratar_colunas_texto(df_limpo)
    df_tratado = tratar_colunas_numericas(df_tratado)
    df_tratado = adicionar_colunas_empenho(df_tratado)
    df_final = reorganizar_colunas(df_tratado)

    output_dir = OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{file_path.stem}_tratado.xlsx"
    writer, output_file = criar_writer_seguro(output_file)
    df_est.to_excel(writer, index=False, sheet_name="est")
    df_final.to_excel(writer, index=False, sheet_name="est_emp_tratado")

    worksheet = writer.sheets["est_emp_tratado"]
    for i, col in enumerate(df_final.columns):
        column_width = max(df_final[col].astype(str).map(len).max(), len(col)) + 2
        if col == "Histórico":
            column_width = 120
        worksheet.set_column(i, i, column_width)

    writer.close()
    print(f"Planilha salva em: {output_file}")
    return output_file


def _clean_val(val: Any) -> Any:
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    if isinstance(val, str) and val.strip() == "-":
        return None
    return val


def _parse_valor_db(valor: Any) -> float | None:
    if valor is None:
        return None
    s = str(valor).strip()
    if s in (
        "",
        "-",
        "NÃO INFORMADO",
        "NAO INFORMADO",
        "NÃO IDENTIFICADO",
        "NAO IDENTIFICADO",
    ):
        return None
    s_num = re.sub(r"[^\d,.-]", "", s)
    if "," in s_num:
        s_num = s_num.replace(".", "").replace(",", ".")
    try:
        return float(s_num)
    except ValueError:
        return None


def _parse_data_db(valor: Any) -> datetime | None:
    if valor is None:
        return None
    if isinstance(valor, datetime):
        return valor
    s = str(valor).strip()
    if not s or s in ("-", "00/00/0000", "00/00/0000 00:00:00"):
        return None
    s = s.replace("-", "/")
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def montar_registros_para_db(
    df: pd.DataFrame, data_arquivo: datetime, user_email: str, upload_id: int
) -> list[dict[str, Any]]:
    registros: list[dict[str, Any]] = []
    rows = df.to_dict(orient="records")
    for row in rows:
        payload: dict[str, Any] = {}
        for col, val in row.items():
            key = _normalize_col(col)
            db_col = COL_MAP.get(key)
            if not db_col:
                continue
            payload[db_col] = _clean_val(val)

        for col in ("valor_emp", "valor_est_emp_sem_aqs", "valor_est_emp_com_aqs", "valor_emp_liquido"):
            if col in payload:
                payload[col] = _parse_valor_db(payload[col])
        for col in ("data_emissao", "data_criacao"):
            if col in payload:
                payload[col] = _parse_data_db(payload[col])

        safe_row: dict[str, Any] = {}
        for k, v in row.items():
            try:
                if pd.isna(v):
                    safe_row[k] = None
                    continue
            except Exception:
                pass
            if hasattr(v, "isoformat"):
                try:
                    safe_row[k] = v.isoformat()
                    continue
                except Exception:
                    pass
            safe_row[k] = v

        payload["raw_payload"] = json.dumps(safe_row, ensure_ascii=False)
        payload["upload_id"] = upload_id
        payload["data_atualizacao"] = datetime.utcnow()
        payload["data_arquivo"] = data_arquivo
        payload["user_email"] = user_email
        payload["ativo"] = True
        for col in INSERT_COLS:
            payload.setdefault(col, None)
        registros.append(payload)
    return registros


def _enable_fast_executemany() -> None:
    global _FAST_EXEC_ENABLED
    if _FAST_EXEC_ENABLED:
        return

    @event.listens_for(db.engine, "before_cursor_execute")
    def _set_fast_exec(conn, cursor, statement, parameters, context, executemany):  # type: ignore[no-redef]
        if executemany:
            try:
                cursor.fast_executemany = True
            except Exception:
                pass

    _FAST_EXEC_ENABLED = True


def update_database(
    df: pd.DataFrame, data_arquivo: datetime, user_email: str, upload_id: int
) -> int:
    insert_sql = text(
        """
        INSERT INTO est_emp (
            upload_id, exercicio, numero_est, numero_emp, empenho_atual, empenho_rp, numero_ped,
            valor_emp, valor_est_emp_sem_aqs, valor_est_emp_com_aqs, valor_emp_liquido, uo,
            nome_unidade_orcamentaria, ug, nome_unidade_gestora, dotacao_orcamentaria, historico,
            credor, nome_credor, cpf_cnpj_credor, data_criacao, data_emissao, situacao, rp,
            raw_payload, data_atualizacao, data_arquivo, user_email, ativo
        )
        VALUES (
            :upload_id, :exercicio, :numero_est, :numero_emp, :empenho_atual, :empenho_rp, :numero_ped,
            :valor_emp, :valor_est_emp_sem_aqs, :valor_est_emp_com_aqs, :valor_emp_liquido, :uo,
            :nome_unidade_orcamentaria, :ug, :nome_unidade_gestora, :dotacao_orcamentaria, :historico,
            :credor, :nome_credor, :cpf_cnpj_credor, :data_criacao, :data_emissao, :situacao, :rp,
            :raw_payload, :data_atualizacao, :data_arquivo, :user_email, :ativo
        )
        """
    )

    try:
        db.session.execute(text("UPDATE est_emp SET ativo = 0 WHERE ativo = 1"))
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        raise

    _enable_fast_executemany()
    registros = montar_registros_para_db(df, data_arquivo, user_email, upload_id)
    total_registros = len(registros)
    print(f" Gravando {total_registros} registros no banco...")
    total = 0
    for start in range(0, len(registros), BATCH_SIZE):
        chunk = registros[start : start + BATCH_SIZE]
        try:
            db.session.execute(insert_sql, chunk)
            db.session.commit()
            total += len(chunk)
            print(f" Inseridos {total}/{total_registros} registros...")
        except SQLAlchemyError:
            db.session.rollback()
            raise
    return total


def run_est_emp(
    file_path: Path, data_arquivo: datetime, user_email: str, upload_id: int
) -> tuple[int, Path]:
    ensure_dirs()
    move_existing_to_tmp(OUTPUT_DIR)
    output_path = processar_est_emp(file_path)

    df_tratado = pd.read_excel(output_path, sheet_name="est_emp_tratado", dtype=str)
    colunas_data = {"data_emissao", "data_criacao", "data_atualizacao", "data_arquivo"}
    for col in df_tratado.columns:
        if _normalize_col(col) in colunas_data:
            serie_str = df_tratado[col].astype(str).str.strip()
            if serie_str.str.match(r"\d{4}-\d{2}-\d{2}").all():
                df_tratado[col] = pd.to_datetime(serie_str, errors="coerce", dayfirst=False)
            else:
                df_tratado[col] = pd.to_datetime(serie_str, errors="coerce", dayfirst=True)
    total = update_database(df_tratado, data_arquivo, user_email, upload_id)
    return total, output_path
