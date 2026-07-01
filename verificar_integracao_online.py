from __future__ import annotations

import os
import socket
import subprocess
import sys
from pathlib import Path

import pymysql
from dotenv import dotenv_values

BASE = Path(__file__).resolve().parent
ENV_PATH = BASE / ".env"
TABLES = [
    "usuarios",
    "perfis",
    "features",
    "plan20_seduc",
    "plan21_nger",
    "programa_planejamento",
    "acao_planejamento",
    "produto_acao_planejamento",
    "chave_catalogo",
    "dotacao",
    "ped",
    "emp",
    "nob",
    "fip613",
]
HEAVY_TABLES = {"emp", "nob", "fip613"}


def mask(value: str | None) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "***"
    return value[:2] + "***" + value[-2:]


def git_info() -> None:
    print("\n== Codigo local ==")
    try:
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=BASE, text=True, stderr=subprocess.STDOUT
        ).strip()
        commit = subprocess.check_output(
            ["git", "log", "-1", "--format=%h %ci %s"], cwd=BASE, text=True, stderr=subprocess.STDOUT
        ).strip()
        status = subprocess.check_output(
            ["git", "status", "--short"], cwd=BASE, text=True, stderr=subprocess.STDOUT
        ).strip()
        print(f"Branch: {branch}")
        print(f"Ultimo commit: {commit}")
        print("Alteracoes locais: " + ("sim" if status else "nao"))
    except Exception as exc:
        print(f"Git nao disponivel para diagnostico: {exc}")


def short_error(exc: Exception) -> str:
    if getattr(exc, "args", None):
        code = exc.args[0]
        message = exc.args[1] if len(exc.args) > 1 else ""
        return f"{code}: {message}" if message else str(code)
    return str(exc)


def connect_mysql(env: dict[str, str], host: str, port: int):
    return pymysql.connect(
        host=host,
        port=port,
        user=env["DB_USER_CSG"],
        password=env["DB_PASSWORD_CSG"],
        database=env["DB_NAME_CSG"],
        charset="utf8mb4",
        connect_timeout=20,
        read_timeout=60,
        write_timeout=60,
        cursorclass=pymysql.cursors.DictCursor,
    )


def safe_ping(conn) -> bool:
    try:
        conn.ping(reconnect=True)
        return True
    except Exception:
        return False


def main() -> int:
    env = dotenv_values(ENV_PATH)
    print("== Configuracao do banco on-line ==")
    print(f"Env: {ENV_PATH}")
    print(f"Host: {env.get('DB_HOST_CSG')}:{env.get('DB_PORT_CSG', '3306')}")
    print(f"Banco: {env.get('DB_NAME_CSG')}")
    print(f"Usuario: {env.get('DB_USER_CSG')}")
    print(f"Senha: {mask(env.get('DB_PASSWORD_CSG'))}")

    git_info()

    host = env.get("DB_HOST_CSG") or ""
    port = int(env.get("DB_PORT_CSG") or 3306)
    print("\n== Teste de rede ==")
    try:
        with socket.create_connection((host, port), timeout=10):
            print("Socket MySQL: OK")
    except Exception as exc:
        print(f"Socket MySQL: FALHOU ({exc})")
        return 2

    print("\n== Banco on-line ==")
    try:
        conn = connect_mysql(env, host, port)
    except Exception as exc:
        print(f"Conexao MySQL: FALHOU ({exc})")
        return 3

    warnings: list[str] = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DATABASE() db, VERSION() version")
            row = cur.fetchone()
            print(f"Conexao MySQL: OK ({row['db']} / {row['version']})")

        print("\nContagens principais:")
        for table in TABLES:
            try:
                safe_ping(conn)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT TABLE_ROWS table_rows
                        FROM information_schema.tables
                        WHERE table_schema=DATABASE() AND table_name=%s
                        """,
                        (table,),
                    )
                    table_info = cur.fetchone()
                    if not table_info:
                        print(f"- {table}: tabela ausente")
                        continue

                    if table in HEAVY_TABLES:
                        table_rows = table_info.get("table_rows")
                        estimate = "desconhecida" if table_rows is None else f"~{int(table_rows)}"
                        print(f"- {table}: {estimate} (estimativa rapida)")
                        continue

                    cur.execute(f"SELECT COUNT(*) c FROM `{table}`")
                    print(f"- {table}: {cur.fetchone()['c']}")
            except Exception as exc:
                msg = f"{table}: nao foi possivel concluir a contagem ({short_error(exc)})"
                warnings.append(msg)
                print(f"- {msg}")

        print("\nEstrutura de execucao:")
        for table in ("ped", "emp", "nob"):
            try:
                safe_ping(conn)
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=%s ORDER BY ORDINAL_POSITION",
                        (table,),
                    )
                    cols = [r["COLUMN_NAME"] for r in cur.fetchall()]
                    wanted = [c for c in ("chave", "chave_planejamento", "numero_emp", "numero_ped", "valor_ped", "valor_emp_devolucao_gcv", "valor_nob") if c in cols]
                    print(f"- {table}: {len(cols)} colunas; campos-chave: {', '.join(wanted) if wanted else 'nao detectados'}")
            except Exception as exc:
                msg = f"{table}: nao foi possivel ler estrutura ({short_error(exc)})"
                warnings.append(msg)
                print(f"- {msg}")

        if warnings:
            print("\nAvisos do diagnostico:")
            for item in warnings:
                print(f"- {item}")
            print("\nDiagnostico concluido com avisos. A conexao principal ao banco esta OK; os avisos acima nao bloqueiam a abertura do ambiente.")
        else:
            print("\nDiagnostico concluido.")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
