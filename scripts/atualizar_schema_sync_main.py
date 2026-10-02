"""Atualiza o schema do banco DESTE ambiente para o código sincronizado da
main (docs/claude.md, seção 21.7).

Cada ambiente (homologação, dev/cleber, dev/jean) tem banco próprio; as
mudanças de schema feitas na main entre 31/08 e 29/09 precisam ser aplicadas
em cada um. O script usa a conexão do .env local (a mesma da aplicação).

Uso (a partir da raiz do projeto):
    python scripts/atualizar_schema_sync_main.py            # só mostra o que falta
    python scripts/atualizar_schema_sync_main.py --aplicar  # aplica

Pode ser executado mais de uma vez: o que já estiver aplicado é pulado.

O que verifica/aplica:
 1. momp.uo (VARCHAR(5)) + índice ix_momp_exercicio_uo + histórico marcado
    como UO 14101 (antes de 2027 o FMTE estava dentro da 14101 - seção 20).
 2. plan20_seduc: 8 colunas do layout 2027 do Plan20 (seção 18).
 3. AUTO_INCREMENT em logs_login.id, active_sessions.id e perfil.id - o
    código não gera mais o id na mão (seção 9, item 5); sem isso o login falha.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import create_engine, text  # noqa: E402

from config import Config  # noqa: E402

PLAN20_COLUNAS_NOVAS = [
    ("eixo_programa", "VARCHAR(255)"),
    ("objetivo_estrategico", "TEXT"),
    ("publico_alvo", "VARCHAR(255)"),
    ("tipo", "VARCHAR(255)"),
    ("uo_responsavel", "VARCHAR(255)"),
    ("ods", "TEXT"),
    ("codigo_meta_ods", "TEXT"),
    ("metas_ods", "TEXT"),
]
TABELAS_AUTO_INCREMENT = ["logs_login", "active_sessions", "perfil"]


def _tabela_existe(conn, tabela: str) -> bool:
    return bool(conn.execute(text(
        "SELECT COUNT(*) FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t"
    ), {"t": tabela}).scalar())


def _coluna(conn, tabela: str, coluna: str):
    return conn.execute(text(
        "SELECT COLUMN_TYPE, EXTRA FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t AND COLUMN_NAME = :c"
    ), {"t": tabela, "c": coluna}).first()


def _indice_existe(conn, tabela: str, indice: str) -> bool:
    return bool(conn.execute(text(
        "SELECT COUNT(*) FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t AND INDEX_NAME = :i"
    ), {"t": tabela, "i": indice}).scalar())


def planejar(conn) -> list[tuple[str, str]]:
    """Lista (descrição, SQL) do que falta neste banco."""
    passos: list[tuple[str, str]] = []

    # 1. momp.uo
    if not _tabela_existe(conn, "momp"):
        print("  - tabela momp não existe neste banco (pulado)")
    else:
        if _coluna(conn, "momp", "uo") is None:
            passos.append(("momp: criar coluna uo",
                           "ALTER TABLE momp ADD COLUMN uo VARCHAR(5) NULL"))
            sem_uo = conn.execute(text("SELECT COUNT(*) FROM momp")).scalar()
        else:
            sem_uo = conn.execute(text("SELECT COUNT(*) FROM momp WHERE uo IS NULL")).scalar()
        if not _indice_existe(conn, "momp", "ix_momp_exercicio_uo"):
            passos.append(("momp: criar índice ix_momp_exercicio_uo",
                           "ALTER TABLE momp ADD INDEX ix_momp_exercicio_uo (exercicio, uo, ativo)"))
        if sem_uo:
            passos.append((f"momp: marcar {sem_uo} registro(s) sem UO como 14101",
                           "UPDATE momp SET uo = '14101' WHERE uo IS NULL"))

    # 2. plan20_seduc
    if not _tabela_existe(conn, "plan20_seduc"):
        print("  - tabela plan20_seduc não existe neste banco (pulado)")
    else:
        for coluna, tipo in PLAN20_COLUNAS_NOVAS:
            if _coluna(conn, "plan20_seduc", coluna) is None:
                passos.append((f"plan20_seduc: criar coluna {coluna}",
                               f"ALTER TABLE plan20_seduc ADD COLUMN {coluna} {tipo}"))

    # 3. AUTO_INCREMENT
    for tabela in TABELAS_AUTO_INCREMENT:
        if not _tabela_existe(conn, tabela):
            print(f"  - tabela {tabela} não existe neste banco (pulado)")
            continue
        col = _coluna(conn, tabela, "id")
        if col is None:
            print(f"  - {tabela} não tem coluna id (pulado - verificar manualmente)")
        elif "auto_increment" not in (col.EXTRA or "").lower():
            passos.append((f"{tabela}.id: ativar AUTO_INCREMENT ({col.COLUMN_TYPE})",
                           f"ALTER TABLE {tabela} MODIFY id {col.COLUMN_TYPE} NOT NULL AUTO_INCREMENT"))
    return passos


def main() -> int:
    aplicar = "--aplicar" in sys.argv[1:]
    engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
    print(f"Banco: host={engine.url.host} db={engine.url.database}")
    with engine.connect() as conn:
        passos = planejar(conn)
        if not passos:
            print("Nada a fazer: o schema deste banco já está atualizado.")
            return 0
        print("Pendências encontradas:")
        for descricao, sql in passos:
            print(f"  * {descricao}\n      {sql}")
        if not aplicar:
            print("\nNada foi alterado. Rode novamente com --aplicar para aplicar.")
            return 0
        for descricao, sql in passos:
            print(f"Aplicando: {descricao}")
            conn.execute(text(sql))
            conn.commit()
        restantes = planejar(conn)
        if restantes:
            print("ATENÇÃO: ainda há pendências:", [d for d, _ in restantes])
            return 1
        print("Concluído: schema atualizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
