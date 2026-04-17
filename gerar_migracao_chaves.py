import json
from pathlib import Path


BASE = Path(r"c:\workspace\projetoswebcsg\static\js")
ARQ_PLANEJ = BASE / "chaves_planejamento.json"
ARQ_ARRUMAR = BASE / "chave_arrumar.json"
ARQ_FORCAR = BASE / "forcar_chave.json"
OUT_SQL = Path(r"c:\workspace\projetoswebcsg\migracao_chaves_planejamento_regra.sql")


def load_json(path: Path):
    # utf-8-sig handles files with optional BOM.
    return json.loads(path.read_text(encoding="utf-8-sig"))


def esc(value):
    if value is None:
        return "NULL"
    s = str(value)
    s = s.replace("\\", "\\\\").replace("'", "''")
    return f"'{s}'"


def linha_insert(tipo, origem, destino=None):
    return (
        "INSERT INTO chave_planejamento_regra "
        "(tipo_regra, chave_origem, chave_destino, observacao, usuario_id, ativo, criado_em) "
        f"VALUES ({esc(tipo)}, {esc(origem)}, {esc(destino)}, NULL, NULL, 1, NOW()) "
        "ON DUPLICATE KEY UPDATE "
        "chave_destino = VALUES(chave_destino), "
        "ativo = 1, "
        "excluido_em = NULL, "
        "alterado_em = NOW();"
    )


def main():
    chaves_planejamento = load_json(ARQ_PLANEJ)  # list
    chave_arrumar = load_json(ARQ_ARRUMAR)  # map origem -> destino
    forcar_chave = load_json(ARQ_FORCAR)  # map origem -> destino

    sql = []
    sql.append("SET NAMES utf8mb4;")
    sql.append("START TRANSACTION;")

    # 1) chaves_planejamento.json
    vistos = set()
    for item in chaves_planejamento:
        origem = str(item).strip()
        if not origem or origem in vistos:
            continue
        vistos.add(origem)
        sql.append(linha_insert("chaves_planejamento", origem, None))

    # 2) chave_arrumar.json
    for origem, destino in chave_arrumar.items():
        origem = str(origem).strip()
        destino = str(destino).strip() if destino is not None else None
        if not origem:
            continue
        sql.append(linha_insert("chave_arrumar", origem, destino))

    # 3) forcar_chave.json
    for origem, destino in forcar_chave.items():
        origem = str(origem).strip()
        destino = str(destino).strip() if destino is not None else None
        if not origem:
            continue
        sql.append(linha_insert("forcar_chave", origem, destino))

    sql.append("COMMIT;")
    OUT_SQL.write_text("\n".join(sql) + "\n", encoding="utf-8")
    print(f"Arquivo gerado: {OUT_SQL}")


if __name__ == "__main__":
    main()
