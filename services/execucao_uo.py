"""Desativação da versão anterior por UO e exercício - módulos de execução
(FIP 613, PED, EST EMP; docs/plano_implementacao_novas_uos.md, Fase 1).

Antes, cada upload fazia `UPDATE <tabela> SET ativo = 0 WHERE ativo = 1`, ou
seja, o arquivo de uma UO desativava os dados de todas as outras.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from models import db
from services.uo import exercicio_4d, uo_codigo

# Expressão que extrai o código numérico da UO de qualquer formato gravado:
# "14101", "1101" (sem o zero à esquerda) ou "14.101 - SECRETARIA ...".
SQL_UO_NUMERO = "CAST(REPLACE(SUBSTRING_INDEX(uo, ' - ', 1), '.', '') AS UNSIGNED)"


def sql_uo_em(codigos, tabela: str | None = None) -> str:
    """Condição SQL "UO está em <códigos>" (texto, só inteiros). `tabela`
    qualifica a coluna quando há JOIN."""
    numeros = ", ".join(str(int(uo_codigo(c))) for c in sorted(codigos))
    coluna = f"{tabela}.uo" if tabela else "uo"
    return SQL_UO_NUMERO.replace("uo,", f"{coluna},", 1) + f" IN ({numeros})"


def cond_uo_em(modelo: Any, codigos):
    """Mesma condição para consultas ORM (modelo com coluna `uo`)."""
    from sqlalchemy import literal_column

    expr = SQL_UO_NUMERO.replace("uo,", f"{modelo.__tablename__}.uo,", 1)
    return literal_column(expr).in_([int(uo_codigo(c)) for c in sorted(codigos)])


def desativar_versao_anterior(
    tabela: str,
    uo: str,
    exercicios: list[Any],
    coluna_exercicio: str = "exercicio",
    conn: Any = None,
    commit: bool = True,
) -> int:
    """Desativa só as linhas ativas da MESMA UO e dos MESMOS exercícios do
    arquivo. Compara por número: cargas antigas guardaram o exercício como
    "26" e a UO sem o zero à esquerda. Devolve quantas linhas desativou.

    `conn` serve aos testes (tabela temporária na própria conexão). Com
    `commit=False` o chamador faz o commit (troca da carga numa transação só).
    """
    uo = uo_codigo(uo)
    if not uo:
        raise ValueError("UO não informada para a desativação da versão anterior.")
    anos = {int(e) for e in (exercicio_4d(x) for x in exercicios) if e}
    if not anos:
        raise ValueError("Exercício não informado para a desativação da versão anterior.")
    anos = sorted(anos | {a % 100 for a in anos})
    params: dict[str, int] = {"uo": int(uo)}
    marcadores = []
    for i, ano in enumerate(anos):
        params[f"e{i}"] = ano
        marcadores.append(f":e{i}")
    sql = text(
        f"UPDATE {tabela} SET ativo = 0 WHERE ativo = 1 "
        f"AND {SQL_UO_NUMERO} = :uo "
        f"AND CAST({coluna_exercicio} AS UNSIGNED) IN ({', '.join(marcadores)})"
    )
    if conn is not None:
        return conn.execute(sql, params).rowcount or 0
    try:
        res = db.session.execute(sql, params)
        if commit:
            db.session.commit()
        return res.rowcount or 0
    except SQLAlchemyError:
        db.session.rollback()
        raise
