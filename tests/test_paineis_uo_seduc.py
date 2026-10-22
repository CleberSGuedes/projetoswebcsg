"""Painéis, relatórios e API fixos nas UOs da secretaria
(docs/plano_implementacao_novas_uos.md, Fase 1, D5).

A condição de UO é exercitada numa TEMPORARY TABLE chamada `emp` (some junto
com a conexão e esconde a tabela real só nela): nenhuma linha real é lida.
"""

import pytest
from flask import Flask
from sqlalchemy import select, text

from config import Config
from models import EmpRegistro, db
from services.execucao_uo import cond_uo_em, sql_uo_em
from services.uo import UOS_SEDUC


def test_sql_uo_em_so_inteiros_e_qualificado():
    s = sql_uo_em({"14601", "14101"})
    assert s.endswith("IN (14101, 14601)")
    assert "SUBSTRING_INDEX(uo, ' - ', 1)" in s
    assert "SUBSTRING_INDEX(emp.uo, ' - ', 1)" in sql_uo_em(UOS_SEDUC, tabela="emp")


@pytest.fixture
def conn_emp_temporaria():
    app = Flask("teste_paineis_uo")
    app.config.from_object(Config)
    db.init_app(app)
    with app.app_context():
        with db.engine.connect() as conn:
            conn.execute(
                text("CREATE TEMPORARY TABLE emp (id INT PRIMARY KEY AUTO_INCREMENT, uo VARCHAR(100), ativo TINYINT(1))")
            )
            for uo in ["14101", "14.101 - SECRETARIA", "14601", "26201", "26101", "1101", "01101"]:
                conn.execute(text("INSERT INTO emp (uo, ativo) VALUES (:u, 1)"), {"u": uo})
            yield conn
            conn.execute(text("DROP TEMPORARY TABLE IF EXISTS emp"))


def test_condicao_sql_cru_filtra_so_a_secretaria(conn_emp_temporaria):
    n = conn_emp_temporaria.execute(
        text(f"SELECT COUNT(*) FROM emp WHERE ativo = 1 AND {sql_uo_em(UOS_SEDUC)}")
    ).scalar()
    assert n == 3  # 14101, "14.101 - ..." e 14601


def test_condicao_orm_filtra_so_a_secretaria(conn_emp_temporaria):
    stmt = select(EmpRegistro.uo).where(EmpRegistro.ativo == True, cond_uo_em(EmpRegistro, UOS_SEDUC))  # noqa: E712
    # o modelo EmpRegistro lê a tabela `emp` (aqui, a temporária)
    uos = sorted(r[0] for r in conn_emp_temporaria.execute(stmt))
    assert uos == ["14.101 - SECRETARIA", "14101", "14601"]
    so_nova = select(EmpRegistro.uo).where(cond_uo_em(EmpRegistro, {"26201"}))
    assert [r[0] for r in conn_emp_temporaria.execute(so_nova)] == ["26201"]
