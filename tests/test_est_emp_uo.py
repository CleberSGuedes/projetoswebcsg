"""EST EMP por UO e exercício (docs/plano_implementacao_novas_uos.md, Fase 1).

Helpers de UO e leitura do relatório sem banco. A desativação é testada numa
TEMPORARY TABLE do MySQL: só existe na conexão do teste - nenhuma tabela real
(est_emp) é tocada.
"""

import pandas as pd
import pytest
from flask import Flask
from sqlalchemy import text

from config import Config
from models import db
from services import est_emp_runner as er
from services.uo import (
    UOS_EXECUCAO,
    UOS_SEDUC,
    UORecusada,
    exercicio_4d,
    uo_codigo,
    uo_usa_chave,
    validar_uo_execucao,
)


def test_exercicio_4d_aceita_os_formatos_das_tabelas():
    assert exercicio_4d("26") == "2026"
    assert exercicio_4d("2026") == "2026"
    assert exercicio_4d(2026) == "2026"
    assert exercicio_4d("2026.0") == "2026"
    assert exercicio_4d(None) == ""
    assert exercicio_4d("NÃO INFORMADO") == ""


def test_uo_codigo_completa_zero_a_esquerda():
    assert uo_codigo("1101") == "01101"
    assert uo_codigo("14.101 - SECRETARIA DE ESTADO DE EDUCAÇÃO") == "14101"
    assert uo_codigo("26101") == "26101"
    assert uo_codigo("") == ""


def test_uo_usa_chave_so_para_a_secretaria():
    assert uo_usa_chave("14101") and uo_usa_chave("14.601 - FMTE")
    assert not uo_usa_chave("26201") and not uo_usa_chave(None)


def test_validar_uo_execucao():
    assert validar_uo_execucao(["14101", "14101"]) == "14101"
    assert validar_uo_execucao(["14.101"]) == "14101"
    with pytest.raises(UORecusada, match="mais de uma UO"):
        validar_uo_execucao(["14101", "14601"])
    with pytest.raises(UORecusada, match="identificar a UO"):
        validar_uo_execucao([])
    with pytest.raises(UORecusada, match="não cadastrada"):
        validar_uo_execucao(["99999"])
    # cadastrada, mas ainda não liberada (confirmação oficial pendente)
    with pytest.raises(UORecusada, match="ainda não está liberada"):
        validar_uo_execucao(["26201"], liberadas=set(UOS_SEDUC))
    # liberando (Fase 3) passa
    assert validar_uo_execucao(["26201"], liberadas=set(UOS_EXECUCAO)) == "26201"


def _df(exercicio, n_emp, uo="14101"):
    return pd.DataFrame({"Exercício": exercicio, "Nº EMP": n_emp, "UO": uo})


def test_exercicio_sai_completo_e_atual_rp_continuam_corretos():
    df = _df(["2026", "2026"], ["2026.14101.000001.26", "2025.14101.000002.25"])
    df["Nº EMP"] = ["2026.14101.26.000001", "2025.14101.25.000002"]
    out = er.adicionar_colunas_empenho(df)
    assert list(out["Exercício"]) == ["2026", "2026"]
    assert out.loc[0, "Empenho Atual"] == "2026.14101.26.000001"
    assert out.loc[0, "Empenho RP"] == "NÃO INFORMADO"
    assert out.loc[1, "Empenho RP"] == "2025.14101.25.000002"


def test_identificar_uo_e_exercicios():
    df = _df(["2026", "2026", "2025"], ["a", "b", "c"])
    assert er.identificar_uo_e_exercicios(df) == ("14101", ["2025", "2026"])
    with pytest.raises(UORecusada):
        er.identificar_uo_e_exercicios(_df(["2026", "2026"], ["a", "b"], uo=["14101", "14601"]))
    with pytest.raises(UORecusada, match="ainda não está liberada"):
        er.identificar_uo_e_exercicios(_df(["2026"], ["a"], uo="26201"))


@pytest.fixture
def conn_temporaria():
    app = Flask("teste_est_emp_uo")
    app.config.from_object(Config)
    db.init_app(app)
    with app.app_context():
        with db.engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TEMPORARY TABLE tmp_est_emp_teste ("
                    "id INT PRIMARY KEY AUTO_INCREMENT, uo VARCHAR(100), "
                    "exercicio VARCHAR(50), ativo TINYINT(1))"
                )
            )
            yield conn
            conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tmp_est_emp_teste"))


def test_desativacao_so_da_mesma_uo_e_exercicio(conn_temporaria):
    conn = conn_temporaria
    linhas = [
        ("14101", "26", 1),  # formato antigo do exercício: deve desativar
        ("14101", "2026", 1),
        ("1101", "2026", 1),  # UO sem zero à esquerda de OUTRA UO (01101): não toca
        ("14601", "2026", 1),  # outra UO
        ("14101", "2025", 1),  # outro exercício
        ("14101", "2026", 0),  # já inativa
        ("14.101 - SECRETARIA", "2026", 1),  # formato com nome: deve desativar
    ]
    for uo, ex, at in linhas:
        conn.execute(
            text("INSERT INTO tmp_est_emp_teste (uo, exercicio, ativo) VALUES (:uo, :ex, :at)"),
            {"uo": uo, "ex": ex, "at": at},
        )
    n = er.desativar_versao_anterior("14101", ["2026"], conn=conn, tabela="tmp_est_emp_teste")
    assert n == 3
    ativos = conn.execute(
        text("SELECT uo, exercicio FROM tmp_est_emp_teste WHERE ativo = 1 ORDER BY id")
    ).fetchall()
    assert [tuple(r) for r in ativos] == [("1101", "2026"), ("14601", "2026"), ("14101", "2025")]
