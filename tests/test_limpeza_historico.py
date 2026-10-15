"""Limpeza de Histórico (services/limpeza_historico.py - docs/claude.md, seção 23).

A regra de escolha das cargas é testada sem banco. A remoção é testada de
ponta a ponta numa TEMPORARY TABLE do MySQL: ela só existe na conexão do
teste e some sozinha - nenhuma tabela real (emp, nob, ...) é tocada.
"""

from datetime import datetime, timedelta

import pytest
from flask import Flask
from sqlalchemy import text

from config import Config
from models import db
from services import limpeza_historico as lh
from services.limpeza_historico import Carga, normalizar_exercicio, planejar_tabela, validar_parametros


def _carga(upload_id, data, exercicio=2025, linhas=10, ativos=0):
    return Carga(
        upload_id=upload_id,
        data_arquivo=datetime.fromisoformat(data),
        exercicio=exercicio,
        linhas=linhas,
        ativos=ativos,
        id_min=upload_id * 100,
        id_max=upload_id * 100 + linhas - 1,
    )


def test_normalizar_exercicio_aceita_os_formatos_das_tabelas():
    assert normalizar_exercicio(2026) == 2026
    assert normalizar_exercicio("2026") == 2026
    assert normalizar_exercicio("26") == 2026  # EST EMP
    assert normalizar_exercicio(None) is None
    assert normalizar_exercicio("") is None


def test_mantem_a_ultima_carga_do_mes_e_remove_as_anteriores_do_exercicio():
    cargas = [
        _carga(1, "2025-11-10"),
        _carga(2, "2025-12-05"),
        _carga(3, "2025-12-29"),  # última de dez/2025 -> mantida
        _carga(4, "2026-01-14"),  # posterior ao mês escolhido -> intocada
        _carga(5, "2025-12-20", exercicio=2026),  # outro exercício -> intocada
    ]
    plano = planejar_tabela("emp", cargas, exercicio=2025, ref_mes=12, ref_ano=2025)
    assert plano.mantida.upload_id == 3
    assert [c.upload_id for c in plano.remover] == [1, 2]
    assert plano.linhas_remover == 20


def test_carga_final_no_ano_seguinte():
    """FIP 613 de 2025: a última carga do exercício chegou em 14/01/2026."""
    cargas = [
        _carga(1, "2025-12-15"),
        _carga(6, "2025-12-29"),
        _carga(7, "2026-01-14"),
        _carga(8, "2026-01-19", exercicio=2026),
    ]
    plano = planejar_tabela("fip613", cargas, exercicio=2025, ref_mes=1, ref_ano=2026)
    assert plano.mantida.upload_id == 7
    assert [c.upload_id for c in plano.remover] == [1, 6]


def test_carga_com_registro_ativo_nunca_e_removida():
    cargas = [
        _carga(1, "2026-09-01", exercicio=2026, ativos=10),
        _carga(2, "2026-09-02", exercicio=2026),
        _carga(3, "2026-09-30", exercicio=2026),
    ]
    plano = planejar_tabela("nob", cargas, exercicio=2026, ref_mes=9, ref_ano=2026)
    assert plano.mantida.upload_id == 3
    assert [c.upload_id for c in plano.remover] == [2]
    assert [c.upload_id for c in plano.protegidas] == [1]


def test_sem_carga_no_mes_de_referencia_nada_e_removido():
    cargas = [_carga(1, "2025-10-01"), _carga(2, "2025-11-01")]
    plano = planejar_tabela("ped", cargas, exercicio=2025, ref_mes=12, ref_ano=2025)
    assert plano.mantida is None
    assert plano.remover == []


def test_carga_com_exercicio_misto_nunca_e_removida():
    cargas = [_carga(1, "2025-12-01", exercicio=None), _carga(2, "2025-12-30")]
    plano = planejar_tabela("est_emp", cargas, exercicio=2025, ref_mes=12, ref_ano=2025)
    assert plano.mantida.upload_id == 2
    assert plano.remover == []


def test_empate_de_data_mantem_o_maior_upload_id():
    cargas = [_carga(1, "2025-12-30"), _carga(2, "2025-12-30")]
    plano = planejar_tabela("emp", cargas, exercicio=2025, ref_mes=12, ref_ano=2025)
    assert plano.mantida.upload_id == 2
    assert [c.upload_id for c in plano.remover] == [1]


def test_estimativa_de_espaco_proporcional_as_linhas():
    cargas = [_carga(1, "2025-12-01", linhas=30), _carga(2, "2025-12-30", linhas=10)]
    plano = planejar_tabela("emp", cargas, exercicio=2025, ref_mes=12, ref_ano=2025)
    plano.tamanho_mb = 100.0
    assert plano.mb_estimado == 75.0


def test_validar_parametros():
    assert validar_parametros("2025", "1", "2026", ["nob", "emp", "x"]) == (2025, 1, 2026, ["emp", "nob"])
    for args in [
        ("", 12, 2025, ["emp"]),
        (2025, 13, 2025, ["emp"]),
        (2025, 12, 2024, ["emp"]),  # carga anterior ao exercício
        (2025, 12, 2025, []),
        (2025, 12, 2025, ["tabela_inexistente"]),
    ]:
        with pytest.raises(ValueError):
            validar_parametros(*args)


def test_sem_sinal_de_vida():
    """Processo reciclado pelo servidor no meio do processamento (seção 23.7)."""
    agora = datetime(2026, 10, 15, 10, 0, 0)
    sinal = lambda seg: datetime(2026, 10, 15, 10, 0, 0) - timedelta(seconds=seg)  # noqa: E731
    assert not lh.sem_sinal_de_vida("em andamento", sinal(lh.INTERVALO_SINAL * 3), agora)
    assert not lh.sem_sinal_de_vida("em andamento", sinal(lh.LIMITE_SEM_SINAL), agora)
    assert lh.sem_sinal_de_vida("em andamento", sinal(lh.LIMITE_SEM_SINAL + 1), agora)
    assert lh.sem_sinal_de_vida("em andamento", None, agora)
    # Só o que está em andamento pode ser dado como interrompido.
    for status in ("concluida", "falha", "interrompida"):
        assert not lh.sem_sinal_de_vida(status, sinal(3600), agora)
    # O limite precisa de folga sobre o intervalo do sinal (atraso, banco lento).
    assert lh.LIMITE_SEM_SINAL >= 4 * lh.INTERVALO_SINAL


@pytest.fixture
def conn_temporaria(monkeypatch):
    app = Flask("teste_limpeza")
    app.config.from_object(Config)
    db.init_app(app)
    with app.app_context():
        with db.engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TEMPORARY TABLE tmp_limpeza_teste ("
                    "id INT PRIMARY KEY AUTO_INCREMENT, upload_id INT, data_arquivo DATETIME, "
                    "exercicio VARCHAR(4), ativo TINYINT(1))"
                )
            )
            monkeypatch.setitem(lh.TABELAS, "tmp_limpeza_teste", {"label": "Teste", "exercicio": "exercicio"})
            monkeypatch.setattr(lh, "TAMANHO_LOTE", 7)  # força vários lotes
            yield conn
            conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tmp_limpeza_teste"))


def test_remocao_de_ponta_a_ponta_em_tabela_temporaria(conn_temporaria):
    conn = conn_temporaria
    linhas = []
    # (upload_id, data, exercicio, ativo, quantidade)
    for upload_id, data, ex, ativo, qtd in [
        (1, "2025-11-03", "25", 0, 10),
        (2, "2025-12-10", "25", 0, 10),
        (3, "2025-12-29", "25", 0, 10),  # mantida
        (4, "2026-01-14", "25", 0, 10),  # posterior ao mês -> fica
        (5, "2025-12-15", "26", 0, 10),  # outro exercício -> fica
        (6, "2026-10-14", "26", 1, 10),  # carga ativa -> fica
    ]:
        linhas += [{"u": upload_id, "d": data, "e": ex, "a": ativo}] * qtd
    conn.execute(
        text("INSERT INTO tmp_limpeza_teste (upload_id, data_arquivo, exercicio, ativo) VALUES (:u, :d, :e, :a)"),
        linhas,
    )
    conn.commit()

    cargas = lh.carregar_cargas(conn, "tmp_limpeza_teste")
    plano = lh.planejar_tabela("tmp_limpeza_teste", cargas, exercicio=2025, ref_mes=12, ref_ano=2025)
    assert plano.mantida.upload_id == 3
    assert sorted(c.upload_id for c in plano.remover) == [1, 2]

    # Simula uma carga reativada entre a análise e a execução: o DELETE
    # (ativo = 0) não pode apagá-la.
    conn.execute(text("UPDATE tmp_limpeza_teste SET ativo = 1 WHERE upload_id = 2 AND id % 2 = 0"))
    conn.commit()

    progresso = []
    removidas = lh.remover_cargas(conn, plano, progresso.append)
    assert removidas == 15  # 10 da carga 1 + 5 inativas da carga 2
    assert progresso and progresso[-1] == 100

    restantes = dict(
        conn.execute(text("SELECT upload_id, COUNT(*) FROM tmp_limpeza_teste GROUP BY upload_id")).fetchall()
    )
    assert restantes == {2: 5, 3: 10, 4: 10, 5: 10, 6: 10}
