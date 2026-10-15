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


def test_pausada():
    """Execução sem etapas (tela fechada, conexão caiu) - seção 23.9."""
    agora = datetime(2026, 10, 15, 10, 0, 0)
    ultima = lambda seg: agora - timedelta(seconds=seg)  # noqa: E731
    assert not lh.pausada("em andamento", ultima(30), agora)
    assert not lh.pausada("em andamento", ultima(lh.LIMITE_PAUSA), agora)
    assert lh.pausada("em andamento", ultima(lh.LIMITE_PAUSA + 1), agora)
    assert lh.pausada("em andamento", None, agora)
    for status in ("concluida", "falha", "abandonada", "interrompida"):
        assert not lh.pausada(status, ultima(3600), agora)
    # Uma etapa normal (PRAZO_PASSO + folga do servidor) nunca parece pausa.
    assert lh.LIMITE_PAUSA >= 10 * lh.PRAZO_PASSO


def test_mesclar_cargas_soma_as_faixas():
    acumulado = {}
    lh.mesclar_cargas(acumulado, {7: ["2025-12-01T08:00:00", [2025], 10, 0, 100, 109]})
    lh.mesclar_cargas(acumulado, {7: ["2025-12-02T08:00:00", [2025], 5, 2, 110, 114]})
    lh.mesclar_cargas(acumulado, {8: [None, [2025, 2026], 1, 0, 200, 200]})
    assert acumulado["7"] == ["2025-12-02T08:00:00", [2025], 15, 2, 100, 114]
    cargas = {c.upload_id: c for c in lh.cargas_de_agregados(acumulado)}
    assert cargas[7].exercicio == 2025 and cargas[7].ativos == 2 and cargas[7].linhas == 15
    assert cargas[8].exercicio is None  # exercício misto: nunca removida
    # Exercício diferente numa faixa posterior também torna a carga mista.
    lh.mesclar_cargas(acumulado, {7: ["2025-11-30T08:00:00", [2026], 1, 0, 120, 120]})
    assert {c.upload_id: c for c in lh.cargas_de_agregados(acumulado)}[7].exercicio is None
    # Valor que não é exercício (-1) também.
    assert lh.cargas_de_agregados({"9": [None, [-1], 1, 0, 1, 1]})[0].exercicio is None


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


def _popular_intercalado(conn):
    """Mesmas cargas do teste acima, com as linhas intercaladas por id: cada
    carga se espalha por várias faixas de análise e lotes de remoção."""
    por_carga = [
        [{"u": u, "d": d, "e": e, "a": a}] * 10
        for u, d, e, a in [
            (1, "2025-11-03", "25", 0),
            (2, "2025-12-10", "25", 0),
            (3, "2025-12-29", "25", 0),  # mantida
            (4, "2026-01-14", "25", 0),  # posterior ao mês -> fica
            (5, "2025-12-15", "26", 0),  # outro exercício -> fica
            (6, "2026-10-14", "26", 1),  # carga ativa -> fica
        ]
    ]
    linhas = [carga[i] for i in range(10) for carga in por_carga]
    conn.execute(
        text("INSERT INTO tmp_limpeza_teste (upload_id, data_arquivo, exercicio, ativo) VALUES (:u, :d, :e, :a)"),
        linhas,
    )
    conn.commit()


def _passos(conn, estado, ate=None, limite=500):
    """Roda etapas com prazo já vencido (um pedaço de trabalho por etapa),
    passando o estado por JSON como a rota faz entre requisições."""
    import json
    import time

    passos, progressos = 0, []
    while estado["fase"] != "fim" and (ate is None or estado["fase"] != ate):
        lh.executar_passo(conn, estado, time.monotonic())
        estado = json.loads(json.dumps(estado))
        progressos.append(lh.progresso_estado(estado)[1])
        passos += 1
        assert passos < limite, "a execução não avançou"
    return estado, passos, progressos


@pytest.fixture
def faixas_pequenas(monkeypatch):
    monkeypatch.setattr(lh, "FAIXA_INICIAL", 5)
    monkeypatch.setattr(lh, "FAIXA_MIN", 3)
    monkeypatch.setattr(lh, "FAIXA_MAX", 8)


def test_simulacao_em_etapas_igual_a_analise_unica(conn_temporaria, faixas_pequenas):
    conn = conn_temporaria
    _popular_intercalado(conn)
    estado = lh.novo_estado("simulacao", ["tmp_limpeza_teste"], 2025, 12, 2025)
    estado, passos, progressos = _passos(conn, estado)
    assert passos > 5  # 60 linhas em faixas de 3 a 8 ids
    assert progressos == sorted(progressos) and progressos[-1] == 100

    esperado = lh.planejar_tabela(
        "tmp_limpeza_teste", lh.carregar_cargas(conn, "tmp_limpeza_teste"), 2025, 12, 2025
    ).resumo()
    obtido = estado["resultado"][0]
    for chave in ("tamanho_mb", "mb_estimado"):  # tamanho depende do information_schema
        esperado.pop(chave), obtido.pop(chave)
    assert obtido == esperado
    assert obtido["mantida"]["upload_id"] == 3 and obtido["cargas_remover"] == 2
    # Simulação não remove nada.
    assert conn.execute(text("SELECT COUNT(*) FROM tmp_limpeza_teste")).scalar() == 60


def test_limpeza_em_etapas_retomando_do_cursor(conn_temporaria, faixas_pequenas):
    conn = conn_temporaria
    _popular_intercalado(conn)
    estado = lh.novo_estado("limpeza", ["tmp_limpeza_teste"], 2025, 12, 2025)
    estado, _, _ = _passos(conn, estado, ate="remocao")
    assert sorted(estado["rem"]["ids"]) == [1, 2]

    # Uma parte da remoção, depois metade da carga 2 reativada no meio do
    # caminho (entre duas etapas). Os ids da carga 2 são 6k+2; id % 4 = 0
    # pega um sim, um não. A primeira etapa (lote de 7 ids) já apagou o id 2,
    # que não seria reativado.
    lh.executar_passo(conn, estado, 0)
    conn.execute(text("UPDATE tmp_limpeza_teste SET ativo = 1 WHERE upload_id = 2 AND id % 4 = 0"))
    conn.commit()
    estado, passos, progressos = _passos(conn, estado)
    assert passos >= 2  # lotes de 7 ids: várias etapas
    assert progressos == sorted(progressos) and progressos[-1] == 100

    restantes = dict(
        conn.execute(text("SELECT upload_id, COUNT(*) FROM tmp_limpeza_teste GROUP BY upload_id")).fetchall()
    )
    assert restantes == {2: 5, 3: 10, 4: 10, 5: 10, 6: 10}  # carga 2: só as 5 reativadas
    assert estado["linhas"] == 15 == estado["resultado"][0]["linhas_removidas"]

    # Rodar de novo depois do fim não faz nada.
    lh.executar_passo(conn, estado, 0)
    assert estado["fase"] == "fim" and estado["linhas"] == 15
