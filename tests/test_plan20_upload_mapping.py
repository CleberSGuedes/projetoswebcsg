"""Cobre o mapeamento de colunas da rota de upload do Plan20
(rotas/home_routes.py, _montar_dataframe_plan20_seduc/_combos_uo_exercicio_plan20).

Achado grave nesta investigacao (docs/claude.md, secao 18): o antigo
col_map, embutido direto na rota, tinha as chaves acentuadas corrompidas
(mojibake) desde 17/04/2026 - nunca detectado porque o ultimo upload real
tinha sido em 16/01/2026, antes da corrupcao. Isso fazia 24 das 53 colunas
saírem NULL no banco silenciosamente, sem nenhum erro, incluindo
exercicio/unidade_orcamentaria (o que tambem quebrava a desativacao da
versao anterior, ja que o filtro ficava vazio).

Este teste usa EXTR_HEADERS (services/plan20_runner.py) como fonte da
verdade dos nomes de coluna reais - se o parser e o mapa da rota saírem
de sincronia de novo, esse teste quebra em vez de deixar passar batido.
Nao toca no banco - so exercita as funcoes puras extraidas da rota.
"""
import pandas as pd

from app import create_app
from rotas.home_routes import (
    _plan20_seduc_col_map,
    _montar_dataframe_plan20_seduc,
    _combos_uo_exercicio_plan20,
    PLAN20_RELATORIO_COLUNAS,
)
from services.plan20_runner import EXTR_HEADERS


def _linha_sintetica() -> dict:
    """Uma linha com um valor de teste distinto em CADA coluna que a aba
    Plan20_SEDUC de verdade tem - via as chaves do col_map, não só
    EXTR_HEADERS (que não inclui "Chave de Planejamento" e as colunas
    derivadas dela/da Natureza, adicionadas depois, direto em
    run_plan20()) -, pra garantir que cada uma sobrevive ao casamento de
    nome -> coluna do banco."""
    return {header: f"valor de teste: {header}" for header in _plan20_seduc_col_map()}


def test_col_map_cobre_todas_as_colunas_do_extr_headers():
    # Se o parser (EXTR_HEADERS) ganhar uma coluna nova sem o col_map da
    # rota ser atualizado junto, essa coluna nunca chega no banco -
    # exatamente o tipo de dessincronia que causou o bug do mojibake.
    col_map = _plan20_seduc_col_map()
    normalizadas = set(col_map.keys())
    faltando = [h for h in EXTR_HEADERS if h not in normalizadas]
    assert not faltando, f"colunas do EXTR_HEADERS sem entrada no col_map: {faltando}"


def test_montar_dataframe_nao_perde_nenhuma_coluna():
    app = create_app()
    with app.app_context():
        df = pd.DataFrame([_linha_sintetica()])
        resultado = _montar_dataframe_plan20_seduc(df, data_arquivo=None, user_email="teste@local")

        col_map = _plan20_seduc_col_map()
        colunas_numericas = {"Exercício", "Quantidade", "Valor Unitário", "Valor Total"}
        for header, db_col in col_map.items():
            assert db_col in resultado.columns, f"coluna do banco '{db_col}' (de '{header}') ausente no resultado"
            if header in colunas_numericas:
                # convertidas pra numerico (valor sintetico nao-numerico
                # vira NaN de proposito) - conferidas em teste a parte.
                continue
            valor = resultado.iloc[0][db_col]
            assert valor == f"valor de teste: {header}", (
                f"coluna '{db_col}' (de '{header}') não recebeu o valor esperado - "
                f"veio {valor!r} (nulo = mapeamento quebrado, o bug do mojibake)"
            )

        # Colunas de metadado preenchidas.
        assert resultado.iloc[0]["user_email"] == "teste@local"
        assert bool(resultado.iloc[0]["ativo"]) is True
        assert resultado.iloc[0]["data_atualizacao"] is not None


def test_montar_dataframe_converte_numericos_pt_br():
    app = create_app()
    with app.app_context():
        linha = _linha_sintetica()
        linha["Exercício"] = "2027"
        linha["Quantidade"] = "1,50"
        linha["Valor Unitário"] = "1.234,56"
        linha["Valor Total"] = "1.851,84"
        df = pd.DataFrame([linha])
        resultado = _montar_dataframe_plan20_seduc(df, data_arquivo=None, user_email="teste@local")

        assert resultado.iloc[0]["exercicio"] == 2027
        assert resultado.iloc[0]["ano"] == 2027
        assert float(resultado.iloc[0]["quantidade"]) == 1.5
        assert float(resultado.iloc[0]["valor_unitario"]) == 1234.56
        assert float(resultado.iloc[0]["valor_total"]) == 1851.84


def test_combos_uo_exercicio():
    app = create_app()
    with app.app_context():
        linha1 = _linha_sintetica()
        linha1["Exercício"] = "2027"
        linha1["Unidade Orçamentária"] = "14101 - SECRETARIA DE ESTADO DE EDUCAÇÃO"
        linha2 = _linha_sintetica()
        linha2["Exercício"] = "2027"
        linha2["Unidade Orçamentária"] = "14601 - FUNDO EST DE APOIO..."
        df = pd.DataFrame([linha1, linha2])
        resultado = _montar_dataframe_plan20_seduc(df, data_arquivo=None, user_email="teste@local")

        combos = _combos_uo_exercicio_plan20(resultado)
        assert combos == {
            ("14101 - SECRETARIA DE ESTADO DE EDUCAÇÃO", 2027),
            ("14601 - FUNDO EST DE APOIO...", 2027),
        }


def test_relatorio_colunas_bate_com_col_map():
    # O relatorio (tela + download, rotas/home_routes.py) tem sua propria
    # lista de colunas (PLAN20_RELATORIO_COLUNAS), unificada em 2026-09 a
    # partir de 4 listas independentes que existiam antes (docs/claude.md,
    # secao 19). Se ela saír de sincronia com o col_map do upload - uma
    # coluna gravada no banco mas ausente do relatorio, ou vice-versa -,
    # esse teste quebra em vez de deixar passar batido, mesmo espirito do
    # teste acima para o EXTR_HEADERS.
    colunas_relatorio = {col for _, col, _ in PLAN20_RELATORIO_COLUNAS}
    colunas_upload = set(_plan20_seduc_col_map().values())
    assert colunas_relatorio == colunas_upload, (
        f"so no relatorio: {colunas_relatorio - colunas_upload}; "
        f"so no upload: {colunas_upload - colunas_relatorio}"
    )

    # Cada rotulo e cada coluna de banco aparecem uma unica vez - se algo
    # for duplicado (o que ja aconteceu no relatorio antigo por engano
    # entre "Eixo" e "Eixo do Programa" antes da renomeacao), o SELECT
    # gerado teria uma coluna repetida.
    labels = [label for label, _, _ in PLAN20_RELATORIO_COLUNAS]
    colunas = [col for _, col, _ in PLAN20_RELATORIO_COLUNAS]
    assert len(labels) == len(set(labels))
    assert len(colunas) == len(set(colunas))

    tipos_validos = {"text", "num", "int"}
    assert all(tipo in tipos_validos for _, _, tipo in PLAN20_RELATORIO_COLUNAS)
