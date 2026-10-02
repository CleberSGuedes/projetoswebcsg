"""Teto-SEDUC com duas UOs (14101 e 14601) - docs/claude.md, secao 20.

- Leitura de UO/exercicio/total do proprio relatorio (ler_cabecalho_fiplan) e
  as recusas de validar_cabecalho_fiplan - planilhas sinteticas, sem banco.
- processar_plan134 descartando linhas de valor zero e normalizando o nome da
  Acao por regra (normalizar_acao) - sem banco.
- Gravacao restrita a exercicio + UO (_persistir_plan23/_persistir_plan134) -
  usa exercicios descartaveis (9991/9990) no banco remoto compartilhado e
  remove tudo ao final, mesmo padrao de test_teto_seduc_key_normalization.py.
"""
from __future__ import annotations

from decimal import Decimal

import pandas as pd
import pytest

from app import create_app
from models import db, Momp, PoliticaTeto
from rotas.home_routes import (
    _persistir_plan134,
    _persistir_plan23,
    _teto_mensagem_conferencia,
    _teto_money,
)
from services.teto_seduc import (
    ler_cabecalho_fiplan,
    normalizar_acao,
    normalizar_grupo,
    processar_plan134,
    validar_cabecalho_fiplan,
)

EXERCICIO = "9991"
EXERCICIO_OUTRO = "9990"

# Copia do antigo ACAO_PLAN134_MAP (lista fixa removida na secao 20): a regra
# nova tem que dar exatamente o mesmo resultado para todas as 27 entradas.
ACAO_PLAN134_MAP_ANTIGO = {
    "2009 -Manutenção de ações de informática": "2009 - Manutenção de ações de informática",
    "2010 -Manutenção de órgãos colegiados": "2010 - Manutenção de órgãos colegiados",
    "2014 -Publicidade institucional e propaganda": "2014 - Publicidade institucional e propaganda",
    "2284 -Manutenção do Conselho Estadual de Educação - CEE": "2284 - Manutenção do Conselho Estadual de Educação - CEE",
    "2895 -Alimentação Escolar da Educação de Jovens e Adultos": "2895 - Alimentação Escolar da Educação de Jovens e Adultos",
    "2897 -Alimentação Escolar da Educação Especial": "2897 - Alimentação Escolar da Educação Especial",
    "2898 -Alimentação Escolar do Ensino Fundamental": "2898 - Alimentação Escolar do Ensino Fundamental",
    "2899 -Alimentação Escolar do Ensino Médio": "2899 - Alimentação Escolar do Ensino Médio",
    "2900 -Desenvolvimento da Educação de Jovens e Adultos": "2900 - Desenvolvimento da Educação de Jovens e Adultos",
    "2936 -Desenvolvimento das Modalidades de Ensino": "2936 - Desenvolvimento das Modalidades de Ensino",
    "2957 -Desenvolvimento da Educação Especial": "2957 - Desenvolvimento da Educação Especial",
    "4172 -Desenvolvimento do Ensino Fundamental": "4172 - Desenvolvimento do Ensino Fundamental",
    "4173 -Infraestrutura do Ensino Fundamental": "4173 - Infraestrutura do Ensino Fundamental",
    "4174 -Desenvolvimento do Ensino Médio": "4174 - Desenvolvimento do Ensino Médio",
    "4175 -Infraestrutura da Educação de Jovens e Adultos": "4175 - Infraestrutura da Educação de Jovens e Adultos",
    "4177 -Infraestrutura do Ensino Médio": "4177 - Infraestrutura do Ensino Médio",
    "4178 -Infraestrutura da Educação Especial": "4178 - Infraestrutura da Educação Especial",
    "4179 -Transporte Escolar da Educação Especial": "4179 - Transporte Escolar da Educação Especial",
    "4180 -Infraestrutura de Administração e Gestão": "4180 - Infraestrutura de Administração e Gestão",
    "4181 -Transporte Escolar do Ensino Fundamental": "4181 - Transporte Escolar do Ensino Fundamental",
    "4182 -Transporte Escolar do Ensino Médio": "4182 - Transporte Escolar do Ensino Médio",
    "4491 -Pagamento de verbas indenizatórias a servidores estaduais.": "4491 - Pagamento de verbas indenizatórias a servidores estaduais",
    "4524 -FMTE - Ensino Fundamental": "4524 - FMTE - Ensino Fundamental",
    "4525 -FMTE - Educação Infantil": "4525 - FMTE - Educação Infantil",
    "8002 -Recolhimento do PIS-PASEP e pagamento do abono": "8002 - Recolhimento do PIS-PASEP e pagamento do abono",
    "8003 -Cumprimento de sentenças judiciais transitadas em julgado - Adm. Direta": "8003 - Cumprimento de sentenças judiciais transitadas em julgado - Adm. Direta",
    "8040 -Recolhimento de encargos e obrigações previdenciárias de inativos e pensionistas do Estado de Mato Grosso": "8040 - Recolhimento de encargos e obrigações previdenciárias de inativos e pensionistas do Estado de Mato Grosso",
}

PLAN134_COLS = [
    "U.O", "Prog. Governo", "Função", "Ação (PAOE)", "Subfunção", "Esfera",
    "Produto da Ação", "Unid. de Medida", "Meta Ação", "Subação/entrega", "UG",
    "Etapa", "Item Despesa", "Fonte", "Cat.Econ", "Grupo", "Modalidade",
    "Elemento", "Subelemento", "Iduso", "Região", "Tipificação da Despesa",
    "Valor PTA", "Motivo Análise Não OK",
]
CHAVE = "* R600 * 361.2 * SAAS * INFRAESTRUTURA * P_INFRA_ * E_INFRA * _INFRA * XII *"


# ---------------------------------------------------------------------------
# Planilhas sinteticas no layout real do FIPLAN
# ---------------------------------------------------------------------------
def _salvar(rows, path, ncols):
    pd.DataFrame([r + [None] * (ncols - len(r)) for r in rows]).to_excel(
        path, header=False, index=False
    )
    return path


def _plan23_xlsx(path, uo="14601", exercicio=EXERCICIO, uo_linha=None, total="52.000.000,00"):
    uo_linha = uo_linha or uo
    rows = [
        ["Teto Orçamentário PTA"],
        [],
        [f"*Exercício igual a {exercicio}"],
        ["*Utilização do Teto (1-PTA / 2-LDO) igual a 1"],
        [f"Código da Unidade Orçamentária igual a {uo}"],
        [f"UO : {uo_linha} - FUNDO EST DE APOIO À MELHORIA"],
        [],
        ["FONTE", "GRUPO DE DESPESA / QUADRO ORÇAMENTÁRIO", "TETO ANUAL", "SALDO ANUAL"],
        ["15000000", "4 - INVESTIMENTOS", "2.000.000,00", "0,00"],
        [None, "a. Despesas Obrigatórias", "2.000.000,00", "0,00"],
        [None, "Total da Fonte:", "2.000.000,00", "0,00"],
        ["15460000", "3 - OUTRAS DESPESAS CORRENTES", "50.000.000,00", "0,00"],
        [None, "c. Despesas Prioridades Estratégicas", "50.000.000,00", "0,00"],
        [None, "Total da Fonte:", "50.000.000,00", "0,00"],
        [],
        [None, "Total da UO:", total, "0,00"],
    ]
    return _salvar(rows, path, 4)


def _linha134(uo_texto, acao, fonte, grupo, tipificacao, valor, sub="1-"):
    row = [None] * len(PLAN134_COLS)
    valores = {
        "U.O": uo_texto,
        "Ação (PAOE)": acao,
        "Produto da Ação": "Produto teste",
        "Subação/entrega": f"{sub}{CHAVE} Subacao teste",
        "Fonte": fonte,
        "Grupo": grupo,
        "Tipificação da Despesa": tipificacao,
        "Valor PTA": valor,
    }
    for col, val in valores.items():
        row[PLAN134_COLS.index(col)] = val
    return row


def _plan134_xlsx(path, uo="14601", exercicio=EXERCICIO, uo_linhas=None, zero=True):
    uo_linhas = uo_linhas or [f"{uo} - FMTE - MT"]
    rows = [
        ["PLAN 134 - Emitir PTA Detalhado por UO, Programa, Ação, Subação e Etapa"],
        [],
        [f"*Exercício igual a {exercicio}"],
        [f"Código da Unidade Orçamentária igual a {uo}"],
        list(PLAN134_COLS),
    ]
    for i, uo_texto in enumerate(uo_linhas):
        rows.append(
            _linha134(
                uo_texto, "4545 -FMTE - Ensino MÉDIO",
                "15460000 - Transferências do FUNDEB - Complementação da União - ETI",
                "3-OUTRAS DESPESAS CORRENTES", "Despesas Prioridades Estratégicas",
                "30000000", sub=f"{i + 1}-",
            )
        )
    if zero:
        # Subacao sem item de despesa: fonte "-" e valor 0 - deve ser descartada.
        rows.append(_linha134(uo_linhas[0], "4545 -FMTE - Ensino MÉDIO", "-", "-", "-", "0", sub="9-"))
    rows.append([f"SUBTOTAL UO {uo}:\xa0\xa0\xa030.000.000,00"])
    rows.append(["TOTAL GERAL 30.000.000,00"])
    return _salvar(rows, path, len(PLAN134_COLS))


# ---------------------------------------------------------------------------
# normalizar_acao
# ---------------------------------------------------------------------------
def test_normalizar_acao_igual_ao_mapa_antigo():
    for bruto, esperado in ACAO_PLAN134_MAP_ANTIGO.items():
        assert normalizar_acao(bruto) == esperado
        # idempotente: valor ja normalizado nao muda
        assert normalizar_acao(esperado) == esperado


def test_normalizar_grupo_unifica_pelo_codigo():
    # 2025/2026 gravados com "Corrente" (sem S) e 2027 com "Correntes" tem que
    # virar um grupo so no dashboard (docs/claude.md, secao 20.8).
    assert normalizar_grupo("3 - Outras Despesas Corrente") == "3 - Outras Despesas Correntes"
    assert normalizar_grupo("3 - Outras Despesas Correntes") == "3 - Outras Despesas Correntes"
    assert normalizar_grupo("3 - OUTRAS DESPESAS CORRENTES") == "3 - Outras Despesas Correntes"
    assert normalizar_grupo("1 - Pessoal e Encargos Sociais") == "1 - Pessoal e Encargos Sociais"
    assert normalizar_grupo("4 - Investimentos") == "4 - Investimentos"
    assert normalizar_grupo("9 - Grupo novo") == "9 - Grupo novo"
    assert normalizar_grupo(None) == ""


def test_normalizar_acao_casos_novos():
    assert normalizar_acao("4541 -Educação que Protege Meninas") == "4541 - Educação que Protege Meninas"
    assert normalizar_acao("4545 -FMTE - Ensino MÉDIO") == "4545 - FMTE - Ensino MÉDIO"
    assert normalizar_acao("  4537 -  Desenvolvimento  do Regime ") == "4537 - Desenvolvimento do Regime"
    assert normalizar_acao("Sem codigo numerico") == "Sem codigo numerico"
    assert normalizar_acao(None) == ""


# ---------------------------------------------------------------------------
# ler_cabecalho_fiplan / validar_cabecalho_fiplan
# ---------------------------------------------------------------------------
def test_cabecalho_plan23(tmp_path):
    cab = ler_cabecalho_fiplan(_plan23_xlsx(tmp_path / "p23.xlsx"))
    assert cab["exercicio"] == EXERCICIO
    assert cab["uo_filtro"] == "14601"
    assert cab["uos_conteudo"] == {"14601"}
    assert cab["total_relatorio"] == 52000000.0
    assert validar_cabecalho_fiplan(cab, EXERCICIO) == "14601"


def test_cabecalho_plan134(tmp_path):
    cab = ler_cabecalho_fiplan(_plan134_xlsx(tmp_path / "p134.xlsx"))
    assert cab["exercicio"] == EXERCICIO
    assert cab["uo_filtro"] == "14601"
    assert cab["uos_conteudo"] == {"14601"}
    assert cab["total_relatorio"] == 30000000.0
    assert validar_cabecalho_fiplan(cab, EXERCICIO) == "14601"


def test_cabecalho_aceita_uo_com_ponto(tmp_path):
    cab = ler_cabecalho_fiplan(_plan23_xlsx(tmp_path / "p23.xlsx", uo="14.101"))
    assert validar_cabecalho_fiplan(cab, EXERCICIO) == "14101"


def test_recusa_exercicio_divergente(tmp_path):
    cab = ler_cabecalho_fiplan(_plan23_xlsx(tmp_path / "p23.xlsx"))
    with pytest.raises(ValueError, match="exercício informado"):
        validar_cabecalho_fiplan(cab, "2026")


def test_recusa_uo_nao_cadastrada(tmp_path):
    cab = ler_cabecalho_fiplan(_plan23_xlsx(tmp_path / "p23.xlsx", uo="99999"))
    with pytest.raises(ValueError, match="não está cadastrada"):
        validar_cabecalho_fiplan(cab, EXERCICIO)


def test_recusa_filtro_e_conteudo_divergentes(tmp_path):
    cab = ler_cabecalho_fiplan(
        _plan23_xlsx(tmp_path / "p23.xlsx", uo="14601", uo_linha="14101")
    )
    with pytest.raises(ValueError, match="mais de uma Unidade"):
        validar_cabecalho_fiplan(cab, EXERCICIO)


def test_recusa_plan134_com_duas_uos(tmp_path):
    cab = ler_cabecalho_fiplan(
        _plan134_xlsx(tmp_path / "p134.xlsx", uo_linhas=["14601 - FMTE - MT", "14101 - SEDUC"])
    )
    with pytest.raises(ValueError, match="mais de uma Unidade"):
        validar_cabecalho_fiplan(cab, EXERCICIO)


def test_recusa_sem_uo():
    with pytest.raises(ValueError, match="identificar a Unidade"):
        validar_cabecalho_fiplan(
            {"exercicio": EXERCICIO, "uo_filtro": None, "uos_conteudo": set()}, EXERCICIO
        )


# ---------------------------------------------------------------------------
# processar_plan134
# ---------------------------------------------------------------------------
def test_plan134_descarta_valor_zero_e_normaliza_acao(tmp_path):
    df = processar_plan134(_plan134_xlsx(tmp_path / "p134.xlsx"))
    assert len(df) == 1, "a linha de valor 0 (fonte '-') deveria ter sido descartada"
    assert df.iloc[0]["teto_politica_decreto"] == 30000000.0
    assert df.iloc[0]["acao_paoe"] == "4545 - FMTE - Ensino MÉDIO"


# ---------------------------------------------------------------------------
# Mensagens
# ---------------------------------------------------------------------------
def test_teto_money_e_conferencia():
    assert _teto_money(Decimal("6485317811")) == "R$ 6.485.317.811,00"
    assert _teto_money(Decimal("-30341313.5")) == "R$ -30.341.313,50"
    assert "confere" in _teto_mensagem_conferencia(Decimal("100"), 100.0)
    assert "diferença de R$ -10,00" in _teto_mensagem_conferencia(Decimal("90"), 100.0)
    assert _teto_mensagem_conferencia(Decimal("90"), None) == ""


# ---------------------------------------------------------------------------
# Gravacao restrita a exercicio + UO (banco real, exercicios descartaveis)
# ---------------------------------------------------------------------------
def _limpar():
    ids = [
        m.id
        for m in Momp.query.filter(Momp.exercicio.in_([EXERCICIO, EXERCICIO_OUTRO])).all()
    ]
    if ids:
        PoliticaTeto.query.filter(PoliticaTeto.momp_id.in_(ids)).delete(
            synchronize_session=False
        )
        Momp.query.filter(Momp.id.in_(ids)).delete(synchronize_session=False)
    db.session.commit()


def _momp(uo, fonte, grupo, subteto, valor, exercicio=EXERCICIO):
    item = Momp(
        exercicio=exercicio,
        uo=uo,
        fonte=fonte,
        grupo_despesa=grupo,
        teto_despesa_momp="4 - A Classificar",
        subteto_despesa_momp=subteto,
        teto_anual=valor,
        ativo=True,
    )
    db.session.add(item)
    return item


def _df23(linhas):
    return pd.DataFrame(
        [
            {
                "exercicio": EXERCICIO,
                "fonte": fonte,
                "grupo_despesa": grupo,
                "teto_despesa_momp": "4 - A Classificar",
                "subteto_despesa_momp": subteto,
                "teto_anual": valor,
            }
            for fonte, grupo, subteto, valor in linhas
        ]
    )


def _df134(linhas):
    return pd.DataFrame(
        [
            {
                "regiao": "R600", "subfuncao_ug": "361.2", "adj": "SAAS",
                "macropolitica": "INFRA", "pilar": "P", "eixo": "E",
                "politica_decreto": "POL", "publico_transversal": "XII",
                "chave_planejamento": CHAVE, "acao_paoe": "4545 - FMTE - Ensino MÉDIO",
                "fonte": fonte, "grupo_despesa": grupo,
                "subteto_despesa_momp": subteto, "teto_politica_decreto": valor,
            }
            for fonte, grupo, subteto, valor in linhas
        ]
    )


def test_plan23_de_uma_uo_nao_desativa_a_outra():
    """Cenario que motivou a secao 20: subir o Plan 23 da 14601 nao pode
    tratar o teto da 14101 como 'sumido do arquivo'."""
    app = create_app()
    with app.app_context():
        try:
            _limpar()
            sedu = _momp("14101", "15001001 - MDE", "3 - Outras Despesas Correntes",
                         "D - Essenciais Finalísticas", 1000)
            outro_ex = _momp("14601", "15000000 - Imp", "4 - Investimentos",
                             "A - Despesas Obrigatórias", 7, exercicio=EXERCICIO_OUTRO)
            db.session.commit()
            sedu_id, outro_ex_id = sedu.id, outro_ex.id

            resultado = _persistir_plan23(
                _df23([("15000000 - Imp", "4 - Investimentos", "A - Despesas Obrigatórias", 2000000)]),
                "14601",
            )
            db.session.commit()

            assert resultado["inseridas"] == 1
            assert resultado["removidos"] == 0, "nao pode remover registros de outra UO"
            assert resultado["total_gravado"] == Decimal("2000000")
            assert db.session.get(Momp, sedu_id).ativo is True
            assert db.session.get(Momp, outro_ex_id).ativo is True, "outro exercicio intocado"
            novo = Momp.query.filter_by(exercicio=EXERCICIO, uo="14601", ativo=True).one()
            assert novo.teto_anual == Decimal("2000000")

            # Reenvio da 14601 sem a combinacao: remove so dentro da 14601.
            resultado = _persistir_plan23(
                _df23([("15460000 - ETI", "3 - Outras Despesas Correntes",
                        "C - Prioridades Estratégicas LDO", 50000000)]),
                "14601",
            )
            db.session.commit()
            assert resultado["removidos"] == 1
            assert db.session.get(Momp, sedu_id).ativo is True
        finally:
            _limpar()


def test_plan134_casa_so_com_teto_da_mesma_uo_e_nao_deixa_orfaos():
    app = create_app()
    with app.app_context():
        try:
            _limpar()
            # Mesma combinacao nas duas UOs - o Plan 134 da 14601 so pode
            # vincular ao MOMP da 14601.
            m14101 = _momp("14101", "15460000 - ETI", "3 - Outras Despesas Correntes",
                           "C - Prioridades Estratégicas LDO", 999)
            m14601_c = _momp("14601", "15460000 - ETI", "3 - Outras Despesas Correntes",
                             "C - Prioridades Estratégicas LDO", 50000000)
            m14601_a = _momp("14601", "15000000 - Imp", "4 - Investimentos",
                             "A - Despesas Obrigatórias", 2000000)
            db.session.commit()
            db.session.add(PoliticaTeto(momp_id=m14101.id, teto_politica_decreto=999, ativo=True))
            db.session.commit()

            # 1o envio: duas combinacoes + uma sem teto na 14601 (15740000).
            resultado = _persistir_plan134(
                _df134([
                    ("15460000 - ETI", "3 - Outras Despesas Correntes", "C - Prioridades Estratégicas LDO", 30000000),
                    ("15000000 - Imp", "4 - Investimentos", "A - Despesas Obrigatórias", 2000000),
                    ("15740000 - Op Cred", "3 - Outras Despesas Correntes", "B - Essenciais à Manutenção da Unidade", 13900000),
                ]),
                EXERCICIO,
                "14601",
            )
            db.session.commit()
            assert resultado["inseridas"] == 2
            assert resultado["total_gravado"] == Decimal("32000000")
            assert resultado["nao_gravados"] == {("15740000", "3", "B"): Decimal("13900000")}
            vinculos = {p.momp_id for p in PoliticaTeto.query.filter(
                PoliticaTeto.momp_id.in_([m14601_c.id, m14601_a.id]), PoliticaTeto.ativo == True  # noqa: E712
            )}
            assert vinculos == {m14601_c.id, m14601_a.id}

            # 2o envio: a combinacao 15000000/4/A sumiu - o PTA antigo dela
            # nao pode ficar ativo (orfao), e o PTA da 14101 nao e tocado.
            resultado = _persistir_plan134(
                _df134([
                    ("15460000 - ETI", "3 - Outras Despesas Correntes", "C - Prioridades Estratégicas LDO", 30000000),
                ]),
                EXERCICIO,
                "14601",
            )
            db.session.commit()
            assert resultado["desativadas"] == 2
            ativos_14601 = PoliticaTeto.query.filter(
                PoliticaTeto.momp_id.in_([m14601_c.id, m14601_a.id]), PoliticaTeto.ativo == True  # noqa: E712
            ).all()
            assert [p.momp_id for p in ativos_14601] == [m14601_c.id]
            assert PoliticaTeto.query.filter_by(momp_id=m14101.id, ativo=True).count() == 1
        finally:
            _limpar()


def test_plan134_sem_teto_da_uo_recusa_sem_desativar():
    app = create_app()
    with app.app_context():
        try:
            _limpar()
            m14101 = _momp("14101", "15460000 - ETI", "3 - Outras Despesas Correntes",
                           "C - Prioridades Estratégicas LDO", 999)
            db.session.commit()
            db.session.add(PoliticaTeto(momp_id=m14101.id, teto_politica_decreto=999, ativo=True))
            db.session.commit()
            with pytest.raises(ValueError, match="Carregue primeiro o Plan 23 desta UO"):
                _persistir_plan134(
                    _df134([("15460000 - ETI", "3 - Outras Despesas Correntes",
                             "C - Prioridades Estratégicas LDO", 1)]),
                    EXERCICIO,
                    "14601",
                )
            db.session.rollback()
            assert PoliticaTeto.query.filter_by(momp_id=m14101.id, ativo=True).count() == 1
        finally:
            _limpar()
