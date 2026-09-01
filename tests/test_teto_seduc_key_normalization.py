"""Confirma que o casamento de registros do Teto-SEDUC (Plan 23) usa o codigo
estavel (fonte/grupo/subteto) em vez do texto completo, reproduzindo o cenario
real de hoje: um registro antigo gravado com a fonte em texto "cru" (sem
descricao, porque na epoca FONTE_MAP nao tinha essa chave) deve ser reconhecido
e desativado quando um novo upload traz a mesma fonte ja com a descricao
completa. Usa um exercicio descartavel no banco remoto compartilhado, limpo ao
final.
"""
from __future__ import annotations

import pandas as pd

from app import create_app
from models import db, Momp
from rotas.home_routes import _persistir_plan23
from services.teto_seduc import fonte_key, grupo_key, subteto_key

TEST_EXERCICIO = "9995"


def test_fonte_key():
    assert fonte_key("15460000 - Transferências do FUNDEB - Complementação da União - ETI") == "15460000"
    assert fonte_key("15460000") == "15460000"


def test_grupo_key():
    assert grupo_key("3 - Outras Despesas Corrente") == "3"
    assert grupo_key("1 - Pessoal e Encargos Sociais") == "1"
    assert grupo_key("4 - Investimentos") == "4"


def test_subteto_key():
    assert subteto_key("A - Despesas Obrigatórias") == "A"
    assert subteto_key("c - Prioridades Estratégicas LDO") == "C"


def test_reenvio_reconhece_registro_antigo_com_texto_diferente():
    """Reproduz o cenario real: fonte gravada antes sem descricao (texto cru)
    deve ser desativada quando o novo upload traz a mesma fonte com o nome
    completo - porque agora casa pelo codigo, nao pelo texto."""
    app = create_app()
    with app.app_context():
        try:
            antigo = Momp(
                exercicio=TEST_EXERCICIO,
                fonte="15460000",  # texto "cru", como ficou o registro real antes do fix
                grupo_despesa="3 - Outras Despesas Corrente",
                teto_despesa_momp="4 - A Classificar",
                subteto_despesa_momp="C - Prioridades Estratégicas LDO",
                teto_anual=11178276,
                ativo=True,
            )
            db.session.add(antigo)
            db.session.commit()
            antigo_id = antigo.id

            df = pd.DataFrame(
                [
                    {
                        "exercicio": TEST_EXERCICIO,
                        "fonte": "15460000 - Transferências do FUNDEB - Complementação da União - ETI",
                        "grupo_despesa": "3 - Outras Despesas Corrente",
                        "teto_despesa_momp": "4 - A Classificar",
                        "subteto_despesa_momp": "C - Prioridades Estratégicas LDO",
                        "teto_anual": 11178276,
                    }
                ]
            )
            resultado = _persistir_plan23(df)
            db.session.commit()

            assert resultado["inseridas"] == 1
            assert resultado["desativadas"] == 1, (
                "o registro antigo (texto cru) deveria ter sido reconhecido "
                "e desativado pelo codigo estavel da fonte"
            )

            ativos = (
                Momp.query.filter_by(exercicio=TEST_EXERCICIO, ativo=True).all()
            )
            assert len(ativos) == 1, f"esperado 1 ativo, encontrado {len(ativos)}"

            antigo_recarregado = db.session.get(Momp, antigo_id)
            assert antigo_recarregado.ativo is False
        finally:
            Momp.query.filter_by(exercicio=TEST_EXERCICIO).delete()
            db.session.commit()
