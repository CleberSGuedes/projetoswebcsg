from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from sqlalchemy import inspect

from app import create_app
from services.teto_seduc import processar_plan23, processar_plan134


def main():
    with TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)

        plan23_path = root / "plan23.xlsx"
        plan23_rows = [
            ["Relatório Plan 23", None, None, None],
            ["FONTE", "GRUPO DE DESPESA / QUADRO ORÇAMENTÁRIO", "TETO ANUAL", "SALDO ANUAL"],
            ["15000000", "3 - OUTRAS DESPESAS CORRENTES", None, None],
            [None, "a. Despesas Obrigatórias", "1.234,56", "0,00"],
            [None, "Total da Fonte", None, None],
        ]
        pd.DataFrame(plan23_rows).to_excel(plan23_path, index=False, header=False)
        plan23 = processar_plan23(plan23_path, "2026")
        assert len(plan23) == 1
        assert plan23.iloc[0]["exercicio"] == "2026"
        assert float(plan23.iloc[0]["teto_anual"]) == 1234.56
        assert plan23.iloc[0]["subteto_despesa_momp"] == "A - Despesas Obrigatórias"

        plan134_path = root / "plan134.xlsx"
        plan134_source = pd.DataFrame(
            [
                {
                    "Ação (PAOE)": "2009 - Manutenção",
                    "Produto da Ação": "Produto",
                    "Subação/entrega": (
                        "*Região I*123-14101*ADJ*Macro*Pilar*Eixo*Política*Público* Entrega"
                    ),
                    "Fonte": "15000000",
                    "Grupo": "3-OUTRAS DESPESAS CORRENTES",
                    "Tipificação da Despesa": "Despesas Obrigatórias",
                    "Valor PTA": "2.500,00",
                }
            ]
        )
        plan134_source.to_excel(plan134_path, index=False)
        plan134 = processar_plan134(plan134_path)
        assert len(plan134) == 1
        assert plan134.iloc[0]["publico_transversal"] == "Público"
        assert plan134.iloc[0]["politica_decreto"] == "Política"
        assert float(plan134.iloc[0]["teto_politica_decreto"]) == 2500.0

    app = create_app()
    rules = {rule.rule for rule in app.url_map.iter_rules()}
    assert "/partial/atualizar/teto-seduc" in rules
    assert "/api/teto-seduc/upload" in rules
    assert "/api/teto-seduc/status/<int:job_id>" in rules

    with app.app_context():
        schema = inspect(app.extensions["sqlalchemy"].engine)
        momp_columns = {column["name"] for column in schema.get_columns("momp")}
        politica_columns = {
            column["name"] for column in schema.get_columns("politicateto")
        }
        assert {
            "id",
            "fonte",
            "grupo_despesa",
            "teto_despesa_momp",
            "subteto_despesa_momp",
            "teto_anual",
            "ativo",
            "alterado_em",
            "excluido_em",
            "exercicio",
        }.issubset(momp_columns)
        assert {
            "id",
            "momp_id",
            "regiao",
            "subfuncao_ug",
            "adj",
            "macropolitica",
            "pilar",
            "eixo",
            "politica_decreto",
            "acao_paoe",
            "teto_politica_decreto",
            "chave_planejamento",
            "saldo_anual",
            "ativo",
            "alterado_em",
            "excluido_em",
            "publico_transversal",
        }.issubset(politica_columns)

    print("teto_seduc processors, routes and schema: ok")


if __name__ == "__main__":
    main()
