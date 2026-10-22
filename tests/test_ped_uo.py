"""PED por UO (docs/plano_implementacao_novas_uos.md, Fase 1).

Sem tocar no banco: o recálculo de dotação usa objetos falsos no lugar das
consultas, e a saída é conferida em DataFrames sintéticos.
"""

from types import SimpleNamespace

import pandas as pd
import pytest

from services import ped_runner as pr
from services.uo import UORecusada


def _ped(uo, paoe="8026", dot="DOT.2026.14101.01.0001.01.0001"):
    return pd.Series(
        {
            "UO": uo,
            "Exercício": "2026",
            "Nº PED Estorno/Estornado": "",
            "Nº EMP": "",
            "Histórico": "NÃO INFORMADO",
            "PAOE": paoe,
            "Dotação Orçamentária": "2026.14101.12.361.0036.8026.1.44905200.15000000.0.80",
        }
    )


def _chave(df_row):
    df = pd.DataFrame([df_row])
    return pr.identificar_chave_planejamento(df, [], {}).loc[0, "Chave"]


def test_regra_do_8026_so_vale_para_a_secretaria():
    # dotação com região na 7ª parte: ...8026.<regiao>... (partes_dotacao[6])
    dot = "2026.14101.12.361.0036.8026.99.44905200.15000000.0.80"
    sed = _ped("14101")
    sed["Dotação Orçamentária"] = dot
    assert _chave(sed).startswith("* R99 * 126.1 * SAAS")
    sed601 = _ped("14601")
    sed601["Dotação Orçamentária"] = dot
    assert _chave(sed601).startswith("* R99")
    outra = _ped("26201")
    outra["Dotação Orçamentária"] = dot
    assert _chave(outra) == "NÃO IDENTIFICADO"


def test_forcar_chave_so_para_a_secretaria():
    df = pd.DataFrame(
        {
            "Nº PED": ["100", "100"],
            "UO": ["14101", "26201"],
            "Chave de Planejamento": ["-", "-"],
        }
    )
    out = pr.forcar_chaves_manualmente(df, {"100": "* CHAVE FORCADA *"})
    assert list(out["Chave de Planejamento"]) == ["* CHAVE FORCADA *", "-"]


def test_identificar_uo_e_exercicios_do_relatorio_bruto():
    df = pd.DataFrame({"UO": ["14101", "14101"], "Exercício": ["2026", "2026"]})
    assert pr.identificar_uo_e_exercicios(df) == ("14101", [2026])
    with pytest.raises(UORecusada, match="mais de uma UO"):
        pr.identificar_uo_e_exercicios(pd.DataFrame({"UO": ["14101", "14601"], "Exercício": ["2026"] * 2}))
    with pytest.raises(UORecusada, match="ainda não está liberada"):
        pr.identificar_uo_e_exercicios(pd.DataFrame({"UO": ["26201"], "Exercício": ["2026"]}))
    with pytest.raises(UORecusada, match="Coluna UO"):
        pr.identificar_uo_e_exercicios(pd.DataFrame({"Exercício": ["2026"]}))


def test_colunas_da_dotacao_e_da_chave_sempre_existem():
    df = pd.DataFrame(
        {
            "Exercício": ["2026"],
            "Dotação Orçamentária": ["2026.14101.12.361.0036.8026.1.44905200.15000000.0.80"],
            "Histórico": ["x"],
            "Credor": ["1"],
            "Nome do Credor": ["A"],
            "Natureza de Despesa": ["44905200"],
            "Chave": ["NÃO IDENTIFICADO"],  # nenhuma linha com chave
        }
    )
    out = pr.adicionar_novas_colunas(df)
    out = pr.preencher_novas_colunas(out)
    for col in ("Região", "Subfunção + UG", "Política_Decreto", "Função", "PAOE", "Fonte", "Iduso", "Cat.Econ"):
        assert col in out.columns
    assert out.loc[0, "PAOE"] == "8026"


class _FakeQuery:
    def __init__(self, rows):
        self.rows = rows

    def with_entities(self, *a):
        return self

    def filter(self, *a):
        return self

    def all(self):
        return self.rows


def test_recalculo_de_dotacao_so_da_uo_do_arquivo(monkeypatch):
    dot_sed = SimpleNamespace(
        chave_dotacao="DOT.2026.A.1", uo="14.101 - SECRETARIA DE ESTADO DE EDUCAÇÃO", valor_dotacao="1000,00",
        valor_ped_emp=None, valor_atual=None,
    )
    dot_outra = SimpleNamespace(
        chave_dotacao="DOT.2026.A.1", uo="26201", valor_dotacao="500,00", valor_ped_emp="999", valor_atual="999",
    )
    emps = [
        SimpleNamespace(valor_emp_devolucao_gcv="100,00", chave="DOT.2026.A.1*", uo="14101"),
        SimpleNamespace(valor_emp_devolucao_gcv="7.000,00", chave="DOT.2026.A.1*", uo="26201"),
    ]
    monkeypatch.setattr(
        pr,
        "EmpRegistro",
        SimpleNamespace(query=_FakeQuery(emps), ativo=1, valor_emp_devolucao_gcv=1, chave=1, uo=1),
    )
    monkeypatch.setattr(pr, "Dotacao", SimpleNamespace(query=_FakeQuery([dot_sed, dot_outra]), ativo=1))
    monkeypatch.setattr(pr, "db", SimpleNamespace(session=SimpleNamespace(commit=lambda: None)))

    df = pd.DataFrame({"Chave": ["DOT.2026.A.1*"], "Valor PED": ["50,00"]})
    pr._update_dotacao_from_ped(df, "14101")

    assert float(dot_sed.valor_ped_emp) == 150.0  # 50 do PED + 100 do EMP da própria UO
    assert float(dot_sed.valor_atual) == 850.0
    assert (dot_outra.valor_ped_emp, dot_outra.valor_atual) == ("999", "999")  # outra UO intacta
