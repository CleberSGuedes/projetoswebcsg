"""FIP 613 por UO (docs/plano_implementacao_novas_uos.md, Fase 1).

Planilhas sintéticas em diretório temporário; nada é gravado no banco.
A desativação por UO e ano é testada em TEMPORARY TABLE.
"""

import pytest
from flask import Flask
from openpyxl import Workbook
from sqlalchemy import text

from config import Config
from models import db
from services import fip613_runner as fr
from services.execucao_uo import desativar_versao_anterior
from services.uo import UOS_EXECUCAO, UORecusada

CABECALHO = [
    "UO", "UG", "Função", "Subfunção", "Programa", "Projeto/Atividade", "Regional",
    "Natureza de Despesa", "Fonte de Recurso", "Iduso", "Tipo de Recurso",
    "Dotação Inicial", "Créd. Suplementar", "Créd. Especial", "Créd. Extraordinário",
    "Redução", "Créd. Autorizado", "Bloqueado/Conting.", "Reserva Empenho",
    "Saldo de Destaque", "Saldo Dotação", "Empenhado", "Liquidado", "A liquidar",
    "Valor Pago", "Valor a Pagar",
]


def _linha(uo, ug, valor):
    # 15 valores; só a Dotação Inicial e o Empenhado variam
    v = ["0,00"] * 15
    v[0] = valor
    v[10] = valor
    return [uo, ug, "12", "361", "36", "2009", "REG", "33903900", "15000000", 0, "1"] + v


def _planilha(tmp_path, uo_linhas, uo_total, filtro_uo, valor_total, nome="fip.xlsx"):
    wb = Workbook()
    ws = wb.active
    ws.append(["FIPLAN"])
    ws.append(["Relatório"])
    ws.append([])
    ws.append(["*Exercício igual a 2026"])
    ws.append([f"Código da Unidade Orçamentária igual a {filtro_uo}"])
    ws.append(CABECALHO)
    soma = 0.0
    for uo, ug, valor in uo_linhas:
        ws.append(_linha(uo, ug, valor))
    # total impresso: Dotação Inicial (col. 12) e Empenhado (col. 22)
    total = [f"Total UO {uo_total}"] + [None] * 10 + ["0,00"] * 15
    total[11] = valor_total
    total[21] = valor_total
    ws.append(total)
    ws.append(["Total por Função", "12", None])
    caminho = tmp_path / nome
    wb.save(caminho)
    return caminho


@pytest.fixture
def todas_liberadas(monkeypatch):
    from services import uo as u

    monkeypatch.setattr(u, "UOS_EXECUCAO_LIBERADAS", set(UOS_EXECUCAO))


def test_corte_generico_ug_como_texto_e_conferencia_ok(tmp_path, todas_liberadas):
    f = _planilha(tmp_path, [("26201", "0011", "100,00"), ("26201", "0000", "50,50")], "26201", "26201", "150,50")
    d = fr.load_clean_data(f)
    assert len(d) == 2
    assert d.attrs["uo"] == "26201"
    assert sorted(d["ug"]) == ["0000", "0011"]  # texto, com zeros à esquerda
    assert d.attrs["conferencia"] == "Confere com o total do relatório."


def test_total_diferente_apenas_avisa(tmp_path, todas_liberadas):
    f = _planilha(tmp_path, [("26201", "0011", "100,00")], "26201", "26201", "999,00")
    d = fr.load_clean_data(f)
    assert len(d) == 1  # não bloqueia
    assert "ATENÇÃO" in d.attrs["conferencia"] and "dotacao_inicial" in d.attrs["conferencia"]


def test_so_a_secretaria_esta_liberada_por_enquanto(tmp_path):
    f = _planilha(tmp_path, [("26201", "0011", "100,00")], "26201", "26201", "100,00")
    with pytest.raises(UORecusada, match="ainda não está liberada"):
        fr.load_clean_data(f)
    f2 = _planilha(tmp_path, [("14101", "0011", "100,00")], "14101", "14101", "100,00", nome="s.xlsx")
    assert fr.load_clean_data(f2).attrs["uo"] == "14101"


def test_recusa_mais_de_uma_uo_e_cabecalho_divergente(tmp_path, todas_liberadas):
    f = _planilha(tmp_path, [("26201", "0011", "1,00"), ("26202", "0011", "1,00")], "26201", "26201", "2,00")
    with pytest.raises(UORecusada, match="mais de uma UO"):
        fr.load_clean_data(f)
    f2 = _planilha(tmp_path, [("26201", "0011", "1,00")], "26201", "26202", "1,00", nome="b.xlsx")
    with pytest.raises(UORecusada, match="mais de uma UO"):
        fr.load_clean_data(f2)


@pytest.fixture
def conn_temporaria():
    app = Flask("teste_fip613_uo")
    app.config.from_object(Config)
    db.init_app(app)
    with app.app_context():
        with db.engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TEMPORARY TABLE tmp_fip613_teste ("
                    "id INT PRIMARY KEY AUTO_INCREMENT, uo VARCHAR(50), ano INT, ativo TINYINT(1))"
                )
            )
            yield conn
            conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tmp_fip613_teste"))


def test_desativacao_por_uo_e_ano(conn_temporaria):
    conn = conn_temporaria
    for uo, ano, at in [("14101", 2026, 1), ("26201", 2026, 1), ("26201", 2025, 1), ("26201", 2026, 0)]:
        conn.execute(
            text("INSERT INTO tmp_fip613_teste (uo, ano, ativo) VALUES (:u, :a, :t)"), {"u": uo, "a": ano, "t": at}
        )
    n = desativar_versao_anterior("tmp_fip613_teste", "26201", [2026], coluna_exercicio="ano", conn=conn)
    assert n == 1
    ativos = conn.execute(text("SELECT uo, ano FROM tmp_fip613_teste WHERE ativo = 1 ORDER BY id")).fetchall()
    assert [tuple(r) for r in ativos] == [("14101", 2026), ("26201", 2025)]
