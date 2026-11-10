"""PLAN 51 (Créditos Adicionais) - Atualizar > Execução / Relatórios.

- Sem banco: cabeçalho (exercício, status, totais impressos), recusas,
  tratamento de uma planilha sintética no formato do FIPLAN, conferência com o
  total impresso, lista única das 66 colunas x model, detecção de layout e
  permissões/menu.
- Com banco (exercícios descartáveis 9991/9990, removidos ao final - mesmo
  padrão dos testes do Teto-SEDUC): a nova carga troca a versão anterior só do
  MESMO exercício; falha no meio não derruba a versão ativa; a Limpeza de
  Histórico enxerga a tabela.
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest
from openpyxl import Workbook, load_workbook
from sqlalchemy import text

from services import plan51_runner as pr

CABECALHO = [
    "N° FIPLAN", "DECRETO", "DATA CONTÁBIL", "N° D.O.E.", "TIPO CRÉDITO",
    "UO/PROJETO ATIV.", "ELEMENTO SUPLEMENTADO", "FONTE", "IDUSO", "VALOR",
    "UO/PROJETO ATIV.", "ELEMENTO ANULADO", "FONTE", "IDUSO", "VALOR",
]


def _planilha(tmp_path, exercicio="2026", status="5", linhas=None, total_supl="88.988,00", total_anul="88.988,00"):
    """Arquivo no formato do FIPLAN: título, filtros, cabeçalho, dados e totais."""
    linhas = linhas if linhas is not None else [
        # supl e anul com elementos diferentes -> o script divide em duas linhas
        ["1", "", "14/01/2026", "", "400", "19301.06.126.036.2009.9900", "3.3.90.00", "17490000", "4", "88.988,00",
         "19301.06.126.036.2009.9900", "3.3.91.00", "17490000", "4", "88.988,00"],
    ]
    wb = Workbook()
    ws = wb.active
    ws.append(["PLAN 51 - Relação Detalhada de Créditos Adicionais"])
    ws.append([])
    if exercicio is not None:
        ws.append([f"*Exercício igual a {exercicio}"])
    if status is not None:
        ws.append([f"FLAG_STATUS igual a {status}"])
    ws.append(CABECALHO)
    for linha in linhas:
        ws.append(linha)
    ws.append([])
    ws.append(["", "", "", "", "Total Suplementado", "", total_supl, "Total Anulado", "", "", total_anul])
    caminho = tmp_path / "plan51.xlsx"
    wb.save(caminho)
    return caminho


# ---------------------------------------------------------------- sem banco

def test_cabecalho_le_exercicio_status_e_totais(tmp_path):
    cab = pr.ler_cabecalho(pr.ler_bruto(_planilha(tmp_path)))
    assert cab["exercicio"] == 2026
    assert cab["status"] == "5"
    assert cab["totais"] == {"supl": 88988.0, "anul": 88988.0}
    assert pr.validar_cabecalho(cab) == 2026


@pytest.mark.parametrize("status", ["5", "Efetivado", "efetivado"])
def test_aceita_os_dois_formatos_do_filtro_de_status(tmp_path, status):
    cab = pr.ler_cabecalho(pr.ler_bruto(_planilha(tmp_path, status=status)))
    assert pr.validar_cabecalho(cab) == 2026


def test_recusa_status_diferente_de_efetivado(tmp_path):
    cab = pr.ler_cabecalho(pr.ler_bruto(_planilha(tmp_path, status="1")))
    with pytest.raises(pr.Plan51Recusado, match="Efetivado"):
        pr.validar_cabecalho(cab)


def test_recusa_arquivo_sem_filtro_de_status_ou_de_exercicio(tmp_path):
    with pytest.raises(pr.Plan51Recusado, match="FLAG_STATUS"):
        pr.validar_cabecalho(pr.ler_cabecalho(pr.ler_bruto(_planilha(tmp_path, status=None))))
    with pytest.raises(pr.Plan51Recusado, match="Exercício"):
        pr.validar_cabecalho(pr.ler_cabecalho(pr.ler_bruto(_planilha(tmp_path, exercicio=None))))


def test_tratamento_divide_linha_com_elementos_diferentes_e_confere_totais(tmp_path):
    caminho = _planilha(tmp_path)
    raw = pr.ler_bruto(caminho)
    df, _resumo_do_script, _estilo = pr.processar_plan51(str(caminho), "TODOS", df_raw=raw)
    assert list(df.columns[:3]) == ["__UO (S)", "__UO (A)", "__UO (DEF)"]
    assert len(df.columns) == 66
    assert len(df) == 2  # supl 3.3.90.00 x anul 3.3.91.00 vira duas linhas
    assert pr._somar(df["VALOR SUPL"]) == 88988.0
    assert pr._somar(df["VALOR ANUL"]) == 88988.0
    assert pr.conferir_totais(df, {"supl": 88988.0, "anul": 88988.0}) == "Confere com o total do relatório."
    assert "ATENÇÃO" in pr.conferir_totais(df, {"supl": 90000.0, "anul": 88988.0})
    assert "não encontrados" in pr.conferir_totais(df, {})


def test_resumo_do_script_nao_serve_para_conferencia(tmp_path):
    """O `resumo` do td_plan51.py reaplica _as_money em número decimal e infla a
    soma (~34x no arquivo real). A carga usa _somar, que é a soma correta."""
    caminho = _planilha(
        tmp_path,
        linhas=[["1", "", "14/01/2026", "", "400", "19301.06.126.036.2009.9900", "3.3.90.00", "17490000", "4",
                 "356.590,29", "", "", "", "", ""]],
        total_supl="356.590,29", total_anul="0,00",
    )
    df, resumo, _ = pr.processar_plan51(str(caminho), "TODOS", df_raw=pr.ler_bruto(caminho))
    assert pr._somar(df["VALOR SUPL"]) == 356590.29
    assert resumo["subtotal_suplementacao"] != 356590.29  # o resumo do script está errado


def test_registros_traduz_as_66_colunas_para_o_banco(tmp_path):
    caminho = _planilha(tmp_path)
    df, _, _ = pr.processar_plan51(str(caminho), "TODOS", df_raw=pr.ler_bruto(caminho))
    regs = pr._registros(df)
    assert len(regs) == 2
    assert set(regs[0]) == {c for _, c, _ in pr.PLAN51_COLUNAS}
    assert regs[0]["uo_def"] == "19301" and regs[0]["valor_supl"] == 88988.0
    assert regs[1]["valor_supl"] is None and regs[1]["valor_anul"] == 88988.0  # vazio vira NULL


def test_lista_de_colunas_e_o_model_nao_se_afastam():
    from models import Plan51Registro

    colunas_model = {c.name for c in Plan51Registro.__table__.columns}
    colunas_runner = [c for _, c, _ in pr.PLAN51_COLUNAS]
    assert len(colunas_runner) == 66 and len(set(colunas_runner)) == 66
    assert set(colunas_runner) <= colunas_model
    assert colunas_model - set(colunas_runner) == {
        "id", "upload_id", "exercicio", "data_arquivo", "user_email", "ativo", "estilo", "created_at",
    }


def test_layout_do_plan51_e_reconhecido_e_nao_se_mistura(tmp_path):
    from rotas.home_routes import _detect_upload_layouts, _validate_upload_layout

    caminho = _planilha(tmp_path)
    assert _detect_upload_layouts(caminho) == {"plan51"}
    assert _validate_upload_layout(caminho, "plan51", "PLAN51") is None
    assert "PLAN51" in _validate_upload_layout(caminho, "fip613", "FIP613")
    assert "PLAN51" in _validate_upload_layout(caminho, "ped", "PED")


def test_permissoes_menu_e_limpeza_de_historico_incluem_o_plan51():
    from services import limpeza_historico as lh
    from services.features import flatten_features

    ids = {f["id"] if isinstance(f, dict) else f for f in flatten_features()}
    assert {"atualizar/plan51", "relatorios/plan51"} <= ids
    assert lh.TABELAS["plan51"] == {"label": "PLAN 51", "exercicio": "exercicio"}
    base = open("templates/base.html", encoding="utf-8-sig").read()
    assert 'data-route="atualizar/plan51"' in base and 'data-route="relatorios/plan51"' in base


# ---------------------------------------------------------------- com banco

EXERCICIO = 9991
EXERCICIO_OUTRO = 9990
UPLOADS = (910001, 910002, 910003, 910009)


def _limpar():
    from models import db

    db.session.rollback()
    ids = ",".join(str(i) for i in UPLOADS)
    db.session.execute(text(f"DELETE FROM plan51 WHERE upload_id IN ({ids})"))
    db.session.commit()


def _df(tmp_path, valores):
    linhas = [
        [str(i + 1), "", "14/01/2026", "", "400", "19301.06.126.036.2009.9900", "3.3.90.00", "17490000", "4", v,
         "", "", "", "", ""]
        for i, v in enumerate(valores)
    ]
    caminho = _planilha(tmp_path, linhas=linhas)
    df, _, _ = pr.processar_plan51(str(caminho), "TODOS", df_raw=pr.ler_bruto(caminho))
    return df


def _ativos(exercicio):
    from models import db

    return db.session.execute(
        text("SELECT upload_id, COUNT(*) FROM plan51 WHERE ativo = 1 AND exercicio = :e GROUP BY upload_id"),
        {"e": exercicio},
    ).fetchall()


def test_nova_carga_troca_so_a_versao_do_mesmo_exercicio(tmp_path):
    from app import create_app
    from models import db

    app = create_app()
    with app.app_context():
        try:
            _limpar()
            agora = datetime(2026, 11, 3, 10, 0)
            pr.update_database(_df(tmp_path, ["10,00", "20,00"]), EXERCICIO_OUTRO, agora, "t@t", 910009)
            pr.update_database(_df(tmp_path, ["10,00", "20,00"]), EXERCICIO, agora, "t@t", 910001)
            assert [tuple(r) for r in _ativos(EXERCICIO)] == [(910001, 2)]

            pr.update_database(_df(tmp_path, ["30,00"]), EXERCICIO, agora, "t@t", 910002)
            assert [tuple(r) for r in _ativos(EXERCICIO)] == [(910002, 1)]  # a anterior foi desativada
            total_antiga = db.session.execute(text("SELECT COUNT(*) FROM plan51 WHERE upload_id = 910001")).scalar()
            assert total_antiga == 2  # continua no banco (inativa) até a Limpeza de Histórico
            assert [tuple(r) for r in _ativos(EXERCICIO_OUTRO)] == [(910009, 2)]  # outro exercício intacto
        finally:
            _limpar()


def test_falha_no_meio_mantem_a_versao_ativa_e_nao_deixa_lixo(tmp_path, monkeypatch):
    from app import create_app
    from models import db

    app = create_app()
    with app.app_context():
        try:
            _limpar()
            agora = datetime(2026, 11, 3, 10, 0)
            pr.update_database(_df(tmp_path, ["10,00"]), EXERCICIO, agora, "t@t", 910001)

            monkeypatch.setattr(pr, "BATCH_SIZE", 1)
            executar = db.session.execute
            chamadas = {"n": 0}

            def falha_no_segundo_lote(stmt, *args, **kwargs):
                if "INSERT INTO plan51" in str(stmt):
                    chamadas["n"] += 1
                    if chamadas["n"] == 2:
                        raise RuntimeError("queda simulada")
                return executar(stmt, *args, **kwargs)

            monkeypatch.setattr(db.session, "execute", falha_no_segundo_lote)
            with pytest.raises(RuntimeError, match="queda simulada"):
                pr.update_database(_df(tmp_path, ["1,00", "2,00", "3,00"]), EXERCICIO, agora, "t@t", 910002)
            monkeypatch.undo()

            assert [tuple(r) for r in _ativos(EXERCICIO)] == [(910001, 1)]  # a anterior segue ativa
            sobra = db.session.execute(text("SELECT COUNT(*) FROM plan51 WHERE upload_id = 910002")).scalar()
            assert sobra == 0  # nada pela metade
        finally:
            monkeypatch.undo()
            _limpar()


def test_limpeza_de_historico_enxerga_as_cargas_do_plan51(tmp_path):
    from app import create_app
    from models import db
    from services import limpeza_historico as lh

    app = create_app()
    with app.app_context():
        try:
            _limpar()
            pr.update_database(_df(tmp_path, ["10,00"]), EXERCICIO, datetime(2026, 10, 5, 9, 0), "t@t", 910001)
            pr.update_database(_df(tmp_path, ["20,00"]), EXERCICIO, datetime(2026, 10, 20, 9, 0), "t@t", 910002)
            with db.engine.connect() as conn:
                cargas = {c.upload_id: c for c in lh.carregar_cargas(conn, "plan51") if c.upload_id in UPLOADS}
            assert cargas[910001].exercicio == EXERCICIO and cargas[910001].ativos == 0  # desativada: pode sair
            assert cargas[910002].exercicio == EXERCICIO and cargas[910002].ativos == 1  # ativa: protegida
        finally:
            _limpar()


# ---------------------------------------------------------------- cores do script

def _linhas_com_flags(tmp_path):
    """Linhas que acionam várias marcações do script: FIPLAN repetido (FLAG15),
    bloco suplementado repetido (FLAG_SUPL) e divisão por elemento diferente."""
    return _planilha(
        tmp_path,
        linhas=[
            ["1", "10", "14/01/2026", "99", "400", "19301.06.126.036.2009.9900", "3.3.90.00", "17490000", "4", "100,00",
             "19301.06.126.036.2009.9900", "3.3.91.00", "17490000", "4", "100,00"],
            ["", "", "", "", "", "", "3.3.90.00", "17490000", "4", "50,00",
             "22101.08.422.512.2295.9900", "3.3.50.00", "16690000", "1", "50,00"],
            ["2", "11", "15/01/2026", "98", "400", "25101.15.451.535.3117.0700", "4.4.90.00", "15000000", "2", "30,00",
             "25101.15.451.535.3117.0700", "4.4.90.00", "15000000", "1", "30,00"],
        ],
        total_supl="180,00", total_anul="180,00",
    )


def test_mascaras_de_estilo_fazem_ida_e_volta(tmp_path):
    caminho = _linhas_com_flags(tmp_path)
    df, _, info = pr.processar_plan51(str(caminho), "TODOS", df_raw=pr.ler_bruto(caminho))
    masks = pr.calcular_estilos(len(df), info)
    assert any(masks), "a planilha de teste deveria acionar marcações"
    assert any(m >> pr.BIT_FLAG15 & 1 for m in masks) and any(m >> pr.BIT_PROX_FLAG15 & 1 for m in masks)
    assert any(m >> pr.BIT_SPLIT_ANUL & 1 for m in masks)
    assert max(masks) < 2**31  # cabe no BIGINT e nos 32 bits do JavaScript
    volta = pr.style_info_das_mascaras(masks)
    for chave in ("rows_flag_15", "rows_flag_supl", "rows_flag_anul", "rows_split_anul", "split_anul_by_base", "red_def_by_base"):
        assert volta[chave] == info[chave], chave


def _celulas(ws, ate_linha):
    out = {}
    for row in ws.iter_rows(min_row=1, max_row=ate_linha):
        for c in row:
            fundo = c.fill.fgColor.rgb if c.fill and c.fill.fill_type == "solid" else None
            cor = c.font.color.rgb if c.font and c.font.color is not None else None
            out[c.coordinate] = (c.value, fundo, cor, bool(c.font.bold), c.alignment.horizontal)
    return out


def test_planilha_regerada_do_banco_e_igual_a_do_script(tmp_path):
    """A planilha que o relatório baixa quando o arquivo gravado no upload não
    existe mais tem que ser a mesma do script: valores, cores, fontes, subtotais."""
    caminho = _linhas_com_flags(tmp_path)
    df, _, info = pr.processar_plan51(str(caminho), "TODOS", df_raw=pr.ler_bruto(caminho))
    sup, anu = pr._somar(df["VALOR SUPL"]), pr._somar(df["VALOR ANUL"])
    resumo = {"subtotal_suplementacao": sup, "subtotal_anulacao": anu, "subtotal_liquido": round(sup - anu, 2)}
    original = tmp_path / "script.xlsx"
    pr.salvar_saida(df, resumo, info, str(original), "ABA", "TODOS", "arq.xlsx", "a", "b", "c")

    registros = pr._registros(df)
    linhas = [tuple(r[c] for _, c, _ in pr.PLAN51_COLUNAS) for r in registros]
    regerada = tmp_path / "banco.xlsx"
    pr.gerar_excel_do_banco(str(regerada), linhas, pr.calcular_estilos(len(df), info), "arq.xlsx", "a")

    ate = len(df) + 5  # dados + linhas de subtotal e a de "Fonte" (as de data/tempo diferem de propósito)
    a = _celulas(load_workbook(original)["ABA"], ate)
    wb_b = load_workbook(regerada)
    b = _celulas(wb_b[wb_b.sheetnames[0]], ate)
    assert a == b
    # e as cores de fato existem (não é uma comparação de duas planilhas lisas)
    assert any(v[1] for v in a.values()), "esperava células preenchidas"
    ws = load_workbook(original)["ABA"]
    rotulos = [c.value for row in ws.iter_rows(min_row=len(df) + 2, max_row=len(df) + 6) for c in row if c.value]
    assert "SUBTOTAL (SUPLEMENT)" in rotulos and "SUBTOTAL (LÍQUIDO)" in rotulos


def test_a_carga_grava_o_estilo_de_cada_linha(tmp_path):
    from app import create_app
    from models import db

    app = create_app()
    with app.app_context():
        try:
            _limpar()
            caminho = _linhas_com_flags(tmp_path)
            df, _, info = pr.processar_plan51(str(caminho), "TODOS", df_raw=pr.ler_bruto(caminho))
            masks = pr.calcular_estilos(len(df), info)
            pr.update_database(df, EXERCICIO, datetime(2026, 11, 3, 10, 0), "t@t", 910001, masks)
            gravados = [int(r[0] or 0) for r in db.session.execute(
                text("SELECT estilo FROM plan51 WHERE upload_id = 910001 ORDER BY id")).fetchall()]
            assert gravados == masks
        finally:
            _limpar()


def test_trava_por_exercicio_bloqueia_a_segunda_gravacao():
    from app import create_app
    from models import db

    app = create_app()
    with app.app_context():
        with pr._trava_exercicio(EXERCICIO):
            with db.engine.connect() as outra:
                assert outra.execute(text("SELECT GET_LOCK(:n, 0)"), {"n": f"plan51:{EXERCICIO}"}).scalar() == 0
        with db.engine.connect() as outra:  # liberada ao sair
            assert outra.execute(text("SELECT GET_LOCK(:n, 0)"), {"n": f"plan51:{EXERCICIO}"}).scalar() == 1
            outra.execute(text("SELECT RELEASE_LOCK(:n)"), {"n": f"plan51:{EXERCICIO}"})
