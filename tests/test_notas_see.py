"""Notas SEE (Área UENs > SAGE): cadastro de escolas, extração de DANFEs, Excel e fluxo de lotes.

- Sem banco e sem PDF: regras do cadastro DRE/Escola e geração do Excel.
- Com PDFs reais (pasta indicada em SEE_PDFS_DIR; pulado se não existir): extração, conferência
  de quantidade e identificação da escola.
- Com banco (catálogo e processamentos descartáveis, removidos ao final): fluxo de envio em
  lotes, duplicados, descarte, aviso de pdfplumber ausente, processamento completo e limpeza dos PDFs.
"""
from __future__ import annotations

import glob
import inspect
import io
import os
import re
import shutil
from pathlib import Path

import pytest
from openpyxl import load_workbook
from sqlalchemy import text

from services.see_escolas import Escola, EscolaIndex, identificar_escola, nucleo_nome, normalizar
from services.see_notes import extract_pdf, generate_xlsx, pdfplumber_disponivel

PDFS_DIR = Path(
    os.getenv(
        "SEE_PDFS_DIR",
        r"C:\Users\00275278166\Desktop\notas\2026\3Bimestre\Notas Fiscais-20261001T141811Z-1-001\Notas Fiscais",
    )
)
precisa_pdfs = pytest.mark.skipif(not PDFS_DIR.exists() or not pdfplumber_disponivel(), reason="PDFs de exemplo ou pdfplumber ausentes")


# ---------------------------------------------------------------- cadastro DRE/Escola (sem banco)
def _indice():
    return EscolaIndex.build([
        Escola("100", "EE PROF MARIA SOUZA", "SINOP", "DRE SINOP"),
        Escola("101", "EE JOAO BATISTA", "SINOP", "DRE SINOP"),
        Escola("200", "EE CORONEL SILVA", "SORRISO", "DRE SINOP"),
        Escola("300", "EE ANTIGA", "SINOP", "DRE SINOP", ativo=False),
        Escola("1", "DRE SINOP", None, "DRE SINOP", tipo="dre"),
    ])


def test_normalizacao_ignora_tipo_acento_e_abreviacao():
    assert normalizar("Escola Estadual João") == normalizar("ESCOLA ESTADUAL JOAO")
    assert nucleo_nome("EE PROF MARIA SOUZA") == nucleo_nome("ESCOLA ESTADUAL PROFESSORA MARIA SOUZA")


def test_codigo_valido_e_dre_conferida():
    r = identificar_escola({"codigo_escola": "100", "dre": "DRE SINOP", "nome_escola": "qualquer"}, _indice())
    assert r["codigo"] == "100" and r["nome"] == "EE PROF MARIA SOUZA" and not r["warnings"]


def test_codigo_com_dre_divergente_alerta_sem_corrigir():
    r = identificar_escola({"codigo_escola": "100", "dre": "DRE OUTRA", "nome_escola": "x"}, _indice())
    codigos = {w["codigo"] for w in r["warnings"]}
    assert "ESCOLA_DRE_DIVERGENTE" in codigos and r["destaque"] == "divergente"
    assert r["codigo"] == "100", "nunca corrige sozinho"


def test_codigo_inexistente():
    r = identificar_escola({"codigo_escola": "999", "dre": "DRE SINOP"}, _indice())
    assert {w["codigo"] for w in r["warnings"]} == {"ESCOLA_CODIGO_NAO_CADASTRADO"}


def test_sem_codigo_preenche_so_com_uma_escola_de_mesmo_nome_no_municipio():
    r = identificar_escola({"nome_escola": "ESCOLA ESTADUAL PROFESSORA MARIA SOUZA", "municipio": "Sinop", "dre": "DRE SINOP"}, _indice())
    assert r["codigo"] == "100" and r["origem_codigo"] == "cadastro (nome + município)" and not r["warnings"]


def test_nome_parecido_vira_sugestao_nunca_preenchimento():
    r = identificar_escola({"nome_escola": "EE JOAO BATISTA DA SILVA", "municipio": "SINOP", "dre": "DRE SINOP"}, _indice())
    assert r["codigo"] is None and r["destaque"] == "sugestao"
    assert "101" in r["conferencia"]


def test_municipio_em_outra_dre_e_dre_fora_do_cadastro():
    r = identificar_escola({"nome_escola": "EE X", "municipio": "SORRISO", "dre": "DRE SINOP2"}, _indice())
    assert {w["codigo"] for w in r["warnings"]} >= {"DRE_FORA_DO_CADASTRO"}


def test_escola_inativa_continua_valida_para_nota_antiga():
    r = identificar_escola({"codigo_escola": "300", "dre": "DRE SINOP"}, _indice())
    assert r["codigo"] == "300" and {w["codigo"] for w in r["warnings"]} == {"ESCOLA_INATIVA"}


# ---------------------------------------------------------------- Excel (sem banco)
def test_excel_tem_as_abas_a_legenda_e_a_conferencia(tmp_path):
    saida = tmp_path / "notas.xlsx"
    produtos = [{"codigo": "930001", "nome": "Livro A"}, {"codigo": "930002", "nome": "Livro B"}]
    itens = [
        {"arquivo": "a.pdf", "dre": "DRE SINOP", "codigo_escola": "100", "nome_escola": "EE X", "numero_danfe": "1", "codigo": "930001", "nome": "Livro A", "quantidade": 10, "destaque": None, "conferencia": None, "escola_nota": "EE X", "origem_codigo": "nota"},
        {"arquivo": "b.pdf", "dre": "DRE SINOP", "codigo_escola": None, "nome_escola": "EE Y", "numero_danfe": "2", "codigo": "930002", "nome": "Livro B", "quantidade": 4, "destaque": "sugestao", "conferencia": "Sugestão: 101", "escola_nota": "EE Y", "origem_codigo": None},
    ]
    generate_xlsx(saida, [{"Arquivo PDF": "a.pdf", "Status": "finalizado"}], itens, [], produtos)
    wb = load_workbook(saida)
    assert wb.sheetnames[0] == "Planilha Consolidada" and len(wb.sheetnames) >= 3
    ws = wb["Planilha Consolidada"]
    cab = [c.value for c in ws[1]]
    assert cab[:4] == ["DRE", "COD", "Escola", "Número DANFE"] and "Livro A" in cab and "Conferência" in cab
    assert sum(1 for r in range(2, ws.max_row + 1) if ws.cell(row=r, column=1).value) == 2, "2 escolas"
    textos = [str(c.value) for row in ws.iter_rows() for c in row if c.value]
    assert any("sugest" in t.lower() for t in textos), "legenda ao lado da tabela"
    cores = {ws.cell(row=r, column=1).fill.fgColor.rgb for r in (2, 3)}
    assert len(cores) == 2, "a linha com sugestão é pintada, a outra não"


# ---------------------------------------------------------------- PDFs reais
def _amostra(n=8):
    pdfs = sorted(glob.glob(str(PDFS_DIR / "**" / "*.pdf"), recursive=True))
    passo = max(1, len(pdfs) // n)
    return [Path(p) for p in pdfs[::passo][:n]]


def _catalogo_dos_pdfs(pdfs):
    import collections

    import pdfplumber

    cnt = collections.Counter()
    for p in pdfs:
        with pdfplumber.open(p) as pdf:
            for pg in pdf.pages:
                for w in pg.extract_words():
                    if re.fullmatch(r"9\d{5}", w["text"]):
                        cnt[w["text"]] += 1
    return [{"id": i, "codigo": c, "nome": f"Produto {c}"} for i, (c, _) in enumerate(cnt.most_common(40), start=1)]


@precisa_pdfs
def test_extracao_de_pdfs_reais_dos_dois_layouts():
    pdfs = _amostra(10)
    produtos = _catalogo_dos_pdfs(pdfs)
    assert produtos
    for p in pdfs:
        r = extract_pdf(p, produtos)
        md = r["metadata"]
        assert md.get("numero_danfe") and md.get("chave_acesso"), p.name
        assert md.get("dre"), p.name
        assert r["items"], f"nenhum produto em {p.name}"
        codigos = {w["codigo"] for w in r["warnings"]}
        assert "QUANTIDADE_INCONSISTENTE" not in codigos, f"quantidade x valor unitário não confere em {p.name}"
        for it in r["items"]:
            assert it["quantidade"] > 0 and it["quantidade"] < 100000


@precisa_pdfs
def test_quantidade_nao_e_o_valor_total_da_linha():
    """Regressão do bug do dev: 210 un x R$ 28,00 era gravado como 5.880."""
    pdfs = _amostra(6)
    produtos = _catalogo_dos_pdfs(pdfs)
    for p in pdfs:
        for it in extract_pdf(p, produtos)["items"]:
            texto = it["texto_origem"]
            numeros = re.findall(r"\d[\d.]*,\d{2,4}", texto)
            if len(numeros) >= 3:
                qtd = it["quantidade"]
                total = float(numeros[2].replace(".", "").replace(",", "."))
                assert float(qtd) != total or float(qtd) * 1.0 == total and total < 100, p.name


# ---------------------------------------------------------------- banco / rotas
@pytest.fixture(scope="module")
def app():
    from app import create_app

    return create_app()


@pytest.fixture()
def ctx(app):
    """Chama as views como o usuário admin (sem login), com um catálogo descartável (exercício 2099)."""
    from models import SeeCatalogo, SeeCatalogoProduto, SeeProcessamento, Usuario, db
    from rotas import notas_see_routes as nr

    with app.app_context():
        email = db.session.query(Usuario.email).filter(Usuario.email.isnot(None)).order_by(Usuario.id).first()[0]
        db.session.rollback()
        db.session.query(SeeCatalogo).filter(SeeCatalogo.exercicio == 2099).delete()
        db.session.commit()

        class C:
            pass

        c = C()
        c.app, c.nr, c.email, c.db = app, nr, email, db
        c.jobs = []

        def chamar(view, *, method="GET", data=None, json=None, **kw):
            fn = inspect.unwrap(view)
            kwargs = {"method": method}
            if json is not None:
                kwargs["json"] = json
            if data is not None:
                kwargs["data"] = data
                kwargs["content_type"] = "multipart/form-data"
            with app.test_request_context("/x", **kwargs):
                from flask import g, session

                session["user"] = {"email": email}
                g.user = object()
                g.user_nivel = 1
                resp = fn(**kw)
                if isinstance(resp, tuple):
                    body, code = resp[0], resp[1]
                else:
                    body, code = resp, 200
                return code, (body.get_json() if hasattr(body, "get_json") else None)

        c.chamar = chamar
        yield c

        db.session.rollback()
        for job_id in c.jobs:
            for tab in ("see_processamento_eventos", "see_ocorrencias", "see_itens_extraidos", "see_processamento_arquivos"):
                db.session.execute(text(f"DELETE FROM {tab} WHERE processamento_id = :i"), {"i": job_id})
            db.session.execute(text("DELETE FROM see_processamentos WHERE id = :i"), {"i": job_id})
            shutil.rmtree(nr.SEE_UPLOAD_DIR / str(job_id), ignore_errors=True)
        cat_ids = [r[0] for r in db.session.execute(text("SELECT id FROM see_catalogos WHERE exercicio = 2099")).fetchall()]
        for cid in cat_ids:
            db.session.execute(text("DELETE FROM see_catalogo_produtos WHERE catalogo_id = :i"), {"i": cid})
            db.session.execute(text("DELETE FROM see_catalogos WHERE id = :i"), {"i": cid})
        db.session.commit()
        for f in glob.glob(str(nr.SEE_OUTPUT_DIR / "notas_see_*_*.xlsx")):
            pass  # saídas de teste são removidas pelo próprio teste


def _catalogo(ctx):
    code, body = ctx.chamar(ctx.nr.api_notas_see_catalogos, method="POST", json={"nome": "TESTE CLAUDE", "exercicio": 2099})
    assert code == 201, body
    cid = body["catalogo"]["id"]
    code, body = ctx.chamar(ctx.nr.api_notas_see_produto_criar, method="POST", json={"codigo": "930001", "nome": "Livro A"}, catalogo_id=cid)
    assert code in (200, 201), body
    return cid


def _pdf(n: int) -> bytes:
    return b"%PDF-1.4\n" + str(n).encode() * 50 + b"\n%%EOF"


def _novo_job(ctx, cid):
    code, body = ctx.chamar(ctx.nr.api_notas_see_processar, method="POST", data={"catalogo_id": str(cid), "append_mode": "novo"})
    assert code == 201, body
    ctx.jobs.append(body["job_id"])
    return body["job_id"]


def test_catalogo_exige_exercicio_e_nome_unico_no_exercicio(ctx):
    code, _ = ctx.chamar(ctx.nr.api_notas_see_catalogos, method="POST", json={"nome": "X", "exercicio": ""})
    assert code == 400
    cid = _catalogo(ctx)
    code, body = ctx.chamar(ctx.nr.api_notas_see_catalogos, method="POST", json={"nome": "teste claude", "exercicio": 2099})
    assert code == 400 and "Já existe" in body["error"]
    code, body = ctx.chamar(ctx.nr.api_notas_see_catalogos, method="POST", json={"nome": "TESTE CLAUDE", "exercicio": 2098})
    assert code == 201, "mesmo nome em outro exercício é permitido"
    ctx.db.session.execute(text("DELETE FROM see_catalogos WHERE exercicio = 2098"))
    ctx.db.session.commit()
    assert cid


def test_envio_em_lotes_ignora_duplicados_e_nao_pdf(ctx):
    cid = _catalogo(ctx)
    job_id = _novo_job(ctx, cid)
    lote1 = {"pdfs": [(io.BytesIO(_pdf(1)), "pasta/a.pdf"), (io.BytesIO(_pdf(2)), "pasta/b.pdf"), (io.BytesIO(_pdf(1)), "pasta/copia_de_a.pdf")]}
    code, body = ctx.chamar(ctx.nr.api_notas_see_enviar_arquivos, method="POST", data=lote1, processamento_id=job_id)
    assert code == 200 and body["added"] == 2 and len(body["ignored"]) == 1
    lote2 = {"pdfs": [(io.BytesIO(_pdf(2)), "outra/b_de_novo.pdf"), (io.BytesIO(b"texto"), "nota.txt"), (io.BytesIO(b"nao e pdf"), "falso.pdf"), (io.BytesIO(_pdf(3)), "c.pdf")]}
    code, body = ctx.chamar(ctx.nr.api_notas_see_enviar_arquivos, method="POST", data=lote2, processamento_id=job_id)
    assert body["added"] == 1 and len(body["ignored"]) == 3
    razoes = " | ".join(i["reason"] for i in body["ignored"])
    assert "duplicado" in razoes and "Extensão" in razoes and "não é um PDF" in razoes
    assert (ctx.nr.SEE_UPLOAD_DIR / str(job_id)).exists()


def test_iniciar_sem_arquivos_descarta_e_apaga(ctx):
    cid = _catalogo(ctx)
    job_id = _novo_job(ctx, cid)
    code, body = ctx.chamar(ctx.nr.api_notas_see_iniciar, method="POST", processamento_id=job_id)
    assert code == 400 and "Nenhum" in body["error"]
    from models import SeeProcessamento

    ctx.db.session.rollback()
    assert ctx.db.session.get(SeeProcessamento, job_id).status == "descartado"
    code, _ = ctx.chamar(ctx.nr.api_notas_see_enviar_arquivos, method="POST", data={"pdfs": [(io.BytesIO(_pdf(9)), "a.pdf")]}, processamento_id=job_id)
    assert code == 409, "processamento descartado não recebe mais lotes"


def test_iniciar_dispara_o_worker_e_avisa_se_faltar_pdfplumber(ctx, monkeypatch):
    chamado = []
    monkeypatch.setattr(ctx.nr, "_start_see_worker", lambda job: chamado.append(job.id))
    cid = _catalogo(ctx)
    job_id = _novo_job(ctx, cid)
    ctx.chamar(ctx.nr.api_notas_see_enviar_arquivos, method="POST", data={"pdfs": [(io.BytesIO(_pdf(5)), "a.pdf")]}, processamento_id=job_id)
    monkeypatch.setattr(ctx.nr, "pdfplumber_disponivel", lambda: True)
    code, body = ctx.chamar(ctx.nr.api_notas_see_iniciar, method="POST", processamento_id=job_id)
    assert code == 202 and body["added"] == 1 and chamado == [job_id]

    job2 = _novo_job(ctx, cid)
    ctx.chamar(ctx.nr.api_notas_see_enviar_arquivos, method="POST", data={"pdfs": [(io.BytesIO(_pdf(6)), "b.pdf")]}, processamento_id=job2)
    monkeypatch.setattr(ctx.nr, "pdfplumber_disponivel", lambda: False)
    code, body = ctx.chamar(ctx.nr.api_notas_see_iniciar, method="POST", processamento_id=job2)
    assert code == 503 and "pdfplumber" in body["error"]
    assert chamado == [job_id], "sem a biblioteca o worker não é disparado"


@precisa_pdfs
def test_processamento_completo_gera_excel_e_apaga_os_pdfs(ctx, monkeypatch):
    from models import SeeCatalogo, SeeCatalogoProduto, SeeItemExtraido, SeeProcessamento

    pdfs = _amostra(3)
    produtos = _catalogo_dos_pdfs(pdfs)[:6]
    code, body = ctx.chamar(ctx.nr.api_notas_see_catalogos, method="POST", json={"nome": "TESTE CLAUDE", "exercicio": 2099})
    cid = body["catalogo"]["id"]
    for pr in produtos:
        ctx.chamar(ctx.nr.api_notas_see_produto_criar, method="POST", json={"codigo": pr["codigo"], "nome": pr["nome"]}, catalogo_id=cid)
    job_id = _novo_job(ctx, cid)
    arquivos = [(io.BytesIO(p.read_bytes()), p.name) for p in pdfs]
    code, body = ctx.chamar(ctx.nr.api_notas_see_enviar_arquivos, method="POST", data={"pdfs": arquivos}, processamento_id=job_id)
    assert body["added"] == 3
    monkeypatch.setattr(ctx.nr, "_start_see_worker", lambda job: None)
    monkeypatch.setattr(ctx.nr, "pdfplumber_disponivel", lambda: True)
    code, _ = ctx.chamar(ctx.nr.api_notas_see_iniciar, method="POST", processamento_id=job_id)
    assert code == 202

    ctx.nr._run_see_processamento(ctx.app, job_id)  # o que o worker faz (em outro contexto/sessão)
    ctx.db.session.rollback()  # encerra o snapshot desta sessão para enxergar o que o worker gravou
    job = ctx.db.session.get(SeeProcessamento, job_id)
    assert job.status in {"finalizado", "finalizado_com_alertas"}, (job.status, job.erro_geral)
    assert job.arquivos_processados == 3 and job.arquivos_erro == 0
    saida = Path(job.caminho_arquivo_saida)
    try:
        assert saida.exists() and saida.stat().st_size > 0
        wb = load_workbook(saida)
        assert "Planilha Consolidada" in wb.sheetnames
        assert ctx.db.session.query(SeeItemExtraido).filter_by(processamento_id=job_id).count() > 0
        assert not (ctx.nr.SEE_UPLOAD_DIR / str(job_id)).exists(), "PDFs apagados ao concluir (SEE_MANTER_PDFS não definido)"
        code, hist = ctx.chamar(ctx.nr.api_notas_see_historico)
        assert code == 200
    finally:
        saida.unlink(missing_ok=True)
