"""API de dados (somente leitura): /export, /meta, /schema, paginação por after_id e PLAN 51.

- Sem banco: rótulos únicos por relatório e todos existentes no model.
- Com banco: cliente de API descartável (criado e removido aqui), carga descartável
  do PLAN 51 no exercício 9991 (upload 910101), removida ao final. Os demais
  relatórios só são lidos (EST EMP, a menor tabela).
"""
from __future__ import annotations

import csv
import io
import uuid
from datetime import datetime

import pytest
from sqlalchemy import inspect, text
from werkzeug.security import generate_password_hash

from rotas import api_dados_routes as ad

EX = 9991
UPLOAD = 910101
SEGREDO = "segredo-de-teste"


# ---------------------------------------------------------------- sem banco
@pytest.mark.parametrize("rel", sorted(ad.RELATORIOS))
def test_rotulos_unicos_e_existentes_no_model(rel):
    cfg = ad.RELATORIOS[rel]
    chaves = {c.key for c in inspect(cfg["modelo"]).columns}
    assert set(cfg["rotulos"]) <= chaves, f"{rel}: rótulo de coluna que não existe no model"
    cols = ad._colunas_modelo(cfg, "rotulo")
    nomes = [c for _, c in cols]
    assert len(nomes) == len(set(nomes)), f"{rel}: cabeçalho repetido"
    assert not ({k for k, _ in cols} & ad._TECNICAS)


def test_rotulos_seguem_o_excel():
    cols = dict(ad._colunas_modelo(ad.RELATORIOS["nob"], "rotulo"))
    assert cols["numero_nob"] == "Nº NOB" and cols["valor_nob"] == "Valor NOB"
    assert "upload_id" not in cols and "id" not in cols
    banco = dict(ad._colunas_modelo(ad.RELATORIOS["nob"], "banco"))
    assert "upload_id" in banco and "raw_payload" not in banco


def test_escopo_do_plan51_na_tela_de_acessos():
    from rotas.home_routes import API_SCOPE_SET

    assert "dados.plan51.read" in API_SCOPE_SET


# ---------------------------------------------------------------- com banco
@pytest.fixture(scope="module")
def app():
    from app import create_app

    return create_app()


@pytest.fixture(scope="module")
def ambiente(app):
    from models import ApiAccessLog, ApiClient, ApiClientScope, ApiKey, Plan51Registro, Plan51Upload, db

    cid = "cli_teste_" + uuid.uuid4().hex[:10]
    with app.app_context():
        cli = ApiClient(nome="teste-api-dados", client_id=cid, client_secret_hash=generate_password_hash(SEGREDO), status="ativo")
        db.session.add(cli)
        db.session.flush()
        for sc in ("dados.plan51.read", "dados.est_emp.read"):
            db.session.add(ApiClientScope(client_id=cli.id, scope=sc, created_at=datetime.now()))
        db.session.add(Plan51Upload(id=UPLOAD, user_email="teste@local", original_filename="t.xlsx", stored_filename="t.xlsx", data_arquivo=datetime(2026, 11, 4)))
        for i, (sup, anu) in enumerate([("10.50", None), (None, "4.25"), ("1.00", None)]):
            db.session.add(Plan51Registro(upload_id=UPLOAD, exercicio=EX, uo_def="19301", valor_supl=sup, valor_anul=anu, ativo=True, estilo=0))
        db.session.commit()
        db_id = cli.id
    yield {"client_id": cid}
    with app.app_context():
        db.session.rollback()
        db.session.execute(text("DELETE FROM plan51 WHERE upload_id = :u"), {"u": UPLOAD})
        db.session.execute(text("DELETE FROM plan51_uploads WHERE id = :u"), {"u": UPLOAD})
        db.session.query(ApiAccessLog).filter_by(client_id=db_id).delete()
        db.session.query(ApiKey).filter_by(client_id=db_id).delete()
        db.session.query(ApiClientScope).filter_by(client_id=db_id).delete()
        db.session.query(ApiClient).filter_by(id=db_id).delete()
        db.session.commit()


@pytest.fixture(scope="module")
def http(app, ambiente):
    c = app.test_client()
    r = c.post("/api/auth/token", json={"client_id": ambiente["client_id"], "client_secret": SEGREDO})
    assert r.status_code == 200, r.get_data(as_text=True)
    return c, {"Authorization": "Bearer " + r.get_json()["access_token"]}


def _csv(resp):
    return list(csv.reader(io.StringIO(resp.get_data(as_text=True))))


def test_sem_token_e_sem_escopo(http):
    c, h = http
    assert c.get("/api/v1/dados/plan51/export").status_code == 401
    assert c.get("/api/v1/dados/nob/export", headers=h).status_code == 403
    assert c.get("/api/v1/dados/nob/meta", headers=h).status_code == 403
    assert c.get("/api/v1/dados/nob/schema", headers=h).status_code == 403


def test_export_csv_do_plan51(http):
    c, h = http
    r = c.get(f"/api/v1/dados/plan51/export?exercicio={EX}", headers=h)
    assert r.status_code == 200 and r.headers["Content-Type"].startswith("text/csv")
    linhas = _csv(r)
    assert len(linhas) - 1 == 3 == int(r.headers["X-Total"])
    assert r.headers["X-Upload-Id"] == str(UPLOAD)
    cab = linhas[0]
    assert "VALOR SUPL" in cab and "VALOR ANUL" in cab and "estilo" not in cab
    i = cab.index("VALOR SUPL")
    assert [l[i] for l in linhas[1:]] == ["10.50", "", "1.00"], "decimal com ponto, vazio quando nulo"


def test_export_filtra_pelo_rotulo_e_fields(http):
    c, h = http
    r = c.get(f"/api/v1/dados/plan51/export?exercicio={EX}&fields=VALOR SUPL,valor_anul", headers=h)
    linhas = _csv(r)
    assert linhas[0] == ["VALOR SUPL", "VALOR ANUL"] and len(linhas) == 4


def test_export_json_e_colunas_banco(http):
    c, h = http
    j = c.get(f"/api/v1/dados/plan51/export?exercicio={EX}&formato=json&colunas=banco&fields=upload_id,valor_supl", headers=h).get_json()
    assert j["ok"] and j["total"] == 3 and j["upload_id"] == [UPLOAD] and j["colunas"] == ["upload_id", "valor_supl"]
    assert j["data"][0] == {"upload_id": UPLOAD, "valor_supl": 10.5}


def test_parametros_invalidos(http):
    c, h = http
    assert c.get("/api/v1/dados/plan51/export?formato=xml", headers=h).status_code == 400
    assert c.get("/api/v1/dados/plan51/export?colunas=x", headers=h).status_code == 400
    assert c.get("/api/v1/dados/est-emp/export?uo=abc", headers=h).status_code == 400


def test_meta_do_plan51(http):
    c, h = http
    j = c.get(f"/api/v1/dados/plan51/meta?exercicio={EX}", headers=h).get_json()
    assert j["ok"] and j["total"] == 3 and j["upload_id"] == [UPLOAD]
    assert j["cargas"][0]["linhas"] == 3 and j["cargas"][0]["enviado_em"] and j["atualizado_em"]
    assert "user_email" not in str(j)


def test_schema(http):
    c, h = http
    j = c.get("/api/v1/dados/plan51/schema", headers=h).get_json()
    tipos = {x["rotulo"]: x["tipo"] for x in j["colunas"]}
    assert tipos["VALOR SUPL"] == "numero" and j["total_colunas"] >= 66


def test_paginacao_por_after_id_nao_repete_nem_perde(http):
    c, h = http
    vistos, after = [], None
    for _ in range(5):
        url = f"/api/v1/dados/plan51?exercicio={EX}&limit=2&order_by=id&order_dir=asc&fields=id,valor_supl"
        if after:
            url += f"&after_id={after}"
        j = c.get(url, headers=h).get_json()
        vistos += [d["id"] for d in j["data"]]
        assert j["upload_id"] == [UPLOAD]
        after = j["next_after_id"]
        if not after:
            break
    assert len(vistos) == 3 == len(set(vistos)) and vistos == sorted(vistos)
    assert c.get("/api/v1/dados/plan51?after_id=x", headers=h).status_code == 400
    assert c.get("/api/v1/dados/plan51?after_id=1&order_by=exercicio", headers=h).status_code == 400


def test_offset_antigo_continua_funcionando(http):
    c, h = http
    j = c.get(f"/api/v1/dados/plan51?exercicio={EX}&limit=2&offset=2&order_by=id&order_dir=asc", headers=h).get_json()
    assert j["total"] == 3 and j["count"] == 1 and "estilo" not in j["data"][0]


def test_est_emp_export_bate_com_meta(http):
    c, h = http
    meta = c.get("/api/v1/dados/est-emp/meta", headers=h).get_json()
    r = c.get("/api/v1/dados/est-emp/export", headers=h)
    linhas = _csv(r)
    assert len(linhas) - 1 == meta["total"] == int(r.headers["X-Total"])
    assert linhas[0][0].startswith("Exerc")
    assert r.headers["X-UO"] == "seduc"
