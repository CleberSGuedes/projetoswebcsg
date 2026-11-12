"""Trava 409 atomica, spawn do worker e jobs orfaos (incidentes de 11 e 12/11/2026).

- A trava 409 do EMP quebrava com TypeError (horario "Z" do Node x utcnow) e liberava o envio.
- Checagem e criacao nao eram atomicas.
- Jobs gerenciados (FIP 613, PED, EST EMP, Receita) com worker morto travavam o tipo para sempre.
- O Reprocessar do EMP/NOB reaproveitava o id do upload.
"""
import importlib.util
import inspect
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from app import create_app
from models import EmpUpload, ProcessamentoEvento, ProcessamentoJob, db
from services import job_status
from services import processamento_jobs as pj

RAIZ = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def app():
    return create_app()


@pytest.fixture
def status_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(job_status, "STATUS_DIR", tmp_path)
    return tmp_path


def _pid_morto() -> int:
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


def _grava(base, kind, upload_id, updated_at, state="em processamento", pid=None):
    base.mkdir(parents=True, exist_ok=True)
    dados = {"kind": kind, "upload_id": upload_id, "state": state, "pid": pid, "updated_at": updated_at}
    (base / f"{kind}_{upload_id}.json").write_text(json.dumps(dados), encoding="utf-8")


# --- horario do Node ("Z") -------------------------------------------------------------

def test_parse_utc_aceita_python_e_node():
    agora = datetime.utcnow().replace(microsecond=0)
    assert job_status.parse_utc(agora.isoformat()) == agora
    assert job_status.parse_utc(agora.isoformat() + "Z") == agora  # toISOString do Node
    assert job_status.parse_utc((agora - timedelta(hours=3)).isoformat() + "-03:00") == agora
    assert job_status.parse_utc("lixo") is None and job_status.parse_utc(None) is None


def test_trava_409_com_horario_do_node_nao_quebra(status_dir):
    """Antes: TypeError (naive x aware) -> a checagem falhava e o 256 era aceito."""
    node_iso = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
    _grava(status_dir, "emp", 255, node_iso, pid=os.getpid())
    assert job_status.upload_em_andamento("emp") == 255


# --- identificacao do worker do SPO ------------------------------------------------------

def test_worker_da_automacao_nao_e_confundido_com_o_do_spo():
    spo = b"/opt/python3\0/home/proj5954/projetoswebcsg_app/worker.py\0--kind\0emp\0--upload-id\x00257\0"
    automacao = b"/usr/bin/python3\0/home/proj5954/automacao/pedidos_worker.py\0"
    assert job_status._args_worker_spo(spo) == ["--kind", "emp", "--upload-id", "257"]
    assert job_status._args_worker_spo(automacao) is None


# --- trava atomica (concorrencia real, GET_LOCK no banco) ---------------------------------

def _corrida(app, tipo_a, tipo_b, segura_s=1.5):
    from rotas import home_routes

    def lenta():
        time.sleep(segura_s)
        return "ok", 200

    views = {t: home_routes._upload_exclusivo(t)(lenta) for t in {tipo_a, tipo_b}}
    resultados = []

    def chamar(tipo, atraso):
        time.sleep(atraso)
        with app.test_request_context("/", method="POST"):
            resp = views[tipo]()
            resultados.append(resp[1] if isinstance(resp, tuple) else resp.status_code)

    threads = [threading.Thread(target=chamar, args=(tipo_a, 0)),
               threading.Thread(target=chamar, args=(tipo_b, 0.3))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    return sorted(resultados)


def test_dois_uploads_simultaneos_do_mesmo_tipo_um_200_um_409(app):
    assert _corrida(app, "teste_trava_a", "teste_trava_a") == [200, 409]


def test_tipos_diferentes_nao_se_bloqueiam(app):
    assert _corrida(app, "teste_trava_b", "teste_trava_c") == [200, 200]


def test_trava_e_liberada_depois_da_requisicao(app):
    assert _corrida(app, "teste_trava_d", "teste_trava_d", segura_s=0.1) == [200, 200]


def test_nome_da_trava_leva_o_banco(app):
    from rotas import home_routes

    with app.app_context():
        nome = home_routes._nome_trava_upload("receita_anexo10")
        assert db.engine.url.database[:20] in nome and len(nome) <= 64


# --- spawn do worker ---------------------------------------------------------------------

def test_spawn_destacado_sem_stdin_e_com_threads_limitadas(app, tmp_path, status_dir, monkeypatch):
    from rotas import home_routes

    chamadas = []

    class _Proc:
        pid = 4242

        def poll(self):
            return 0

    def _popen(args, **kwargs):
        chamadas.append(kwargs)
        return _Proc()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(subprocess, "Popen", _popen)
    with app.test_request_context("/"):
        home_routes._start_worker("emp", 9001)
        home_routes._start_worker("emp", 9002)  # recolhe o anterior (poll) antes
    kw = chamadas[0]
    assert kw["stdin"] is subprocess.DEVNULL and kw["close_fds"] is True
    assert kw["env"]["OPENBLAS_NUM_THREADS"] and kw["env"]["OMP_NUM_THREADS"]
    if not sys.platform.startswith("win"):
        assert kw["start_new_session"] is True
    assert job_status.read_status("emp", 9001)["pid"] == 4242
    assert all(p.pid == 4242 for p in home_routes._WORKERS_INICIADOS)
    assert len(home_routes._WORKERS_INICIADOS) == 1


# --- Reprocessar do EMP --------------------------------------------------------------------

@pytest.fixture
def upload_emp_origem(app, tmp_path, monkeypatch):
    from rotas import home_routes

    pasta = tmp_path / "upload_emp"
    pasta.mkdir()
    (pasta / "emp_teste_reprocesso.xlsx").write_bytes(b"x")
    monkeypatch.setattr(home_routes, "EMP_UPLOAD_DIR", pasta)
    with app.app_context():
        origem = EmpUpload(user_email="teste-claude@local", original_filename="EMP teste.xlsx",
                           stored_filename="emp_teste_reprocesso.xlsx", data_arquivo=datetime(2026, 11, 12),
                           uploaded_at=datetime.utcnow())
        db.session.add(origem)
        db.session.commit()
        origem_id = origem.id
    criados = [origem_id]
    yield origem_id, criados
    with app.app_context():
        db.session.execute(text("DELETE FROM emp_uploads WHERE id IN :ids").bindparams(
            __import__("sqlalchemy").bindparam("ids", expanding=True)), {"ids": criados})
        db.session.commit()


def _reprocessar_emp(app, upload_id):
    from rotas import home_routes

    view = inspect.unwrap(home_routes.api_emp_reprocess)  # sem login/permissao/trava
    with app.test_request_context("/api/emp/reprocess", method="POST", json={"upload_id": upload_id}):
        resp = view()
    return (resp[0], resp[1]) if isinstance(resp, tuple) else (resp, resp.status_code)


def test_reprocessar_emp_cria_id_novo(app, status_dir, upload_emp_origem, monkeypatch):
    from rotas import home_routes

    origem_id, criados = upload_emp_origem
    iniciados = []
    monkeypatch.setattr(home_routes, "_start_worker", lambda kind, uid: iniciados.append((kind, uid)))
    resp, codigo = _reprocessar_emp(app, origem_id)
    novo_id = resp.get_json()["job_id"]
    criados.append(novo_id)
    assert codigo == 200 and novo_id != origem_id
    assert iniciados == [("emp", novo_id)]
    assert job_status.read_status("emp", novo_id)["state"] == "em processamento"
    assert job_status.read_status("emp", origem_id) is None  # o status do original nao foi tocado


def test_reprocessar_emp_com_job_ativo_da_409(app, status_dir, upload_emp_origem, monkeypatch):
    from rotas import home_routes

    origem_id, _ = upload_emp_origem
    monkeypatch.setattr(home_routes, "_start_worker", lambda *a: pytest.fail("nao pode iniciar worker"))
    _grava(status_dir, "emp", origem_id, datetime.utcnow().isoformat(), pid=os.getpid())
    resp, codigo = _reprocessar_emp(app, origem_id)
    assert codigo == 409 and resp.get_json()["em_andamento"] is True


# --- jobs gerenciados: orfao e Cancelar ------------------------------------------------------

def test_motivo_orfao_regras():
    vivo, morto = (lambda pid: True), (lambda pid: False)
    assert pj.motivo_orfao(123, 30, morto) is None  # carencia de partida
    assert pj.motivo_orfao(123, 30, morto, ignorar_carencia=True)
    assert pj.motivo_orfao(123, 600, morto)
    assert pj.motivo_orfao(123, 99999, vivo) is None  # worker vivo nunca e orfao
    assert pj.motivo_orfao(None, 300, vivo) is None
    assert "nenhum worker" in pj.motivo_orfao(None, 700, vivo)


@pytest.fixture
def job_teste(app):
    tipo = f"teste_orfao_{os.getpid() % 100000}"
    with app.app_context():
        job = pj.create_job(tipo, SimpleNamespace(id=0, user_email="teste-claude@local",
                                                  original_filename="x.xlsx", stored_filename="x.xlsx"))
        job_id = job.id
    yield tipo, job_id
    with app.app_context():
        db.session.rollback()
        db.session.execute(text("DELETE FROM processamento_eventos WHERE processamento_id = :id"), {"id": job_id})
        db.session.execute(text("DELETE FROM processamento_jobs WHERE id = :id"), {"id": job_id})
        db.session.commit()


def _envelhecer(job_id, minutos):
    db.session.execute(text(
        "UPDATE processamento_jobs SET atualizado_em = NOW() - INTERVAL :m MINUTE WHERE id = :id"
    ), {"m": minutos, "id": job_id})
    db.session.commit()


def test_gerenciado_com_worker_morto_vira_falha_e_libera_o_tipo(app, job_teste):
    tipo, job_id = job_teste
    with app.app_context():
        job = db.session.get(ProcessamentoJob, job_id)
        job.worker_pid = _pid_morto()
        db.session.commit()
        _envelhecer(job_id, 10)
        assert pj.job_ativo(tipo) is None
        db.session.expire_all()
        job = db.session.get(ProcessamentoJob, job_id)
        assert job.status == "falha" and "worker" in job.mensagem_atual


def test_gerenciado_com_worker_vivo_continua_ativo(app, job_teste):
    tipo, job_id = job_teste
    with app.app_context():
        job = db.session.get(ProcessamentoJob, job_id)
        job.worker_pid = os.getpid()
        db.session.commit()
        _envelhecer(job_id, 60)
        assert pj.job_ativo(tipo).id == job_id


def test_cancelar_gerenciado_com_worker_morto_encerra_na_hora(app, job_teste):
    tipo, job_id = job_teste
    with app.app_context():
        job = db.session.get(ProcessamentoJob, job_id)
        job.worker_pid = _pid_morto()
        db.session.commit()
        aceito, msg = pj.solicitar_cancelamento_job(job)
        assert aceito and "encerrado" in msg
        assert db.session.get(ProcessamentoJob, job_id).status == "cancelado"
        aceito, msg = pj.solicitar_cancelamento_job(db.session.get(ProcessamentoJob, job_id))
        assert not aceito and "finalizado" in msg


def test_segundo_cancelar_gerenciado_sem_resposta_encerra(app, job_teste):
    tipo, job_id = job_teste
    with app.app_context():
        job = db.session.get(ProcessamentoJob, job_id)
        job.worker_pid = os.getpid()  # "vivo", mas nao responde ao pedido
        db.session.commit()
        aceito, msg = pj.solicitar_cancelamento_job(job)
        assert aceito and "de novo" in msg
        assert db.session.get(ProcessamentoJob, job_id).status == "cancelamento_solicitado"
        aceito, _ = pj.solicitar_cancelamento_job(db.session.get(ProcessamentoJob, job_id))
        assert db.session.get(ProcessamentoJob, job_id).status == "cancelamento_solicitado"  # antes do prazo
        db.session.execute(text(
            "UPDATE processamento_eventos SET created_at = NOW() - INTERVAL 10 MINUTE "
            "WHERE processamento_id = :id AND tipo_evento = 'cancelamento'"), {"id": job_id})
        db.session.commit()
        aceito, msg = pj.solicitar_cancelamento_job(db.session.get(ProcessamentoJob, job_id))
        assert aceito and "cancelado" in msg
        assert db.session.get(ProcessamentoJob, job_id).status == "cancelado"


# --- script de limpeza ------------------------------------------------------------------------

def _script():
    spec = importlib.util.spec_from_file_location("limpar_jobs_orfaos", RAIZ / "scripts" / "limpar_jobs_orfaos.py")
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


def test_script_limpeza_arquivos_respeita_worker_rodando_e_e_idempotente(status_dir):
    script = _script()
    velho = (datetime.utcnow() - timedelta(minutes=30)).isoformat()
    morto = _pid_morto()
    _grava(status_dir, "emp", 63, velho, pid=morto)
    _grava(status_dir, "nob", 3, velho)  # sem pid
    _grava(status_dir, "emp", 64, velho, pid=morto)  # "rodando" segundo o /proc
    _grava(status_dir, "emp", 65, datetime.utcnow().isoformat(), pid=morto)  # recente: carencia
    _grava(status_dir, "emp", 66, velho, state="processamento finalizado")
    workers = {("emp", 64): 777}
    achados = {(i["tipo"], i["id"]) for i in script.orfaos_em_arquivo(workers)}
    assert achados == {("emp", 63), ("nob", 3)}
    for item in script.orfaos_em_arquivo(workers):
        script.encerrar(item)
    assert job_status.read_status("emp", 63)["state"] == "falha no processamento"
    assert job_status.read_status("emp", 64)["state"] == "em processamento"
    assert script.orfaos_em_arquivo(workers) == []  # idempotente
