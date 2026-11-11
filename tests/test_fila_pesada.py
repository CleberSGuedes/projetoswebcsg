"""Fila de processamento pesado e protecoes contra reenvio (incidente EMP de 11/11).

O limite de processos da conta (NPROC) fazia o Popen do worker falhar (Errno 11); o plano B por
thread tambem falhava, o upload ficava preso em "em processamento" e a automacao reenviava o
arquivo ao receber 503 na consulta de status.
"""
import json
import subprocess
from datetime import datetime, timedelta

import pytest

from app import create_app
from services import fila_pesada
from services import job_status


def _grava_status(base, kind, upload_id, state="em processamento", minutos_atras=0, pid=None):
    base.mkdir(parents=True, exist_ok=True)
    dados = {
        "kind": kind, "upload_id": upload_id, "state": state, "pid": pid,
        "updated_at": (datetime.utcnow() - timedelta(minutes=minutos_atras)).isoformat(),
    }
    (base / f"{kind}_{upload_id}.json").write_text(json.dumps(dados), encoding="utf-8")


def test_fila_so_deixa_um_por_vez(tmp_path):
    caminho = tmp_path / "fila.lock"
    primeiro = fila_pesada.aguardar_vez(lock_path=caminho)
    avisos = []
    with pytest.raises(TimeoutError):
        fila_pesada.aguardar_vez(timeout_s=0.1, on_wait=lambda: avisos.append(1), lock_path=caminho)
    assert avisos == [1]  # avisou uma unica vez que ficou esperando
    primeiro.close()  # liberou a vez
    segundo = fila_pesada.aguardar_vez(timeout_s=1, lock_path=caminho)
    assert segundo is not None
    segundo.close()


def test_fila_pode_ser_desligada(tmp_path, monkeypatch):
    monkeypatch.setenv("FILA_PESADA", "0")
    assert fila_pesada.aguardar_vez(lock_path=tmp_path / "x.lock") is None


def test_upload_em_andamento_ignora_status_antigo_ou_finalizado(tmp_path, monkeypatch):
    monkeypatch.setattr(job_status, "STATUS_DIR", tmp_path)
    assert job_status.upload_em_andamento("emp") is None
    _grava_status(tmp_path, "emp", 10, state="processamento finalizado")
    _grava_status(tmp_path, "emp", 11, minutos_atras=300)  # esquecido (parado ha 5h)
    _grava_status(tmp_path, "nob", 12)  # outro tipo
    assert job_status.upload_em_andamento("emp") is None
    _grava_status(tmp_path, "emp", 13, minutos_atras=2)
    assert job_status.upload_em_andamento("emp") == 13
    assert job_status.upload_em_andamento("nob") == 12


def test_resposta_409_quando_ja_ha_upload_em_andamento(monkeypatch):
    from rotas import home_routes

    app = create_app()
    monkeypatch.setattr(home_routes, "upload_em_andamento", lambda tipo: 248)
    with app.test_request_context("/"):
        resp = home_routes._upload_em_andamento_409("emp")
        assert resp.status_code == 409
        assert resp.headers["Retry-After"] == "180"
        assert resp.get_json()["job_id"] == 248
    monkeypatch.setattr(home_routes, "upload_em_andamento", lambda tipo: None)
    with app.test_request_context("/"):
        assert home_routes._upload_em_andamento_409("emp") is None


def test_falha_ao_iniciar_worker_nao_roda_em_thread_e_marca_falha(tmp_path, monkeypatch):
    """Errno 11 no Popen: sem plano B por thread; o status vira falha (nao fica preso)."""
    from rotas import home_routes

    monkeypatch.chdir(tmp_path)
    chamadas = []
    monkeypatch.setattr(home_routes, "_start_thread", lambda *a: chamadas.append(a))

    def _popen(*a, **k):
        raise BlockingIOError(11, "Resource temporarily unavailable")

    monkeypatch.setattr(subprocess, "Popen", _popen)
    app = create_app()
    with app.test_request_context("/"):
        with pytest.raises(BlockingIOError):
            home_routes._start_worker("emp", 249)
        resp = home_routes._erro_upload_resposta(BlockingIOError(11, "x"), "emp")
    assert chamadas == []
    status = job_status.read_status("emp", 249)
    assert status["state"] == "falha no processamento"
    assert "tente novamente" in status["message"]
    assert resp.status_code == 503 and resp.headers["Retry-After"] == "180"
    assert job_status.upload_em_andamento("emp") is None  # nao bloqueia o proximo envio


def test_outro_erro_continua_500():
    from rotas import home_routes

    app = create_app()
    with app.test_request_context("/"):
        resp, codigo = home_routes._erro_upload_resposta(ValueError("arquivo ruim"), "emp")
    assert codigo == 500
