"""Job orfao do EMP/NOB (incidente do upload 253 do EMP, 12/11/2026).

O worker morreu com o status gravado como "em processamento"; o Cancelar so gravava o
pedido (que ninguem lia) e renovava o updated_at, estendendo a trava 409. Agora o status
de worker morto vira falha, o Cancelar encerra o job orfao e nao renova a trava.
"""
import json
import subprocess
import sys
from datetime import datetime, timedelta

import pytest

from services import job_status


def _grava(base, kind, upload_id, state="em processamento", minutos_atras=0, pid=None):
    base.mkdir(parents=True, exist_ok=True)
    dados = {
        "kind": kind, "upload_id": upload_id, "state": state, "pid": pid, "progress": 0,
        "message": "Processamento iniciado.",
        "updated_at": (datetime.utcnow() - timedelta(minutes=minutos_atras)).isoformat(),
    }
    (base / f"{kind}_{upload_id}.json").write_text(json.dumps(dados), encoding="utf-8")


@pytest.fixture
def status_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(job_status, "STATUS_DIR", tmp_path)
    return tmp_path


def _morto(monkeypatch, *pids_mortos):
    monkeypatch.setattr(job_status, "_pid_vivo", lambda pid: int(pid) not in pids_mortos)


def test_worker_morto_vira_falha_e_libera_a_trava(status_dir, monkeypatch):
    _morto(monkeypatch, 4321)
    _grava(status_dir, "emp", 253, pid=4321, minutos_atras=5)
    assert job_status.upload_em_andamento("emp") is None
    dados = job_status.read_status("emp", 253)
    assert dados["state"] == "falha no processamento"
    assert "não está mais em execução" in dados["message"]


def test_carencia_bloqueia_segundo_envio_logo_depois(status_dir, monkeypatch):
    """Status de 1 min atras com pid "morto": ainda bloqueia (EMP 256)."""
    _morto(monkeypatch, 4321)
    _grava(status_dir, "emp", 255, pid=4321, minutos_atras=1)
    assert job_status.upload_em_andamento("emp") == 255
    assert job_status.read_status("emp", 255)["state"] == "em processamento"


def test_worker_vivo_continua_em_processamento(status_dir, monkeypatch):
    _morto(monkeypatch)
    _grava(status_dir, "emp", 254, pid=999, minutos_atras=1)
    assert job_status.encerrar_se_orfao("emp", 254)["state"] == "em processamento"
    assert job_status.upload_em_andamento("emp") == 254


def test_sem_pid_so_encerra_depois_do_prazo(status_dir):
    _grava(status_dir, "nob", 10, minutos_atras=5)
    assert job_status.encerrar_se_orfao("nob", 10)["state"] == "em processamento"
    _grava(status_dir, "nob", 11, minutos_atras=120)
    assert job_status.encerrar_se_orfao("nob", 11)["state"] == "falha no processamento"


def test_cancelar_job_ja_finalizado_e_recusado(status_dir):
    _grava(status_dir, "emp", 1, state="processamento finalizado")
    aceito, msg = job_status.solicitar_cancelamento("emp", 1)
    assert not aceito and "finalizado" in msg
    assert job_status.read_status("emp", 1)["state"] == "processamento finalizado"
    assert not job_status.read_cancel_flag("emp", 1)


def test_cancelar_worker_morto_encerra_na_hora(status_dir, monkeypatch):
    _morto(monkeypatch, 4321)
    _grava(status_dir, "emp", 253, pid=4321, minutos_atras=5)
    aceito, _ = job_status.solicitar_cancelamento("emp", 253)
    assert aceito
    assert job_status.read_status("emp", 253)["state"] == "processamento cancelado"
    assert not job_status.read_cancel_flag("emp", 253)
    assert job_status.upload_em_andamento("emp") is None


def test_cancelar_worker_vivo_pede_e_nao_renova_a_trava(status_dir, monkeypatch):
    _morto(monkeypatch)
    _grava(status_dir, "emp", 300, pid=999, minutos_atras=30)
    antes = job_status.read_status("emp", 300)["updated_at"]
    aceito, msg = job_status.solicitar_cancelamento("emp", 300)
    assert aceito and "de novo" in msg
    dados = job_status.read_status("emp", 300)
    assert dados["state"] == "em processamento"
    assert dados["message"] == "Cancelamento solicitado."
    assert dados["updated_at"] == antes  # o clique nao estende os 90 minutos
    assert job_status.read_cancel_flag("emp", 300)


def test_segundo_cancelar_sem_resposta_encerra(status_dir, monkeypatch):
    _morto(monkeypatch)
    _grava(status_dir, "emp", 301, pid=999)
    agora = datetime.utcnow()
    job_status.solicitar_cancelamento("emp", 301, agora=agora)
    # antes do prazo: continua aguardando o worker
    job_status.solicitar_cancelamento("emp", 301, agora=agora + timedelta(seconds=60))
    assert job_status.read_status("emp", 301)["state"] == "em processamento"
    # depois do prazo (pedido gravado ha mais de 5 min): encerra
    (status_dir / "emp_301.cancel").write_text((agora - timedelta(minutes=6)).isoformat(), encoding="utf-8")
    aceito, msg = job_status.solicitar_cancelamento("emp", 301, agora=agora)
    assert aceito and "encerrado" in msg
    assert job_status.read_status("emp", 301)["state"] == "processamento cancelado"
    assert job_status.upload_em_andamento("emp") is None


def test_pid_de_processo_encerrado_e_morto():
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    assert job_status._pid_vivo(proc.pid) is False
    assert job_status._pid_vivo(None) is True  # sem pid: nao afirma que morreu


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="processo zumbi so no Linux")
def test_processo_zumbi_conta_como_morto():
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    import time

    time.sleep(0.5)  # terminou, mas nao foi recolhido (sem wait): zumbi
    try:
        assert job_status._pid_vivo(proc.pid) is False
    finally:
        proc.wait()


def test_worker_espera_a_vez_antes_de_carregar_a_aplicacao(tmp_path):
    """Com a fila ocupada, o worker grava "Aguardando a vez" sem importar o app (pesado)."""
    import os
    import time
    from pathlib import Path

    from services import fila_pesada

    raiz = Path(__file__).resolve().parents[1]
    upload_id = 990000 + os.getpid() % 9999
    status = raiz / "outputs" / "status" / f"emp_{upload_id}.json"
    trava = fila_pesada.aguardar_vez(lock_path=raiz / fila_pesada.LOCK_PATH, timeout_s=5)
    env = dict(os.environ, FILA_PESADA_TIMEOUT_S="30", PYTHONDONTWRITEBYTECODE="1")
    log = tmp_path / "importtime.log"
    log_handle = open(log, "wb")  # arquivo, nao pipe: o importtime enche o pipe e trava o worker
    proc = subprocess.Popen(
        [sys.executable, "-X", "importtime", "worker.py", "--kind", "emp", "--upload-id", str(upload_id)],
        cwd=str(raiz), env=env, stdout=subprocess.DEVNULL, stderr=log_handle,
    )
    try:
        limite = time.monotonic() + 20
        while time.monotonic() < limite and not status.exists():
            time.sleep(0.2)
        assert status.exists(), "worker nao avisou que esta aguardando a vez"
        assert "Aguardando a vez" in json.loads(status.read_text(encoding="utf-8"))["message"]
        time.sleep(1)
        assert proc.poll() is None  # continua esperando
    finally:
        proc.kill()
        proc.wait(timeout=20)
        log_handle.close()
        if trava:
            trava.close()
        status.unlink(missing_ok=True)
    importados = {linha.split("|")[-1].strip() for linha in log.read_text(errors="replace").splitlines()}
    assert "app" not in importados and "flask" not in importados
