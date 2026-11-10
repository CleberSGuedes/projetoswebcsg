"""Sessao: prazo configuravel e encerramento da sessao antiga (login forcado em outro local).

Usa apenas linhas descartaveis em active_sessions (email de teste) e NAO grava em
configuracoes_sistema, para nao alterar a configuracao real do banco compartilhado.
"""
from datetime import datetime, timedelta

import pytest

from app import create_app
from models import db, ActiveSession
from services import session_config

EMAIL = "teste-pytest-sessao@local"


def test_clamp_limites():
    assert session_config._clamp(120) == 120
    assert session_config._clamp(1) == session_config.MIN_MINUTOS
    assert session_config._clamp(99999) == session_config.MAX_MINUTOS
    assert session_config._clamp("abc") == session_config.PADRAO_MINUTOS


def test_set_timeout_rejeita_fora_do_limite():
    with pytest.raises(ValueError):
        session_config.set_timeout_minutos(5)
    with pytest.raises(ValueError):
        session_config.set_timeout_minutos(session_config.MAX_MINUTOS + 1)
    with pytest.raises(ValueError):
        session_config.set_timeout_minutos("x")


@pytest.fixture()
def client_com_sessao():
    app = create_app()
    with app.app_context():
        ActiveSession.query.filter_by(email=EMAIL).delete()
        db.session.add(
            ActiveSession(email=EMAIL, session_token="tok-atual", last_activity=datetime.utcnow())
        )
        db.session.commit()
        client = app.test_client()
        with client.session_transaction() as sess:
            sess["user"] = {"id": 0, "email": EMAIL, "nome": "Teste", "perfil": "x", "perfil_id": None, "nivel": 9}
            sess["session_token"] = "tok-atual"
        try:
            yield client
        finally:
            db.session.rollback()
            ActiveSession.query.filter_by(email=EMAIL).delete()
            db.session.commit()


def _row():
    db.session.expire_all()
    return ActiveSession.query.filter_by(email=EMAIL).first()


def test_status_ok_nao_conta_como_atividade(client_com_sessao):
    antigo = (datetime.utcnow() - timedelta(minutes=10)).replace(microsecond=0)
    row = _row()
    row.last_activity = antigo
    db.session.commit()
    res = client_com_sessao.get("/api/sessao/status")
    assert res.status_code == 200
    assert res.get_json()["ok"] is True
    assert _row().last_activity == antigo  # heartbeat nao renova a inatividade


def test_status_outro_login_derruba(client_com_sessao):
    row = _row()
    row.session_token = "tok-do-outro-navegador"
    db.session.commit()
    res = client_com_sessao.get("/api/sessao/status")
    assert res.status_code == 401
    assert res.get_json()["reason"] == "outro_local"


def test_status_expirada(client_com_sessao):
    row = _row()
    row.last_activity = datetime.utcnow() - session_config.get_session_timeout() - timedelta(minutes=1)
    db.session.commit()
    res = client_com_sessao.get("/api/sessao/status")
    assert res.status_code == 401
    assert res.get_json()["reason"] == "expirada"


def test_linha_inexistente_nao_recria_sessao(client_com_sessao):
    ActiveSession.query.filter_by(email=EMAIL).delete()
    db.session.commit()
    res = client_com_sessao.get("/api/sessao/status")
    assert res.status_code == 401
    assert res.get_json()["reason"] == "encerrada"
    assert _row() is None  # nao foi recriada


def test_sem_cookie_de_sessao():
    app = create_app()
    res = app.test_client().get("/api/sessao/status")
    assert res.status_code == 401
    assert res.get_json()["reason"] == "sem_sessao"
