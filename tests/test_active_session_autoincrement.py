"""Confirma que logs_login/active_sessions continuam usando o AUTO_INCREMENT
nativo do MySQL (sem geracao manual de id em Python). Este teste grava e
remove, na sequencia, uma linha descartavel no banco remoto compartilhado -
nenhum dado real e alterado.
"""
from datetime import datetime

from app import create_app
from models import db, ActiveSession, LogLogin

TEST_EMAIL = "teste-pytest-autoincrement@local"


def test_log_login_gets_id_from_mysql():
    app = create_app()
    with app.app_context():
        entry = LogLogin(email=TEST_EMAIL, status="teste", motivo="pytest autoincrement")
        db.session.add(entry)
        db.session.commit()
        try:
            assert entry.id is not None
            assert entry.id > 0
        finally:
            db.session.delete(entry)
            db.session.commit()


def test_active_session_gets_id_from_mysql():
    app = create_app()
    with app.app_context():
        active = ActiveSession(
            email=TEST_EMAIL,
            session_token="teste-pytest-token",
            last_activity=datetime.utcnow(),
        )
        db.session.add(active)
        db.session.commit()
        try:
            assert active.id is not None
            assert active.id > 0
        finally:
            db.session.delete(active)
            db.session.commit()
