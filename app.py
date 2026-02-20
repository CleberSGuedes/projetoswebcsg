from dotenv import load_dotenv
load_dotenv()

from datetime import datetime, timedelta
import secrets
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import uuid
from werkzeug.exceptions import HTTPException
from sqlalchemy import update, select, delete, func
from sqlalchemy.orm.exc import StaleDataError
from sqlalchemy.exc import SQLAlchemyError
from flask import Flask, g, session, request, jsonify
from flask_mail import Mail
from config import Config
from models import db, ActiveSession, Perfil
from rotas import register_blueprints

mail = Mail()
SESSION_TIMEOUT = timedelta(hours=2)


def _setup_logging(app: Flask) -> None:
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        log_dir / "app.log", maxBytes=2_000_000, backupCount=5, encoding="utf-8"
    )
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)
    app.logger.setLevel(logging.INFO)



def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    _setup_logging(app)

    db.init_app(app)
    mail.init_app(app)
    # Garante que as tabelas existam quando subir sem migrações
    with app.app_context():
        db.create_all()


    @app.errorhandler(Exception)
    def handle_exception(err):
        if isinstance(err, HTTPException):
            if request.path.startswith("/api/"):
                return jsonify({"error": err.description}), err.code
            return err

        trace_id = uuid.uuid4().hex
        app.logger.exception("Unhandled exception trace_id=%s path=%s", trace_id, request.path)
        if request.path.startswith("/api/"):
            return jsonify({"error": "Erro interno", "trace_id": trace_id}), 500
        return "Erro interno", 500

    @app.before_request
    def load_current_user():
        g.user = None
        g.active_sessions_count = 0
        g.user_perfil_id = None
        g.user_nivel = None
        user = session.get("user")
        token = session.get("session_token")
        if not user or not token:
            session.clear()
            return

        now = datetime.utcnow()
        cutoff = now - SESSION_TIMEOUT
        try:
            active_row = (
                db.session.execute(
                    select(
                        ActiveSession.email,
                        ActiveSession.session_token,
                        ActiveSession.last_activity,
                    ).where(ActiveSession.email == user.get("email"))
                )
                .mappings()
                .first()
            )
        except SQLAlchemyError:
            db.session.rollback()
            session.clear()
            return

        if not active_row or active_row.get("session_token") != token:
            session.clear()
            return

        last_activity = active_row.get("last_activity")
        if isinstance(last_activity, str):
            try:
                last_activity = datetime.fromisoformat(last_activity)
            except ValueError:
                last_activity = None

        if last_activity and last_activity < cutoff:
            try:
                db.session.execute(
                    delete(ActiveSession).where(
                        ActiveSession.email == user.get("email"),
                        ActiveSession.session_token == token,
                    )
                )
                db.session.commit()
            except SQLAlchemyError:
                db.session.rollback()
            session.clear()
            return

        try:
            result = db.session.execute(
                update(ActiveSession)
                .where(
                    ActiveSession.email == user.get("email"),
                    ActiveSession.session_token == token,
                )
                .values(last_activity=now)
            )
            db.session.commit()
            if result.rowcount == 0:
                session.clear()
                return
        except SQLAlchemyError:
            db.session.rollback()
            session.clear()
            return
        g.user = user
        perfil_id = user.get("perfil_id")
        try:
            perfil_row = db.session.get(Perfil, perfil_id) if perfil_id else None
        except SQLAlchemyError:
            db.session.rollback()
            session.clear()
            return
        if not perfil_row:
            session.clear()
            return
        if perfil_row:
            # mantem nome do perfil sincronizado para exibicao/compatibilidade.
            user["perfil"] = (perfil_row.nome or "").strip()
            session["user"] = user
        if perfil_row:
            g.user_perfil_id = perfil_row.id
            g.user_nivel = perfil_row.nivel
        try:
            g.active_sessions_count = (
                db.session.execute(
                    select(func.count())
                    .select_from(ActiveSession)
                    .where(ActiveSession.last_activity >= cutoff)
                ).scalar()
                or 0
            )
        except SQLAlchemyError:
            db.session.rollback()
            session.clear()
            return

    register_blueprints(app)
    return app


app = create_app()
application = app  # WSGI entrypoint para IIS/wfastcgi

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
