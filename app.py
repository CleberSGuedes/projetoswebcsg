from dotenv import load_dotenv
load_dotenv()

from datetime import datetime, timedelta
import secrets
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import uuid
from werkzeug.exceptions import HTTPException
from flask import Flask, g, session, request, jsonify
from flask_mail import Mail
from config import Config
from models import db, ActiveSession, Perfil
from sqlalchemy import func
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
        active = ActiveSession.query.filter_by(email=user.get("email")).first()

        if not active or active.session_token != token:
            session.clear()
            return

        last_activity = active.last_activity
        if isinstance(last_activity, str):
            try:
                last_activity = datetime.fromisoformat(last_activity)
            except ValueError:
                last_activity = None

        if last_activity and last_activity < cutoff:
            db.session.delete(active)
            db.session.commit()
            session.clear()
            return

        active.last_activity = now
        db.session.commit()
        g.user = user
        perfil_row = None
        perfil_id = user.get("perfil_id")
        if perfil_id:
            perfil_row = db.session.get(Perfil, perfil_id)
        if not perfil_row:
            perfil_nome = (user.get("perfil") or "").strip()
            if perfil_nome:
                normalized = func.lower(func.ltrim(func.rtrim(Perfil.nome)))
                perfil_row = Perfil.query.filter(normalized == perfil_nome.lower()).first()
                if not perfil_row:
                    perfil_row = Perfil.query.filter(Perfil.nome.ilike(perfil_nome)).first()
        if perfil_row and not perfil_id:
            # atualiza sessao com id resolvido
            user["perfil_id"] = perfil_row.id
            session["user"] = user
        if perfil_row:
            g.user_perfil_id = perfil_row.id
            g.user_nivel = perfil_row.nivel
        g.active_sessions_count = ActiveSession.query.filter(
            ActiveSession.last_activity >= cutoff
        ).count()

    register_blueprints(app)
    return app


app = create_app()
application = app  # WSGI entrypoint para IIS/wfastcgi

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
