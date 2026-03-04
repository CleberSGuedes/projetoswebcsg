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
from sqlalchemy.exc import SQLAlchemyError, OperationalError, ResourceClosedError
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


def _safe_session_rollback() -> None:
    try:
        db.session.rollback()
    except Exception:
        try:
            db.session.remove()
        except Exception:
            pass
        try:
            db.engine.dispose()
        except Exception:
            pass


def _execute_with_retry(stmt, attempts: int = 2, backoff_s: float = 0.2):
    for idx in range(max(1, attempts)):
        try:
            return db.session.execute(stmt)
        except (OperationalError, ResourceClosedError):
            _safe_session_rollback()
            if idx < attempts - 1:
                try:
                    import time

                    time.sleep(backoff_s * (idx + 1))
                except Exception:
                    pass
    return None


def _best_effort_clear_active_session(email: str, token: str | None = None) -> None:
    if not email:
        return
    try:
        stmt = delete(ActiveSession).where(ActiveSession.email == email)
        if token:
            stmt = stmt.where(ActiveSession.session_token == token)
        db.session.execute(stmt)
        db.session.commit()
        return
    except Exception:
        _safe_session_rollback()
    try:
        stmt = delete(ActiveSession).where(ActiveSession.email == email)
        if token:
            stmt = stmt.where(ActiveSession.session_token == token)
        with db.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
    except Exception:
        pass


def _fetch_active_session(email: str):
    stmt = select(
        ActiveSession.email,
        ActiveSession.session_token,
        ActiveSession.last_activity,
    ).where(ActiveSession.email == email)
    result = _execute_with_retry(stmt)
    if result is None:
        return None
    try:
        return result.mappings().first()
    except Exception:
        _safe_session_rollback()
    try:
        with db.engine.connect() as conn:
            return conn.execute(stmt).mappings().first()
    except Exception:
        return None


def _next_pk_active_session() -> int:
    try:
        max_id = db.session.query(func.max(ActiveSession.id)).scalar() or 0
        return int(max_id) + 1
    except Exception:
        _safe_session_rollback()
        return 1


def _ensure_active_session(email: str, token: str, now: datetime) -> bool:
    if not email or not token:
        return False
    try:
        active = ActiveSession.query.filter_by(email=email).first()
        if not active:
            active = ActiveSession(
                id=_next_pk_active_session(),
                email=email,
                session_token=token,
                last_activity=now,
            )
            db.session.add(active)
        else:
            active.session_token = token
            active.last_activity = now
        db.session.commit()
        return True
    except Exception:
        _safe_session_rollback()
        return False



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

    @app.after_request
    def force_utf8_response(response):
        mimetype = response.mimetype or ""
        content_type = response.content_type or ""
        wants_utf8 = (
            mimetype.startswith("text/")
            or mimetype in ("application/javascript", "application/json")
        )
        if wants_utf8 and "charset=" not in content_type.lower():
            response.headers["Content-Type"] = f"{mimetype}; charset=utf-8"
        response.charset = "utf-8"
        return response

    @app.before_request
    def load_current_user():
        if request.path.startswith("/static/") or request.path == "/favicon.ico":
            return
        g.user = None
        g.active_sessions_count = 0
        g.user_perfil_id = None
        g.user_nivel = None
        user = session.get("user")
        token = session.get("session_token")
        if not user or not token:
            try:
                app.logger.info(
                    "auth preload missing session user=%s token=%s path=%s",
                    bool(user),
                    bool(token),
                    request.path,
                )
            except Exception:
                pass
            session.clear()
            return

        now = datetime.utcnow()
        cutoff = now - SESSION_TIMEOUT
        try:
            active_row = _fetch_active_session(user.get("email"))
        except SQLAlchemyError:
            _safe_session_rollback()
            _best_effort_clear_active_session(user.get("email"), token)
            try:
                app.logger.warning(
                    "auth preload db error, session cleared email=%s path=%s",
                    user.get("email"),
                    request.path,
                )
            except Exception:
                pass
            session.clear()
            return
        except IndexError:
            _safe_session_rollback()
            _best_effort_clear_active_session(user.get("email"), token)
            try:
                app.logger.warning(
                    "auth preload db index error, session cleared email=%s path=%s",
                    user.get("email"),
                    request.path,
                )
            except Exception:
                pass
            session.clear()
            return

        if active_row is None:
            if _ensure_active_session(user.get("email"), token, now):
                active_row = {"email": user.get("email"), "session_token": token, "last_activity": now}
            else:
                _best_effort_clear_active_session(user.get("email"), token)
                try:
                    app.logger.warning(
                        "auth preload active_row=None cleared email=%s path=%s",
                        user.get("email"),
                        request.path,
                    )
                except Exception:
                    pass
                session.clear()
                return

        if not active_row or active_row.get("session_token") != token:
            _best_effort_clear_active_session(user.get("email"), token)
            try:
                app.logger.warning(
                    "auth preload token mismatch cleared email=%s path=%s",
                    user.get("email"),
                    request.path,
                )
            except Exception:
                pass
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
                result = _execute_with_retry(
                    delete(ActiveSession).where(
                        ActiveSession.email == user.get("email"),
                        ActiveSession.session_token == token,
                    )
                )
                if result is None:
                    _safe_session_rollback()
                else:
                    db.session.commit()
            except SQLAlchemyError:
                _safe_session_rollback()
            try:
                app.logger.info(
                    "auth preload session expired email=%s path=%s",
                    user.get("email"),
                    request.path,
                )
            except Exception:
                pass
            session.clear()
            return

        try:
            result = _execute_with_retry(
                update(ActiveSession)
                .where(
                    ActiveSession.email == user.get("email"),
                    ActiveSession.session_token == token,
                )
                .values(last_activity=now)
            )
            if result is None:
                _safe_session_rollback()
                _best_effort_clear_active_session(user.get("email"), token)
                try:
                    app.logger.warning(
                        "auth preload last_activity update failed, cleared email=%s path=%s",
                        user.get("email"),
                        request.path,
                    )
                except Exception:
                    pass
                session.clear()
                return
            db.session.commit()
            if result.rowcount == 0:
                if not _ensure_active_session(user.get("email"), token, now):
                    _best_effort_clear_active_session(user.get("email"), token)
                    try:
                        app.logger.warning(
                            "auth preload last_activity rowcount=0 cleared email=%s path=%s",
                            user.get("email"),
                            request.path,
                        )
                    except Exception:
                        pass
                    session.clear()
                    return
        except SQLAlchemyError:
            _safe_session_rollback()
            _best_effort_clear_active_session(user.get("email"), token)
            try:
                app.logger.warning(
                    "auth preload last_activity exception cleared email=%s path=%s",
                    user.get("email"),
                    request.path,
                )
            except Exception:
                pass
            session.clear()
            return
        g.user = user
        perfil_id = user.get("perfil_id")
        perfil_nome = (user.get("perfil") or "").strip()
        try:
            perfil_row = db.session.get(Perfil, perfil_id) if perfil_id else None
            if not perfil_row and perfil_nome:
                perfil_row = (
                    Perfil.query.filter(
                        func.lower(func.ltrim(func.rtrim(Perfil.nome))) == perfil_nome.lower()
                    ).first()
                )
        except (SQLAlchemyError, IndexError):
            _safe_session_rollback()
            _best_effort_clear_active_session(user.get("email"), token)
            try:
                app.logger.warning(
                    "auth preload perfil fetch error cleared email=%s perfil_id=%s path=%s",
                    user.get("email"),
                    perfil_id,
                    request.path,
                )
            except Exception:
                pass
            session.clear()
            return
        if not perfil_row:
            _best_effort_clear_active_session(user.get("email"), token)
            try:
                app.logger.warning(
                    "auth preload perfil missing cleared email=%s path=%s",
                    user.get("email"),
                    request.path,
                )
            except Exception:
                pass
            session.clear()
            return
        if perfil_row:
            # mantem nome do perfil sincronizado para exibicao/compatibilidade.
            user["perfil"] = (perfil_row.nome or "").strip()
            user["perfil_id"] = perfil_row.id
            session["user"] = user
        if perfil_row:
            g.user_perfil_id = perfil_row.id
            g.user_nivel = perfil_row.nivel
        try:
            result = _execute_with_retry(
                select(func.count())
                .select_from(ActiveSession)
                .where(ActiveSession.last_activity >= cutoff)
            )
            if result is None:
                _safe_session_rollback()
                _best_effort_clear_active_session(user.get("email"), token)
                try:
                    app.logger.warning(
                        "auth preload active_sessions_count failed cleared email=%s path=%s",
                        user.get("email"),
                        request.path,
                    )
                except Exception:
                    pass
                session.clear()
                return
            g.active_sessions_count = result.scalar() or 0
        except SQLAlchemyError:
            _safe_session_rollback()
            _best_effort_clear_active_session(user.get("email"), token)
            try:
                app.logger.warning(
                    "auth preload active_sessions_count exception cleared email=%s path=%s",
                    user.get("email"),
                    request.path,
                )
            except Exception:
                pass
            session.clear()
            return

    register_blueprints(app)
    return app


app = create_app()
application = app  # WSGI entrypoint para IIS/wfastcgi

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
