from typing import Optional
import secrets
from datetime import datetime, timedelta
from flask import Blueprint, current_app, flash, redirect, render_template, request, session, url_for
from flask_mail import Message
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from werkzeug.security import check_password_hash
from models import db, Usuario, LogLogin, ActiveSession
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError


auth_bp = Blueprint("auth", __name__)
SESSION_TIMEOUT = timedelta(hours=2)


def _next_pk(model) -> int:
    """Gera próximo ID para tabelas sem IDENTITY/auto_increment (SQL Server 2008)."""
    max_id = db.session.query(func.max(model.id)).scalar() or 0
    return int(max_id) + 1


def _log_login(email: str, status: str, motivo: Optional[str] = None) -> None:
    try:
        entry = LogLogin(id=_next_pk(LogLogin), email=email or "", status=status, motivo=motivo)
        db.session.add(entry)
        db.session.commit()
    except Exception:
        db.session.rollback()

def _set_active_session(email: str) -> None:
    token = secrets.token_hex(16)
    now = datetime.utcnow()
    try:
        active = ActiveSession.query.filter_by(email=email).first()
        if not active:
            active = ActiveSession(
                id=_next_pk(ActiveSession),
                email=email,
                session_token=token,
                last_activity=now,
            )
            db.session.add(active)
        else:
            active.session_token = token
            active.last_activity = now
        db.session.commit()
        session["session_token"] = token
    except SQLAlchemyError as exc:
        db.session.rollback()
        current_app.logger.error("Erro ao registrar sessao ativa: %s", exc, exc_info=True)
        raise


def _clear_active_session(email: str) -> None:
    active = ActiveSession.query.filter_by(email=email).first()
    if active:
        db.session.delete(active)
        db.session.commit()


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if session.get("user"):
        return redirect(url_for("home.index"))
    is_fetch = request.headers.get("X-Requested-With") == "fetch"
    if request.method == "POST":
        try:
            data = request.get_json(silent=True) if is_fetch else request.form
            email = (data.get("email") or "").lower().strip()
            senha = data.get("password") or ""
            force_login = bool(data.get("force_login"))

            usuario = Usuario.query.filter_by(email=email).first()
            if not usuario:
                _log_login(email, "inexistente", "usuario nao encontrado")
                if is_fetch:
                    return {"error": "Usuario nao cadastrado."}, 404
                flash("Usuario nao cadastrado.", "error")
                return redirect(url_for("auth.login"))
            if not usuario.ativo:
                _log_login(email, "inativo", "usuario inativo")
                if is_fetch:
                    return {"error": "Usuario inativo."}, 403
                flash("Usuario inativo.", "error")
                return redirect(url_for("auth.login"))
            if not usuario.password_hash or not check_password_hash(usuario.password_hash, senha):
                _log_login(email, "erro", "credenciais invalidas")
                if is_fetch:
                    return {"error": "Email ou senha invalidos."}, 401
                flash("Email ou senha invalidos.", "error")
                return redirect(url_for("auth.login"))

            cutoff = datetime.utcnow() - SESSION_TIMEOUT
            active = ActiveSession.query.filter_by(email=email).first()
            if active and active.last_activity and active.last_activity >= cutoff and not force_login:
                if is_fetch:
                    return {
                        "conflict": True,
                        "message": "Usuario ja esta conectado em outra sessao. Deseja continuar?",
                    }, 409
                flash("Usuario ja conectado em outra sessao. Confirme para continuar.", "error")
                return redirect(url_for("auth.login"))

            # Anexa perfil_id para evitar divergencia de nome/case
            perfil_row = None
            if usuario.perfil:
                from models import Perfil  # import local para evitar ciclo no topo
                from sqlalchemy import func

                normalized = func.lower(func.ltrim(func.rtrim(Perfil.nome)))
                perfil_row = (
                    Perfil.query.filter(normalized == usuario.perfil.lower()).first()
                    or Perfil.query.filter(Perfil.nome.ilike(usuario.perfil)).first()
                )
            session["user"] = {
                "email": usuario.email,
                "nome": usuario.nome,
                "perfil": usuario.perfil,
                "perfil_id": perfil_row.id if perfil_row else None,
            }
            _set_active_session(usuario.email)
            _log_login(email, "sucesso", None)
            if is_fetch:
                return {"ok": True, "redirect": url_for("home.index")}
            return redirect(url_for("home.index"))
        except Exception as exc:
            db.session.rollback()
            session.clear()
            current_app.logger.error("Erro ao processar login: %s", exc, exc_info=True)
            if is_fetch:
                return {"error": "Falha interna ao processar login."}, 500
            flash("Falha interna ao processar login.", "error")
            return redirect(url_for("auth.login"))

    return render_template("login.html")


@auth_bp.route("/logout", methods=["POST", "GET"])
def logout():
    user = session.get("user")
    if user:
        _clear_active_session(user.get("email"))
    session.clear()
    return redirect(url_for("auth.login"))


def _ts():
    secret = current_app.config["SECRET_KEY"]
    return URLSafeTimedSerializer(secret_key=secret)


def _send_mail(subject: str, recipients: list[str], body: str) -> None:
    mail = current_app.extensions.get("mail")
    if not mail:
        current_app.logger.warning("Mail extension not configured; skipping email.")
        return
    username = current_app.config.get("MAIL_USERNAME")
    password = current_app.config.get("MAIL_PASSWORD")
    if not username or not password:
        current_app.logger.warning("MAIL_USERNAME or MAIL_PASSWORD not set; skipping email.")
        return
    msg = Message(subject=subject, recipients=recipients, body=body)
    sender = current_app.config.get("MAIL_DEFAULT_SENDER")
    if sender:
        msg.sender = sender
    try:
        mail.send(msg)
    except Exception as exc:
        current_app.logger.error("Falha ao enviar email: %s", exc, exc_info=True)
        # Em dev, não falhar se SMTP estiver indisponível ou com credencial errada.
        return


@auth_bp.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        email = (request.form.get("email") or "").lower().strip()
        usuario = Usuario.query.filter_by(email=email).first()
        # Mensagem genérica para não revelar cadastros
        flash("Se o email estiver cadastrado, enviaremos instrucoes.", "info")
        if usuario and usuario.ativo:
            token = _ts().dumps(email, salt="reset-senha")
            reset_url = url_for("auth.reset_password", token=token, _external=True)
            body = f"Use o link para redefinir sua senha (expira em 1 hora): {reset_url}"
            _send_mail("Redefinir senha - Sistema NGER", [email], body)
        return redirect(url_for("auth.login"))
    return render_template("forgot_password.html")


@auth_bp.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token):
    try:
        email = _ts().loads(token, salt="reset-senha", max_age=3600)
    except SignatureExpired:
        flash("Link expirado. Solicite novamente.", "error")
        return redirect(url_for("auth.forgot_password"))
    except BadSignature:
        flash("Link invalido.", "error")
        return redirect(url_for("auth.forgot_password"))

    usuario = Usuario.query.filter_by(email=email).first()
    if not usuario or not usuario.ativo:
        flash("Usuario nao encontrado ou inativo.", "error")
        return redirect(url_for("auth.login"))

    if request.method == "POST":
        senha = (request.form.get("password") or "").strip()
        if not senha:
            flash("Informe uma senha.", "error")
            return redirect(request.url)
        usuario.set_password(senha)
        db.session.commit()
        flash("Senha redefinida com sucesso.", "info")
        return redirect(url_for("auth.login"))

    return render_template("reset_password.html", email=email)
