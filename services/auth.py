from functools import wraps
from flask import session, redirect, url_for, abort, g, request


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("user") or not getattr(g, "user", None):
            try:
                from flask import current_app

                current_app.logger.info(
                    "auth required redirect user=%s g.user=%s path=%s",
                    bool(session.get("user")),
                    bool(getattr(g, "user", None)),
                    request.path,
                )
            except Exception:
                pass
            session.clear()
            if request.headers.get("X-Requested-With"):
                return ("", 401)
            return redirect(url_for("auth.login"))
        return view(*args, **kwargs)

    return wrapped


def role_required(*roles):
    roles_normalized = {r.lower() for r in roles}

    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            user = session.get("user")
            if not user:
                if request.headers.get("X-Requested-With"):
                    return ("", 401)
                return redirect(url_for("auth.login"))
            nivel = getattr(g, "user_nivel", None)
            # Regras por papel baseadas em nivel/perfil_id.
            if "admin" in roles_normalized:
                if nivel == 1:
                    return view(*args, **kwargs)
                if request.headers.get("X-Requested-With"):
                    return ("", 403)
                abort(403)
            if request.headers.get("X-Requested-With"):
                return ("", 403)
            abort(403)

        return wrapped

    return decorator


def current_user():
    return getattr(g, "user", None)
