from functools import wraps
from flask import session, redirect, url_for, abort, g, request


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("user") or not getattr(g, "user", None):
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
            perfil = (user.get("perfil") or "").lower()
            if perfil not in roles_normalized:
                if request.headers.get("X-Requested-With"):
                    return ("", 403)
                abort(403)
            return view(*args, **kwargs)

        return wrapped

    return decorator


def current_user():
    return getattr(g, "user", None)
