"""Configuracao comum dos testes.

O usuario do banco tem max_user_connections = 15 (hospedagem compartilhada) e cada chamada de
create_app() cria um engine/pool proprio que deixa conexoes abertas ate o fim do processo.
Com ~30 chamadas espalhadas pela suite, o limite estoura nos ultimos arquivos
("Too many connections ... max_user_connections"). Por isso a suite inteira reutiliza UMA
aplicacao (a mesma ja usada por todos os testes) e fecha as conexoes no fim.
"""
import app as app_module

_original_create_app = app_module.create_app
_shared = {}


def _create_app_compartilhado(*args, **kwargs):
    if args or kwargs:  # configuracao especifica: nao compartilha
        return _original_create_app(*args, **kwargs)
    if "app" not in _shared:
        _shared["app"] = _original_create_app()
    return _shared["app"]


app_module.create_app = _create_app_compartilhado


def pytest_sessionfinish(session, exitstatus):
    shared = _shared.get("app")
    if shared is None:
        return
    try:
        from models import db

        with shared.app_context():
            db.session.remove()
            db.engine.dispose()
    except Exception:
        pass
