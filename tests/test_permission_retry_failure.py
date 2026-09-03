"""Cobre o bug do menu vazio no login: quando o banco remoto tem uma
instabilidade de conexao que esgota todas as tentativas de retry ao carregar
permissoes, o sistema deve propagar a falha (pra quem chama decidir - ex.:
mostrar um aviso) em vez de devolver silenciosamente uma lista vazia, que
antes era indistinguivel de "usuario sem nenhuma permissao" e renderizava o
menu quase todo oculto sem nenhuma explicacao.

Nao toca o banco real - simula a falha via monkeypatch em db.session.execute.
"""
from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy.exc import OperationalError, ProgrammingError

from app import create_app
from models import PerfilPermissao
from rotas.home_routes import (
    _fetch_scalars_all_with_retry,
    _load_permissoes_perfil,
    _load_permissoes_nivel,
)


def _fake_operational_error(*args, **kwargs):
    raise OperationalError("SELECT 1", {}, Exception("Lost connection to MySQL server during query"))


def test_fetch_scalars_default_swallows_failure_after_retries():
    app = create_app()
    with app.app_context():
        stmt = select(PerfilPermissao.feature).where(PerfilPermissao.id == -1)
        with patch("rotas.home_routes.db.session.execute", side_effect=_fake_operational_error):
            result = _fetch_scalars_all_with_retry(stmt, attempts=2, backoff_s=0)
        assert result == []


def test_fetch_scalars_raise_on_failure_propagates_after_retries():
    app = create_app()
    with app.app_context():
        stmt = select(PerfilPermissao.feature).where(PerfilPermissao.id == -1)
        with patch("rotas.home_routes.db.session.execute", side_effect=_fake_operational_error):
            with pytest.raises(OperationalError):
                _fetch_scalars_all_with_retry(stmt, attempts=2, backoff_s=0, raise_on_failure=True)


def test_load_permissoes_perfil_propagates_persistent_connection_failure():
    app = create_app()
    with app.app_context():
        with patch("rotas.home_routes.db.session.execute", side_effect=_fake_operational_error):
            with patch.dict("os.environ", {"DB_RETRY_ATTEMPTS": "2", "DB_RETRY_BACKOFF": "0"}):
                with pytest.raises(OperationalError):
                    _load_permissoes_perfil(perfil_id=1)


def test_load_permissoes_nivel_propagates_persistent_connection_failure():
    app = create_app()
    with app.app_context():
        with patch("rotas.home_routes.db.session.execute", side_effect=_fake_operational_error):
            with patch.dict("os.environ", {"DB_RETRY_ATTEMPTS": "2", "DB_RETRY_BACKOFF": "0"}):
                with pytest.raises(OperationalError):
                    _load_permissoes_nivel(nivel=1)


def test_load_permissoes_perfil_still_returns_empty_for_missing_table():
    # Tabela/coluna realmente ausente (banco novo sem migracao) continua
    # tratada como "sem permissoes", nao como erro pra propagar.
    app = create_app()
    with app.app_context():
        def _raise_programming_error(*args, **kwargs):
            raise ProgrammingError("SELECT 1", {}, Exception("Table 'perfil_permissoes' doesn't exist"))

        with patch("rotas.home_routes.db.session.execute", side_effect=_raise_programming_error):
            with patch.dict("os.environ", {"DB_RETRY_ATTEMPTS": "2", "DB_RETRY_BACKOFF": "0"}):
                assert _load_permissoes_perfil(perfil_id=1) == []
