"""scripts/atualizar_schema_sync_main.py (docs/claude.md, seção 21.7): o
planejamento detecta o que falta num banco desatualizado e não propõe nada
num banco já atualizado. Sem banco real - as consultas ao information_schema
são simuladas."""
from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

_spec = importlib.util.spec_from_file_location(
    "atualizar_schema_sync_main",
    Path(__file__).resolve().parent.parent / "scripts" / "atualizar_schema_sync_main.py",
)
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


class _Conn:
    def __init__(self, contagem=0):
        self.contagem = contagem

    def execute(self, *_args, **_kwargs):
        return SimpleNamespace(scalar=lambda: self.contagem)


def _fake(monkeypatch, *, colunas, indices, tabelas=None):
    if tabelas is None:
        tabelas = {"momp", "plan20_seduc", "logs_login", "active_sessions", "perfil"}
    monkeypatch.setattr(mod, "_tabela_existe", lambda conn, t: t in tabelas)
    monkeypatch.setattr(mod, "_coluna", lambda conn, t, c: colunas.get((t, c)))
    monkeypatch.setattr(mod, "_indice_existe", lambda conn, t, i: (t, i) in indices)


def _col(tipo="bigint(20)", extra=""):
    return SimpleNamespace(COLUMN_TYPE=tipo, EXTRA=extra)


def test_banco_desatualizado_lista_todas_as_pendencias(monkeypatch):
    _fake(monkeypatch, colunas={
        ("logs_login", "id"): _col(extra=""),
        ("active_sessions", "id"): _col(extra="auto_increment"),
        ("perfil", "id"): _col("int(11)", ""),
    }, indices=set())
    passos = mod.planejar(_Conn(contagem=41))
    sqls = [sql for _, sql in passos]
    assert "ALTER TABLE momp ADD COLUMN uo VARCHAR(5) NULL" in sqls
    assert any("ix_momp_exercicio_uo" in s for s in sqls)
    assert "UPDATE momp SET uo = '14101' WHERE uo IS NULL" in sqls
    assert sum(s.startswith("ALTER TABLE plan20_seduc ADD COLUMN") for s in sqls) == 8
    assert "ALTER TABLE logs_login MODIFY id bigint(20) NOT NULL AUTO_INCREMENT" in sqls
    assert "ALTER TABLE perfil MODIFY id int(11) NOT NULL AUTO_INCREMENT" in sqls
    assert not any("active_sessions" in s for s in sqls), "ja era AUTO_INCREMENT"
    # ordem: a coluna uo precisa existir antes do indice e do UPDATE
    assert sqls.index("ALTER TABLE momp ADD COLUMN uo VARCHAR(5) NULL") < sqls.index(
        "UPDATE momp SET uo = '14101' WHERE uo IS NULL"
    )


def test_banco_atualizado_nao_propoe_nada(monkeypatch):
    colunas = {("momp", "uo"): _col("varchar(5)")}
    colunas.update({("plan20_seduc", c): _col("text") for c, _ in mod.PLAN20_COLUNAS_NOVAS})
    colunas.update({(t, "id"): _col(extra="auto_increment") for t in mod.TABELAS_AUTO_INCREMENT})
    _fake(monkeypatch, colunas=colunas, indices={("momp", "ix_momp_exercicio_uo")})
    assert mod.planejar(_Conn(contagem=0)) == []


def test_tabela_ausente_e_pulada(monkeypatch):
    _fake(monkeypatch, colunas={}, indices=set(), tabelas=set())
    assert mod.planejar(_Conn()) == []
