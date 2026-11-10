"""Painel: a UO das cargas do EMP vem de uma ida ao banco (LIMIT 1 por carga) e do cache.

Sem índice em upload_id, cada consulta por carga varria a tabela emp inteira
(1,2 mi de linhas, ~0,7 s) e o painel chegou a levar ~30 s para abrir.
"""
from __future__ import annotations

import pytest

from rotas import home_routes as hr


class _Res:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


@pytest.fixture(autouse=True)
def _limpa_cache():
    hr._UO_CARGA_CACHE.clear()
    hr._UO_CARGA_SEM_LINHAS.clear()
    yield
    hr._UO_CARGA_CACHE.clear()
    hr._UO_CARGA_SEM_LINHAS.clear()


def _stub(monkeypatch, rows_por_chamada):
    chamadas = []

    def execute(stmt, *a, **k):
        chamadas.append(str(stmt))
        return _Res(rows_por_chamada.pop(0))

    monkeypatch.setattr(hr.db.session, "execute", execute)
    return chamadas


def test_uma_consulta_para_varias_cargas(monkeypatch):
    chamadas = _stub(monkeypatch, [[(10, "14101"), (11, "26101")]])
    r = hr._uos_das_cargas("emp", [10, 11, 12, 10])
    assert r == {10: "14101", 11: "26101"}, "carga sem linhas fica de fora"
    assert len(chamadas) == 1
    assert chamadas[0].count("UNION ALL") == 2, "uma parte por carga, numa só ida ao banco"
    assert all(f"upload_id = {i}" in chamadas[0] for i in (10, 11, 12)) and "LIMIT 1" in chamadas[0]


def test_cache_e_carga_sem_linhas_so_volta_a_ser_consultada_depois_do_prazo(monkeypatch):
    chamadas = _stub(monkeypatch, [[(10, "14101")], [(12, "26101")]])
    assert hr._uos_das_cargas("emp", [10, 12]) == {10: "14101"}
    assert len(chamadas) == 1
    # 10 está no cache e 12 (sem linhas) foi consultada há pouco: nada de consulta nova
    assert hr._uos_das_cargas("emp", [10, 12]) == {10: "14101"} and len(chamadas) == 1
    # passado o prazo, 12 é consultada de novo (as linhas já chegaram)
    monkeypatch.setattr(hr, "_UO_CARGA_SEM_LINHAS_TTL_S", 0.0)
    assert hr._uos_das_cargas("emp", [10, 12]) == {10: "14101", 12: "26101"} and len(chamadas) == 2
    assert hr._uos_das_cargas("emp", [10, 12]) == {10: "14101", 12: "26101"} and len(chamadas) == 2, "agora ambas no cache"


def test_sem_ids_nao_consulta(monkeypatch):
    chamadas = _stub(monkeypatch, [])
    assert hr._uos_das_cargas("emp", []) == {} and chamadas == []


def test_erro_na_consulta_nao_derruba_o_painel(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("banco fora")

    monkeypatch.setattr(hr.db.session, "execute", boom)
    monkeypatch.setattr(hr, "_safe_session_rollback", lambda: None)
    assert hr._uos_das_cargas("emp", [10]) == {}
