"""Mapa de rotas da aplicação: URL, métodos e endpoint de cada rota, comparados com uma referência gravada.

[jean] feat/teste-mapa-rotas (2026-11-17). Fase 0 do backend na modularização do `rotas/home_routes.py`
(sugestão do Cleber, 2026-11-13): mover uma rota de arquivo não pode mudar a URL, os métodos nem o nome do
endpoint (o `url_for` depende dele). A referência fica em `referencia_mapa_rotas.json`.

O teste reprova quando uma rota some, aparece ou muda. Rota nova ou mudança DE PROPÓSITO: registre na
referência rodando este arquivo direto, e o PR mostra a diferença no JSON:
    .venv\\Scripts\\python.exe tests\\test_mapa_rotas.py
"""

import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
REFERENCIA = Path(__file__).resolve().parent / "referencia_mapa_rotas.json"

# HEAD e OPTIONS o Flask acrescenta sozinho a toda rota
METODOS_AUTOMATICOS = {"HEAD", "OPTIONS"}


def _chave(rota: dict) -> str:
    return f"{rota['url']} [{','.join(rota['metodos'])}]"


def mapa_atual(app=None) -> list[dict]:
    if app is None:
        if str(RAIZ) not in sys.path:
            sys.path.insert(0, str(RAIZ))
        from app import create_app

        app = create_app()
    rotas = []
    for regra in app.url_map.iter_rules():
        rota = {
            "url": regra.rule,
            "metodos": sorted(set(regra.methods or ()) - METODOS_AUTOMATICOS),
            "endpoint": regra.endpoint,
        }
        if regra.defaults:
            rota["padroes"] = {k: regra.defaults[k] for k in sorted(regra.defaults)}
        rotas.append(rota)
    return sorted(rotas, key=lambda r: (r["url"], r["metodos"], r["endpoint"]))


def _referencia() -> list[dict]:
    return json.loads(REFERENCIA.read_text(encoding="utf-8"))["rotas"]


def diferencas(antes: list[dict], depois: list[dict]) -> list[str]:
    a = {_chave(r): r for r in antes}
    d = {_chave(r): r for r in depois}
    linhas = []
    for k in sorted(a.keys() - d.keys()):
        linhas.append(f"sumiu: {k} -> {a[k]['endpoint']}")
    for k in sorted(d.keys() - a.keys()):
        linhas.append(f"nova: {k} -> {d[k]['endpoint']}")
    for k in sorted(a.keys() & d.keys()):
        if a[k] != d[k]:
            linhas.append(f"mudou: {k}: {a[k]} -> {d[k]}")
    if not linhas and len(antes) != len(depois):  # mesma URL e métodos repetidos com endpoints diferentes
        linhas.append(f"quantidade mudou: {len(antes)} -> {len(depois)} rotas")
    return linhas


def test_mapa_de_rotas_igual_a_referencia():
    from app import create_app

    difs = diferencas(_referencia(), mapa_atual(create_app()))
    assert not difs, (
        "O mapa de rotas mudou em relação a tests/referencia_mapa_rotas.json. Se foi de propósito (rota nova ou "
        "mudança combinada), registre rodando: .venv\\Scripts\\python.exe tests\\test_mapa_rotas.py\n  "
        + "\n  ".join(difs[:40])
    )


def test_comparacao_acusa_rota_que_some_aparece_ou_muda():
    base = [
        {"url": "/a", "metodos": ["GET"], "endpoint": "home.a"},
        {"url": "/b", "metodos": ["GET", "POST"], "endpoint": "home.b"},
    ]
    assert diferencas(base, list(base)) == []
    movida = [base[0], {"url": "/b", "metodos": ["GET", "POST"], "endpoint": "relatorios.b"}]
    assert diferencas(base, movida) == ["mudou: /b [GET,POST]: " + str(base[1]) + " -> " + str(movida[1])]
    assert diferencas(base, base[:1]) == ["sumiu: /b [GET,POST] -> home.b"]
    assert diferencas(base[:1], base) == ["nova: /b [GET,POST] -> home.b"]
    so_post = [base[0], {"url": "/b", "metodos": ["POST"], "endpoint": "home.b"}]
    assert diferencas(base, so_post) == ["sumiu: /b [GET,POST] -> home.b", "nova: /b [POST] -> home.b"]


if __name__ == "__main__":
    atual = mapa_atual()
    if REFERENCIA.exists():
        difs = diferencas(_referencia(), atual)
        print("\n".join(difs) if difs else "Sem diferença em relação à referência.")
    # uma rota por linha: a diferença no PR mostra exatamente as rotas que mudaram
    linhas = ",\n".join("  " + json.dumps(r, ensure_ascii=False) for r in atual)
    REFERENCIA.write_text(f'{{"total": {len(atual)}, "rotas": [\n{linhas}\n]}}\n', encoding="utf-8")
    print(f"Referência gravada: {len(atual)} rotas.")
