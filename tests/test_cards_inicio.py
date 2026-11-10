"""Cards da tela Início: o card Status e controlado por permissao e nao aparece no menu."""
from flask import render_template

from app import create_app
from services.features import build_menu_tree, flatten_features

CARD = "dashboard/card-status"


def _ids(nodes):
    out = []
    for n in nodes:
        out.append(n["id"])
        out.extend(_ids(n.get("children") or []))
    return out


def test_card_esta_no_catalogo_mas_fora_do_menu():
    assert CARD in list(flatten_features())
    assert CARD not in _ids(build_menu_tree())
    inicio = next(n for n in build_menu_tree() if n["id"] == "dashboard")
    assert not inicio["children"]  # Início continua sendo um item simples do menu


def _render(pode_ver):
    app = create_app()
    with app.test_request_context("/"):
        return render_template(
            "partials/dashboard.html",
            can_view_sessions=pode_ver,
            active_sessions=[{"nome": "Fulano"}],
            emp_record={"dias_sem_erro": 0},
            avisos_uo=[],
        )


def test_card_status_so_aparece_com_permissao():
    assert "home-status-card" in _render(True)
    assert "Fulano" in _render(True)
    html = _render(False)
    assert "home-status-card" not in html
    assert "Bem-vindo" in html  # fixo para todos
    assert "Próximos passos" not in html
