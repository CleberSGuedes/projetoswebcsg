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


def test_vinculos_fica_visivel_como_em_manutencao_e_upload_abre_a_tela_de_chaves():
    """'Gerenciar Vinculos' so libera recursos dentro de Componentes (sem tela propria): fica no menu
    e no painel marcado como em manutencao e a rota mostra um aviso. O 'Upload anual de chaves' abre
    a propria tela Correcao de Erros de Chave (onde o upload existe)."""
    from services.features import find_feature

    app = create_app()
    fid = "atualizar/estrutura-planejamento/vinculos"
    item = find_feature(fid)
    assert item and item.get("manutencao")
    assert fid in _ids(build_menu_tree()) and fid in list(flatten_features())
    with app.test_request_context("/"):
        html = render_template("partials/funcionalidade_manutencao.html", nome=item["nome"], detalhe=item["manutencao"])
    assert "Em manutenção" in html and item["nome"] in html
    rules = {r.rule for r in app.url_map.iter_rules()}
    assert "/partial/" + fid in rules
    assert "/partial/atualizar/chaves_planejamento_upload" in rules
    assert not find_feature("atualizar/chaves_planejamento_upload").get("manutencao")
