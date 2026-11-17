"""[jean] Avisos da Início como cartões (feat/alerta-cartao #174 e feat/pendencias-cartao, 2026-11-17; PROPOSTA ao
dev. Cleber). Nada abre sozinho ao entrar: o Alerta é o cartão "Chave do planejamento" e as pendências são um
cartão com a contagem por tipo, cujo detalhe fica num <template> (aberto por "Ver detalhes")."""
from types import SimpleNamespace

from flask import render_template

from app import create_app


def _render(**extra):
    app = create_app()
    dados = dict(can_view_sessions=False, active_sessions=[], emp_record={"dias_sem_erro": 7, "recorde": 12}, avisos_uo=[])
    dados.update(extra)
    with app.test_request_context("/"):
        return render_template("partials/dashboard.html", **dados)


def test_alerta_virou_cartao_sem_janela():
    html = _render()
    assert 'id="spo-indice-card"' in html
    assert "emp-record-modal" not in html
    assert "<span>0</span><span>0</span><span>7</span>" in html
    assert "Recorde: <strong>12 dias</strong>" in html
    assert "recorde atual" not in html  # 7 < 12
    assert "Colabore para melhorar este índice." in html


def test_sem_pendencias_sem_cartao_nem_janela():
    html = _render()
    assert "spo-pendencias-card" not in html
    assert "dotacao-aguardando-modal" not in html


def test_pendencias_em_cartao_com_detalhe_em_template():
    dot = SimpleNamespace(chave_dotacao="2027.1", valor_atual="10,00", status_aprovacao="aguardando", adj_concedente="NGER")
    meta = SimpleNamespace(controle_meta="M-1", status_aprovacao="aguardando", produto_acao="P", perfil="NGER")
    aviso = {"rotulo": "14101", "ped_dotacao": ["X"], "ped_linhas": [], "emp_linhas": [], "emp_dotacao": []}
    html = _render(dotacoes_aguardando=[dot, dot], metas_aguardando=[meta], avisos_uo=[aviso])
    assert 'id="spo-pendencias-card"' in html
    assert "dotacao-aguardando-modal" not in html  # nada abre sozinho
    cartao = html.split('id="spo-pendencias-card"')[1].split("</section>")[0]
    assert ">3<" in cartao  # total: 2 dotações + 1 meta (avisos por UO à parte)
    assert "Dotações aguardando aprovação</span><strong>2</strong>" in cartao
    assert "<strong>1 UO</strong>" in cartao
    detalhe = html.split('<template id="spo-pendencias-detalhe">')[1].split("</template>")[0]
    assert 'data-rotulo="Controle de Dotação">2027.1<' in detalhe
    assert "UO 14101" in detalhe and "Controle de Dotação não encontrado: X" in detalhe
    assert detalhe.index("Avisos de carga") < detalhe.index("Dotações aguardando aprovação")
