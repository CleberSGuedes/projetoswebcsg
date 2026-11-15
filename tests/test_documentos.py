"""Conversor de markdown dos documentos publicados (services/documentos.py).

Não usa banco nem sobe a aplicação: só a conversão e o documento real do padrão SPO-AA+.
"""

from html.parser import HTMLParser

from services.documentos import DOCUMENTOS, carregar, markdown_para_html


class _Balanco(HTMLParser):
    """Confere que toda tag aberta foi fechada, na ordem."""

    VAZIAS = {"br", "hr", "img", "input", "meta", "link"}

    def __init__(self):
        super().__init__()
        self.pilha = []
        self.erros = []

    def handle_starttag(self, tag, attrs):
        if tag not in self.VAZIAS:
            self.pilha.append(tag)

    def handle_endtag(self, tag):
        if not self.pilha or self.pilha[-1] != tag:
            self.erros.append(f"fecha {tag} com {self.pilha[-1:] or 'nada'} aberto")
            return
        self.pilha.pop()


def _balanceado(html):
    p = _Balanco()
    p.feed(html)
    p.close()
    return not p.erros and not p.pilha, p.erros + [f"sobrou {p.pilha}"]


def test_titulos_paragrafos_e_sumario():
    html, sumario = markdown_para_html("# Título\n\nUm texto\nem duas linhas.\n\n## Metas\n\n### Detalhe\n")
    assert '<h3 id="doc-t-tulo" class="spo-doc-h1">Título</h3>' in html
    assert "<p>Um texto em duas linhas.</p>" in html
    assert sumario == [("doc-metas", "Metas")]
    assert '<h4 id="doc-detalhe" class="spo-doc-h3">Detalhe</h4>' in html


def test_escapa_html_e_codigo():
    html, _ = markdown_para_html("O `<body>` e <script>alert(1)</script> e **forte**.\n")
    assert "<code>&lt;body&gt;</code>" in html
    assert "<script>" not in html and "&lt;script&gt;" in html
    assert "<strong>forte</strong>" in html


def test_links_so_http():
    html, _ = markdown_para_html("[W3C](https://www.w3.org/) e [local](docs/x.md) e [js](javascript:alert(1))\n")
    assert 'href="https://www.w3.org/"' in html and 'target="_blank"' in html
    assert "javascript:" not in html.replace("&quot;", "")
    assert "docs/x.md" not in html  # link relativo vira só o texto


def test_tabela_com_barra_dentro_de_codigo():
    md = "| Uso | Padrão |\n|---|---|\n| padrão `a|b` | **sim** |\n"
    html, _ = markdown_para_html(md)
    assert '<th scope="col">Uso</th>' in html
    assert "<td>padrão <code>a|b</code></td><td><strong>sim</strong></td>" in html


def test_listas_aninhadas_marcar_e_continuacao():
    md = (
        "- um\n"
        "  - dois\n"
        "    continua\n"
        "    - três\n"
        "- quatro\n"
        "\n"
        "1. primeiro\n"
        "2. segundo\n"
        "\n"
        "- [ ] a fazer\n"
        "- [x] feito\n"
    )
    html, _ = markdown_para_html(md)
    ok, erros = _balanceado(html)
    assert ok, erros
    assert "<ul><li>um<ul><li>dois continua<ul><li>três</li></ul></li></ul></li><li>quatro</li></ul>" in html
    assert "<ol><li>primeiro</li><li>segundo</li></ol>" in html
    assert '<li class="spo-doc-marcar"><span aria-hidden="true">☐</span> a fazer</li>' in html
    assert "☑" in html


def test_citacao():
    html, _ = markdown_para_html("> Versão 2, 2026-11-14.\n> Segunda linha.\n")
    assert html == "<blockquote><p>Versão 2, 2026-11-14. Segunda linha.</p></blockquote>"


def test_documento_real_do_padrao():
    for nome in DOCUMENTOS:
        doc = carregar(nome)
        ok, erros = _balanceado(doc["html"])
        assert ok, (nome, erros[:5])
        assert len(doc["sumario"]) >= 5
        # nada de markdown cru sobrando: separadores de tabela, títulos e negrito
        assert "|---" not in doc["html"]
        assert "\n#" not in doc["html"] and "**" not in doc["html"]
    doc = carregar("padrao-spo-aa-plus")
    assert "SPO-AA+" in doc["html"]
    assert carregar("nao-existe") is None
