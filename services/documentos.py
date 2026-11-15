"""Documentos do SPO publicados para quem está logado (feat/padrao-spo-aa-plus, 2026-11-14).

O documento técnico do padrão SPO-AA+ (docs/padrao-visual-spo-aa-plus.md) é a fonte única:
a rota /api/documentos/<nome> lê o arquivo do próprio repositório e devolve o HTML. Assim, cada
PR que atualiza o documento atualiza também o que o usuário vê, sem cópia para manter.

O conversor de markdown é próprio e pequeno, de propósito (evita uma dependência nova no
servidor). Cobre o que os documentos usam: títulos (#, ##, ###), parágrafos, listas com - ou
número (aninhadas por 2 espaços), listas de marcar (- [ ]), citações (>), tabelas, `código`,
**negrito** e links [texto](http...). Todo texto é escapado; links só http/https.
Elemento novo num documento publicado: acrescentar aqui e no teste (tests/test_documentos.py).
"""

from __future__ import annotations

import html
import re
from pathlib import Path

# nome na URL -> (arquivo, título)
DOCUMENTOS = {
    "padrao-spo-aa-plus": ("docs/padrao-visual-spo-aa-plus.md", "Padrão SPO-AA+: documento técnico"),
}

_RAIZ = Path(__file__).resolve().parent.parent

_RE_TITULO = re.compile(r"^(#{1,3})\s+(.*)$")
_RE_ITEM = re.compile(r"^(\s*)(-|\d+\.)\s+(.*)$")
_RE_SEPARADOR = re.compile(r"^\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)*\|?\s*$")


def _inline(texto: str) -> str:
    """Escapa o texto e aplica `código`, **negrito** e links http(s).

    O código sai do texto antes (marcadores \\x00n\\x00), para o negrito poder envolver um
    trecho com código e para nada dentro do código virar negrito ou link.
    """
    codigos: list[str] = []

    def _guardar(m: re.Match) -> str:
        codigos.append(f"<code>{html.escape(m.group(1), quote=False)}</code>")
        return f"\x00{len(codigos) - 1}\x00"

    t = re.sub(r"`([^`]*)`", _guardar, texto)
    t = html.escape(t, quote=False)
    t = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", t)

    def _link(m: re.Match) -> str:
        rotulo, url = m.group(1), html.unescape(m.group(2))
        if not re.match(r"^https?://", url):
            return rotulo
        return (f'<a href="{html.escape(url, quote=True)}" target="_blank" rel="noopener noreferrer">'
                f'{rotulo}<span class="spo-sr-only"> (abre em nova aba)</span></a>')

    t = re.sub(r"\[([^\]]+)\]\(([^)\s]+)\)", _link, t)
    return re.sub(r"\x00(\d+)\x00", lambda m: codigos[int(m.group(1))], t)


def _celulas(linha: str) -> list[str]:
    """Divide uma linha de tabela pelos | que estão fora de `código`."""
    s = linha.strip()
    if s.startswith("|"):
        s = s[1:]
    if s.endswith("|"):
        s = s[:-1]
    celulas, atual, em_codigo = [], [], False
    for ch in s:
        if ch == "`":
            em_codigo = not em_codigo
        if ch == "|" and not em_codigo:
            celulas.append("".join(atual).strip())
            atual = []
        else:
            atual.append(ch)
    celulas.append("".join(atual).strip())
    return celulas


def _slug(texto: str, usados: set[str]) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", texto.lower()).strip("-") or "secao"
    slug, n = f"doc-{base}", 2
    while slug in usados:
        slug, n = f"doc-{base}-{n}", n + 1
    usados.add(slug)
    return slug


def markdown_para_html(texto: str) -> tuple[str, list[tuple[str, str]]]:
    """Converte o markdown em HTML. Devolve (html, sumário [(id, título dos ##)])."""
    linhas = texto.replace("\r\n", "\n").split("\n")
    out: list[str] = []
    sumario: list[tuple[str, str]] = []
    usados: set[str] = set()
    paragrafo: list[str] = []
    # pilha de listas abertas (recuo, "ul"|"ol"); cada nível da pilha tem um <li> aberto
    listas: list[tuple[int, str]] = []

    def fechar_paragrafo():
        if paragrafo:
            out.append(f"<p>{_inline(' '.join(paragrafo))}</p>")
            paragrafo.clear()

    def fechar_listas(ate: int = -1):
        while listas and listas[-1][0] > ate:
            out.append(f"</li></{listas.pop()[1]}>")

    i = 0
    while i < len(linhas):
        linha = linhas[i]
        if not linha.strip():
            fechar_paragrafo()
            fechar_listas()
            i += 1
            continue

        m = _RE_TITULO.match(linha)
        if m:
            fechar_paragrafo()
            fechar_listas()
            nivel = len(m.group(1))
            tag = {1: "h3", 2: "h3", 3: "h4"}[nivel]
            ident = _slug(m.group(2), usados)
            if nivel == 2:
                sumario.append((ident, m.group(2)))
            out.append(f'<{tag} id="{ident}" class="spo-doc-h{nivel}">{_inline(m.group(2))}</{tag}>')
            i += 1
            continue

        if linha.lstrip().startswith("|") and i + 1 < len(linhas) and _RE_SEPARADOR.match(linhas[i + 1].strip()):
            fechar_paragrafo()
            fechar_listas()
            cab = _celulas(linha)
            i += 2
            corpo = []
            while i < len(linhas) and linhas[i].lstrip().startswith("|"):
                corpo.append(_celulas(linhas[i]))
                i += 1
            out.append('<div class="spo-doc-tabela" tabindex="0" role="region" aria-label="Tabela"><table>')
            out.append("<thead><tr>" + "".join(f'<th scope="col">{_inline(c)}</th>' for c in cab) + "</tr></thead><tbody>")
            for cel in corpo:
                out.append("<tr>" + "".join(f"<td>{_inline(c)}</td>" for c in cel) + "</tr>")
            out.append("</tbody></table></div>")
            continue

        if linha.startswith(">"):
            fechar_paragrafo()
            fechar_listas()
            bloco = []
            while i < len(linhas) and linhas[i].startswith(">"):
                bloco.append(linhas[i][1:].strip())
                i += 1
            out.append(f"<blockquote><p>{_inline(' '.join(b for b in bloco if b))}</p></blockquote>")
            continue

        m = _RE_ITEM.match(linha)
        if m:
            fechar_paragrafo()
            recuo, marca, conteudo = len(m.group(1)), m.group(2), m.group(3)
            tipo = "ol" if marca[0].isdigit() else "ul"
            fechar_listas(recuo)  # fecha os níveis mais recuados que este item
            if listas and listas[-1][0] == recuo:
                if listas[-1][1] == tipo:
                    out.append("</li>")  # irmão no mesmo nível
                else:
                    out.append(f"</li></{listas.pop()[1]}><{tipo}>")
                    listas.append((recuo, tipo))
            else:  # primeiro item da lista, ou nível mais fundo (dentro do <li> aberto)
                out.append(f"<{tipo}>")
                listas.append((recuo, tipo))
            # continuação do item nas linhas seguintes (recuadas além da marca)
            texto_item = [conteudo]
            j = i + 1
            while j < len(linhas) and linhas[j].strip() and not _RE_ITEM.match(linhas[j]) \
                    and len(linhas[j]) - len(linhas[j].lstrip()) > recuo and not linhas[j].lstrip().startswith("|"):
                texto_item.append(linhas[j].strip())
                j += 1
            corpo_item = " ".join(texto_item)
            marcar = re.match(r"^\[( |x|X)\]\s+(.*)$", corpo_item)
            if marcar:
                feito = marcar.group(1).lower() == "x"
                caixa = "☑" if feito else "☐"
                corpo_item = marcar.group(2)
                out.append(f'<li class="spo-doc-marcar"><span aria-hidden="true">{caixa}</span> {_inline(corpo_item)}')
            else:
                out.append(f"<li>{_inline(corpo_item)}")
            i = j
            continue

        # linha comum: continua o item aberto (texto recuado) ou o parágrafo
        if listas and len(linha) - len(linha.lstrip()) > 0:
            out.append(" " + _inline(linha.strip()))
            i += 1
            continue
        fechar_listas()
        paragrafo.append(linha.strip())
        i += 1

    fechar_paragrafo()
    fechar_listas()
    return "".join(out), sumario


def carregar(nome: str) -> dict | None:
    """Lê e converte um documento publicado. None se o nome não existe."""
    if nome not in DOCUMENTOS:
        return None
    caminho, titulo = DOCUMENTOS[nome]
    arquivo = _RAIZ / caminho
    texto = arquivo.read_text(encoding="utf-8")
    corpo, sumario = markdown_para_html(texto)
    return {"nome": nome, "titulo": titulo, "html": corpo, "sumario": [{"id": i, "titulo": t} for i, t in sumario]}
