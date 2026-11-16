"""Gera a escala tipográfica do mockup PTA 2027 no padrão SPO-AA+ (feat/mockup-pta2027-aa-plus).

O mockup (static/pta2027_v63r/assets/pta-2027.css) tem cerca de 80 regras com fonte de 8 a 11 px,
abaixo do mínimo de 12 px de leitura do SPO-AA+. Este script lê o CSS do mockup e escreve, em
static/pta2027_v63r/assets/spo-escala.css, uma regra para cada uma, no mesmo @media:
 - 12 px no texto comum;
 - 11 px nos rótulos em maiúsculas (text-transform: uppercase), como o sobretítulo do SPO;
 - só no modo embutido com a ponte de preferências (body.spo-ponte), com a MESMA
   especificidade da regra original (:where), vencendo por carregar depois; !important só
   onde o original já usa. O spo-escala.css tem de ser carregado depois do pta-2027.css.
O mockup em si não muda. Se o pta-2027.css mudar, rode de novo (a partir da raiz do projeto):

    python scripts/gerar_escala_pta2027.py
"""

from __future__ import annotations

import re
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
ORIGEM = RAIZ / "static" / "pta2027_v63r" / "assets" / "pta-2027.css"
DESTINO = RAIZ / "static" / "pta2027_v63r" / "assets" / "spo-escala.css"
MINIMO, MINIMO_MAIUSCULAS = 12, 11


def _sem_comentarios(css: str) -> str:
    return re.sub(r"/\*.*?\*/", "", css, flags=re.S)


def regras(css: str):
    """Percorre o CSS e devolve (contexto @media, seletor, declarações), mantendo o aninhamento."""
    css = _sem_comentarios(css)
    pilha: list[str] = []
    i, inicio = 0, 0
    while i < len(css):
        ch = css[i]
        if ch == "{":
            prelude = css[inicio:i].strip()
            if prelude.startswith("@"):
                pilha.append(prelude)
                inicio = i + 1
            else:
                fim = css.index("}", i)
                yield tuple(pilha), prelude, css[i + 1:fim]
                i = fim
                inicio = fim + 1
        elif ch == "}":
            if pilha:
                pilha.pop()
            inicio = i + 1
        i += 1


def _dividir(seletores: str) -> list[str]:
    partes, atual, nivel = [], [], 0
    for ch in seletores:
        if ch in "([":
            nivel += 1
        elif ch in ")]":
            nivel -= 1
        if ch == "," and nivel == 0:
            partes.append("".join(atual).strip())
            atual = []
        else:
            atual.append(ch)
    partes.append("".join(atual).strip())
    return [p for p in partes if p]


def _com_ponte(sel: str) -> str | None:
    if sel.startswith((":root", "html", "@", "*")):
        return None
    # :where() não soma especificidade: a regra gerada empata com a original e vence por vir
    # depois; uma regra mais específica do mockup (que aumenta o tamanho num caso) continua valendo
    if re.match(r"^body(?![\w-])", sel):
        return "body:where(.spo-ponte)" + sel[4:]
    return ":where(body.spo-ponte) " + sel


def gerar() -> str:
    css = ORIGEM.read_text(encoding="utf-8")
    blocos: dict[tuple, list[str]] = {}
    total = 0
    for contexto, seletor, corpo in regras(css):
        m = re.search(r"font-size:\s*(\d+(?:\.\d+)?)px\s*(!important)?", corpo)
        if not m or float(m.group(1)) >= MINIMO:
            continue
        if any(c.startswith(("@keyframes", "@font-face")) for c in contexto):
            continue
        alvo = MINIMO_MAIUSCULAS if re.search(r"text-transform:\s*uppercase", corpo) else MINIMO
        if float(m.group(1)) >= alvo:
            continue
        novos = [s for s in (_com_ponte(x) for x in _dividir(seletor)) if s]
        if not novos:
            continue
        imp = " !important" if m.group(2) else ""
        blocos.setdefault(contexto, []).append(f"{','.join(novos)}{{font-size:{alvo}px{imp}}}")
        total += 1
    linhas = [
        "/* ==========================================================================",
        "   [jean] Escala tipográfica do PTA 2027 no padrão SPO-AA+ (feat/mockup-pta2027-aa-plus)",
        "   GERADO por scripts/gerar_escala_pta2027.py a partir do pta-2027.css: não edite à mão.",
        f"   {total} regras do mockup com fonte abaixo de {MINIMO} px sobem para {MINIMO} px",
        f"   ({MINIMO_MAIUSCULAS} px nos rótulos em maiúsculas), só no modo embutido (body.spo-ponte), com a mesma especificidade da original.",
        "   ========================================================================== */",
    ]
    for contexto, itens in blocos.items():
        abre = "".join(f"{c}{{" for c in contexto)
        fecha = "}" * len(contexto)
        linhas.append(abre + "".join(itens) + fecha)
    return "\n".join(linhas) + "\n", total


if __name__ == "__main__":
    texto, total = gerar()
    DESTINO.write_text(texto, encoding="utf-8")
    print(f"{DESTINO.relative_to(RAIZ)}: {total} regras")
