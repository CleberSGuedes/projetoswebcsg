"""Fatores de zoom da fonte maior x --spo-zoom (avisos modais com fonte maior).

[cleber] fix/modal-pendencias-fonte-maior (2026-11-16).

A fonte maior do Personalizar SPO é um `zoom` no `.main` (`body[data-spo-text-size] .main`). Os avisos
modais ficam dentro do `.main` e dividem as medidas em vw/vh/dvh por `--spo-zoom` (bloco "[cleber] Avisos
modais com fonte maior" no fim do style.css). Se um dos dois lados mudar sozinho, o botão de fechar volta a
sair da tela. O teste reprova quando os fatores divergem (inclusive o do "Muito grande" abaixo de 480 px).
"""

import re
from pathlib import Path

CSS = (Path(__file__).resolve().parent.parent / "static" / "css" / "style.css").read_text(encoding="utf-8")

TAMANHOS = ("large", "larger", "xlarge")


def _sem_comentarios(css):
    return re.sub(r"/\*.*?\*/", "", css, flags=re.S)


def _fatores(css, propriedade, sufixo):
    """{tamanho: fator} do que está fora do @media e {tamanho: fator} do que está no @media (max-width: 479.98px)."""
    css = _sem_comentarios(css)
    padrao = r'body(?:\.login-body)?\[data-spo-text-size="(\w+)"\]%s\s*\{\s*%s:\s*([\d.]+)\s*;?\s*\}' % (sufixo, propriedade)
    normal, celular = {}, {}
    pos_media = [m.start() for m in re.finditer(r"@media \(max-width: 479\.98px\)", css)]
    # trechos de @media (max-width: 479.98px) { ... }: do abre-chaves até a chave que fecha
    blocos = []
    for ini in pos_media:
        i = css.index("{", ini)
        nivel, j = 1, i + 1
        while nivel and j < len(css):
            nivel += {"{": 1, "}": -1}.get(css[j], 0)
            j += 1
        blocos.append((i, j))
    for m in re.finditer(padrao, css):
        no_media = any(a < m.start() < b for a, b in blocos)
        (celular if no_media else normal)[m.group(1)] = float(m.group(2))
    return normal, celular


def test_spo_zoom_espelha_o_zoom_do_main():
    zoom_normal, zoom_celular = _fatores(CSS, "zoom", r"\s+\.main")
    var_normal, var_celular = _fatores(CSS, "--spo-zoom", "")
    assert set(zoom_normal) == set(TAMANHOS), f"zoom do .main sem algum tamanho: {zoom_normal}"
    assert var_normal == zoom_normal, f"--spo-zoom {var_normal} diverge do zoom do .main {zoom_normal}"
    assert var_celular == zoom_celular, f"--spo-zoom no celular {var_celular} diverge do zoom do .main {zoom_celular}"
    assert zoom_celular, "o 'Muito grande' abaixo de 480 px deixou de ter fator próprio: confira o espelho"
