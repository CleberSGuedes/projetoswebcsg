"""A escala tipográfica do mockup PTA 2027 (spo-escala.css) é gerada por
scripts/gerar_escala_pta2027.py a partir do pta-2027.css. Se o mockup mudar e o
arquivo gerado não for refeito, este teste avisa. Não usa banco."""

import importlib.util
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent


def _gerador():
    spec = importlib.util.spec_from_file_location("gerar_escala_pta2027", RAIZ / "scripts" / "gerar_escala_pta2027.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_spo_escala_css_igual_ao_gerado():
    mod = _gerador()
    texto, total = mod.gerar()
    gravado = mod.DESTINO.read_text(encoding="utf-8").replace("\r\n", "\n")
    assert gravado == texto, "spo-escala.css desatualizado: rode python scripts/gerar_escala_pta2027.py"
    assert total > 50


def test_nenhuma_regra_gerada_abaixo_do_minimo_nem_com_mais_especificidade():
    import re
    texto, _ = _gerador().gerar()
    texto = re.sub(r"/\*.*?\*/", "", texto, flags=re.S)
    tamanhos = [int(x) for x in re.findall(r"font-size:(\d+)px", texto)]
    assert tamanhos and min(tamanhos) >= 11
    # toda regra gerada passa pelo :where (mesma especificidade do original)
    regras = re.findall(r"([^{}]+)\{font-size", texto)
    for seletores in regras:
        for s in seletores.split(","):
            assert ":where(" in s, s
