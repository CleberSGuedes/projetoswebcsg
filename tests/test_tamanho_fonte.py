"""Tamanho de fonte só pelas variáveis da "Densidade do texto" (padrão SPO-AA+ v4 §2.2.1).

[jean] feat/densidade-teste-px (2026-11-15), etapa 4 do plano de tipografia.

O `static/css/style.css` ainda tem `font-size` em px escritos à mão (regras antigas, a maior parte
anterior à escala). Elas não somem de uma vez: a contagem de hoje, por valor, fica registrada em
REFERENCIA. O teste reprova quando aparece um valor em px que não estava na referência ou quando a
contagem de algum valor SOBE — ou seja, quando uma regra nova escreve o tamanho à mão.

Componente novo usa as variáveis: `font-size: var(--spo-fs-texto)` (ou -dado, -controle, -leitura,
-titulo, -rotulo, -secao, -pagina). Quando uma regra antiga passar a usar a variável, a contagem
DESCE: o teste aceita e avisa para baixar a referência aqui (assim ela não volta a subir).
Exceção justificada (ex.: ícone de tamanho fixo, quadro do PDF): somar à referência com um
comentário dizendo por quê.
"""

import re
from collections import Counter
from pathlib import Path

import pytest

CSS = Path(__file__).resolve().parent.parent / "static" / "css" / "style.css"

# contagem de `font-size: <n>px` por valor em 2026-11-15 (depois da etapa 3 e dos dois itens da
# lista "mínimo de 12 px" que passaram às variáveis)
REFERENCIA = {
    "12": 88, "11": 61, "13": 34, "10": 21, "14": 17, "24": 10, "16": 10, "8": 9, "20": 9,
    "15": 8, "23": 7, "18": 7, "17": 6, "22": 3, "11.5": 3, "9": 2, "26": 2, "25": 2, "21": 2,
    "10.5": 2, "9.5": 1, "9.2": 1, "56": 1, "42": 1, "34": 1, "32": 1, "12.5": 1,
}

VARIAVEIS = ("dado", "controle", "texto", "leitura", "titulo", "rotulo", "secao", "pagina")


def _sem_comentarios(texto: str) -> str:
    return re.sub(r"/\*.*?\*/", "", texto, flags=re.S)


def _contagem_px() -> Counter:
    css = _sem_comentarios(CSS.read_text(encoding="utf-8"))
    return Counter(re.findall(r"font-size\s*:\s*([0-9]+(?:\.[0-9]+)?)px", css))


def test_nenhum_font_size_novo_em_px():
    atual = _contagem_px()
    novos = sorted(v for v in atual if v not in REFERENCIA)
    subiram = sorted(f"{v}px: {REFERENCIA[v]} -> {atual[v]}" for v in atual if v in REFERENCIA and atual[v] > REFERENCIA[v])
    assert not novos and not subiram, (
        "font-size em px escrito à mão no style.css. Use as variáveis da densidade "
        "(var(--spo-fs-texto), -dado, -controle, -leitura, -titulo…; padrão SPO-AA+ §2.2.1). "
        f"Valores novos: {novos or '-'}; contagens que subiram: {subiram or '-'}."
    )


def test_referencia_acompanha_as_regras_migradas():
    """Se uma regra antiga passou a usar a variável, baixe a referência (para não voltar a subir)."""
    atual = _contagem_px()
    desceram = sorted(f"{v}px: {REFERENCIA[v]} -> {atual.get(v, 0)}" for v in REFERENCIA if atual.get(v, 0) < REFERENCIA[v])
    if desceram:
        pytest.skip("Contagem desceu (bom): atualize REFERENCIA em tests/test_tamanho_fonte.py: " + "; ".join(desceram))


def test_densidades_definem_todas_as_variaveis():
    css = _sem_comentarios(CSS.read_text(encoding="utf-8"))
    blocos = {
        "padrão (body, Intermediário)": r"\bbody\s*\{([^}]*--spo-fs-dado[^}]*)\}",
        "Legível": r'body\[data-spo-table-text="legible"\]\s*\{([^}]*)\}',
        "Compacto": r'body\[data-spo-table-text="compact"\]\s*\{([^}]*)\}',
    }
    padrao = re.search(blocos["padrão (body, Intermediário)"], css)
    assert padrao, "bloco body com as variáveis --spo-fs-* não encontrado"
    faltando = [v for v in VARIAVEIS if f"--spo-fs-{v}:" not in padrao.group(1)]
    assert not faltando, f"variáveis sem valor padrão: {faltando}"
    for nome in ("Legível", "Compacto"):
        bloco = re.search(blocos[nome], css)
        assert bloco, f"bloco da densidade {nome} não encontrado"
        # rótulo, seção e página são iguais nas três densidades; as outras cinco mudam
        for v in ("dado", "controle", "texto", "leitura", "titulo"):
            assert f"--spo-fs-{v}:" in bloco.group(1), f"{nome} sem --spo-fs-{v}"
