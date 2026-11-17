"""Cor e tamanho de texto não vão em style="…" nos templates nem em .style.* com valor escrito à mão no JS.

[jean] feat/teste-style-templates (2026-11-15). Padrão SPO-AA+ §6.1 ("Nada de style="color:…"/background
no HTML", #52) e §3 (cor só por token), §2.2.1 (tamanho só pelas variáveis da densidade).

Os testes do CSS (test_cor_literal.py, test_tamanho_fonte.py) não veem o que é escrito direto na marcação:
uma tela nova podia pôr style="color:#1a5fb4; font-size:15px" e passar. Este teste fecha essa porta:
- templates (templates/**/*.html): declarações de cor (color, background, border, outline, fill, stroke,
  box-shadow) e de texto (font, font-size, font-family, line-height) dentro de style="…". Definição de
  variável (--swatch: #…, as amostras do Personalizar, §6.3) não conta. As de hoje ficam em REFERENCIA,
  por arquivo; reprova arquivo novo na lista ou contagem que sobe.
- main.js: `.style.color/backgroundColor/borderColor/… = "<texto>"` e `setProperty("color", "<texto>")`
  só com `var(--…)`; `.style.fontSize = "<texto>"` nunca; e, no HTML montado pelo JS, nenhum style="…"
  com cor (#…, rgb(…)) ou tamanho de texto escritos à mão (valor calculado por ${…} pode: é a cor do
  grupo vinda da paleta do tema, §3.5).
"""

import re
from collections import Counter
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parent.parent
TEMPLATES = RAIZ / "templates"
MAIN_JS = RAIZ / "static" / "js" / "main.js"

PROPRIEDADES = {
    "color", "background", "background-color", "border", "border-color", "border-top", "border-right",
    "border-bottom", "border-left", "outline", "outline-color", "fill", "stroke", "box-shadow",
    "font", "font-size", "font-family", "line-height",
}

# style="…" com cor ou texto que já existiam em 2026-11-15 (a migrar; os do login e das telas de senha
# são do dev. Cleber): links "Esqueci a senha"/"Voltar ao login" (12px) e o bloco monoespaçado de Acessos à API
# (12px). O título "UO …" do aviso da Início (0.95rem) saiu com o feat/pendencias-cartao (2026-11-17).
REFERENCIA = {
    "forgot_password.html": 1,
    "login.html": 1,
    "partials/usuarios_api_acessos.html": 2,
    "reset_password.html": 1,
}

ATRIBUTO = re.compile(r"""\sstyle\s*=\s*("([^"]*)"|'([^']*)')""", re.I)
DECLARACAO = re.compile(r"(?:^|;)\s*([-\w]+)\s*:\s*([^;]+)")


def _contagem_templates() -> Counter:
    contagem = Counter()
    for arquivo in sorted(TEMPLATES.rglob("*.html")):
        texto = re.sub(r"\{#.*?#\}", "", arquivo.read_text(encoding="utf-8"), flags=re.S)
        nome = arquivo.relative_to(TEMPLATES).as_posix()
        for m in ATRIBUTO.finditer(texto):
            valor = m.group(2) if m.group(2) is not None else m.group(3)
            for d in DECLARACAO.finditer(valor):
                if d.group(1).lower() in PROPRIEDADES:
                    contagem[nome] += 1
    return contagem


def test_templates_sem_cor_nem_tamanho_novos_em_style():
    atual = _contagem_templates()
    novos = sorted(f"{a} ({n})" for a, n in atual.items() if a not in REFERENCIA)
    subiram = sorted(f"{a}: {REFERENCIA[a]} -> {n}" for a, n in atual.items() if a in REFERENCIA and n > REFERENCIA[a])
    assert not novos and not subiram, (
        "cor ou tamanho de texto em style=\"…\" num template. Use uma classe no style.css com os tokens "
        "(var(--text), var(--spo-accent-text)…) e as variáveis da densidade (var(--spo-fs-texto)…); padrão "
        f"SPO-AA+ §6.1. Arquivos novos: {novos or '-'}; contagens que subiram: {subiram or '-'}."
    )


def test_referencia_acompanha_os_templates_migrados():
    atual = _contagem_templates()
    desceram = sorted(f"{a}: {n} -> {atual.get(a, 0)}" for a, n in REFERENCIA.items() if atual.get(a, 0) < n)
    if desceram:
        pytest.skip("style=\"…\" antigo migrado (bom): baixe REFERENCIA em tests/test_style_inline.py: " + "; ".join(desceram))


def test_main_js_sem_cor_nem_tamanho_escritos_a_mao():
    js = MAIN_JS.read_text(encoding="utf-8")
    problemas = []
    # .style.color = "…" (e afins): só var(--…)
    for m in re.finditer(r"\.style\.(color|background|backgroundColor|borderColor|outlineColor|fill|stroke)\s*=\s*([\"'`])(.*?)\2", js):
        if not m.group(3).strip().startswith("var(--"):
            problemas.append(f".style.{m.group(1)} = {m.group(2)}{m.group(3)}{m.group(2)}")
    for m in re.finditer(r"\.style\.setProperty\(\s*[\"'](color|background|background-color|border-color)[\"']\s*,\s*([\"'`])(.*?)\2", js):
        if not m.group(3).strip().startswith("var(--"):
            problemas.append(f"setProperty('{m.group(1)}', {m.group(3)})")
    # tamanho de texto nunca por .style.fontSize
    problemas += [f".style.fontSize = {m.group(0)}" for m in re.finditer(r"\.style\.fontSize\s*=\s*[\"'`][^\"'`]*[\"'`]", js)]
    # HTML montado pelo JS com style="…" de cor ou tamanho ESCRITO À MÃO. Valor calculado (${…}, ex.: a cor
    # do grupo vinda da paleta do tema, §3.5) e var(--…) são aceitos; reprova #…, rgb(…), hsl(…) e número
    # em font-size/font/line-height.
    for m in re.finditer(r"style\s*=\s*\\?[\"']([^\"'`]*)", js):
        for d in DECLARACAO.finditer(m.group(1)):
            prop = d.group(1).lower()
            if prop not in PROPRIEDADES:
                continue
            valor = re.sub(r"\$\{[^}]*\}", "", d.group(2))
            cor_escrita = re.search(r"#[0-9a-fA-F]{3,8}\b|\b(?:rgba?|hsla?)\(\s*[0-9.]", valor)
            tamanho_escrito = prop in {"font", "font-size", "line-height"} and re.search(r"\d", valor)
            if cor_escrita or tamanho_escrito:
                problemas.append(f'style="{m.group(1)[:60]}"')
    assert not problemas, (
        "cor ou tamanho de texto escrito à mão pelo main.js (use classes do style.css ou var(--…)): "
        + "; ".join(problemas[:10])
    )
