"""Cor só por token no style.css (padrão SPO-AA+ §3, "regra de ouro"; Fase B do roteiro do N5).

[jean] feat/teste-cor-literal (2026-11-15).

Em componente, a cor vem de variável (`var(--text)`, `var(--spo-accent-text)`, `var(--spo-positive)`…).
Cor escrita à mão (`#1a5fb4`, `rgb(…)`, `rgba(…)`, `hsl(…)`) só na DEFINIÇÃO de um token
(`--nome: #…`), que este teste não conta, ou numa exceção registrada no §6.3.

O `static/css/style.css` ainda tem cores literais em regras antigas (a troca é o N5, na divisão do CSS).
Elas não somem de uma vez: a contagem de hoje, por valor, fica em `referencia_cor_literal.json`. O teste
reprova quando aparece um valor que não estava na referência ou quando a contagem de algum valor SOBE —
ou seja, quando uma regra nova escreve a cor à mão.

Quando uma regra antiga passar a usar o token, a contagem DESCE: o teste avisa (skip) para baixar a
referência, rodando este arquivo direto:
    .venv\\Scripts\\python.exe tests\\test_cor_literal.py
Exceção justificada nova (§6.3): registre no padrão e rode o mesmo comando.
"""

import json
import re
import sys
from collections import Counter
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parent.parent
CSS = RAIZ / "static" / "css" / "style.css"
REFERENCIA = Path(__file__).resolve().parent / "referencia_cor_literal.json"

# hexadecimal (#abc, #aabbcc, #aabbccdd) e funções de cor com números (rgba(var(--x), .1) não conta)
LITERAL = re.compile(r"#[0-9a-fA-F]{3,8}\b|\b(?:rgba?|hsla?)\(\s*[0-9.]+[^)]*\)")
DECLARACAO = re.compile(r"([-\w]+)\s*:\s*([^;{}]+)")


def contagem_atual() -> Counter:
    css = re.sub(r"/\*.*?\*/", "", CSS.read_text(encoding="utf-8"), flags=re.S)
    contagem = Counter()
    for m in DECLARACAO.finditer(css):
        propriedade, valor = m.group(1), m.group(2)
        if propriedade.startswith("--"):
            continue  # definição de token: é onde a cor literal deve ficar
        for literal in LITERAL.findall(valor):
            contagem[re.sub(r"\s+", "", literal.lower())] += 1
    return contagem


def _referencia() -> dict:
    return json.loads(REFERENCIA.read_text(encoding="utf-8"))["contagem"]


def test_nenhuma_cor_literal_nova():
    ref = _referencia()
    atual = contagem_atual()
    novos = sorted(v for v in atual if v not in ref)
    subiram = sorted(f"{v}: {ref[v]} -> {atual[v]}" for v in atual if v in ref and atual[v] > ref[v])
    assert not novos and not subiram, (
        "cor escrita à mão no style.css. Use um token (var(--text), var(--spo-accent-text), "
        "var(--spo-control-border), var(--spo-positive)…; padrão SPO-AA+ §3) ou, se precisar de cor nova, "
        "crie um token (--nome: …) definido por tema. "
        f"Valores novos: {novos or '-'}; contagens que subiram: {subiram or '-'}."
    )


def test_referencia_acompanha_as_regras_migradas():
    """Se uma regra antiga passou a usar o token, baixe a referência (para não voltar a subir)."""
    ref = _referencia()
    atual = contagem_atual()
    desceram = sorted(f"{v}: {ref[v]} -> {atual.get(v, 0)}" for v in ref if atual.get(v, 0) < ref[v])
    if desceram:
        pytest.skip("Contagem de cores literais desceu (bom): rode `python tests/test_cor_literal.py` para "
                    "atualizar a referência. " + "; ".join(desceram[:10]))


def gravar_referencia() -> None:
    atual = contagem_atual()
    REFERENCIA.write_text(json.dumps({
        "sobre": "Cores literais fora de definição de token no static/css/style.css, por valor "
                 "(tests/test_cor_literal.py). Gerado pelo próprio teste; só baixa.",
        "total": sum(atual.values()),
        "valores": len(atual),
        "contagem": dict(sorted(atual.items())),
    }, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    antes = _referencia() if REFERENCIA.exists() else {}
    atual = contagem_atual()
    novos = [v for v in atual if v not in antes] if antes else []
    subiram = [v for v in atual if v in antes and atual[v] > antes[v]]
    if (novos or subiram) and "--forcar" not in sys.argv:
        sys.exit("A contagem SUBIU (valores novos ou mais usos): corrija o CSS em vez de atualizar a referência. "
                 "Só para uma exceção registrada no §6.3, rode com --forcar. " + ", ".join((novos + subiram)[:10]))
    gravar_referencia()
    print(f"Referência gravada: {sum(atual.values())} cores literais, {len(atual)} valores.")
