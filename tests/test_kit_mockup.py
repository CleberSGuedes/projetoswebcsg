"""Kit de mockup SPO-AA+ (static/kit/): continua lendo do main.js e segue o próprio padrão.

[jean] feat/kit-mockup (2026-11-15).

O kit não copia paleta, opções do Personalizar nem o teclado das tabelas: lê esses trechos do
main.js (kit-mockup.js, `lerMainJs`). Estes testes reprovam quando um desses trechos muda de nome
ou de forma no main.js (o kit deixaria de funcionar sem erro visível no SPO), quando o Personalizar
ganha uma opção que o kit não mostra e quando o CSS do kit escreve tamanho em px ou cor literal.
"""

import re
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
MAIN_JS = RAIZ / "static" / "js" / "main.js"
KIT = RAIZ / "static" / "kit"

# marcadores que o kit-mockup.js procura no main.js (mesmas strings nos dois arquivos)
MARCADORES = [
    "const ACCENT_COLORS = {",
    "const COMMON_VISUAL_OPTIONS = Object.freeze({",
    "const COMMON_VISUAL_DEFAULTS = Object.freeze({",
    "(function initFontesPersonalizar()",
    "const OPTIONS = Object.freeze({",
    "const DEFAULTS = Object.freeze({",
    "const ATTRS = Object.freeze({",
    "(function initNavegacaoTabelas() {",
    "(function initTecladoPaginacao() {",
    # regra da oferta "Leitura confortável" (Atkinson)
    "(function initSugestaoAtkinson() {",
    "const ORDEM = {",
    "const ALVO = {",
    "const SEM_VALOR = {",
    "const NOMES = {",
    "const BOTAO = [",
]
# opções do Personalizar que não fazem sentido no kit (não há atalhos de uma tecla nele)
FORA_DO_KIT = {"keys"}


def _main() -> str:
    return MAIN_JS.read_text(encoding="utf-8")


def _trecho(fonte: str, marcador: str, desde_o_marcador: bool) -> str:
    """Mesma regra do `trecho` do kit-mockup.js: do marcador até fechar a primeira chave aberta."""
    i = fonte.index(marcador)
    abre = fonte.index("{", i + len(marcador) - 1)
    nivel = 0
    for j in range(abre, len(fonte)):
        if fonte[j] == "{":
            nivel += 1
        elif fonte[j] == "}":
            nivel -= 1
            if nivel == 0:
                return fonte[i if desde_o_marcador else abre: j + 1]
    raise AssertionError(f"chave sem fecho: {marcador}")


def test_marcadores_estao_no_main_js_e_no_kit():
    main = _main()
    kit_js = (KIT / "kit-mockup.js").read_text(encoding="utf-8")
    faltam_main = [m for m in MARCADORES if m not in main]
    faltam_kit = [m for m in MARCADORES if m not in kit_js and m != "const BOTAO = ["]  # o kit acha o BOTAO por expressão regular
    assert not faltam_main, f"o kit de mockup lê estes trechos do main.js, que mudaram: {faltam_main}"
    assert not faltam_kit, f"kit-mockup.js sem os marcadores: {faltam_kit}"
    assert re.search(r'const DEFAULT_ACCENT = "[a-z]+"', main), "DEFAULT_ACCENT mudou de forma no main.js"
    sugestao = _trecho(main, "(function initSugestaoAtkinson() {", True)
    for m in ("const ORDEM = {", "const ALVO = {", "const SEM_VALOR = {", "const NOMES = {"):
        assert m in sugestao, f"a regra da oferta Atkinson saiu do initSugestaoAtkinson: {m}"
    assert re.search(r"const BOTAO = \[[^\]]*\];", sugestao), "BOTAO da oferta Atkinson mudou de forma"


def test_trechos_do_teclado_sao_funcoes_completas():
    main = _main()
    for marcador in ("(function initNavegacaoTabelas() {", "(function initTecladoPaginacao() {"):
        bloco = _trecho(main, marcador, True)
        fim = main.index(bloco) + len(bloco)
        assert main[fim:fim + 4] == ")();", f"{marcador}: o kit espera a função fechada por '}})();'"


def test_kit_mostra_todas_as_opcoes_do_personalizar():
    main = _main()
    fontes = main[main.index("(function initFontesPersonalizar()"):]
    opcoes = _trecho(fontes, "const OPTIONS = Object.freeze({", False)
    chaves = set(re.findall(r"^\s{4}(\w+): \{", opcoes, flags=re.M))
    assert chaves, "não achei as opções do initFontesPersonalizar"
    kit_js = (KIT / "kit-mockup.js").read_text(encoding="utf-8")
    grupos = set(re.findall(r'\["(\w+)", "[^"]+"\]', kit_js[kit_js.index("const GRUPOS_FONTES"):kit_js.index("const GRUPOS_VISUAL")]))
    faltam = sorted(chaves - grupos - FORA_DO_KIT)
    assert not faltam, f"o Personalizar tem opções que o kit de mockup não mostra (GRUPOS_FONTES): {faltam}"


def test_css_do_kit_segue_o_padrao():
    css = re.sub(r"/\*.*?\*/", "", (KIT / "kit-mockup.css").read_text(encoding="utf-8"), flags=re.S)
    assert not re.search(r"font-size\s*:\s*[0-9.]+px", css), "kit-mockup.css: tamanho em px; use var(--spo-fs-*)"
    assert not re.search(r"#[0-9a-fA-F]{3,8}\b|rgba?\(", css), "kit-mockup.css: cor literal; use os tokens (§3)"
