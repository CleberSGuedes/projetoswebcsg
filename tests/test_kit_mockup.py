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
    # presets do Contrato Visual Protegido (feat/kit-contrato)
    "const PROTECTED_CONTRACT_PRESETS = Object.freeze({",
]


# ---- feat/kit-previas: prévias do Personalizar e gráficos

GRAFICOS = ["initDiagramasDestaque", "initGraficosAcessiveis", "initGraficosEscala"]
TEMPLATE_PERSONALIZAR = RAIZ / "templates" / "partials" / "atualizar_personalizar_spo.html"


def _sem_espacos(texto: str) -> str:
    return re.sub(r"\s+", " ", texto).replace("> <", "><").strip()


def _bloco_div(html: str, abertura: str) -> str:
    """Do `<div ...>` de abertura até o </div> que o fecha (contando divs)."""
    i = html.index(abertura)
    nivel = 0
    for m in re.finditer(r"<div\b|</div>", html[i:]):
        nivel += 1 if m.group(0) == "<div" else -1
        if nivel == 0:
            return html[i: i + m.end()]
    raise AssertionError(f"div sem fecho: {abertura}")



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
    assert re.search(r'const PROTECTED_CONTRACT_STATE_KEY = "[^"]+"', main), (
        "PROTECTED_CONTRACT_STATE_KEY mudou de forma no main.js (o kit lê dele o preset vigente)")
    assert 'body.dataset.visualContractPreset' in main, "o SPO deixou de aplicar o preset por data-visual-contract-preset"


def test_trechos_do_teclado_sao_funcoes_completas():
    main = _main()
    for marcador in ("(function initNavegacaoTabelas() {", "(function initTecladoPaginacao() {"):
        bloco = _trecho(main, marcador, True)
        fim = main.index(bloco) + len(bloco)
        assert main[fim:fim + 4] == ")();", f"{marcador}: o kit espera a função fechada por '}})();'"


def _artigos(html: str) -> list:
    """Os <article class="card personalizar-card..."> de primeiro nível, inteiros."""
    out = []
    pos = 0
    while True:
        i = html.find('<article class="card personalizar-card', pos)
        if i < 0:
            return out
        nivel = 0
        for m in re.finditer(r"<article\b|</article>", html[i:]):
            nivel += 1 if m.group(0) == "<article" else -1
            if nivel == 0:
                out.append(html[i: i + m.end()])
                pos = i + m.end()
                break


def _sem_jinja(t: str) -> str:
    return re.sub(r"\{#.*?#\}", "", t, flags=re.S)


# cartões do Personalizar que não entram no painel do kit (o kit não tem atalhos; prévias e declaração
# ficam no conteúdo do kit ou não se aplicam)
FORA_DO_PAINEL = ('id="spo-teclado-card"', "personalizar-preview", "spo-acessibilidade", "protected-contract-card")


def test_painel_do_kit_tem_os_cartoes_do_personalizar():
    tpl = _sem_jinja(TEMPLATE_PERSONALIZAR.read_text(encoding="utf-8"))
    kit = (KIT / "mockup-spo-aa-plus.html").read_text(encoding="utf-8")
    originais = [a for a in _artigos(tpl) if not any(f in a for f in FORA_DO_PAINEL)]
    copias = [a for a in _artigos(kit) if "personalizar-preview" not in a and "kit-contrato-card" not in a]
    assert originais, "não achei os cartões do Personalizar no template"
    assert [_sem_espacos(a) for a in copias] == [_sem_espacos(a) for a in originais], (
        "os cartões do painel do kit de mockup não são mais iguais aos do Personalizar "
        "(templates/partials/atualizar_personalizar_spo.html): copie a marcação nova para o kit")
    # presets do Contrato: cartões dinâmicos gerados pelo kit a partir do PROTECTED_CONTRACT_PRESETS (com
    # label, summary, scope, state, rules e details, que o painel de contexto mostra)
    assert "data-kit-presets" in kit and 'id="kit-preset-contexto"' in kit
    presets = _trecho(_main(), "const PROTECTED_CONTRACT_PRESETS = Object.freeze({", False)
    for campo in ("label:", "summary:", "scope:", "state:", "rules:", "details:"):
        assert campo in presets, f"PROTECTED_CONTRACT_PRESETS sem {campo} (o painel de contexto do kit usa)"


def test_css_do_kit_segue_o_padrao():
    css = re.sub(r"/\*.*?\*/", "", (KIT / "kit-mockup.css").read_text(encoding="utf-8"), flags=re.S)
    assert not re.search(r"font-size\s*:\s*[0-9.]+px", css), "kit-mockup.css: tamanho em px; use var(--spo-fs-*)"
    assert not re.search(r"#[0-9a-fA-F]{3,8}\b|rgba?\(", css), "kit-mockup.css: cor literal; use os tokens (§3)"


def test_previas_do_kit_iguais_as_do_personalizar():
    tpl = TEMPLATE_PERSONALIZAR.read_text(encoding="utf-8")
    kit = (KIT / "mockup-spo-aa-plus.html").read_text(encoding="utf-8")
    for abertura in ('<div class="preview-surface">', '<div class="spo-fontes-amostra preview-surface"'):
        original = _sem_espacos(_bloco_div(tpl, abertura))
        copia = _sem_espacos(_bloco_div(kit, abertura))
        assert copia == original, (
            f"a prévia {abertura!r} do kit de mockup não é mais igual à do Personalizar "
            "(templates/partials/atualizar_personalizar_spo.html): copie a marcação nova para o kit")


def test_graficos_lidos_do_main_js():
    main = _main()
    kit_js = (KIT / "kit-mockup.js").read_text(encoding="utf-8")
    assert "const COMMON_DIAGRAM_THEMES = Object.freeze({" in main and "const COMMON_DIAGRAM_THEMES = Object.freeze({" in kit_js
    for nome in GRAFICOS:
        bloco = _trecho(main, f"(function {nome}() {{", True)
        fim = main.index(bloco) + len(bloco)
        assert main[fim:fim + 4] == ")();", f"{nome}: o kit espera a função fechada por '}})();'"
        assert f'"{nome}"' in kit_js, f"o kit de mockup não lê o {nome}"
    # o initGraficosEscala só pode depender destas funções globais (o kit dá a segunda)
    escala = _trecho(main, "(function initGraficosEscala() {", True)
    globais = set(re.findall(r"window\.(spo\w+)", escala))
    assert globais <= {"spoPaletaDaltonismo", "spoGetDiagramSemanticTokens"}, (
        f"o initGraficosEscala passou a usar {sorted(globais)}: o kit de mockup precisa oferecer essas funções")
    assert "window.spoTemaDiagramaComDestaque" in _trecho(main, "(function initDiagramasDestaque() {", True)


def test_kit_usa_o_mesmo_plotly_do_spo():
    base = (RAIZ / "templates" / "base.html").read_text(encoding="utf-8")
    kit = (KIT / "mockup-spo-aa-plus.html").read_text(encoding="utf-8")
    plotly = re.search(r'<script src="(https://cdn\.plot\.ly/[^"]+)"', base)
    assert plotly, "base.html sem o Plotly"
    assert plotly.group(1) in kit, f"o kit de mockup deve carregar o mesmo Plotly do SPO ({plotly.group(1)})"


# ---- feat/kit-catalogo: catálogo de padrões (exemplos recortados dos templates de origem)

PARTIALS = RAIZ / "templates" / "partials"
CATALOGO = [
    ("atualizar_governanca_resultados_eap_politicas.html", '<div class="governanca-grid semantic-flow-grid"', "div"),
    ("atualizar_governanca_resultados_eap_politicas.html", '<div class="flow-context-panel"', "div"),
    ("atualizar_governanca_resultados_mapa.html", '<article class="card governanca-card">', "article"),
    ("atualizar_governanca_resultados_mapa.html", '<div class="governanca-grid">', "div"),
    ("institucional_diretrizes.html", '<div class="noticia-box">', "div"),
    ("paineis_teto_orcamentario.html", '<div class="teto-kpi-grid">', "div"),
    ("notas_see.html", '<div class="see-upload-options">', "div"),
]


def _bloco(html: str, abertura: str, tag: str) -> str:
    i = html.index(abertura)
    nivel = 0
    for m in re.finditer(rf"<{tag}\b|</{tag}>", html[i:]):
        nivel += -1 if m.group(0).startswith("</") else 1
        if nivel == 0:
            return html[i: i + m.end()]
    raise AssertionError(f"<{tag}> sem fecho: {abertura}")


def test_catalogo_igual_aos_templates():
    kit = _sem_espacos((KIT / "mockup-spo-aa-plus.html").read_text(encoding="utf-8"))
    diferentes = []
    for arquivo, abertura, tag in CATALOGO:
        original = _sem_espacos(_bloco((PARTIALS / arquivo).read_text(encoding="utf-8"), abertura, tag))
        if original not in kit:
            diferentes.append(f"{arquivo}: {abertura}")
    assert not diferentes, (
        "o catálogo de padrões do kit de mockup não é mais igual aos templates (copie a marcação nova para o "
        f"kit): {diferentes}")


def test_fluxos_de_governanca_lidos_do_main_js():
    main = _main()
    kit_js = (KIT / "kit-mockup.js").read_text(encoding="utf-8")
    marcador = "function initGovernancaSemanticFlows() {"
    assert marcador in main and marcador in kit_js, "o kit lê o initGovernancaSemanticFlows do main.js"
    bloco = _trecho(main, marcador, True)
    globais = set(re.findall(r"\b(init[A-Z]\w+|spo[A-Z]\w+)\(", bloco)) - {"initGovernancaSemanticFlows"}
    assert not globais, f"o initGovernancaSemanticFlows passou a chamar {sorted(globais)}: o kit precisa delas"
