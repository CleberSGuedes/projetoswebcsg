// [jean] feat/kit-mockup (2026-11-15) — Kit de mockup SPO-AA+ (ver o comentário do mockup-spo-aa-plus.html).
//
// Nada de paleta, opções ou teclado é copiado para cá: o kit lê do main.js do SPO
//  - ACCENT_COLORS (os 8 destaques), DEFAULT_ACCENT, COMMON_VISUAL_OPTIONS/DEFAULTS;
//  - OPTIONS e DEFAULTS do initFontesPersonalizar (as opções do Personalizar);
//  - initNavegacaoTabelas e initTecladoPaginacao (navegação pelo teclado nas tabelas);
//  - initBuscaListasLongas (campo "Filtrar a lista" nas listas de 40 opções ou mais);
//  - a regra da oferta "Leitura confortável" do initSugestaoAtkinson (ORDEM, ALVO, SEM_VALOR, NOMES, BOTAO);
//  - os presets do Contrato Visual Protegido (PROTECTED_CONTRACT_PRESETS) e a chave onde o SPO guarda o
//    preset vigente (PROTECTED_CONTRACT_STATE_KEY): Padrão SPO, Compacto gerencial, Contraste técnico,
//    Pressão operacional (feat/kit-contrato, pedido do dev. Jean);
//  - os gráficos (feat/kit-previas): COMMON_DIAGRAM_THEMES e os blocos initDiagramasDestaque (destaque e
//    daltonismo), initGraficosAcessiveis (resumo e tabela de dados) e initGraficosEscala (fonte, densidade,
//    padrões do daltonismo, tela estreita), com o mesmo Plotly do base.html;
//  - o initGovernancaSemanticFlows (cartões dinâmicos com painel de contexto do catálogo de padrões);
//  - o initPadraoSpoAaPlus (a janela "O padrão SPO-AA+": resumo e documento técnico, o mesmo do SPO;
//    o documento técnico vem da rota /api/documentos/padrao-spo-aa-plus e pede o SPO logado).
// Ao abrir, o kit usa as preferências vigentes no SPO deste navegador (como qualquer tela do SPO);
// "Padrão SPO" volta ao padrão e recarregar a página volta às vigentes. Nada é gravado.
// O tests/test_kit_mockup.py confere que esses trechos continuam no main.js e que a extração funciona.
(function kitMockup() {
  "use strict";
  const body = document.body;
  const aviso = document.getElementById("kit-aviso");
  const opcoes = document.getElementById("kit-opcoes");

  function avisar(texto) {
    aviso.textContent = texto;
    aviso.hidden = !texto;
  }

  // ---------------------------------------------------------------- leitura do main.js
  // Devolve o trecho entre `marcador` e o fecho da primeira chave aberta depois dele (contando chaves;
  // os trechos lidos não têm chave dentro de texto); com inicioDoMarcador, o trecho começa no próprio marcador.
  function trecho(fonte, marcador, inicioDoMarcador) {
    const i = fonte.indexOf(marcador);
    if (i < 0) throw new Error(`trecho não encontrado no main.js: ${marcador}`);
    const abre = fonte.indexOf("{", i + marcador.length - 1);
    let nivel = 0;
    for (let j = abre; j < fonte.length; j += 1) {
      const c = fonte[j];
      if (c === "{") nivel += 1;
      else if (c === "}") {
        nivel -= 1;
        if (nivel === 0) return fonte.slice(inicioDoMarcador ? i : abre, j + 1);
      }
    }
    throw new Error(`chave sem fecho no main.js: ${marcador}`);
  }
  const objeto = (fonte, marcador) => new Function(`return (${trecho(fonte, marcador, false)});`)();

  function lerMainJs(fonte) {
    const fontes = fonte.slice(fonte.indexOf("(function initFontesPersonalizar()"));
    const sugestao = trecho(fonte, "(function initSugestaoAtkinson() {", true);
    const botao = sugestao.match(/const BOTAO = (\[[^\]]*\]);/);
    if (!botao) throw new Error("BOTAO da sugestão Atkinson não encontrado no main.js");
    const chaveContrato = fonte.match(/const PROTECTED_CONTRACT_STATE_KEY = "([^"]+)"/);
    if (!chaveContrato) throw new Error("PROTECTED_CONTRACT_STATE_KEY não encontrado no main.js");
    const bloco = (nome) => `${trecho(fonte, `(function ${nome}() {`, true)})();`;
    // funções de dentro do bloco principal do main.js, pela assinatura inteira (o trecho começa nela)
    const funcao = (assinatura) => trecho(fonte, assinatura, true);
    const linha = (nome) => {
      const m = fonte.match(new RegExp(`const ${nome} = [^;]+;`));
      if (!m) throw new Error(`${nome} não encontrado no main.js`);
      return m[0];
    };
    return {
      fluxos: `${trecho(fonte, "function initGovernancaSemanticFlows() {", true)}
initGovernancaSemanticFlows();`,
      padraoDoc: `${trecho(fonte, "(function initPadraoSpoAaPlus() {", true)})();`,
      diagramas: objeto(fonte, "const COMMON_DIAGRAM_THEMES = Object.freeze({"),
      graficos: ["initDiagramasDestaque", "initGraficosAcessiveis", "initGraficosEscala"].map(bloco),
      presets: objeto(fonte, "const PROTECTED_CONTRACT_PRESETS = Object.freeze({"),
      // [jean] feat/kit-sem-copias: destaque, densidade automática e modo dos diagramas pelas funções do SPO
      // (antes o kit tinha cópias delas)
      fonteDestaque: funcao("function applyAccentColor(accent, persist = true) {"),
      densidadeAuto: bloco("initDensidadeAutomatica"),
      // "Última atualização por UO" dos relatórios: do fuso de Manaus até o renderUltimasAtualizacoes
      fonteUltimas: (() => {
        const i = fonte.indexOf("const AMAZON_TZ = ");
        const marca = "const renderUltimasAtualizacoes = (meta, data) => {";
        const j = fonte.indexOf(marca);
        if (i < 0 || j < i) throw new Error("renderUltimasAtualizacoes não encontrado no main.js");
        return `${fonte.slice(i, j)}${trecho(fonte, marca, true)};\nreturn renderUltimasAtualizacoes;`;
      })(),
      fonteModoDiagramas: [linha("systemContrastQuery"), linha("systemForcedColorsQuery"),
        funcao("function getSystemPrefersMoreContrast() {"), funcao("function getSystemForcedColorsActive() {"),
        funcao("function normalizeCommonVisualPreferences(raw = {}) {"),
        funcao("function getResolvedCommonDiagramMode(preferences = readCommonVisualPreferences()) {")].join(";\n"),
      chaveContrato: chaveContrato[1],
      sugestao: {
        ordem: objeto(sugestao, "const ORDEM = {"),
        alvo: objeto(sugestao, "const ALVO = {"),
        semValor: objeto(sugestao, "const SEM_VALOR = {"),
        nomes: objeto(sugestao, "const NOMES = {"),
        botao: new Function(`return ${botao[1]};`)(),
      },
      destaques: objeto(fonte, "const ACCENT_COLORS = {"),
      destaquePadrao: (fonte.match(/const DEFAULT_ACCENT = "([a-z]+)"/) || [])[1] || "classic",
      visual: objeto(fonte, "const COMMON_VISUAL_OPTIONS = Object.freeze({"),
      visualPadrao: objeto(fonte, "const COMMON_VISUAL_DEFAULTS = Object.freeze({"),
      fontes: objeto(fontes, "const OPTIONS = Object.freeze({"),
      fontesPadrao: objeto(fontes, "const DEFAULTS = Object.freeze({"),
      atributos: objeto(fontes, "const ATTRS = Object.freeze({"),
      teclado: [trecho(fonte, "(function initNavegacaoTabelas() {", true) + ")();",
                trecho(fonte, "(function initTecladoPaginacao() {", true) + ")();"],
      // busca nas listas longas dos formulários (feat/busca-listas-longas): o mesmo bloco do SPO
      buscaListas: trecho(fonte, "(function initBuscaListasLongas() {", true) + ")();",
      // barra de título da tela (fix/menu-cabecalho-pta): o menu compacto, que leva a lupa e o "?" e alinha o "›"
      menuCompacto: trecho(fonte, "(function initMenuCompacto() {", true) + ")();",
      // filtro no desktop (fix/filtro-desktop): abre alinhado pela direita quando sairia da área da tabela
      filtroPosicao: trecho(fonte, "(function initFiltroPosicao() {", true) + ")();",
    };
  }

  // ---------------------------------------------------------------- preferências
  const TEMAS = { light: "Claro", dark: "Escuro", auto: "Automático (segue o sistema)" };
  let spo = null;
  let estado = null;

  function padrao() {
    return { tema: "light", destaque: spo.destaquePadrao, preset: "default", ...spo.visualPadrao, ...spo.fontesPadrao };
  }

  function lerJson(chave) {
    try { return JSON.parse(localStorage.getItem(chave) || "{}") || {}; } catch (_) { return {}; }
  }

  // as escolhas gravadas pelo SPO neste navegador (mesma origem); só leitura
  function vigentes() {
    const p = padrao();
    let tema = null;
    let destaque = null;
    try { tema = localStorage.getItem("app-theme"); destaque = localStorage.getItem("app-accent"); } catch (_) { /* bloqueado */ }
    if (TEMAS[tema]) p.tema = tema;
    if (destaque === "spo") destaque = "green";
    if (spo.destaques[destaque]) p.destaque = destaque;
    const visual = lerJson("spo-common-visual-preferences");
    Object.keys(spo.visual).forEach((k) => { if (spo.visual[k][visual[k]]) p[k] = visual[k]; });
    const fontes = lerJson("spo-font-preferences");
    Object.keys(spo.fontes).forEach((k) => { if (spo.fontes[k][fontes[k]]) p[k] = fontes[k]; });
    // preset do Contrato como o getStoredProtectedContractPreset do main.js: prévia, senão rascunho, senão ativo
    const contrato = lerJson(spo.chaveContrato);
    const preset = contrato.preview || contrato.draft || contrato.active;
    if (spo.presets[preset]) p.preset = preset;
    return p;
  }

  function temaEscuro(tema) {
    if (tema === "auto") return !!(window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches);
    return tema === "dark";
  }

  // applyAccentColor do main.js (com persist = false: não grava) e getResolvedCommonDiagramMode, montados com
  // os dados que o kit já leu; a densidade automática vem do initDensidadeAutomatica (window.spoResolverDensidade)
  let aplicarDestaque = null;
  let modoDiagramas = null;
  function montarFuncoesDoSpo() {
    aplicarDestaque = new Function("ACCENT_COLORS", "DEFAULT_ACCENT", `${spo.fonteDestaque};\nreturn applyAccentColor;`)(spo.destaques, spo.destaquePadrao);
    modoDiagramas = new Function("COMMON_VISUAL_DEFAULTS", "COMMON_VISUAL_OPTIONS", "COMMON_DIAGRAM_THEMES",
      `${spo.fonteModoDiagramas};\nreturn getResolvedCommonDiagramMode;`)(spo.visualPadrao, spo.visual, spo.diagramas);
    new Function(spo.densidadeAuto)();
  }

  function aplicar() {
    body.classList.toggle("theme-dark", temaEscuro(estado.tema));
    body.dataset.themePreference = estado.tema;
    aplicarDestaque(estado.destaque, false);
    body.dataset.visualDensityPref = estado.density;
    body.dataset.visualDensity = typeof window.spoResolverDensidade === "function" ? window.spoResolverDensidade(estado.density) : estado.density;
    body.dataset.visualContrast = estado.contrast;
    body.dataset.visualCards = estado.cards;
    // como o setProtectedContractVisualPreset do main.js: no <html> e no <body>
    document.documentElement.dataset.visualContractPreset = estado.preset;
    body.dataset.visualContractPreset = estado.preset;
    // modo dos diagramas pela função do SPO (com cores forçadas do Windows ela devolve "system"; o kit então
    // desenha com a paleta clara, porque o tema "system" é montado por outra função do main.js)
    body.dataset.visualDiagrams = estado.diagrams;
    body.dataset.visualDiagramTheme = modoDiagramas({ ...spo.visualPadrao, density: estado.density, contrast: estado.contrast, cards: estado.cards, diagrams: estado.diagrams });
    Object.keys(spo.atributos).forEach((k) => { if (estado[k] !== undefined) body.dataset[spo.atributos[k]] = estado[k]; });
    sincronizar();
    desenharGraficos();
    barraTela.sincronizar();
  }

  // ---------------------------------------------------------------- barra de título da tela
  // Exemplo vivo numa moldura (iframe) com o style.css do SPO: o corpo da moldura tem special-context-focus (o
  // estado do SPO com a barra da tela no lugar do cabeçalho) e roda o initMenuCompacto do main.js, que leva a lupa
  // e o "?" para a barra e alinha o "›". As preferências do kit (tema, destaque, fontes…) passam para a moldura.
  const barraTela = (function montarBarraTela() {
    const moldura = document.getElementById("kit-barra-tela");
    const modelo = document.getElementById("kit-barra-tela-modelo");
    let pronta = false;
    const css = new URL(document.querySelector('link[href*="css/style.css"]').getAttribute("href"), location.href).href;
    const icones = (document.querySelector('link[href*="bootstrap-icons"]') || {}).href || "";
    function copiarPreferencias() {
      const doc = moldura.contentDocument;
      if (!pronta || !doc || !doc.body) return;
      const compacto = doc.body.classList.contains("spo-menu-compacto");
      [[document.documentElement, doc.documentElement], [body, doc.body]].forEach(([de, para]) => {
        [...para.attributes].forEach((a) => { if (a.name.startsWith("data-")) para.removeAttribute(a.name); });
        [...de.attributes].forEach((a) => { if (a.name.startsWith("data-")) para.setAttribute(a.name, a.value); });
        const estilo = de.getAttribute("style");
        if (estilo) para.setAttribute("style", estilo); else para.removeAttribute("style");
      });
      doc.documentElement.style.setProperty("--spo-contextbar-height", "0px");
      doc.body.className = `${body.className.replace("kit-mockup-body", "")} special-context-focus pta2027-immersive pta2027-context-focus`.trim();
      doc.body.classList.toggle("spo-menu-compacto", compacto);
      medir();
    }
    // no celular (até 600 px), a moldura vai de uma borda à outra da tela, como a barra no SPO
    function encaixar() {
      const celular = window.innerWidth <= 600;
      moldura.classList.toggle("kit-tela-inteira", celular);
      moldura.style.width = "";
      moldura.style.marginLeft = "";
      if (!celular) return;
      const esquerda = moldura.parentElement.getBoundingClientRect().left;
      moldura.style.width = `${document.documentElement.clientWidth}px`;
      moldura.style.marginLeft = `${-esquerda}px`;
    }
    window.addEventListener("resize", () => requestAnimationFrame(encaixar));
    function medir() {
      const doc = moldura.contentDocument;
      if (!doc || !doc.body) return;
      const cabecalho = doc.querySelector(".pta2027-integration-header");
      const altura = Math.ceil((cabecalho ? cabecalho.getBoundingClientRect().bottom : 0) + 72);
      if (Math.abs(moldura.offsetHeight - altura) > 1) moldura.style.height = `${altura}px`;
    }
    function montar(codigoMenu) {
      if (!moldura || !modelo) return;
      encaixar();
      moldura.srcdoc = `<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="${icones}"><link rel="stylesheet" href="${css}">
</head><body class="special-context-focus pta2027-immersive pta2027-context-focus">
<div class="layout">
  <header class="topbar"><div class="user-info"><div class="spo-cabecalho-acoes">
    <button type="button" class="icon-btn spo-cabecalho-btn spo-cabecalho-busca" aria-label="Buscar tela (exemplo)"><i class="bi bi-search" aria-hidden="true"></i></button>
    <button type="button" class="icon-btn spo-cabecalho-btn spo-cabecalho-ajuda" aria-label="Ajuda (exemplo)"><i class="bi bi-question-lg" aria-hidden="true"></i></button>
  </div></div></header>
  <aside class="sidebar collapsed" id="sidebar"><div class="sidebar-header">
    <button class="icon-btn toggle-btn" id="sidebar-toggle" aria-label="Alternar menu (exemplo)"><i class="bi bi-chevron-right" aria-hidden="true"></i></button>
  </div><nav class="menu" id="menu"></nav></aside>
  <div class="main"><main id="content-area"><section class="governanca-resultados pta2027-integration">${modelo.innerHTML}</section>
    <p class="muted">Conteúdo da tela…</p></main></div>
</div></body></html>`;
      moldura.addEventListener("load", () => {
        const doc = moldura.contentDocument;
        const meta = doc.getElementById("pta2027-context-meta");
        if (meta) meta.innerHTML = '<span class="pta2027-context-user-name">Usuária de exemplo</span><span class="pta2027-context-user-detail">17/11/2026, 10:00 | Logados: 3</span>';
        const voltar = doc.getElementById("pta2027-sticky-back");
        if (voltar) voltar.hidden = false;
        pronta = true;
        copiarPreferencias();
        // a moldura é baixa de propósito: no SPO, janela com até 560 px de altura é celular deitado (menu compacto);
        // aqui só a largura decide, para o exemplo mostrar o tablet e o desktop
        const janela = moldura.contentWindow;
        const consultar = janela.matchMedia.bind(janela);
        janela.matchMedia = (q) => consultar(String(q).replace(/,\s*\(max-height:\s*560px\)/, ""));
        janela.Function(codigoMenu)();
        if ("ResizeObserver" in window) new janela.ResizeObserver(() => janela.requestAnimationFrame(medir)).observe(doc.body);
        setTimeout(medir, 300);
      }, { once: true });
    }
    return { montar, sincronizar: copiarPreferencias };
  })();

  // ---------------------------------------------------------------- gráficos
  // Tema como o getCommonDiagramTheme do main.js: paleta do modo dos diagramas (data-visual-diagram-theme),
  // passada pelo spoTemaDiagramaComDestaque (destaque e daltonismo, do initDiagramasDestaque). O layout
  // segue o plotLayout dos gráficos do Teto; fonte, tamanho, legenda e padrões vêm do initGraficosEscala.
  let graficosProntos = false;
  function temaDosDiagramas() {
    const modo = body.dataset.visualDiagramTheme || "light";
    const base = spo.diagramas[modo] || spo.diagramas.light;
    return typeof window.spoTemaDiagramaComDestaque === "function" ? window.spoTemaDiagramaComDestaque(base, modo) : base;
  }
  function desenharGraficos() {
    if (!graficosProntos || !window.Plotly) return;
    const t = temaDosDiagramas();
    const eixo = { gridcolor: t.grid, zerolinecolor: t.grid, linecolor: t.axis, tickfont: { color: t.text } };
    const layout = (extra) => ({
      margin: { t: 20, r: 24, b: 70, l: 70 }, paper_bgcolor: t.panel, plot_bgcolor: t.plot,
      font: { family: "Arial, sans-serif", size: 11, color: t.text }, separators: ",.",
      legend: { font: { color: t.text } }, xaxis: eixo, yaxis: eixo, ...extra,
    });
    const config = { responsive: true, displaylogo: false, modeBarButtonsToRemove: ["lasso2d"] };
    const grupos = ["1 - Pessoal e Encargos Sociais", "3 - Outras Despesas Correntes", "4 - Investimentos"];
    window.Plotly.react("kit-grafico-pizza", [{
      type: "pie", labels: grupos, values: [3850000000, 2410000000, 275000000],
      textinfo: "percent", textposition: "outside",
      marker: { colors: grupos.map((_, i) => t.palette[i % t.palette.length]), line: { color: t.border, width: 1 } },
    }], layout({ margin: { t: 8, r: 20, b: 80, l: 20 }, legend: { orientation: "h", y: -0.18, font: { color: t.text } } }), config);
    const regioes = ["Baixada Cuiabana", "Norte", "Sul", "Oeste", "Leste"];
    window.Plotly.react("kit-grafico-barras", [
      { type: "bar", name: "Empenhado", x: regioes, y: [412, 285, 330, 198, 240], marker: { color: t.palette[0], line: { color: t.border, width: 1 } } },
      { type: "bar", name: "Liquidado", x: regioes, y: [356, 230, 301, 150, 205], marker: { color: t.palette[1], line: { color: t.border, width: 1 } } },
    ], layout({ barmode: "group", yaxis: { ...eixo, title: { text: "R$ milhões", font: { color: t.text } } } }), config);
  }

  // ---------------------------------------------------------------- oferta "Leitura confortável"
  // Mesma regra do initSugestaoAtkinson (main.js): ao escolher a Atkinson na fonte da interface, oferece o
  // que falta (Atkinson Mono nos dados; tamanho, entrelinha, leitura e densidade até o sugerido, sem reduzir
  // quem já usa mais). Nada muda sem o clique; aparece toda vez que a Atkinson é escolhida.
  function faltandoAtkinson() {
    const s = spo.sugestao;
    const abaixo = (k) => s.ordem[k].indexOf(estado[k] || s.semValor[k]) < s.ordem[k].indexOf(s.alvo[k]);
    return [...(estado.data === "atkinson-mono" ? [] : ["data"]), ...Object.keys(s.alvo).filter(abaixo)];
  }
  function removerOferta() { document.getElementById("kit-sugestao-atkinson")?.remove(); }
  function mostrarOferta() {
    removerOferta();
    const falta = faltandoAtkinson();
    if (!falta.length) return;
    const s = spo.sugestao;
    const lista = (itens) => (itens.length < 2 ? itens.join("") : `${itens.slice(0, -1).join(", ")} e ${itens[itens.length - 1]}`);
    const caixa = document.createElement("div");
    caixa.id = "kit-sugestao-atkinson";
    caixa.className = "spo-sugestao";
    caixa.setAttribute("role", "status");
    caixa.innerHTML = `<p><strong>Leitura confortável:</strong> quer aplicar também ${lista(falta.map((k) => s.nomes[k]))}? `
      + `${falta.length > 1 ? "Cada opção continua ajustável depois." : "A opção continua ajustável depois."}</p>`
      + '<div class="spo-sugestao-acoes">'
      + `<button type="button" class="btn btn-primary sm" data-sugestao="aplicar">${s.botao[falta.length]}</button>`
      + '<button type="button" class="btn btn-secondary sm" data-sugestao="dispensar">Agora não</button></div>';
    opcoes.querySelector('[data-spo-font-group="ui"]').after(caixa);
    caixa.addEventListener("click", (ev) => {
      const b = ev.target.closest("[data-sugestao]");
      if (!b) return;
      if (b.dataset.sugestao === "aplicar") {
        faltandoAtkinson().forEach((k) => { estado[k] = k === "data" ? "atkinson-mono" : s.alvo[k]; });
        aplicar();
      }
      opcoes.querySelector('[data-spo-font-toggle="ui"]').focus();
      removerOferta();
    });
  }

  // ---------------------------------------------------------------- painel com os cartões do Personalizar
  // A marcação é a do template do Personalizar (copiada no HTML do kit); o comportamento é o do main.js
  // (syncThemeControls, syncCommonVisualControls, initFontesPersonalizar, presets do Contrato): a escolha
  // fica com .active e aria-pressed, o resumo do grupo mostra a opção escolhida, um grupo aberto por vez
  // e o grupo fecha ao escolher. Só não grava nada.
  function marcar(seletor, atributo, valor) {
    opcoes.querySelectorAll(seletor).forEach((b) => {
      const ativo = b.getAttribute(atributo) === valor;
      b.classList.toggle("active", ativo);
      b.setAttribute("aria-pressed", String(ativo));
    });
  }
  function sincronizar() {
    marcar("[data-theme-choice]", "data-theme-choice", estado.tema);
    marcar("[data-accent-choice]", "data-accent-choice", estado.destaque);
    marcar("[data-contract-preset]", "data-contract-preset", estado.preset);
    opcoes.querySelectorAll("[data-contract-preset]").forEach((b) => b.classList.toggle("is-selected", b.dataset.contractPreset === estado.preset));
    contextoDoPreset();
    // os presets do catálogo (marcação do Contrato) mostram o mesmo preset como selecionado
    document.querySelectorAll(".kit-catalogo [data-contract-preset]").forEach((b) => {
      const ativo = b.dataset.contractPreset === estado.preset;
      b.classList.toggle("active", ativo);
      b.setAttribute("aria-pressed", String(ativo));
    });
    opcoes.querySelectorAll("[data-visual-pref]").forEach((b) => {
      const ativo = estado[b.dataset.visualPref] === b.dataset.visualValue;
      b.classList.toggle("active", ativo);
      b.setAttribute("aria-pressed", String(ativo));
    });
    opcoes.querySelectorAll("[data-visual-pref-current]").forEach((t) => {
      const k = t.dataset.visualPrefCurrent;
      t.textContent = (spo.visual[k] && spo.visual[k][estado[k]]) || estado[k] || "";
    });
    opcoes.querySelectorAll("[data-spo-font-pref]").forEach((b) => {
      const ativo = estado[b.dataset.spoFontPref] === b.dataset.spoFontValue;
      b.classList.toggle("active", ativo);
      b.setAttribute("aria-pressed", String(ativo));
    });
    opcoes.querySelectorAll("[data-spo-font-current]").forEach((t) => {
      const k = t.dataset.spoFontCurrent;
      t.textContent = (spo.fontes[k] && spo.fontes[k][estado[k]]) || "";
    });
  }
  // um grupo aberto por vez dentro de cada cartão (como setCommonVisualGroupOpen e setOpen do main.js)
  function abrirGrupo(cartao, grupoAberto) {
    cartao.querySelectorAll(".visual-pref-group").forEach((g) => {
      const aberto = g === grupoAberto;
      g.classList.toggle("is-open", aberto);
      const toggle = g.querySelector("[data-visual-pref-toggle], [data-spo-font-toggle]");
      const painel = g.querySelector("[data-visual-pref-panel], [data-spo-font-panel]");
      if (toggle) toggle.setAttribute("aria-expanded", String(aberto));
      if (painel) painel.hidden = !aberto;
    });
  }
  function escolher(chave, valor) {
    estado[chave] = valor;
    aplicar();
    if (chave === "ui") { if (valor === "atkinson") mostrarOferta(); else removerOferta(); }
  }

  // presets do Contrato: um cartão dinâmico por preset; o escolhido mostra embaixo escopo, estado, regras e
  // detalhes (os mesmos dados que o Catálogo protegido do Contrato mostra)
  const ICONES_PRESET = { default: "bi-shield-check", compact: "bi-arrows-collapse", contrast: "bi-circle-half", mobile: "bi-phone" };
  function montarPresets() {
    const grade = opcoes.querySelector("[data-kit-presets]");
    grade.innerHTML = Object.entries(spo.presets).map(([k, p]) => `<button class="flow-node" type="button" data-contract-preset="${esc(k)}" aria-controls="kit-preset-contexto">`
      + `<i class="bi ${ICONES_PRESET[k] || "bi-sliders"} governanca-icon" aria-hidden="true"></i>`
      + `<strong>${esc(p.label || k)}</strong><small>${esc(p.summary || "")}</small></button>`).join("");
  }
  function contextoDoPreset() {
    const painel = document.getElementById("kit-preset-contexto");
    const p = spo.presets[estado.preset];
    if (!painel || !p) return;
    const regras = Object.entries(p.rules || {}).map(([r, v]) => `<li><span class="kit-regra-nome">${esc(r)}:</span> ${esc(v)}</li>`).join("");
    const detalhes = (p.details || []).map((d) => `<li>${esc(d)}</li>`).join("");
    painel.innerHTML = `<span class="flow-context-label">${esc(p.scope || "Preset")} · ${esc(p.state || "")}</span>`
      + `<strong>${esc(p.label)}</strong><p>${esc(p.summary || "")}</p>`
      + (regras ? `<ul class="kit-preset-regras">${regras}</ul>` : "")
      + (detalhes ? `<ul class="kit-preset-detalhes">${detalhes}</ul>` : "");
  }

  function montarPainel() {
    montarPresets();
    opcoes.querySelectorAll(".visual-pref-group").forEach((g) => abrirGrupo(g.closest(".card"), null));
    opcoes.addEventListener("click", (ev) => {
      const alvo = ev.target.closest("button");
      if (!alvo || !opcoes.contains(alvo) || alvo.closest(".spo-sugestao")) return;
      const cartao = alvo.closest(".card");
      if (alvo.matches("[data-visual-pref-toggle], [data-spo-font-toggle]")) {
        const grupo = alvo.closest(".visual-pref-group");
        abrirGrupo(cartao, grupo.classList.contains("is-open") ? null : grupo);
        return;
      }
      if (alvo.dataset.themeChoice) return escolher("tema", alvo.dataset.themeChoice);
      if (alvo.dataset.accentChoice && spo.destaques[alvo.dataset.accentChoice]) return escolher("destaque", alvo.dataset.accentChoice);
      if (alvo.dataset.contractPreset && spo.presets[alvo.dataset.contractPreset]) return escolher("preset", alvo.dataset.contractPreset);
      if (alvo.dataset.visualPref && spo.visual[alvo.dataset.visualPref]?.[alvo.dataset.visualValue]) {
        abrirGrupo(cartao, null);
        return escolher(alvo.dataset.visualPref, alvo.dataset.visualValue);
      }
      if (alvo.dataset.spoFontPref && spo.fontes[alvo.dataset.spoFontPref]?.[alvo.dataset.spoFontValue]) {
        abrirGrupo(cartao, null);
        const status = cartao.querySelector(".spo-fontes-status");
        if (status) status.textContent = `"${spo.fontes[alvo.dataset.spoFontPref][alvo.dataset.spoFontValue]}" aplicada nesta página.`;
        return escolher(alvo.dataset.spoFontPref, alvo.dataset.spoFontValue);
      }
    });
    document.getElementById("kit-padrao").addEventListener("click", () => {
      removerOferta();
      opcoes.querySelectorAll(".spo-fontes-status").forEach((st) => { st.textContent = ""; });
      estado = padrao();
      aplicar();
    });
  }

  const recolher = document.getElementById("kit-recolher");
  recolher.addEventListener("click", () => {
    const aberto = recolher.getAttribute("aria-expanded") === "true";
    recolher.setAttribute("aria-expanded", String(!aberto));
    recolher.textContent = aberto ? "Mostrar" : "Recolher";
    opcoes.hidden = aberto;
    document.getElementById("kit-previas-bloco").hidden = aberto;
  });

  // ---------------------------------------------------------------- conteúdo de exemplo
  // catálogo: os indicadores do Teto vêm zerados no template (o JavaScript da tela preenche); valores de exemplo
  [["teto-kpi-total", "R$ 6.535.243.129,00"], ["teto-kpi-fontes", "12"], ["teto-kpi-grupos", "3"], ["teto-kpi-paoes", "41"]]
    .forEach(([id, valor]) => { const el = document.getElementById(id); if (el) el.textContent = valor; });

  // tabela: dados fictícios, filtros de lista e paginação no padrão dos relatórios
  const COLUNAS = ["uo", "programa", "acao", "regiao", "situacao", "valor", "saldo"];
  const NUMERICAS = new Set(["valor", "saldo"]);
  const PROGRAMAS = ["533 - Educação 10 Anos", "534 - Escola Segura", "036 - Apoio administrativo"];
  const ACOES = ["2936 - Desenvolvimento das Modalidades de Ensino", "2957 - Bem-estar escolar", "2009 - Manutenção"];
  const REGIOES = ["Baixada Cuiabana", "Norte", "Sul", "Oeste", "Leste", "Centro"];
  const SITUACOES = ["Empenhado", "Liquidado", "Pago"];
  const linhas = Array.from({ length: 37 }, (_, i) => ({
    uo: i % 7 === 0 ? "14601" : "14101",
    programa: PROGRAMAS[i % 3],
    acao: ACOES[(i * 2) % 3],
    regiao: REGIOES[i % 6],
    situacao: SITUACOES[(i * 5) % 3],
    valor: 125000 + ((i * 7919) % 900000) + 0.5 * (i % 2),
    saldo: ((i * 4517) % 300000) - 120000,
  }));
  const filtros = {};
  COLUNAS.forEach((c) => { filtros[c] = new Set(); });
  let pagina = 1;
  const moeda = (v) => v.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const esc = (t) => String(t).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

  const tabela = document.getElementById("kit-tabela");
  const corpo = tabela.tBodies[0];
  const paginacao = document.getElementById("kit-pagination");
  const tamanho = document.getElementById("kit-page-size");

  function filtradas() {
    return linhas.filter((l) => COLUNAS.every((c) => !filtros[c].size || filtros[c].has(String(l[c]))));
  }

  function desenhar() {
    const dados = filtradas();
    const porPagina = Number(tamanho.value);
    const paginas = Math.max(1, Math.ceil(dados.length / porPagina));
    pagina = Math.min(pagina, paginas);
    const vista = dados.slice((pagina - 1) * porPagina, pagina * porPagina);
    corpo.innerHTML = vista.length
      ? vista.map((l) => `<tr>${COLUNAS.map((c) => (NUMERICAS.has(c)
        ? `<td class="num${c === "saldo" ? (l[c] < 0 ? " neg" : (l[c] > 0 ? " pos" : "")) : ""}">${moeda(l[c])}</td>`
        : `<td>${esc(l[c])}</td>`)).join("")}</tr>`).join("")
      : `<tr><td colspan="${COLUNAS.length}" class="muted">Nenhum resultado com estes filtros.</td></tr>`;
    // totais do topo (recortado do FIP 613), preenchidos como o JavaScript da tela: no FIP, crédito e teto ficam
    // sem cor e bloqueado e saldo recebem pos/neg pelo sinal
    const tot = (id, texto, sinal) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = texto;
      el.classList.remove("pos", "neg");
      if (sinal > 0) el.classList.add("pos");
      if (sinal < 0) el.classList.add("neg");
    };
    const credito = dados.reduce((s, l) => s + l.valor, 0);
    const saldo = Math.round(credito * 0.29 * 100) / 100; // saldo de dotação de exemplo (positivo, como costuma ser)
    tot("tot-paoe", new Set(dados.map((l) => l.acao)).size > 1 ? "Vários PAOEs" : (dados[0] ? dados[0].acao.split(" - ")[0] : "-"));
    tot("tot-grupo", "1*3*4");
    tot("tot-cred-autorizado", moeda(credito));
    tot("tot-bloqueado", moeda(-201449949.07), -1);
    tot("tot-teto", moeda(credito - 201449949.07));
    tot("tot-saldo-dotacao", moeda(saldo), Math.sign(saldo));
    paginacao.innerHTML = "";
    const botao = (rotulo, destino, desligado, ativo) => {
      const b = document.createElement("button");
      b.type = "button";
      b.className = "page-btn";
      b.textContent = rotulo;
      if (desligado) b.disabled = true;
      if (ativo) { b.classList.add("active"); b.setAttribute("aria-current", "page"); }
      b.addEventListener("click", () => { if (!desligado && destino !== pagina) { pagina = destino; desenhar(); } });
      paginacao.appendChild(b);
    };
    botao("<<", 1, pagina === 1);
    botao("<", Math.max(1, pagina - 1), pagina === 1);
    for (let p = 1; p <= paginas; p += 1) botao(String(p), p, false, p === pagina);
    botao(">", Math.min(paginas, pagina + 1), pagina === paginas);
    botao(">>", paginas, pagina === paginas);
  }
  tamanho.addEventListener("change", () => { pagina = 1; desenhar(); });

  // filtros de lista (.mf-*), como os dos relatórios: Aplicar/Cancelar, Esc cancela e devolve o foco
  function montarFiltro(coluna) {
    const th = document.createElement("th");
    const wrap = document.createElement("div");
    wrap.className = "mf-wrapper";
    const display = document.createElement("button");
    display.type = "button";
    display.className = "mf-display";
    display.setAttribute("aria-expanded", "false");
    display.innerHTML = '<span>(Todos)</span><i class="bi bi-chevron-down" aria-hidden="true"></i>';
    const painel = document.createElement("div");
    painel.className = "mf-panel";
    const busca = document.createElement("input");
    busca.type = "text";
    busca.className = "mf-search";
    busca.placeholder = "Buscar...";
    busca.setAttribute("aria-label", "Buscar valores");
    const lista = document.createElement("div");
    lista.className = "mf-options";
    const valores = [...new Set(linhas.map((l) => (NUMERICAS.has(coluna) ? moeda(l[coluna]) : String(l[coluna]))))].sort();
    const caixas = valores.map((v) => {
      const row = document.createElement("label");
      row.className = "mf-option";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.value = NUMERICAS.has(coluna) ? String(linhas.find((l) => moeda(l[coluna]) === v)[coluna]) : v;
      const txt = document.createElement("span");
      txt.textContent = v;
      row.append(cb, txt);
      lista.appendChild(row);
      return { row, cb, txt };
    });
    const acoes = document.createElement("div");
    acoes.className = "mf-actions";
    const cancelar = document.createElement("button");
    cancelar.type = "button";
    cancelar.className = "mf-btn ghost";
    cancelar.textContent = "Cancelar";
    const aplicarBtn = document.createElement("button");
    aplicarBtn.type = "button";
    aplicarBtn.className = "mf-btn primary";
    aplicarBtn.textContent = "Aplicar";
    acoes.append(cancelar, aplicarBtn);
    painel.append(busca, lista, acoes);
    wrap.append(display, painel);
    th.appendChild(wrap);

    const fechar = (foco) => {
      painel.classList.remove("open");
      display.setAttribute("aria-expanded", "false");
      if (foco) display.focus();
    };
    display.addEventListener("click", () => {
      const abrir = !painel.classList.contains("open");
      document.querySelectorAll("#kit-tabela .mf-panel.open").forEach((p) => p.classList.remove("open"));
      document.querySelectorAll('#kit-tabela .mf-display[aria-expanded="true"]').forEach((d) => d.setAttribute("aria-expanded", "false"));
      if (!abrir) return;
      caixas.forEach(({ row, cb }) => { cb.checked = filtros[coluna].has(cb.value); row.style.display = ""; });
      busca.value = "";
      painel.classList.add("open");
      display.setAttribute("aria-expanded", "true");
    });
    busca.addEventListener("input", () => {
      const termo = busca.value.toLowerCase();
      caixas.forEach(({ row, txt }) => { row.style.display = txt.textContent.toLowerCase().includes(termo) ? "" : "none"; });
    });
    cancelar.addEventListener("click", () => fechar(true));
    aplicarBtn.addEventListener("click", () => {
      filtros[coluna] = new Set(caixas.filter(({ cb }) => cb.checked).map(({ cb }) => cb.value));
      display.querySelector("span").textContent = filtros[coluna].size ? `${filtros[coluna].size} selecionado(s)` : "(Todos)";
      pagina = 1;
      fechar(true);
      desenhar();
    });
    // Esc com a lista aberta (no botão ou dentro dela) cancela e devolve o foco ao botão, como o initEscFiltros
    wrap.addEventListener("keydown", (ev) => {
      if (ev.key === "Escape" && painel.classList.contains("open")) { ev.preventDefault(); ev.stopPropagation(); fechar(true); }
    });
    return th;
  }
  const linhaFiltros = tabela.querySelector("[data-kit-filtros]");
  COLUNAS.forEach((c) => linhaFiltros.appendChild(montarFiltro(c)));
  desenhar();

  // ---------------------------------------------------------------- início
  fetch("../js/main.js", { cache: "no-cache" })
    .then((r) => { if (!r.ok) throw new Error(`main.js: HTTP ${r.status}`); return r.text(); })
    .then((fonte) => {
      spo = lerMainJs(fonte);
      montarFuncoesDoSpo();
      new Function(spo.fonteUltimas)()(document.getElementById("fip613-relatorio-meta"), {
        ultimas_por_uo: [{ rotulo: "14101 - SEDUC", user_email: "usuario.exemplo@spo-dev.test",
          uploaded_at: "2026-11-15T13:59:25", data_arquivo: "2026-11-15T09:14:00" }],
      });
      estado = vigentes(); // as preferências vigentes no SPO deste navegador (sem nenhuma, é o padrão SPO)
      montarPainel();
      aplicar();
      spo.teclado.forEach((codigo) => new Function(codigo)());
      new Function(spo.buscaListas)();
      new Function(spo.filtroPosicao)();
      new Function(spo.fluxos)();
      new Function(spo.padraoDoc)();
      barraTela.montar(spo.menuCompacto);
      if (window.Plotly) {
        // o initGraficosEscala pergunta o modo dos diagramas por esta função do main.js
        window.spoGetDiagramSemanticTokens = () => ({ mode: body.dataset.visualDiagramTheme || "light", tokens: {} });
        spo.graficos.forEach((codigo) => new Function(codigo)());
        graficosProntos = true;
        desenharGraficos();
      } else {
        avisar("O Plotly não carregou (sem internet?): os gráficos de exemplo ficam vazios.");
      }
    })
    .catch((err) => {
      avisar(`Não foi possível ler o main.js do SPO (${err.message}). Abra o kit pelo SPO local `
        + "(http://127.0.0.1:5000/static/kit/mockup-spo-aa-plus.html): aberto do disco, a tela fica no "
        + "padrão SPO, sem os destaques, as opções nem o teclado das tabelas.");
    });
})();

/* Cartão "Pendências" (feat/pendencias-cartao): "Ver detalhes" abre o cartão flutuante (o mesmo .spo-busca-fundo
   da busca e de "O padrão SPO-AA+"): Esc e toque fora fecham, o Tab fica dentro, o foco volta ao botão. No SPO, o mesmo
   comportamento está no bloco [jean] initPendenciasDetalhe do main.js. */
(function kitPendencias() {
  const botao = document.getElementById("kit-pendencias-ver");
  const previa = document.querySelector(".kit-pendencias-previa");
  if (!botao || !previa) return;
  const fundo = document.createElement("div");
  fundo.className = "spo-busca-fundo";
  fundo.hidden = true;
  const caixa = previa.cloneNode(true);
  caixa.classList.remove("kit-pendencias-previa");
  caixa.setAttribute("role", "dialog");
  caixa.setAttribute("aria-modal", "true");
  caixa.removeAttribute("aria-label");
  const titulo = caixa.querySelector(".spo-busca-titulo");
  titulo.id = "kit-pendencias-dlg-titulo";
  titulo.tabIndex = -1;
  caixa.setAttribute("aria-labelledby", titulo.id);
  fundo.appendChild(caixa);
  document.body.appendChild(fundo);
  const focaveis = () => [...caixa.querySelectorAll("button, summary, [tabindex='0']")].filter((el) => el.offsetParent);
  function fechar() {
    fundo.hidden = true;
    document.body.classList.remove("spo-busca-aberta");
    botao.focus();
  }
  botao.addEventListener("click", () => {
    fundo.hidden = false;
    document.body.classList.add("spo-busca-aberta");
    titulo.focus();
  });
  caixa.querySelector(".spo-busca-rodape button").addEventListener("click", fechar);
  fundo.addEventListener("mousedown", (ev) => { if (ev.target === fundo) fechar(); });
  fundo.addEventListener("keydown", (ev) => {
    if (ev.key === "Escape") { ev.preventDefault(); fechar(); return; }
    if (ev.key !== "Tab") return;
    const f = focaveis(), primeiro = f[0], ultimo = f[f.length - 1];
    if (ev.shiftKey && (document.activeElement === primeiro || document.activeElement === titulo)) { ev.preventDefault(); ultimo.focus(); }
    else if (!ev.shiftKey && document.activeElement === ultimo) { ev.preventDefault(); primeiro.focus(); }
  });
})();

/* feat/kit-movel (2026-11-17): "Seu aparelho" (o que o navegador informa, para confirmar as medidas por busca do
   plano de readequação; nada é enviado) e o filtro SPO de exemplo abrindo de verdade, com o véu do celular (o mesmo
   do bloco [jean] initFiltroCelular do main.js, que o kit não carrega). */
(function kitAparelho() {
  const dl = document.getElementById("kit-aparelho-dados");
  if (!dl) return;
  const casa = (q) => window.matchMedia(q).matches;
  // tamanho DESENHADO (altura da linha com entrelinha 1), e não o calculado: o mínimo de fonte do Firefox e o
  // aumento de texto do Android mudam o desenho sem mudar o getComputedStyle
  const sondar = (tamanho) => {
    const s = document.createElement("span");
    s.textContent = "Xg";
    s.style.position = "absolute";
    s.style.visibility = "hidden";
    s.style.whiteSpace = "nowrap";
    s.style.fontFamily = "Arial, sans-serif";
    s.style.lineHeight = "1";
    s.style.fontSize = tamanho;
    document.body.appendChild(s);
    const px = s.getBoundingClientRect().height;
    s.remove();
    return Math.round(px);
  };
  function motor() {
    const ua = navigator.userAgent;
    const marcas = navigator.userAgentData && navigator.userAgentData.brands
      ? navigator.userAgentData.brands.map((b) => b.brand).filter((b) => !/not.?a.?brand/i.test(b)) : [];
    const nomes = (marcas.length > 1 ? marcas.filter((b) => b !== "Chromium") : marcas).join(", ");
    if (/Firefox\//.test(ua)) return `Firefox (Gecko)${/Android/.test(ua) ? ", Android" : ""}`;
    if (/(iPhone|iPad|iPod)/.test(ua) || (/Safari\//.test(ua) && !/Chrom(e|ium)\//.test(ua))) return `Safari ou navegador do iOS (WebKit)`;
    return `${nomes || "Chrome, Edge ou outro"} (Chromium)${/Android/.test(ua) ? ", Android" : ""}`;
  }
  function dados() {
    const vv = window.visualViewport;
    const dpr = window.devicePixelRatio || 1;
    const faixas = [];
    if (casa("(max-width: 600px), (max-height: 560px)")) faixas.push("menu só com o \"›\" e filtro como cartão");
    if (casa("(max-width: 820px)")) faixas.push("cabeçalho em duas colunas");
    if (casa("(max-width: 1200px)") && !casa("(max-width: 600px), (max-height: 560px)")) faixas.push("coluna de ícones do menu");
    if (!faixas.length) faixas.push("desktop (menu lateral completo)");
    return [
      ["Janela (área útil)", `${innerWidth} × ${innerHeight} px`],
      ["Área visível agora", vv ? `${Math.round(vv.width)} × ${Math.round(vv.height)} px${vv.scale !== 1 ? `, zoom de pinça ${vv.scale.toFixed(2)}×` : ""}` : "não informado"],
      ["Tela inteira", `${screen.width} × ${screen.height} px (${casa("(orientation: portrait)") ? "em pé" : "deitada"})`],
      ["Densidade", `${dpr}× (≈ ${Math.round(screen.width * dpr)} × ${Math.round(screen.height * dpr)} pixels físicos)`],
      ["Texto padrão do navegador", `${sondar("medium")} px (16 = sem aumento)`],
      ["Texto mínimo", `${sondar("8px")} px (8 = sem mínimo)`],
      ["Toque", `${casa("(pointer: coarse)") ? "dedo" : "mouse ou caneta"}${casa("(hover: hover)") ? ", passa o cursor" : ", sem cursor"}; ${navigator.maxTouchPoints || 0} ponto(s) de toque`],
      ["Navegador", motor()],
      ["Preferências do sistema", [
        casa("(prefers-color-scheme: dark)") ? "tema escuro" : "tema claro",
        casa("(prefers-reduced-motion: reduce)") ? "reduzir movimento" : null,
        casa("(prefers-contrast: more)") ? "mais contraste" : null,
        casa("(forced-colors: active)") ? "cores forçadas" : null,
      ].filter(Boolean).join(", ")],
      ["Faixa do SPO nesta janela", faixas.join("; ")],
    ];
  }
  function desenhar() {
    dl.replaceChildren(...dados().flatMap(([rotulo, valor]) => {
      const dt = document.createElement("dt"); dt.textContent = rotulo;
      const dd = document.createElement("dd"); dd.textContent = valor;
      return [dt, dd];
    }));
  }
  desenhar();
  let espera = 0;
  const depois = () => { clearTimeout(espera); espera = setTimeout(desenhar, 150); };
  window.addEventListener("resize", depois);
  if (window.visualViewport) window.visualViewport.addEventListener("resize", depois);
  const status = document.getElementById("kit-aparelho-status");
  document.getElementById("kit-aparelho-copiar").addEventListener("click", async () => {
    const texto = ["Seu aparelho (kit SPO-AA+)", ...dados().map(([r, v]) => `${r}: ${v}`), `Agente: ${navigator.userAgent}`].join("\n");
    try {
      await navigator.clipboard.writeText(texto);
      status.textContent = "Copiado. Cole na mensagem.";
    } catch (err) {
      const area = document.createElement("textarea");
      area.value = texto; document.body.appendChild(area); area.select();
      const ok = document.execCommand && document.execCommand("copy");
      area.remove();
      status.textContent = ok ? "Copiado. Cole na mensagem." : "Não foi possível copiar: mande um print.";
    }
  });
})();

(function kitFiltro() {
  const wrap = document.querySelector(".kit-filtro");
  if (!wrap) return;
  const botao = wrap.querySelector(".mf-display"), painel = wrap.querySelector(".mf-panel");
  const celular = window.matchMedia("(max-width: 600px), (max-height: 560px)");
  let veu = null;
  function fechar() {
    painel.classList.remove("open");
    botao.setAttribute("aria-expanded", "false");
    if (veu) { veu.remove(); veu = null; }
  }
  function abrir() {
    painel.classList.add("open");
    botao.setAttribute("aria-expanded", "true");
    if (celular.matches && !veu) {
      veu = document.createElement("div");
      veu.className = "spo-filtro-veu";
      veu.setAttribute("aria-hidden", "true");
      veu.addEventListener("click", (ev) => { ev.preventDefault(); ev.stopPropagation(); fechar(); });
      document.body.appendChild(veu);
    }
    painel.querySelector(".mf-search").focus();
  }
  botao.addEventListener("click", () => (painel.classList.contains("open") ? fechar() : abrir()));
  painel.querySelectorAll(".mf-btn").forEach((b) => b.addEventListener("click", () => { fechar(); botao.focus(); }));
  document.addEventListener("click", (ev) => { if (painel.classList.contains("open") && !wrap.contains(ev.target) && ev.target !== veu) fechar(); });
  painel.addEventListener("keydown", (ev) => { if (ev.key === "Escape") { fechar(); botao.focus(); } });
})();
