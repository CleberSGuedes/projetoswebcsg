// [jean] feat/kit-mockup (2026-11-15) — Kit de mockup SPO-AA+ (ver o comentário do mockup-spo-aa-plus.html).
//
// Nada de paleta, opções ou teclado é copiado para cá: o kit lê do main.js do SPO
//  - ACCENT_COLORS (os 8 destaques), DEFAULT_ACCENT, COMMON_VISUAL_OPTIONS/DEFAULTS;
//  - OPTIONS e DEFAULTS do initFontesPersonalizar (as opções do Personalizar);
//  - initNavegacaoTabelas e initTecladoPaginacao (navegação pelo teclado nas tabelas);
//  - a regra da oferta "Leitura confortável" do initSugestaoAtkinson (ORDEM, ALVO, SEM_VALOR, NOMES, BOTAO).
// Ao abrir, o kit usa as preferências vigentes no SPO deste navegador (como qualquer tela do SPO);
// "Padrão SPO" volta ao padrão. Nada é gravado.
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
    return {
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
    };
  }

  // ---------------------------------------------------------------- preferências
  const TEMAS = { light: "Claro", dark: "Escuro", auto: "Automático (segue o sistema)" };
  // ordem e rótulos dos grupos no painel (os valores e os nomes das opções vêm do main.js)
  const GRUPOS_FONTES = [
    ["ui", "Fonte da interface"], ["data", "Fonte de tabelas e dados"], ["size", "Tamanho do texto"],
    ["table", "Densidade do texto"], ["line", "Espaçamento entre linhas"], ["reading", "Leitura"],
    ["cvd", "Cores dos gráficos"], ["focus", "Realce do foco"], ["links", "Links"],
    ["brightness", "Brilho do tema claro"], ["motion", "Movimento"],
  ];
  const GRUPOS_VISUAL = [["density", "Densidade visual"], ["contrast", "Contraste"], ["cards", "Estilo dos cartões"], ["diagrams", "Diagramas"]];

  let spo = null;
  let estado = null;

  function padrao() {
    return { tema: "light", destaque: spo.destaquePadrao, ...spo.visualPadrao, ...spo.fontesPadrao };
  }

  function lerJson(chave) {
    try { return JSON.parse(localStorage.getItem(chave) || "{}") || {}; } catch (_) { return {}; }
  }

  // as escolhas gravadas pelo SPO neste navegador (mesma origem); só leitura
  function minhas() {
    const p = padrao();
    let tema = null;
    let destaque = null;
    try { tema = localStorage.getItem("app-theme"); destaque = localStorage.getItem("app-accent"); } catch (_) { /* bloqueado */ }
    if (TEMAS[tema]) p.tema = tema;
    if (destaque === "spo") destaque = "green";
    if (spo.destaques[destaque]) p.destaque = destaque;
    const visual = lerJson("spo-common-visual-preferences");
    GRUPOS_VISUAL.forEach(([k]) => { if (spo.visual[k][visual[k]]) p[k] = visual[k]; });
    const fontes = lerJson("spo-font-preferences");
    Object.keys(spo.fontes).forEach((k) => { if (spo.fontes[k][fontes[k]]) p[k] = fontes[k]; });
    return p;
  }

  function temaEscuro(tema) {
    if (tema === "auto") return !!(window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches);
    return tema === "dark";
  }

  // como o applyAccentColor do main.js
  function aplicarDestaque(chave) {
    const paleta = spo.destaques[chave] || spo.destaques[spo.destaquePadrao];
    const alvos = [document.documentElement, body];
    const pôr = (nome, valor) => alvos.forEach((a) => a.style.setProperty(nome, valor));
    Object.entries(paleta.vars).forEach(([n, v]) => pôr(n, v));
    pôr("--accent", "var(--color-500)");
    pôr("--accent-strong", "var(--color-400)");
    pôr("--lab-accent", "var(--color-400)");
    if (body.classList.contains("theme-dark")) Object.entries(paleta.dark).forEach(([n, v]) => pôr(n, v));
    body.dataset.accentPreference = chave;
  }

  // como o login-theme.js: densidade "auto" pela largura da janela
  function densidadeEmUso(pref) {
    if (pref !== "auto") return pref;
    const largura = window.innerWidth || 1280;
    return largura <= 720 ? "spacious" : (largura <= 1200 ? "compact" : "comfortable");
  }

  function aplicar() {
    body.classList.toggle("theme-dark", temaEscuro(estado.tema));
    body.dataset.themePreference = estado.tema;
    aplicarDestaque(estado.destaque);
    body.dataset.visualDensityPref = estado.density;
    body.dataset.visualDensity = densidadeEmUso(estado.density);
    body.dataset.visualContrast = estado.contrast;
    body.dataset.visualCards = estado.cards;
    // como o getResolvedCommonDiagramMode do main.js (sem as cores forçadas do Windows)
    const escuro = body.classList.contains("theme-dark");
    const maisContraste = !!(window.matchMedia && window.matchMedia("(prefers-contrast: more)").matches);
    body.dataset.visualDiagrams = estado.diagrams;
    body.dataset.visualDiagramTheme = estado.diagrams === "high" || (estado.diagrams === "auto" && maisContraste)
      ? (escuro ? "highDark" : "highLight")
      : (estado.diagrams === "auto" ? (escuro ? "dark" : "light") : estado.diagrams);
    Object.keys(spo.atributos).forEach((k) => { if (estado[k] !== undefined) body.dataset[spo.atributos[k]] = estado[k]; });
    opcoes.querySelectorAll("select[data-kit-pref]").forEach((s) => { s.value = estado[s.dataset.kitPref]; });
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
    opcoes.querySelector('select[data-kit-pref="ui"]').closest(".field").after(caixa);
    caixa.addEventListener("click", (ev) => {
      const b = ev.target.closest("[data-sugestao]");
      if (!b) return;
      if (b.dataset.sugestao === "aplicar") {
        faltandoAtkinson().forEach((k) => { estado[k] = k === "data" ? "atkinson-mono" : s.alvo[k]; });
        aplicar();
      }
      opcoes.querySelector('select[data-kit-pref="ui"]').focus();
      removerOferta();
    });
  }

  function campo(chave, rotulo, valores) {
    const label = document.createElement("label");
    label.className = "field";
    const span = document.createElement("span");
    span.textContent = rotulo;
    const select = document.createElement("select");
    select.dataset.kitPref = chave;
    Object.entries(valores).forEach(([v, nome]) => select.add(new Option(nome, v)));
    select.addEventListener("change", () => {
      estado[chave] = select.value;
      aplicar();
      if (chave === "ui") { if (select.value === "atkinson") mostrarOferta(); else removerOferta(); }
    });
    label.append(span, select);
    opcoes.appendChild(label);
  }

  function montarPainel() {
    campo("tema", "Modo de aparência", TEMAS);
    const destaques = {};
    Object.entries(spo.destaques).forEach(([k, d]) => { destaques[k] = d.label || k; });
    campo("destaque", "Cor de destaque", destaques);
    GRUPOS_VISUAL.forEach(([k, r]) => campo(k, r, spo.visual[k]));
    GRUPOS_FONTES.forEach(([k, r]) => { if (spo.fontes[k]) campo(k, r, spo.fontes[k]); });
    document.getElementById("kit-padrao").addEventListener("click", () => { removerOferta(); estado = padrao(); aplicar(); });
    document.getElementById("kit-minhas").addEventListener("click", () => { removerOferta(); estado = minhas(); aplicar(); });
    window.addEventListener("resize", () => { body.dataset.visualDensity = densidadeEmUso(estado.density); });
  }

  const recolher = document.getElementById("kit-recolher");
  recolher.addEventListener("click", () => {
    const aberto = recolher.getAttribute("aria-expanded") === "true";
    recolher.setAttribute("aria-expanded", String(!aberto));
    recolher.textContent = aberto ? "Mostrar" : "Recolher";
    opcoes.hidden = aberto;
  });

  // ---------------------------------------------------------------- conteúdo de exemplo
  // cartões dinâmicos: um selecionado por vez
  document.querySelectorAll("[data-kit-cartoes]").forEach((grupo) => {
    grupo.addEventListener("click", (ev) => {
      const cartao = ev.target.closest(".flow-node");
      if (!cartao) return;
      grupo.querySelectorAll(".flow-node").forEach((c) => {
        c.setAttribute("aria-pressed", String(c === cartao));
        c.classList.toggle("is-selected", c === cartao);
      });
    });
    const primeiro = grupo.querySelector('.flow-node[aria-pressed="true"]');
    if (primeiro) primeiro.classList.add("is-selected");
  });

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
        ? `<td class="num ${l[c] < 0 ? "neg" : ""}">${moeda(l[c])}</td>`
        : `<td>${esc(l[c])}</td>`)).join("")}</tr>`).join("")
      : `<tr><td colspan="${COLUNAS.length}" class="muted">Nenhum resultado com estes filtros.</td></tr>`;
    document.getElementById("kit-total").textContent = moeda(dados.reduce((s, l) => s + l.valor, 0));
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
      estado = minhas(); // as preferências vigentes no SPO deste navegador (sem nenhuma, é o padrão SPO)
      montarPainel();
      aplicar();
      spo.teclado.forEach((codigo) => new Function(codigo)());
    })
    .catch((err) => {
      avisar(`Não foi possível ler o main.js do SPO (${err.message}). Abra o kit pelo SPO local `
        + "(http://127.0.0.1:5000/static/kit/mockup-spo-aa-plus.html): aberto do disco, a tela fica no "
        + "padrão SPO, sem os destaques, as opções nem o teclado das tabelas.");
    });
})();
