/* ==========================================================================
   [jean] Ponte de preferências do SPO para o PTA 2027 (feat/ponte-pta2027, 2026-11-14)
   Recebe do SPO (mensagem "spo-preferencias", mesma origem) as escolhas do
   Personalizar e grava no <body> os mesmos atributos data-spo-* e data-visual-*;
   o spo-preferencias.css aplica. Também devolve ao SPO os atalhos do teclado
   (Ctrl+K, "/", "?", Alt+M): com o foco dentro do mockup, as teclas chegam aqui
   e não à página do SPO. Fora do modo embutido, não faz nada.
   ========================================================================== */
(function spoPontePreferencias() {
  if (!document.body || !document.body.classList.contains("embedded-mode") || !window.parent || window.parent === window) return;
  const ORIGEM = window.location.origin;
  const ATRIBUTOS = ["spoFontUi", "spoFontData", "spoTableText", "spoLineHeight", "spoReading", "spoCvd",
    "spoFocus", "spoLinks", "spoBrightness", "spoMotion", "spoKeys", "visualContrast", "visualDensity"];
  const TOKENS = ["--spo-focus-ring", "--spo-focus-halo", "--spo-accent-text", "--spo-accent-graphic"];

  function aplicar(dados) {
    const body = document.body;
    body.classList.add("spo-ponte");
    const prefs = (dados && dados.prefs) || {};
    ATRIBUTOS.forEach((chave) => {
      if (typeof prefs[chave] === "string" && /^[a-z0-9-]{1,20}$/.test(prefs[chave])) body.dataset[chave] = prefs[chave];
      else delete body.dataset[chave];
    });
    const tokens = (dados && dados.tokens) || {};
    TOKENS.forEach((nome) => {
      const valor = tokens[nome];
      if (typeof valor === "string" && valor && valor.length < 60 && !/[;{}]/.test(valor)) document.documentElement.style.setProperty(nome, valor);
      else document.documentElement.style.removeProperty(nome);
    });
  }

  window.addEventListener("message", (ev) => {
    if (ev.origin !== ORIGEM || ev.source !== window.parent) return;
    if (ev.data && ev.data.type === "spo-preferencias") aplicar(ev.data);
  });

  // atalhos do SPO com o foco dentro do mockup: o SPO decide (e respeita "atalhos de uma tecla" desligados)
  document.addEventListener("keydown", (ev) => {
    if (ev.defaultPrevented) return;
    const campo = ev.target && ev.target.closest && ev.target.closest("input, textarea, select, [contenteditable]");
    let acao = "";
    if ((ev.ctrlKey || ev.metaKey) && !ev.altKey && (ev.key === "k" || ev.key === "K")) acao = "busca";
    else if (ev.altKey && !ev.ctrlKey && !ev.metaKey && (ev.code === "KeyM" || ev.key === "m" || ev.key === "M")) acao = "menu";
    else if (!campo && !ev.ctrlKey && !ev.metaKey && !ev.altKey && ev.key === "/") acao = "busca-uma-tecla";
    else if (!campo && !ev.ctrlKey && !ev.metaKey && !ev.altKey && ev.key === "?") acao = "ajuda-uma-tecla";
    if (!acao) return;
    if (acao.endsWith("uma-tecla") && document.body.dataset.spoKeys === "off") return;
    ev.preventDefault();
    window.parent.postMessage({ type: "spo-atalho", acao }, ORIGEM);
  });

  // botão "×" de remover linha (feat/mockup-pta2027-aa-plus): o leitor de tela lia só "×"
  function nomearBotoes() {
    document.querySelectorAll(".remove-row:not([aria-label])").forEach((b) => {
      b.setAttribute("aria-label", "Remover linha");
      b.title = "Remover linha";
    });
    // [jean] o "×" que fecha a Visão processual, a Visão estratégica e a lista de eixos (fix/detalhe-abaixo-cartao)
    document.querySelectorAll(".close-expansion:not([aria-label])").forEach((b) => {
      b.setAttribute("aria-label", "Fechar");
      b.title = "Fechar";
    });
  }
  nomearBotoes();
  new MutationObserver(nomearBotoes).observe(document.body, { childList: true, subtree: true });

  // "Anterior" na 1ª etapa da jornada (feat/nome-pta, pedido do dev. Jean): o mockup desativava o
  // botão (disabled = state.step === 0) e não havia como voltar à tela inicial ("Da estratégia à
  // entrega pública") pela jornada. Agora, na 1ª etapa, ele volta para lá (returnToStrategicChain,
  // do próprio mockup). O state e a função são globais do pta-2027.js.
  const anterior = document.getElementById("previous-step");
  const naPrimeira = () => typeof state !== "undefined" && state.step === 0;
  if (anterior) {
    const ajustar = () => {
      if (anterior.disabled && naPrimeira()) anterior.disabled = false;
      if (naPrimeira()) anterior.setAttribute("aria-label", "Anterior: voltar à tela inicial da programação");
      else anterior.removeAttribute("aria-label");
    };
    new MutationObserver(ajustar).observe(anterior, { attributes: true, attributeFilter: ["disabled"] });
    new MutationObserver(ajustar).observe(document.body, { attributes: true, attributeFilter: ["data-pta2027-step"] });
    ajustar();
    anterior.addEventListener("click", (ev) => {
      if (!naPrimeira() || typeof returnToStrategicChain !== "function") return;
      ev.stopImmediatePropagation();
      returnToStrategicChain();
      window.parent.postMessage({ type: "pta2027-scroll-top" }, ORIGEM);
    }, true);
  }

  // [jean] Movimento reduzido (fix/movimento-reduzido, 2026-11-15): o mockup chama
  // scrollIntoView({ behavior: "smooth" }) em 4 pontos, e o ajuste do main.js não vale dentro do
  // iframe. Com "Reduzir sempre" (data-spo-motion, recebido do SPO) ou o sistema pedindo, a rolagem
  // suave vira imediata; sem a preferência, nada muda.
  (function movimentoReduzido() {
    const consulta = window.matchMedia ? window.matchMedia("(prefers-reduced-motion: reduce)") : { matches: false };
    // a chegada esmaece (0,35 → 1 em 0,22 s), como no SPO, só quando algo de fato se moveu;
    // o mockup rola a página do SPO (o iframe não tem rolagem própria), por isso se mede o alvo
    const esmaecer = (el) => {
      if (!el || typeof el.animate !== "function") return;
      try { el.animate([{ opacity: 0.35 }, { opacity: 1 }], { duration: 220, easing: "ease-out" }); } catch (err) { /* sem Web Animations */ }
    };
    const imediata = (fn) => function rolagemRespeitandoMovimento(arg) {
      const reduzir = consulta.matches || document.body.dataset.spoMotion === "reduce";
      if (reduzir && arg && typeof arg === "object" && arg.behavior === "smooth") {
        const alvo = this instanceof Element && this !== document.documentElement && this !== document.body ? this : document.body;
        const antes = alvo.getBoundingClientRect().top + (window.parent !== window ? window.frameElement?.getBoundingClientRect().top || 0 : 0);
        const resultado = fn.call(this, { ...arg, behavior: "instant" });
        const depois = alvo.getBoundingClientRect().top + (window.parent !== window ? window.frameElement?.getBoundingClientRect().top || 0 : 0);
        if (Math.abs(depois - antes) > 2) esmaecer(alvo);
        return resultado;
      }
      return fn.apply(this, arguments);
    };
    ["scrollIntoView", "scrollTo", "scroll", "scrollBy"].forEach((nome) => {
      if (typeof Element.prototype[nome] === "function") Element.prototype[nome] = imediata(Element.prototype[nome]);
      if (nome !== "scrollIntoView" && typeof window[nome] === "function") window[nome] = imediata(window[nome]);
    });
  })();

  // [jean] Rolagem ao abrir um detalhamento (eixos da macropolítica, metas do pilar), com
  // "Reduzir sempre": nenhuma, se o começo do painel (título + 1ª linha, ~170 px) já está na tela;
  // senão, só o bastante para mostrá-lo, de uma vez e sem esmaecer. Rolar o painel inteiro para a
  // tela (block: "nearest"/"center") movia a página até 275 px em tela baixa (paisagem) e
  // esmaecia o painel a cada clique (a "cintilância" do grupo C, cuja lista é maior que a tela).
  // O mockup rola a página do SPO (o iframe não tem rolagem própria).
  // [jean] fix/rolagem-automatica: rola a página do SPO até `descer` px abaixo de onde está. Logo depois de abrir um
  // painel, a página ainda não cresceu (o SPO aumenta o iframe um instante depois): perto do fim, a rolagem começava
  // sem espaço e parava em 0. Espera a página crescer (até ~0,5 s) e então rola até o ponto, ou até onde der.
  function rolarPaginaDoSpo(descer, suave) {
    const pai = window.parent !== window ? window.parent : window;
    const alvo = pai.scrollY + descer;
    let quadros = 0;
    const rolar = () => {
      const maximo = pai.document.documentElement.scrollHeight - pai.innerHeight;
      if (maximo >= alvo - 1 || quadros++ > 30) { pai.scrollTo({ top: Math.min(alvo, maximo), behavior: suave ? "smooth" : "instant" }); return; }
      pai.requestAnimationFrame(rolar);
    };
    rolar();
  }
  function rolarSoOComeco(painel, suave) {
    const pai = window.parent !== window ? window.parent : window;
    const topoFrame = pai !== window && window.frameElement ? window.frameElement.getBoundingClientRect().top : 0;
    const falta = topoFrame + painel.getBoundingClientRect().top + 170 - pai.innerHeight;
    if (falta <= 0) return;
    rolarPaginaDoSpo(falta, suave);
  }
  // [jean] Tela de celular, tablet deitado ou janela dividida (fix/telas-estreitas-rolagem-janelas e
  // fix/rolagem-sem-retrato, pedidos do dev. Jean): a janela do SPO com até 1200 px de largura (as
  // faixas compacta e ampla da densidade automática) E até 1000 px de altura; o celular (até 720 px
  // de largura) entra sempre, qualquer que seja a altura; o preset "Pressão operacional" do
  // Contrato Visual (que realça o modo móvel e split) também. É onde a rolagem automática ajuda: a
  // tela é estreita e baixa, e o painel aberto costuma ficar fora dela. Ficam de fora a tela larga
  // e as telas estreitas mas altas (monitor em retrato, ~1060 × 1900; tablet em pé), onde o painel
  // abre à vista.
  function telaEstreita() {
    try {
      const pai = window.parent !== window ? window.parent : window;
      if (pai.document.body && pai.document.body.dataset.visualContractPreset === "mobile") return true;
      const casa = (q) => (pai.matchMedia ? pai.matchMedia(q).matches : false);
      return casa("(max-width: 720px)") || casa("(max-width: 1200px) and (max-height: 1000px)");
    } catch (err) { return false; }
  }
  // O mockup chama painel.scrollIntoView(...) dentro da função que abre; durante a chamada, o
  // scrollIntoView daquele painel é trocado (o pta-2027.js não muda):
  // - "Reduzir sempre" (ou o sistema pedindo): rolarSoOComeco, de uma vez;
  // - "Automático" em tela estreita: rolagem suave até o cartão tocado ficar no alto, com o painel embaixo
  //   (rolarAteOCartao; a original do mockup chegava a subir a página);
  // - "Automático" em tela larga: nenhuma rolagem, a não ser que o começo do painel fique fora da
  //   tela; então rolarSoOComeco, suave (para o clique não parecer sem efeito).
  // [jean] fix/rolagem-automatica (2026-11-17; achado do dev. Jean): no "Automático" em tela estreita, a rolagem
  // original do mockup (painel.scrollIntoView({ block: "center" })) com um painel mais alto que a tela, dentro do
  // iframe, levava a página PARA CIMA (762 px no iPhone com a lista de eixos de 1839 px; 148 px a 1100 × 650) e o
  // cartão tocado saía da tela. Agora a rolagem suave só desce, e só até o cartão tocado ficar logo abaixo do
  // cabeçalho do SPO, com o painel aberto embaixo dele (o painel entra logo depois da linha do cartão, #173); sem
  // a linha do cartão (metas do pilar), até o começo do painel ficar a 1/3 da altura da tela. Já à vista: não rola.
  function rolarAteOCartao(painel) {
    const pai = window.parent !== window ? window.parent : window;
    const topoFrame = pai !== window && window.frameElement ? window.frameElement.getBoundingClientRect().top : 0;
    let cabecalho = 0;
    try { cabecalho = parseFloat(pai.getComputedStyle(pai.document.documentElement).getPropertyValue("--spo-topbar-height")) || 0; } catch (err) { /* outra origem */ }
    const cartao = painel.classList.contains("spo-na-grade") && painel.previousElementSibling;
    const descer = cartao
      ? topoFrame + cartao.getBoundingClientRect().top - cabecalho - 12
      : topoFrame + painel.getBoundingClientRect().top - Math.round(pai.innerHeight / 3);
    if (descer <= 4) return;
    rolarPaginaDoSpo(descer, true);
  }
  function comRolagemContida(painel, fn) {
    return function abrirComRolagemContida() {
      const reduzir = (window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches) || document.body.dataset.spoMotion === "reduce";
      const estreita = !reduzir && telaEstreita();
      painel.scrollIntoView = estreita ? () => rolarAteOCartao(painel) : () => rolarSoOComeco(painel, !reduzir);
      try { return fn.apply(this, arguments); } finally { delete painel.scrollIntoView; }
    };
  }
  const metas = document.getElementById("pillar-goals");
  if (metas && typeof window.expandPillarGoals === "function") window.expandPillarGoals = comRolagemContida(metas, window.expandPillarGoals);

  // [jean] Eixos junto da macropolítica (fix/movimento-reduzido, pedido do dev. Jean, 2026-11-15):
  // com os grupos de macropolíticas um embaixo do outro (até 1180 px), a lista de eixos abria no
  // fim do último grupo, até ~1000 px abaixo do cartão clicado, e a página saltava até lá
  // (block: "center"). Agora ela abre logo abaixo do grupo do cartão, como as metas dos pilares, e
  // com "Reduzir sempre" só rola até o começo da lista (rolarSoOComeco). O lugar vale nos dois
  // modos de movimento (no "Automático", a rolagem suave original percorreria ~1000 px). O
  // pta-2027.js não muda: expandMacro e renderMacros são globais e são envolvidas aqui; antes de
  // redesenhar os grupos, a caixa volta ao lugar original (o redesenho apagaria a caixa).
  // [jean] Detalhe logo abaixo da LINHA do cartão (fix/detalhe-abaixo-cartao, 2026-11-17; ponto 3 do plano de
  // readequação para celular e navegadores): abaixo do grupo ainda ficava longe no celular (até ~480 px abaixo
  // do 1º cartão do grupo C, com os outros cartões no meio). Agora o detalhe entra na própria grade, depois do
  // último cartão da mesma linha, ocupando a linha inteira (.spo-na-grade, grid-column: 1 / -1).
  function ultimoDaLinha(cartao) {
    const topo = Math.round(cartao.getBoundingClientRect().top);
    let ultimo = cartao;
    for (let el = cartao.nextElementSibling; el && el.classList.contains(cartao.classList[0]); el = el.nextElementSibling) {
      if (Math.abs(Math.round(el.getBoundingClientRect().top) - topo) > 2) break;
      ultimo = el;
    }
    return ultimo;
  }

  (function eixosJuntoDaMacro() {
    const caixa = document.getElementById("policy-expansion");
    if (!caixa || typeof window.expandMacro !== "function" || typeof window.renderMacros !== "function") return;
    const marcador = document.createComment(" lugar original de #policy-expansion ");
    caixa.parentNode.insertBefore(marcador, caixa);
    const devolver = () => { caixa.classList.remove("spo-na-grade"); if (caixa.previousSibling !== marcador) marcador.after(caixa); };
    const empilhados = (cadeia) => getComputedStyle(cadeia).gridTemplateColumns.trim().split(/\s+/).length === 1;

    const abrirOriginal = comRolagemContida(caixa, window.expandMacro);
    window.expandMacro = function expandirJuntoDoCartao(index) {
      const cartao = document.querySelector(`.macro-card[data-macro="${index}"]`);
      const grupo = cartao && cartao.closest(".process-lane");
      const cadeia = grupo && grupo.parentElement;
      devolver(); // antes de medir as linhas: a caixa no meio da grade empurraria os cartões
      if (grupo && cadeia && empilhados(cadeia)) {
        ultimoDaLinha(cartao).after(caixa);
        caixa.classList.add("spo-na-grade");
      }
      return abrirOriginal.apply(this, arguments);
    };

    const redesenharOriginal = window.renderMacros;
    window.renderMacros = function redesenharComACaixaNoLugar() {
      devolver();
      return redesenharOriginal.apply(this, arguments);
    };
  })();

  // [jean] Visão processual (fix/detalhe-abaixo-cartao): o detalhe do processo abria depois da grade dos três
  // cartões; no celular (1 coluna), até ~270 px abaixo do cartão tocado. Agora entra logo depois da linha dele.
  if (typeof window.selectProcessDetail === "function") {
    const detalheOriginal = window.selectProcessDetail;
    window.selectProcessDetail = function detalheJuntoDoCartao(key) {
      const cartao = document.querySelector(`.process-summary-card[data-process="${key}"]`);
      const painel = document.getElementById("process-detail-panel");
      const grade = cartao && cartao.parentElement;
      if (cartao && painel && grade) {
        if (painel.parentElement === grade) grade.after(painel); // lugar original: logo depois da grade
        painel.classList.remove("spo-na-grade");
        ultimoDaLinha(cartao).after(painel);
        painel.classList.add("spo-na-grade");
      }
      return detalheOriginal.apply(this, arguments);
    };
  }

  // [jean] Cartões que abrem um detalhamento (pilares, macropolíticas, processos da Visão
  // processual): o selecionado só se via pela cor; agora o leitor de tela também ouve
  // "expandido/recolhido" (aria-expanded, pela classe .active que o próprio mockup põe).
  const sincronizarExpandidos = () => {
    document.querySelectorAll(".pillar, .macro-card, .process-summary-card").forEach((b) => {
      const aberto = b.classList.contains("active") ? "true" : "false";
      if (b.getAttribute("aria-expanded") !== aberto) b.setAttribute("aria-expanded", aberto);
    });
  };
  document.addEventListener("click", () => setTimeout(sincronizarExpandidos, 0));
  new MutationObserver(sincronizarExpandidos).observe(document.body, { childList: true, subtree: true });
  sincronizarExpandidos();

  // pede as preferências assim que carrega (o SPO também manda a cada mudança)
  window.parent.postMessage({ type: "spo-preferencias-pedido" }, ORIGEM);
})();

// [jean] feat/siglas-explicadas (2026-11-18): o mesmo initSiglas do main.js do SPO (cópia exata; o
// tests/test_kit_mockup.py reprova se divergir): siglas com balão também dentro do mockup.
(function initSiglas() {
  let seq = 0;
  const abertas = new Set();
  const fechar = (w) => {
    const b = w.querySelector(".spo-sigla-botao"), balao = w.querySelector(".spo-sigla-balao");
    if (!b || !balao) return;
    balao.hidden = true;
    b.setAttribute("aria-expanded", "false");
    delete w.dataset.fixo;
    abertas.delete(w);
  };
  const abrir = (w, fixo) => {
    const b = w.querySelector(".spo-sigla-botao"), balao = w.querySelector(".spo-sigla-balao");
    if (!b || !balao) return;
    abertas.forEach((o) => { if (o !== w) fechar(o); });
    balao.hidden = false;
    b.setAttribute("aria-expanded", "true");
    if (fixo) w.dataset.fixo = "1";
    abertas.add(w);
    // o balão não sai da tela pela direita nem pela esquerda
    w.style.setProperty("--spo-sigla-desloca", "0px");
    const r = balao.getBoundingClientRect(), margem = 8;
    let desloca = 0;
    if (r.right > innerWidth - margem) desloca = innerWidth - margem - r.right;
    if (r.left + desloca < margem) desloca = margem - r.left;
    // fix/sigla-balao-zoom (lote 8.1): com a "Fonte maior" o conteúdo tem zoom (1,125 a 1,5) e o deslocamento, medido
    // em pixels da tela, era ampliado por ele (a 360 px com "Maior", o balão começava em -29 px). Aplica, mede quanto
    // o balão andou de fato e corrige pela proporção: vale com ou sem zoom, em qualquer navegador.
    if (desloca) {
      w.style.setProperty("--spo-sigla-desloca", `${desloca}px`);
      const andou = balao.getBoundingClientRect().left - r.left;
      if (Math.abs(andou) > 0.5) desloca *= desloca / andou;
    }
    w.style.setProperty("--spo-sigla-desloca", `${Math.round(desloca)}px`);
  };
  const montar = (abbr) => {
    if (abbr.dataset.siglaPronta) return;
    const significado = (abbr.getAttribute("title") || "").trim();
    const sigla = abbr.textContent.trim();
    if (!significado || !sigla) return;
    abbr.dataset.siglaPronta = "1";
    abbr.removeAttribute("title"); // sem a dica nativa repetida
    const id = `spo-sigla-${++seq}`;
    const balao = document.createElement("span");
    balao.className = "spo-sigla-balao";
    balao.id = id;
    balao.setAttribute("role", "tooltip");
    balao.hidden = true;
    balao.textContent = significado;
    const botao = document.createElement("button");
    botao.type = "button";
    botao.className = "spo-sigla-botao";
    botao.setAttribute("aria-expanded", "false");
    botao.setAttribute("aria-describedby", id);
    const dentroDeControle = abbr.parentElement && abbr.parentElement.closest("a, button, [role='button'], summary, label");
    let w;
    if (dentroDeControle) {
      // dentro de um link: o botão "ⓘ" vai logo depois do link
      botao.classList.add("spo-sigla-info");
      botao.setAttribute("aria-label", `O que é ${sigla}?`);
      const icone = document.createElement("i");
      icone.className = "bi bi-info-circle";
      icone.setAttribute("aria-hidden", "true");
      botao.appendChild(icone);
      w = document.createElement("span");
      w.className = "spo-sigla";
      w.append(botao, balao);
      dentroDeControle.insertAdjacentElement("afterend", w);
    } else {
      botao.textContent = sigla;
      abbr.textContent = "";
      abbr.classList.add("spo-sigla");
      abbr.append(botao, balao);
      w = abbr;
    }
    botao.addEventListener("click", (ev) => {
      ev.preventDefault();
      if (w.dataset.fixo) fechar(w); else abrir(w, true);
    });
    w.addEventListener("pointerenter", (ev) => { if (ev.pointerType === "mouse" && !abertas.has(w)) abrir(w, false); });
    w.addEventListener("pointerleave", (ev) => { if (ev.pointerType === "mouse" && !w.dataset.fixo) fechar(w); });
    botao.addEventListener("focus", () => { if (botao.matches(":focus-visible") && !abertas.has(w)) abrir(w, false); });
    w.addEventListener("focusout", (ev) => { if (!w.contains(ev.relatedTarget) && !w.dataset.fixo) fechar(w); });
  };
  const varrer = (raiz) => raiz.querySelectorAll("abbr[title]").forEach(montar);
  document.addEventListener("keydown", (ev) => {
    if (ev.key !== "Escape" || !abertas.size) return;
    abertas.forEach(fechar);
  });
  document.addEventListener("click", (ev) => {
    abertas.forEach((w) => { if (!w.contains(ev.target)) fechar(w); });
  });
  // tocar ou clicar numa moldura (iframe, como a do mockup PTA/OpR) não chega aqui: a página perde o foco, e fecha
  window.addEventListener("blur", () => abertas.forEach(fechar));
  varrer(document);
  // telas carregadas depois (o conteúdo do SPO troca sem recarregar a página)
  let espera = 0;
  new MutationObserver(() => { clearTimeout(espera); espera = setTimeout(() => varrer(document), 60); })
    .observe(document.body, { childList: true, subtree: true });
})();
