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

  // pede as preferências assim que carrega (o SPO também manda a cada mudança)
  window.parent.postMessage({ type: "spo-preferencias-pedido" }, ORIGEM);
})();
