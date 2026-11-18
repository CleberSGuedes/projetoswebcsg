// Bateria SPO-AA+ — retrato dos estilos de uma tela, para a comparação antes × depois (comparar.mjs).
// Roda DENTRO da página (passada ao page.evaluate): sem dependências de fora.

// Para cada elemento do <body> (menos script, style, link, template, noscript), guarda as propriedades de estilo
// calculado que mudam a aparência, a caixa (posição e tamanho) e, quando existem, os ::before/::after.
// O elemento é identificado pelo caminho de tags desde o <body> (ex.: "div[2]>main[1]>section[3]"); os scripts não
// contam na posição, então acrescentar um <script> ou um <link> não muda o caminho dos outros elementos.
// O texto não entra: mudar dados não é mudar estilo (e o texto que cresce aparece na caixa).
export function retratarEstilos() {
  const PROPS = [
    "display", "position", "visibility", "opacity", "z-index", "float", "box-sizing",
    "top", "right", "bottom", "left",
    "margin-top", "margin-right", "margin-bottom", "margin-left",
    "padding-top", "padding-right", "padding-bottom", "padding-left",
    "border-top-width", "border-right-width", "border-bottom-width", "border-left-width",
    "border-top-style", "border-right-style", "border-bottom-style", "border-left-style",
    "border-top-color", "border-right-color", "border-bottom-color", "border-left-color",
    "border-top-left-radius", "border-top-right-radius", "border-bottom-right-radius", "border-bottom-left-radius",
    "outline-style", "outline-width", "outline-color", "outline-offset", "box-shadow",
    "color", "background-color", "background-image", "background-size", "background-position",
    "font-family", "font-size", "font-weight", "font-style", "font-variant-numeric", "line-height", "letter-spacing",
    "word-spacing", "text-align", "text-transform", "text-decoration-line", "text-decoration-color",
    "text-underline-offset", "text-indent", "text-overflow", "white-space", "word-break", "overflow-wrap", "hyphens",
    "vertical-align", "list-style-type",
    "overflow-x", "overflow-y", "flex-direction", "flex-wrap", "flex-grow", "flex-shrink", "flex-basis",
    "justify-content", "align-items", "align-content", "align-self", "order", "row-gap", "column-gap",
    "grid-template-columns", "grid-template-rows", "grid-column-start", "grid-column-end", "grid-row-start", "grid-row-end",
    "min-width", "max-width", "min-height", "max-height",
    "transform", "filter", "backdrop-filter", "mix-blend-mode", "clip-path", "cursor", "pointer-events",
    "fill", "stroke", "stroke-width", "accent-color", "caret-color", "table-layout", "border-collapse", "border-spacing",
    "zoom", "content-visibility", "scrollbar-width", "scrollbar-color",
  ];
  const PSEUDO = ["content", "display", "position", "color", "background-color", "background-image", "width", "height",
    "top", "right", "bottom", "left", "border-top-width", "border-top-color", "border-top-left-radius", "font-size",
    "font-family", "transform", "opacity"];
  const PULAR = new Set(["SCRIPT", "STYLE", "LINK", "TEMPLATE", "NOSCRIPT", "META"]);
  const arred = (v) => Math.round(v * 2) / 2; // meio pixel: abaixo disso é arredondamento do motor
  const elementos = {};
  const nome = (el) => el.tagName.toLowerCase() + (el.id ? "#" + el.id : "")
    + (typeof el.className === "string" && el.className.trim() ? "." + el.className.trim().split(/\s+/).slice(0, 3).join(".") : "");
  const visitar = (el, caminho) => {
    const cs = getComputedStyle(el);
    const valores = PROPS.map((p) => cs.getPropertyValue(p));
    const r = el.getBoundingClientRect();
    const reg = { n: nome(el), v: valores, c: [arred(r.left + scrollX), arred(r.top + scrollY), arred(r.width), arred(r.height)] };
    // o texto próprio entra só como um número (não guarda dado da tela): serve para saber que um texto mudou
    // (relógio, sessões ativas, dado do banco) e que a caixa dele e das que o contêm mudou por isso, não pelo estilo
    const texto = [...el.childNodes].filter((n) => n.nodeType === 3).map((n) => n.textContent.trim()).join(" ").trim();
    if (texto) { let h = 0; for (let i = 0; i < texto.length; i++) h = (h * 31 + texto.charCodeAt(i)) | 0; reg.t = h; }
    for (const ps of ["::before", "::after"]) {
      const p = getComputedStyle(el, ps);
      if (p.content && p.content !== "none" && p.content !== "normal") reg[ps] = PSEUDO.map((k) => p.getPropertyValue(k));
    }
    elementos[caminho] = reg;
    const conta = {};
    for (const filho of el.children) {
      if (PULAR.has(filho.tagName)) continue;
      const t = filho.tagName.toLowerCase();
      conta[t] = (conta[t] || 0) + 1;
      visitar(filho, `${caminho}>${t}[${conta[t]}]`);
    }
  };
  visitar(document.body, "body");
  return {
    props: PROPS, pseudo: PSEUDO, elementos,
    janela: [innerWidth, innerHeight],
    pagina: [document.documentElement.scrollWidth, document.documentElement.scrollHeight],
    // as variáveis (tokens) como o <body> as enxerga também entram: mudar um token é mudar a aparência
    tokens: (() => {
      const cs = getComputedStyle(document.body), t = {};
      const ler = (regras) => {
        for (const r of regras) {
          if (r.style) for (const p of r.style) if (p.startsWith("--")) t[p] = cs.getPropertyValue(p).trim();
          if (r.cssRules) ler(r.cssRules); // dentro de @media, @supports, @layer
        }
      };
      for (const folha of document.styleSheets) { try { ler(folha.cssRules); } catch (e) { /* folha de outra origem */ } }
      return t;
    })(),
  };
}
