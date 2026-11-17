// Bateria SPO-AA+ — medidas que rodam DENTRO da página (passadas ao page.evaluate). Sem dependências de fora.

// Varredura da tela: rolagem lateral, elementos fora da tela, texto cortado, alvos < 24 px (WCAG 2.5.8; links dentro de
// frases são exceção do critério e não contam), avisos abertos e a coluna do menu.
export function medirTela() {
  const vw = innerWidth;
  const visivel = (el) => {
    const r = el.getBoundingClientRect(), cs = getComputedStyle(el);
    return r.width > 2 && r.height > 2 && cs.visibility !== "hidden" && cs.display !== "none" && +cs.opacity !== 0 && !el.closest(".spo-sr-only, .sr-only, .visually-hidden");
  };
  const dentroDeRolagem = (el) => {
    for (let n = el.parentElement; n && n !== document.body; n = n.parentElement) if (/auto|scroll|hidden|clip/.test(getComputedStyle(n).overflowX)) return true;
    return false;
  };
  const nome = (el) => el.tagName.toLowerCase() + (el.id ? "#" + el.id : "") + (typeof el.className === "string" && el.className ? "." + el.className.trim().split(/\s+/).slice(0, 2).join(".") : "");
  const todos = [...document.querySelectorAll("body *")].filter(visivel);
  const fora = todos.filter((e) => e.getBoundingClientRect().right > vw + 1 && getComputedStyle(e).position !== "fixed" && !dentroDeRolagem(e));
  const cortados = todos.filter((e) => {
    if (![...e.childNodes].some((n) => n.nodeType === 3 && n.textContent.trim())) return false;
    const cs = getComputedStyle(e);
    return (cs.overflowX === "hidden" || cs.overflowX === "clip" || cs.textOverflow === "ellipsis") && e.scrollWidth > e.clientWidth + 1;
  });
  const interativos = todos.filter((e) => e.matches("a[href], button, input:not([type=hidden]), select, textarea, [role=button], [role=tab], [tabindex]:not([tabindex='-1'])"));
  const alvoDe = (e) => (e.matches("input, select, textarea") && e.closest("label")) || e;
  const caixa = (e) => {
    const r = e.getBoundingClientRect(), a = getComputedStyle(e, "::after");
    if (a.content && a.content !== "none" && a.position === "absolute") {
      const n = (v) => Math.max(0, -parseFloat(v) || 0);
      return { width: r.width + n(a.left) + n(a.right), height: r.height + n(a.top) + n(a.bottom) };
    }
    return r;
  };
  const emFrase = (e) => e.tagName === "A" && !!e.closest("p, li, dd, td, small, .muted");
  const pequenos = interativos.filter((e) => { const r = caixa(alvoDe(e)); return Math.min(r.width, r.height) < 24 && !emFrase(e); });
  return {
    janela: [vw, innerHeight],
    rolagemLateral: Math.max(0, document.documentElement.scrollWidth - vw),
    foraDaTela: fora.length, foraExemplos: fora.slice(0, 3).map(nome),
    textoCortado: cortados.length, cortadoExemplos: cortados.slice(0, 3).map((e) => nome(e) + " «" + e.textContent.trim().slice(0, 30) + "»"),
    alvosMenor24: pequenos.length,
    alvo24Exemplos: pequenos.slice(0, 4).map((e) => { const r = caixa(alvoDe(e)); return `${nome(e)} ${Math.round(r.width)}x${Math.round(r.height)} «${(e.textContent || e.getAttribute("aria-label") || "").trim().slice(0, 20)}»`; }),
    avisosAbertos: [...document.querySelectorAll(".modal-overlay")].filter(visivel).map((m) => m.id || nome(m)),
  };
}

// Contraste do texto (baterias 1–3 do AA+): para cada texto visível da área, a cor contra o fundo REAL (compõe as
// transparências e a opacidade dos ancestrais até um fundo opaco; lê degradês). AA: 4,5:1 (3:1 texto grande);
// AA+: 7:1 (4,5:1 grande). Ignora texto desabilitado, oculto e só para leitor de tela.
export function medirContraste(areaSel) {
  const d = document;
  const escuro = d.body.classList.contains("theme-dark");
  const parse = (s) => {
    if (!s) return null;
    let m = s.match(/rgba?\(([^)]+)\)/);
    if (m) { const p = m[1].split(/[ ,\/]+/).filter(Boolean).map(Number); return [p[0], p[1], p[2], p.length > 3 ? p[3] : 1]; }
    m = s.match(/color\(srgb ([^)]+)\)/);
    if (m) { const p = m[1].split(/[ \/]+/).filter(Boolean).map(Number); return [p[0] * 255, p[1] * 255, p[2] * 255, p.length > 3 ? p[3] : 1]; }
    return null;
  };
  const hex = (g) => { const h = g.slice(1); const n = h.length === 3 ? h.split("").map((x) => parseInt(x + x, 16)) : [0, 2, 4].map((i) => parseInt(h.substr(i, 2), 16)); return [...n, 1]; };
  const lum = (c) => { const f = (v) => { v /= 255; return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); }; return 0.2126 * f(c[0]) + 0.7152 * f(c[1]) + 0.0722 * f(c[2]); };
  const mix = (t, b) => { const a = t[3]; return [t[0] * a + b[0] * (1 - a), t[1] * a + b[1] * (1 - a), t[2] * a + b[2] * (1 - a), 1]; };
  const razao = (a, b) => { const x = lum(a), y = lum(b); return (Math.max(x, y) + 0.05) / (Math.min(x, y) + 0.05); };
  const fundo = (el) => {
    const cam = [];
    for (let e = el; e; e = e.parentElement) {
      const cs = getComputedStyle(e);
      let c = parse(cs.backgroundColor);
      const img = cs.backgroundImage;
      if (img && img !== "none" && img.includes("gradient")) { const m = img.match(/(rgba?\([^)]+\)|color\(srgb[^)]+\)|#[0-9a-f]{3,8}\b)/i); if (m) c = m[1].startsWith("#") ? hex(m[1]) : parse(m[1]); }
      if (c && c[3] > 0) { cam.push(c); if (c[3] >= 0.99) break; }
    }
    let base = escuro ? [18, 18, 18, 1] : [255, 255, 255, 1];
    for (let i = cam.length - 1; i >= 0; i--) base = mix(cam[i], base);
    return base;
  };
  const area = (areaSel && d.querySelector(areaSel)) || d.getElementById("content-area") || d.body;
  const aa = [], aap = [];
  let textos = 0;
  const vistos = new Set();
  const tw = d.createTreeWalker(area, NodeFilter.SHOW_TEXT);
  while (tw.nextNode()) {
    const t = tw.currentNode;
    if (!t.textContent.trim()) continue;
    const el = t.parentElement;
    if (!el || vistos.has(el)) continue;
    vistos.add(el);
    if (!el.getClientRects().length || el.closest("svg,script,style,option,iframe,.spo-sr-only,.visually-hidden,[hidden]")) continue;
    const cs = getComputedStyle(el);
    if (cs.visibility === "hidden" || parseFloat(cs.opacity) < 0.1) continue;
    const b = el.getBoundingClientRect();
    if (b.width < 2 || b.height < 2) continue;
    if (el.closest("[disabled],.disabled,[aria-disabled='true']")) continue;
    textos++;
    const fs = parseFloat(cs.fontSize), peso = parseInt(cs.fontWeight, 10);
    const grande = fs >= 24 || (fs >= 18.66 && peso >= 700);
    let op = 1;
    for (let e = el; e && e !== d.documentElement; e = e.parentElement) op *= parseFloat(getComputedStyle(e).opacity) || 1;
    const c0 = parse(cs.color);
    if (!c0) continue;
    const bg = fundo(el);
    const x = razao(mix([c0[0], c0[1], c0[2], c0[3] * op], bg), bg);
    const quem = [+x.toFixed(2), el.textContent.trim().slice(0, 30), (typeof el.className === "string" && el.className ? el.className.split(" ")[0] : el.tagName).slice(0, 28)];
    if (x < (grande ? 3 : 4.5)) aa.push(quem); else if (x < (grande ? 4.5 : 7)) aap.push(quem);
  }
  const ord = (a) => a.sort((p, q) => p[0] - q[0]);
  return { textos, abaixoAA: aa.length, abaixoAAexemplos: ord(aa).slice(0, 4), abaixoAAmais: aap.length, abaixoAAmaisExemplos: ord(aap).slice(0, 4) };
}

// Retrato para comparar SPO × kit: atributos do <body>/<html>, variáveis e o estilo calculado das duas prévias do
// Personalizar (#spo-previsualizacao e #spo-previa-fontes), que o kit tem com a mesma marcação.
export function fotografarPreferencias(props) {
  const dados = (el) => Object.fromEntries([...el.attributes].filter((a) => a.name.startsWith("data-") && !/^data-(initial|spo-posto|spo-seta)/.test(a.name)).map((a) => [a.name, a.value]));
  const estilo = (el) => { const c = getComputedStyle(el); return props.map((p) => c.getPropertyValue(p)).join(" | "); };
  const cartoes = {};
  for (const id of ["spo-previsualizacao", "spo-previa-fontes"]) {
    const c = document.getElementById(id);
    cartoes[id] = c ? [c, ...c.querySelectorAll("*")].map((e) => ({ tag: e.tagName + (typeof e.className === "string" && e.className ? "." + e.className.trim().split(/\s+/).join(".") : ""), s: estilo(e) })) : null;
  }
  const b = document.body;
  const vars = Object.fromEntries(["--accent", "--text", "--bg", "--card", "--muted", "--spo-accent-text", "--spo-control-border", "--spo-fs-texto", "--spo-fs-dado", "--spo-fs-titulo"].map((v) => [v, getComputedStyle(b).getPropertyValue(v).trim()]));
  return { escuro: b.classList.contains("theme-dark"), html: dados(document.documentElement), body: dados(b), vars, cartoes };
}
