// Bateria SPO-AA+ — diferenças entre dois retratos da comparação antes × depois (comparar.mjs). Sem dependências e sem
// navegador: os testes (lib/diferencas.test.mjs, rodados pelo pytest em tests/test_comparacao_diferencas.py) usam
// retratos montados à mão.

// ---------- formato compacto (lib/estilos.mjs) → formato completo ----------
// Refaz o caminho de cada elemento (o do pai + o segmento) e põe os valores no lugar dos números. Retratos antigos
// (sem "formato") já vêm completos e passam direto. Os elementos com o mesmo estilo apontam para a mesma lista de
// valores (não é cópia), então a memória continua pequena.
export function expandirRetrato(r) {
  if (r.formato !== 2) return r;
  const caminhos = new Array(r.els.length), elementos = {};
  r.els.forEach(([pai, seg, n, e, x, y, w, h, t, antes, depois], i) => {
    const caminho = pai < 0 ? seg : `${caminhos[pai]}>${seg}`;
    caminhos[i] = caminho;
    const reg = { n: r.nomes[n], v: r.estilos[e], c: [x, y, w, h] };
    if (t !== null) reg.t = t;
    if (antes >= 0) reg["::before"] = r.pseudos[antes];
    if (depois >= 0) reg["::after"] = r.pseudos[depois];
    elementos[caminho] = reg;
  });
  return { props: r.props, pseudo: r.pseudo, elementos, janela: r.janela, pagina: r.pagina, tokens: r.tokens };
}
export const contarElementos = (r) => (r.formato === 2 ? r.els.length : Object.keys(r.elementos).length);

// ---------- alinhamento: qual elemento do "depois" é o mesmo do "antes" ----------
// O caminho (div[2]>section[3]) diz a POSIÇÃO entre os irmãos de mesma tag: um bloco novo no meio de uma seção
// renumera os seguintes, e comparar caminho com caminho compararia um elemento com o vizinho dele (diferenças falsas
// em tudo o que vem depois). Por isso os filhos de cada par de pais são alinhados como as linhas num diff de texto:
//   1. a maior sequência comum pelo nome (tag, id e as 3 primeiras classes) — o que não mudou fica no lugar;
//   2. o elemento com id que mudou de lugar entre os irmãos (uma seção do kit movida) é casado pelo id;
//   3. em cada trecho entre dois casados, o que sobrou é casado pela tag, na ordem (o elemento que trocou de classe
//      continua comparado consigo mesmo e a diferença aparece nas propriedades, como antes).
// O que não casou "sumiu" ou "apareceu". Sem nada inserido ou removido, o alinhamento é o caminho com o caminho,
// e o resultado é o mesmo da comparação por posição.
const paiDe = (k) => (k.includes(">") ? k.slice(0, k.lastIndexOf(">")) : null);
const tagDe = (k) => k.slice(k.lastIndexOf(">") + 1).replace(/\[\d+\]$/, "");
const idDe = (nome) => (nome.match(/#([^.]+)/) || [])[1] || null;
// acima disto (filhos × filhos sem prefixo nem sufixo comum), a maior sequência comum gastaria memória demais; o trecho
// do meio é casado pela tag, na ordem (o mesmo que a comparação por posição)
const LIMITE_LCS = 4_000_000;

function filhosPorPai(elementos) {
  const m = new Map();
  for (const k of Object.keys(elementos)) {
    const p = paiDe(k);
    if (p === null) continue;
    let l = m.get(p);
    if (!l) m.set(p, (l = []));
    l.push(k); // Object.keys segue a ordem de gravação (de cima para baixo), então os irmãos ficam na ordem da tela
  }
  return m;
}

// pares [i, j] da maior sequência comum de xa e xb (nomes iguais), em ordem crescente
function maiorSequenciaComum(xa, xb) {
  const pares = [];
  let ini = 0;
  while (ini < xa.length && ini < xb.length && xa[ini] === xb[ini]) { pares.push([ini, ini]); ini++; }
  let fa = xa.length, fb = xb.length;
  const fim = [];
  while (fa > ini && fb > ini && xa[fa - 1] === xb[fb - 1]) { fa--; fb--; fim.push([fa, fb]); }
  const n = fa - ini, m = fb - ini;
  if (n && m) {
    if (n * m > LIMITE_LCS) return null;
    // tabela de comprimentos (de trás para frente) e caminho de volta
    const L = new Uint32Array((n + 1) * (m + 1)), w = m + 1;
    for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--)
      L[i * w + j] = xa[ini + i] === xb[ini + j] ? L[(i + 1) * w + j + 1] + 1 : Math.max(L[(i + 1) * w + j], L[i * w + j + 1]);
    let i = 0, j = 0;
    while (i < n && j < m) {
      if (xa[ini + i] === xb[ini + j]) { pares.push([ini + i, ini + j]); i++; j++; }
      else if (L[(i + 1) * w + j] >= L[i * w + j + 1]) i++;
      else j++;
    }
  }
  return pares.concat(fim.reverse());
}

function casarFilhos(ca, cb, nomeA, nomeB) {
  const xa = ca.map(nomeA), xb = cb.map(nomeB);
  const comum = maiorSequenciaComum(xa, xb) || [];
  const pares = [...comum];
  const usadoA = new Set(pares.map(([i]) => i)), usadoB = new Set(pares.map(([, j]) => j));
  // 2. id movido: o mesmo id dos dois lados, fora da sequência comum
  const porIdB = new Map();
  cb.forEach((_, j) => { const id = !usadoB.has(j) && idDe(xb[j]); if (id && !porIdB.has(id)) porIdB.set(id, j); });
  ca.forEach((_, i) => {
    const id = !usadoA.has(i) && idDe(xa[i]), j = id ? porIdB.get(id) : undefined;
    if (j !== undefined && !usadoB.has(j)) { pares.push([i, j]); usadoA.add(i); usadoB.add(j); }
  });
  // 3. trechos entre dois elementos da sequência comum (ela já vem em ordem): o que sobrou, pela tag, na ordem
  const limites = [[-1, -1], ...comum, [ca.length, cb.length]];
  for (let k = 0; k + 1 < limites.length; k++) {
    const [a0, b0] = limites[k], [a1, b1] = limites[k + 1];
    const restoB = new Map();
    for (let j = b0 + 1; j < b1; j++) if (!usadoB.has(j)) { const t = tagDe(cb[j]); if (!restoB.has(t)) restoB.set(t, []); restoB.get(t).push(j); }
    for (let i = a0 + 1; i < a1; i++) {
      if (usadoA.has(i)) continue;
      const fila = restoB.get(tagDe(ca[i]));
      if (fila && fila.length) { const j = fila.shift(); pares.push([i, j]); usadoA.add(i); usadoB.add(j); }
    }
  }
  return pares;
}

// Map caminho no "antes" → caminho no "depois", para todos os elementos que casaram
export function alinhar(a, b) {
  const fa = filhosPorPai(a.elementos), fb = filhosPorPai(b.elementos);
  const par = new Map();
  if (!a.elementos.body || !b.elementos.body) return par;
  par.set("body", "body");
  const fila = [["body", "body"]];
  while (fila.length) {
    const [pa, pb] = fila.pop();
    const ca = fa.get(pa) || [], cb = fb.get(pb) || [];
    if (!ca.length || !cb.length) continue;
    for (const [i, j] of casarFilhos(ca, cb, (k) => a.elementos[k].n, (k) => b.elementos[k].n)) {
      par.set(ca[i], cb[j]);
      fila.push([ca[i], cb[j]]);
    }
  }
  return par;
}

// ---------- diferenças entre dois retratos ----------
// ids sorteados a cada carga (os gráficos Plotly criam clip-path: url("#clip82efa9xyplot")) não são estilo
const semIdSorteado = (v) => String(v).replace(/url\("?#[^")]*"?\)/g, "url(#…)");
// até 1 px em cada número da mesma propriedade: arredondamento de uma medida feita pelo script (ex.: o "›" do menu
// no celular, alinhado pelo logotipo); sai na lista como "≤ 1 px" e não reprova. Uma mudança de estilo de verdade
// costuma mudar mais que isso, e uma de 1 px continua visível na lista.
// Só vale para comprimento em px (ou para a caixa): cor, opacidade, peso de fonte e afins nunca têm tolerância.
function ateUmPixel(x, y, caixa = false) {
  const nx = String(x).match(/-?\d+(\.\d+)?/g) || [], ny = String(y).match(/-?\d+(\.\d+)?/g) || [];
  if (!nx.length || nx.length !== ny.length) return false;
  const soPx = (v) => (String(v).match(/-?\d+(\.\d+)?[a-z%]*/g) || []).every((n) => n.endsWith("px"));
  if (!caixa && !(soPx(x) && soPx(y))) return false;
  if (String(x).replace(/-?\d+(\.\d+)?/g, "#") !== String(y).replace(/-?\d+(\.\d+)?/g, "#")) return false;
  return nx.every((n, i) => Math.abs(+n - +ny[i]) <= 1);
}
// linha de lista ou de tabela (li, tr, option, dt, dd), ou algo dentro dela: entra e sai com o dado
const LINHA = /(^|>)(li|tr|option|dt|dd)\[\d+\]/;
// propriedades calculadas que dependem do conteúdo (as faixas de um grid em px): mudam junto com o dado
const DO_CONTEUDO = new Set(["grid-template-rows", "grid-template-columns"]);
export function diferencas(a, b) {
  const difs = [];
  const fmt = (v) => (v === undefined ? "(nada)" : String(v));
  for (const [k, va] of Object.entries(a.tokens)) if (b.tokens[k] !== va) difs.push({ el: ":root", prop: k, antes: va, depois: fmt(b.tokens[k]) });
  for (const k of Object.keys(b.tokens)) if (!(k in a.tokens)) difs.push({ el: ":root", prop: k, antes: "(nada)", depois: b.tokens[k] });
  const propsB = new Map(b.props.map((p, i) => [p, i]));
  const par = alinhar(a, b), deB = new Map([...par].map(([ka, kb]) => [kb, ka]));
  // o que não casou: só a raiz de cada pedaço que sumiu ou apareceu entra na lista, com quantos elementos tem dentro
  const raizes = (elementos, casou) => {
    const raiz = new Map(), dentro = new Map();
    for (const k of Object.keys(elementos)) {
      if (casou(k)) continue;
      const p = paiDe(k), r = p !== null && raiz.has(p) ? raiz.get(p) : k;
      raiz.set(k, r);
      dentro.set(r, (dentro.get(r) || 0) + (r === k ? 0 : 1));
    }
    return dentro;
  };
  const sumiram = raizes(a.elementos, (k) => par.has(k)), apareceram = raizes(b.elementos, (k) => deB.has(k));
  // dado que mudou: texto diferente (relógio, sessões ativas, dado do banco) ou linha de lista/tabela que entrou ou
  // saiu. A caixa do próprio elemento, das que o contêm e das que ele contém, e as faixas dos grids que o contêm, podem
  // mudar sem que o estilo mude: essas diferenças saem como "dado" (aviso); as de estilo continuam como diferença.
  // Tudo no caminho do "antes" (a linha que apareceu entra pela lista que a contém, casada com a do "antes")
  const mudouDado = [
    ...[...par].filter(([ka, kb]) => a.elementos[ka].t !== b.elementos[kb].t).map(([ka]) => ka),
    ...[...sumiram.keys()].filter((k) => LINHA.test(k)).map(paiDe),
    ...[...apareceram.keys()].filter((k) => LINHA.test(k)).map((k) => deB.get(paiDe(k))).filter(Boolean),
  ];
  const porDado = (caminho) => mudouDado.some((k) => k === caminho || k.startsWith(caminho + ">") || caminho.startsWith(k + ">"));
  const dadoNaTela = mudouDado.length > 0;
  // bloco que entrou ou saiu (fora das linhas de dado): ele já reprova; os vizinhos que só se deslocaram por causa dele
  // (mesma largura e altura) saem marcados "deslocou", sem reprovar de novo
  const mudouEstrutura = [...sumiram.keys(), ...apareceram.keys()].some((k) => !LINHA.test(k));
  if (a.pagina.join() !== b.pagina.join()) difs.push({ el: "página", prop: "largura×altura", antes: a.pagina.join("×"), depois: b.pagina.join("×"), ...(dadoNaTela ? { dado: true } : mudouEstrutura ? { deslocou: true } : {}) });
  const comDentro = (n) => (n ? ` (com ${n} elemento(s) dentro)` : "");
  for (const [caminho, ea] of Object.entries(a.elementos)) {
    const kb = par.get(caminho);
    if (kb === undefined) {
      if (sumiram.has(caminho)) difs.push({ el: `${ea.n} (${caminho})`, prop: "elemento", antes: "existe" + comDentro(sumiram.get(caminho)), depois: "sumiu", ...(LINHA.test(caminho) ? { dado: true } : {}) });
      continue;
    }
    const eb = b.elementos[kb];
    // o nome leva o caminho do "antes" entre parênteses (a "segunda olhada" casa por ele) e, se o elemento mudou de
    // posição entre os irmãos, o caminho novo depois da seta
    const el = `${ea.n} (${caminho})${kb !== caminho ? ` → ${kb}` : ""}`;
    a.props.forEach((p, i) => {
      const va = semIdSorteado(ea.v[i]), vb = eb.v[propsB.get(p)] === undefined ? undefined : semIdSorteado(eb.v[propsB.get(p)]);
      if (va === vb) return;
      const extra = DO_CONTEUDO.has(p) && porDado(caminho) ? { dado: true } : ateUmPixel(va, vb) ? { pixel: true } : {};
      difs.push({ el, prop: p, antes: va, depois: fmt(vb), ...extra });
    });
    if (ea.c.join() !== eb.c.join()) {
      // também é dado o vizinho que só se deslocou (mesma largura e altura) dentro de um pai que contém o texto que
      // mudou: numa linha alinhada à direita, a hora que encolhe empurra o nome que vem antes
      const pai = paiDe(caminho);
      const soDeslocou = ea.c[2] === eb.c[2] && ea.c[3] === eb.c[3];
      const extra = porDado(caminho) || (soDeslocou && pai && porDado(pai)) ? { dado: true }
        : ateUmPixel(ea.c.join(), eb.c.join(), true) ? { pixel: true }
        : soDeslocou && mudouEstrutura ? { deslocou: true } : {};
      difs.push({ el, prop: "caixa (x, y, largura, altura)", antes: ea.c.join(", "), depois: eb.c.join(", "), ...extra });
    }
    for (const ps of ["::before", "::after"]) {
      const pa = ea[ps], pb = eb[ps];
      if (!pa && !pb) continue;
      if (!pa || !pb) { difs.push({ el: el + ps, prop: "pseudo-elemento", antes: pa ? "existe" : "(nada)", depois: pb ? "existe" : "(nada)" }); continue; }
      a.pseudo.forEach((p, i) => { if (pa[i] !== pb[i]) difs.push({ el: el + ps, prop: p, antes: pa[i], depois: fmt(pb[i]) }); });
    }
  }
  for (const [caminho, n] of apareceram) {
    const eb = b.elementos[caminho];
    difs.push({ el: `${eb.n} (${caminho})`, prop: "elemento", antes: "(nada)", depois: "apareceu" + comDentro(n), ...(LINHA.test(caminho) ? { dado: true } : {}) });
  }
  return difs;
}
