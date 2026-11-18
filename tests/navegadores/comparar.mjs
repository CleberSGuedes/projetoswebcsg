// Bateria SPO-AA+ — comparação antes × depois: prova que uma mudança (dividir o CSS ou o JS, mover código) não mudou
// a aparência. Grava um retrato de cada tela ANTES da mudança e, depois, compara elemento por elemento.
//   node comparar.mjs gravar   [--nome=antes] [--telas=basicas|principais|todas|kit|inicio,kit,...] [--vistas=padrao|ampla]
//   node comparar.mjs comparar [--nome=antes]
// O retrato de cada tela, em cada vista (aparelho + preferências do Personalizar), guarda o estilo calculado de todos
// os elementos (cor, fundo, fonte, bordas, espaçamentos, caixa...), os ::before/::after, os tokens e uma captura.
// "comparar" usa as mesmas telas e vistas da referência e lista cada diferença: tela, elemento, propriedade,
// antes → depois. Sai com código 1 se houver diferença. Nada é gravado no banco; uma tela por vez.
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { execFileSync } from "node:child_process";
import { abrirPerfil, abrirTela, gravarPreferencias, garantirSessao, nomeDaTela, BASE, KIT, PASTA, RAIZ,
  TELAS_BASICAS, TELAS_PRINCIPAIS, TELAS_TODAS } from "./lib/ambiente.mjs";
import { retratarEstilos } from "./lib/estilos.mjs";
import { registrar, escreverRelatorio, etapa, PASTA_RESULTADO } from "./lib/registro.mjs";
import { comTempo, LIMITE_PASSO_S } from "./lib/tempo.mjs";
import { PREFERENCIAS } from "./verificacoes/contraste.mjs";

// vista = aparelho + preferências; "padrao": claro e escuro no desktop e o celular; "ampla": + Firefox, Safari e
// a combinação de leitura (fonte 150%, Atkinson, entrelinha maior)
const VISTAS = {
  padrao: [["D-chromium-1366", "claro-azul"], ["D-chromium-1366", "escuro-amarelo-reforcado"], ["M-s26-360", "claro-azul"]],
  ampla: [["D-chromium-1366", "claro-azul"], ["D-chromium-1366", "escuro-amarelo-reforcado"], ["D-chromium-1366", "claro-atkinson-150-leitura"],
    ["D-firefox-1366", "claro-azul"], ["D-webkit-1366", "claro-azul"], ["M-s26-360", "claro-azul"], ["M-iphone16-393", "escuro-amarelo-reforcado"]],
};
// "kit" = o kit de mockup (static/kit/mockup-spo-aa-plus.html): usa o mesmo style.css e lê trechos do main.js, então
// uma divisão do CSS ou do JS também pode mudá-lo; entra no fim de cada lista
const TELAS = { basicas: [...TELAS_BASICAS, "kit"], principais: [...TELAS_PRINCIPAIS, "kit"], todas: ["", ...TELAS_TODAS, "kit"], kit: ["kit"] };

// --telas: um conjunto (basicas, principais, todas, kit) ou uma lista escolhida, separada por vírgulas, com "inicio", "kit"
// e rotas do menu (as de TELAS_TODAS): numa mudança que só atinge algumas telas, compara-se só elas
function escolherTelas(valor) {
  if (TELAS[valor]) return TELAS[valor];
  const conhecidas = new Set(["", ...TELAS_TODAS, "kit"]);
  const lista = valor.split(",").map((t) => t.trim()).filter(Boolean).map((t) => (t === "inicio" ? "" : t));
  const fora = lista.filter((t) => !conhecidas.has(t));
  if (!lista.length || fora.length) { if (fora.length) console.log(`tela(s) desconhecida(s): ${fora.join(", ")}`); return null; }
  return lista;
}
const opcao = (n, padrao) => (process.argv.find((a) => a.startsWith(`--${n}=`)) || `=${padrao}`).split("=")[1];
const modo = process.argv[2];
const nome = opcao("nome", "antes");
const PASTA_REF = resolve(PASTA, "resultados", "referencias", nome);
const arquivoDe = (perfil, pref, rota) => `${perfil}__${pref}__${nomeDaTela(rota)}`;

// Conta as requisições ao servidor em andamento (fetch e XMLHttpRequest), desde antes dos scripts do SPO: a tela só
// está pronta quando nenhuma está pendente (as tabelas chegam por fetch depois do carregamento, como a do PLAN 51)
const CONTAR_PENDENTES = () => {
  window.__bateriaPendentes = 0;
  const conta = (d) => { window.__bateriaPendentes += d; };
  const f = window.fetch;
  if (f) window.fetch = function (...a) { conta(1); return f.apply(this, a).finally(() => conta(-1)); };
  const envia = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function (...a) { conta(1); this.addEventListener("loadend", () => conta(-1), { once: true }); return envia.apply(this, a); };
};

// espera a tela ficar pronta, sem tempos fixos: carregamento encerrado, nenhuma requisição pendente, fontes
// e imagens carregadas e nenhum elemento novo por 0,7 s (até 45 s); depois, o fim das transições e animações que
// terminam
async function retratar(pag, rota) {
  if (rota === "kit") {
    await pag.goto(KIT, { waitUntil: "load", timeout: 60000 });
  } else {
    if (pag.url().startsWith(KIT)) await pag.goto(BASE, { waitUntil: "domcontentloaded", timeout: 60000 }); // do kit de volta ao SPO
    await abrirTela(pag, rota, { pausas: false });
  }
  await pag.evaluate(() => { scrollTo(0, 0); delete window.__bateriaEstavel; });
  await pag.waitForFunction(() => {
    const n = document.getElementsByTagName("*").length, agora = Date.now(), s = (window.__bateriaEstavel ||= { n: -1, desde: agora });
    // imagens também: o "›" do menu no celular é alinhado pelo logotipo, medido depois de ele carregar
    const ocupada = (window.__bateriaPendentes || 0) > 0 || (document.fonts && document.fonts.status !== "loaded")
      || [...document.images].some((i) => !i.complete);
    if (n !== s.n || ocupada) { s.n = n; s.desde = agora; }
    return agora - s.desde >= 700;
  }, null, { timeout: 45000, polling: 100 }).catch(() => {});
  await pag.waitForFunction(() => document.getAnimations().every((a) => a.effect?.getTiming().iterations === Infinity || a.playState !== "running"), null, { timeout: 10000, polling: 100 }).catch(() => {});
  await pag.waitForTimeout(250); // fim das transições de 0,2 s que começam no mesmo instante
  const retrato = await pag.evaluate(retratarEstilos);
  const captura = await pag.screenshot({ fullPage: true, animations: "disabled", caret: "hide" }).catch(() => null);
  return { retrato, captura };
}

function commitAtual() {
  try { return execFileSync("git", ["log", "-1", "--format=%h %s"], { cwd: RAIZ, encoding: "utf-8" }).trim(); } catch (e) { return "?"; }
}

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
const contarElementos = (r) => (r.formato === 2 ? r.els.length : Object.keys(r.elementos).length);

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
  // dado que mudou: texto diferente (relógio, sessões ativas, dado do banco) ou linha de lista/tabela que entrou ou
  // saiu. A caixa do próprio elemento, das que o contêm e das que ele contém, e as faixas dos grids que o contêm, podem
  // mudar sem que o estilo mude: essas diferenças saem como "dado" (aviso); as de estilo continuam como diferença
  const linhaQueMudou = (k) => LINHA.test(k) ? k.replace(/>[^>]*$/, "") : null; // a lista/tabela que a contém
  const mudouDado = [
    ...Object.keys(a.elementos).filter((k) => b.elementos[k] && a.elementos[k].t !== b.elementos[k].t),
    ...Object.keys(a.elementos).filter((k) => !b.elementos[k]).map(linhaQueMudou).filter(Boolean),
    ...Object.keys(b.elementos).filter((k) => !a.elementos[k]).map(linhaQueMudou).filter(Boolean),
  ];
  const porDado = (caminho) => mudouDado.some((k) => k === caminho || k.startsWith(caminho + ">") || caminho.startsWith(k + ">"));
  const dadoNaTela = mudouDado.length > 0;
  if (a.pagina.join() !== b.pagina.join()) difs.push({ el: "página", prop: "largura×altura", antes: a.pagina.join("×"), depois: b.pagina.join("×"), ...(dadoNaTela ? { dado: true } : {}) });
  for (const [caminho, ea] of Object.entries(a.elementos)) {
    const eb = b.elementos[caminho];
    const el = `${ea.n} (${caminho})`;
    if (!eb) { difs.push({ el, prop: "elemento", antes: "existe", depois: "sumiu", ...(LINHA.test(caminho) ? { dado: true } : {}) }); continue; }
    a.props.forEach((p, i) => {
      const va = semIdSorteado(ea.v[i]), vb = eb.v[propsB.get(p)] === undefined ? undefined : semIdSorteado(eb.v[propsB.get(p)]);
      if (va === vb) return;
      const extra = DO_CONTEUDO.has(p) && porDado(caminho) ? { dado: true } : ateUmPixel(va, vb) ? { pixel: true } : {};
      difs.push({ el, prop: p, antes: va, depois: fmt(vb), ...extra });
    });
    if (ea.c.join() !== eb.c.join()) {
      // também é dado o vizinho que só se deslocou (mesma largura e altura) dentro de um pai que contém o texto que
      // mudou: numa linha alinhada à direita, a hora que encolhe empurra o nome que vem antes
      const pai = caminho.includes(">") ? caminho.slice(0, caminho.lastIndexOf(">")) : null;
      const soDeslocou = ea.c[2] === eb.c[2] && ea.c[3] === eb.c[3];
      const extra = porDado(caminho) || (soDeslocou && pai && porDado(pai)) ? { dado: true }
        : ateUmPixel(ea.c.join(), eb.c.join(), true) ? { pixel: true } : {};
      difs.push({ el, prop: "caixa (x, y, largura, altura)", antes: ea.c.join(", "), depois: eb.c.join(", "), ...extra });
    }
    for (const ps of ["::before", "::after"]) {
      const pa = ea[ps], pb = eb[ps];
      if (!pa && !pb) continue;
      if (!pa || !pb) { difs.push({ el: el + ps, prop: "pseudo-elemento", antes: pa ? "existe" : "(nada)", depois: pb ? "existe" : "(nada)" }); continue; }
      a.pseudo.forEach((p, i) => { if (pa[i] !== pb[i]) difs.push({ el: el + ps, prop: p, antes: pa[i], depois: fmt(pb[i]) }); });
    }
  }
  for (const [caminho, eb] of Object.entries(b.elementos)) if (!a.elementos[caminho]) difs.push({ el: `${eb.n} (${caminho})`, prop: "elemento", antes: "(nada)", depois: "apareceu", ...(LINHA.test(caminho) ? { dado: true } : {}) });
  return difs;
}

async function percorrer(vistas, telas, aoRetratar) {
  for (const [perfil, pref] of vistas) {
    // abrir a vista (navegador + sessão) pode demorar com o banco remoto lento: tenta duas vezes; não abrindo, registra
    // a falha das telas dessa vista e segue para a próxima, em vez de parar a comparação inteira
    let sessao = null;
    for (let tentativa = 1; tentativa <= 2 && !sessao; tentativa++) {
      try { sessao = await comTempo(abrirPerfil(perfil), LIMITE_PASSO_S, `abrir a vista ${perfil}`); }
      catch (e) { if (tentativa === 2) for (const rota of telas) registrar({ verificacao: "comparacao", perfil, alvo: `${nomeDaTela(rota)} [${pref}]`, situacao: "falha", resumo: "a vista não abriu: " + String(e.message).split("\n")[0].slice(0, 120) }); }
    }
    if (!sessao) continue;
    const { nav, ctx, pag } = sessao;
    await gravarPreferencias(ctx, PREFERENCIAS[pref]);
    await ctx.addInitScript(CONTAR_PENDENTES);
    try {
      for (const rota of telas) {
        const alvo = `${nomeDaTela(rota)} [${pref}]`;
        etapa(`comparação ${perfil}`, alvo);
        try {
          const r = await comTempo(retratar(pag, rota), LIMITE_PASSO_S, `retrato ${alvo} (${perfil})`);
          await aoRetratar(perfil, pref, rota, alvo, r, () => comTempo(retratar(pag, rota), LIMITE_PASSO_S, `2º retrato ${alvo} (${perfil})`));
        } catch (e) {
          registrar({ verificacao: "comparacao", perfil, alvo, situacao: "falha", resumo: "não retratou: " + String(e.message).slice(0, 140) });
        }
      }
    } finally {
      await comTempo(nav.close(), 30, "fechar o navegador").catch(() => {});
    }
  }
}

if (!["gravar", "comparar"].includes(modo)) {
  console.log("uso: node comparar.mjs gravar [--nome=antes] [--telas=basicas|principais|todas|kit|inicio,kit,...] [--vistas=padrao|ampla]\n" +
    "     node comparar.mjs comparar [--nome=antes]");
  process.exit(2);
}
const inicio = Date.now();
await garantirSessao();

if (modo === "gravar") {
  const telas = escolherTelas(opcao("telas", "principais")), vistas = VISTAS[opcao("vistas", "padrao")];
  if (!telas || !vistas) {
    console.log("--telas: basicas|principais|todas|kit, ou uma lista separada por vírgulas (inicio, kit e rotas do menu,\n" +
      "         ex.: --telas=inicio,kit,relatorios/fip613); --vistas: padrao|ampla");
    process.exit(2);
  }
  mkdirSync(PASTA_REF, { recursive: true });
  const meta = { nome, quando: new Date().toISOString(), commit: commitAtual(), base: BASE, telas, vistas };
  console.log(`Gravando a referência "${nome}" (${vistas.length} vistas × ${telas.length} telas), no commit ${meta.commit}`);
  await percorrer(vistas, telas, async (perfil, pref, rota, alvo, { retrato, captura }) => {
    const arq = arquivoDe(perfil, pref, rota);
    writeFileSync(resolve(PASTA_REF, arq + ".json"), JSON.stringify(retrato));
    if (captura) writeFileSync(resolve(PASTA_REF, arq + ".png"), captura);
    registrar({ verificacao: "comparacao", perfil, alvo, situacao: "ok", resumo: `gravado: ${contarElementos(retrato)} elementos${retrato.estilos ? `, ${retrato.estilos.length} estilos diferentes` : ""}, ${Object.keys(retrato.tokens).length} tokens` });
  });
  writeFileSync(resolve(PASTA_REF, "referencia.json"), JSON.stringify(meta, null, 1));
  const r = escreverRelatorio(`comparação — gravar "${nome}"`, inicio);
  console.log(`\nReferência em ${PASTA_REF}. ${r.falhas} falha(s).`);
  process.exit(r.falhas ? 1 : 0);
}

// comparar
if (!existsSync(resolve(PASTA_REF, "referencia.json"))) { console.log(`Sem referência "${nome}": rode antes  node comparar.mjs gravar --nome=${nome}`); process.exit(2); }
const meta = JSON.parse(readFileSync(resolve(PASTA_REF, "referencia.json"), "utf-8"));
const commit = commitAtual();
console.log(`Comparando com a referência "${nome}" (gravada em ${meta.quando}, commit ${meta.commit}); agora: ${commit}`);
const pastaDifs = resolve(PASTA_RESULTADO, "diferencas");
mkdirSync(pastaDifs, { recursive: true });
const todas = [];
const resumir = (difs) => {
  const els = new Set(difs.map((d) => d.el)).size;
  const exemplos = difs.slice(0, 2).map((d) => `${d.el.split(" (")[0]} ${d.prop}: ${String(d.antes).slice(0, 40)} → ${String(d.depois).slice(0, 40)}`).join("; ");
  return `${difs.length} diferença(s) em ${els} elemento(s): ${exemplos}`;
};
await percorrer(meta.vistas, meta.telas, async (perfil, pref, rota, alvo, { retrato, captura }, deNovo) => {
  const arq = arquivoDe(perfil, pref, rota);
  const ref = resolve(PASTA_REF, arq + ".json");
  if (!existsSync(ref)) { registrar({ verificacao: "comparacao", perfil, alvo, situacao: "falha", resumo: "sem retrato na referência" }); return; }
  const antes = expandirRetrato(JSON.parse(readFileSync(ref, "utf-8")));
  let difs = diferencas(antes, expandirRetrato(retrato));
  // segunda olhada: a tela é aberta de novo e só fica a diferença que se repete (uma medida feita pelo script num
  // instante diferente, por exemplo, não se repete; uma mudança de estilo, sim)
  if (difs.some((d) => !d.dado && !d.pixel)) {
    const outra = diferencas(antes, expandirRetrato((await deNovo()).retrato));
    const repete = new Set(outra.map((d) => `${d.el}|${d.prop}`));
    difs = difs.filter((d) => repete.has(`${d.el}|${d.prop}`));
  }
  const estilo = difs.filter((d) => !d.dado && !d.pixel), leves = difs.filter((d) => d.dado || d.pixel);
  for (const d of difs) todas.push({ perfil, alvo, ...d });
  if (estilo.length) {
    const capRef = existsSync(resolve(PASTA_REF, arq + ".png")) ? readFileSync(resolve(PASTA_REF, arq + ".png")) : null;
    if (captura) writeFileSync(resolve(pastaDifs, arq + "__depois.png"), captura);
    if (capRef) writeFileSync(resolve(pastaDifs, arq + "__antes.png"), capRef);
  }
  const elementos = contarElementos(retrato);
  if (estilo.length) registrar({ verificacao: "comparacao", perfil, alvo, situacao: "falha", resumo: resumir(estilo) });
  else if (leves.length) {
    const nDado = new Set(leves.filter((d) => d.dado).map((d) => d.el)).size, nPixel = new Set(leves.filter((d) => d.pixel).map((d) => d.el)).size;
    registrar({ verificacao: "comparacao", perfil, alvo, situacao: "aviso", resumo: `estilo igual (${elementos} elementos); só ${[nDado && `a caixa de ${nDado} elemento(s) com texto que mudou (dado ao vivo)`, nPixel && `${nPixel} elemento(s) com até 1 px (medida do script)`].filter(Boolean).join(" e ")}` });
  }
  else registrar({ verificacao: "comparacao", perfil, alvo, situacao: "ok", resumo: `igual (${elementos} elementos)` });
});
// a lista inteira das diferenças, para ler com calma (o relatório mostra só o resumo de cada tela)
const linhas = [`# Diferenças em relação à referência "${nome}"`, "", `Referência: ${meta.quando}, commit ${meta.commit}. Agora: ${commit}.`, ""];
if (!todas.length) linhas.push("Nenhuma diferença.");
else {
  linhas.push("\"dado\" = caixa de um elemento com texto que mudou (relógio, sessões, dado do banco), ou que o contém; \"≤ 1 px\" = até 1 px em cada número (arredondamento de medida do script). Os dois não reprovam.", "",
    "| Vista | Tela | Elemento | Propriedade | Antes | Depois | |", "|---|---|---|---|---|---|---|");
  const c = (s) => String(s).replace(/\|/g, "/").slice(0, 160);
  for (const d of todas) linhas.push(`| ${d.perfil} | ${d.alvo} | ${c(d.el)} | ${d.prop} | ${c(d.antes)} | ${c(d.depois)} | ${d.dado ? "dado" : d.pixel ? "≤ 1 px" : ""} |`);
}
writeFileSync(resolve(pastaDifs, "diferencas.md"), linhas.join("\n") + "\n");
const r = escreverRelatorio(`comparação com "${nome}"`, inicio);
console.log(`\n${r.ok} tela(s) iguais, ${r.avisos} só com dado ao vivo ou ≤ 1 px, ${r.falhas} com diferença de estilo ou falha. Relatório: ${r.arquivo}; lista: ${resolve(pastaDifs, "diferencas.md")}`);
process.exit(r.falhas ? 1 : 0);
