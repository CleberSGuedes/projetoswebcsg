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
import { contarElementos, diferencas, expandirRetrato } from "./lib/diferencas.mjs";
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
  // o bloco que entrou ou saiu vem primeiro: é a causa das outras diferenças da tela
  const exemplos = [...difs].sort((x, y) => (y.prop === "elemento") - (x.prop === "elemento")).slice(0, 2).map((d) => `${d.el.split(" (")[0]} ${d.prop}: ${String(d.antes).slice(0, 40)} → ${String(d.depois).slice(0, 40)}`).join("; ");
  return `${difs.length} diferença(s) em ${els} elemento(s): ${exemplos}`;
};
const deslocados = (difs) => { const n = new Set(difs.filter((d) => d.deslocou).map((d) => d.el)).size; return n ? `; ${n} elemento(s) só deslocado(s) por ele(s)` : ""; };
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
    // pelo caminho do elemento, não pelo nome: o nome leva o id, e um id sorteado de novo na reabertura (spo-sigla-3 no
    // lugar de spo-sigla-1) fazia a diferença "não se repetir" e sumir da lista
    const chave = (d) => `${(d.el.match(/\((body[^)]*)\)/) || [, d.el])[1]}|${d.prop}`;
    const repete = new Set(outra.map(chave));
    difs = difs.filter((d) => repete.has(chave(d)));
  }
  const estilo = difs.filter((d) => !d.dado && !d.pixel && !d.deslocou), leves = difs.filter((d) => d.dado || d.pixel);
  for (const d of difs) todas.push({ perfil, alvo, ...d });
  if (estilo.length) {
    const capRef = existsSync(resolve(PASTA_REF, arq + ".png")) ? readFileSync(resolve(PASTA_REF, arq + ".png")) : null;
    if (captura) writeFileSync(resolve(pastaDifs, arq + "__depois.png"), captura);
    if (capRef) writeFileSync(resolve(pastaDifs, arq + "__antes.png"), capRef);
  }
  const elementos = contarElementos(retrato);
  if (estilo.length) registrar({ verificacao: "comparacao", perfil, alvo, situacao: "falha", resumo: resumir(estilo) + deslocados(difs) });
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
  linhas.push("\"dado\" = caixa de um elemento com texto que mudou (relógio, sessões, dado do banco), ou que o contém; \"≤ 1 px\" = até 1 px em cada número (arredondamento de medida do script). Os dois não reprovam. \"deslocou\" = elemento que só mudou de lugar (mesma largura e altura) numa tela onde um bloco entrou ou saiu; o bloco reprova, o deslocamento só é listado. Um caminho com \"→\" é o mesmo elemento numa posição nova entre os irmãos.", "",
    "| Vista | Tela | Elemento | Propriedade | Antes | Depois | |", "|---|---|---|---|---|---|---|");
  const c = (s) => String(s).replace(/\|/g, "/").slice(0, 160);
  for (const d of todas) linhas.push(`| ${d.perfil} | ${d.alvo} | ${c(d.el)} | ${d.prop} | ${c(d.antes)} | ${c(d.depois)} | ${d.dado ? "dado" : d.pixel ? "≤ 1 px" : d.deslocou ? "deslocou" : ""} |`);
}
writeFileSync(resolve(pastaDifs, "diferencas.md"), linhas.join("\n") + "\n");
const r = escreverRelatorio(`comparação com "${nome}"`, inicio);
console.log(`\n${r.ok} tela(s) iguais, ${r.avisos} só com dado ao vivo ou ≤ 1 px, ${r.falhas} com diferença de estilo ou falha. Relatório: ${r.arquivo}; lista: ${resolve(pastaDifs, "diferencas.md")}`);
process.exit(r.falhas ? 1 : 0);
