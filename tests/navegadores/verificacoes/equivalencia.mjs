// Equivalência SPO × kit: cada valor de cada opção do Personalizar, clicado pelos próprios controles nos dois lados
// (nada vai ao banco); compara atributos, variáveis e o estilo das duas prévias, elemento por elemento, e confere que
// todo valor fora do padrão muda algo no SPO. Também os presets do Contrato e o tema "Automático" com o sistema escuro.
import { abrirPerfil, abrirTela, KIT } from "../lib/ambiente.mjs";
import { fotografarPreferencias } from "../lib/medidas.mjs";
import { registrar, etapa } from "../lib/registro.mjs";
import { comTempo, LIMITE_PASSO_S, TempoEsgotado } from "../lib/tempo.mjs";

export const GRUPOS = [
  ["tema", "[data-theme-choice]", "data-theme-choice", ["light", "dark", "auto"], "light"],
  ["destaque", "[data-accent-choice]", "data-accent-choice", ["classic", "teal", "blue", "purple", "green", "yellow", "orange", "red"], "classic"],
  ["densidade", '[data-visual-pref="density"]', "data-visual-value", ["auto", "comfortable", "compact", "spacious"], "auto"],
  ["contraste", '[data-visual-pref="contrast"]', "data-visual-value", ["standard", "medium", "enhanced"], "standard"],
  ["cartoes", '[data-visual-pref="cards"]', "data-visual-value", ["soft", "outlined", "flat"], "soft"],
  ["diagramas", '[data-visual-pref="diagrams"]', "data-visual-value", ["auto", "light", "dark", "high"], "auto"],
  ["fonte-ui", '[data-spo-font-pref="ui"]', "data-spo-font-value", ["inter", "classic", "atkinson", "source", "mulish"], "inter"],
  ["fonte-dados", '[data-spo-font-pref="data"]', "data-spo-font-value", ["intel-mono", "classic", "inter", "plex-sans", "atkinson-mono"], "intel-mono"],
  ["tamanho", '[data-spo-font-pref="size"]', "data-spo-font-value", ["default", "large", "larger", "xlarge"], "default"],
  ["densidade-texto", '[data-spo-font-pref="table"]', "data-spo-font-value", ["medium", "compact", "legible"], "medium"],
  ["entrelinha", '[data-spo-font-pref="line"]', "data-spo-font-value", ["default", "ample", "larger"], "default"],
  ["leitura", '[data-spo-font-pref="reading"]', "data-spo-font-value", ["default", "medium", "strict"], "default"],
  ["daltonismo", '[data-spo-font-pref="cvd"]', "data-spo-font-value", ["default", "safe"], "default"],
  ["foco", '[data-spo-font-pref="focus"]', "data-spo-font-value", ["default", "strong"], "default"],
  ["links", '[data-spo-font-pref="links"]', "data-spo-font-value", ["default", "underline"], "default"],
  ["brilho", '[data-spo-font-pref="brightness"]', "data-spo-font-value", ["default", "reduced"], "default"],
  ["movimento", '[data-spo-font-pref="motion"]', "data-spo-font-value", ["auto", "reduce"], "auto"],
];
export const GRUPOS_BASICOS = ["tema", "destaque", "tamanho"];
// sem efeito nas prévias (conferidos pelo atributo): movimento, diagramas e daltonismo (gráficos), e o "Automático"
const SEM_ESTILO = new Set(["movimento", "diagramas", "daltonismo"]);
const PROPS = ["color", "background-color", "font-family", "font-size", "font-weight", "line-height", "letter-spacing",
  "border-top-color", "border-top-width", "border-top-left-radius", "box-shadow", "text-decoration-line", "max-width", "filter"];

const clicar = (pag, raiz, sel, attr, valor) => pag.evaluate(({ raiz, sel, attr, valor }) => {
  const r = raiz ? document.querySelector(raiz) : document;
  const b = r && r.querySelector(`${sel}[${attr}="${valor}"]`);
  if (b) b.click();
  return !!b;
}, { raiz, sel, attr, valor });

function comparar(a, b) {
  const dif = [];
  for (const k of new Set([...Object.keys(a.body), ...Object.keys(b.body)])) if (a.body[k] !== b.body[k]) dif.push(`body[${k}] ${a.body[k]} × ${b.body[k]}`);
  if ((a.html["data-visual-contract-preset"] || "") !== (b.html["data-visual-contract-preset"] || "")) dif.push("preset do html");
  if (a.escuro !== b.escuro) dif.push("tema");
  for (const [v, x] of Object.entries(a.vars)) if (x !== b.vars[v]) dif.push(`${v} ${x} × ${b.vars[v]}`);
  for (const id of Object.keys(a.cartoes)) {
    const x = a.cartoes[id], y = b.cartoes[id];
    if (!x || !y) { dif.push(`${id} falta em ${!x ? "SPO" : "kit"}`); continue; }
    // o kit copia "Componentes do SPO" sem o resumo e o "Restaurar padrão" (3 elementos do Personalizar)
    if (x.length - (id === "spo-previsualizacao" ? 3 : 0) !== y.length) { dif.push(`${id}: ${x.length} × ${y.length} elementos`); continue; }
    y.forEach((e, i) => {
      if (x[i].tag !== e.tag) dif.push(`${id}#${i} ${x[i].tag} × ${e.tag}`);
      else if (x[i].s !== e.s) {
        const sa = x[i].s.split(" | "), sb = e.s.split(" | ");
        dif.push(`${id}#${i} ${e.tag}: ` + PROPS.filter((_, j) => sa[j] !== sb[j]).map((p) => `${p} ${sa[PROPS.indexOf(p)]} × ${sb[PROPS.indexOf(p)]}`).join("; "));
      }
    });
  }
  return dif;
}

export async function equivalencia(perfis, nomesGrupos, comPresets) {
  for (const perfil of perfis) {
    const { nav, ctx, pag: spo } = await abrirPerfil(perfil, { colorScheme: "light", viewport: { width: 1366, height: 900 } });
    try {
      await abrirTela(spo, "atualizar/personalizar-spo/temas");
      const kit = await ctx.newPage();
      await kit.goto(KIT, { waitUntil: "load" });
      await kit.waitForTimeout(3000);
      for (const [grupo, sel, attr, valores, padrao] of GRUPOS.filter(([g]) => nomesGrupos.includes(g))) {
        await clicar(spo, null, sel, attr, padrao); await clicar(kit, "#kit-opcoes", sel, attr, padrao);
        await spo.waitForTimeout(450);
        const base = await spo.evaluate(fotografarPreferencias, PROPS);
        for (const valor of valores) {
          etapa(`equivalencia ${perfil}`, `${grupo} = ${valor}`);
          const { b1, b2, a, b } = await comTempo((async () => {
            const b1 = await clicar(spo, null, sel, attr, valor), b2 = await clicar(kit, "#kit-opcoes", sel, attr, valor);
            await spo.waitForTimeout(450); await kit.waitForTimeout(450);
            return { b1, b2, a: await spo.evaluate(fotografarPreferencias, PROPS), b: await kit.evaluate(fotografarPreferencias, PROPS) };
          })(), LIMITE_PASSO_S, `equivalência ${grupo} = ${valor} (${perfil})`);
          const dif = comparar(a, b);
          const semEfeito = valor !== padrao && !SEM_ESTILO.has(grupo) && !(grupo === "tema" && valor === "auto")
            && JSON.stringify([a.body, a.vars, a.cartoes, a.escuro]) === JSON.stringify([base.body, base.vars, base.cartoes, base.escuro]);
          const falhas = [];
          if (!b1 || !b2) falhas.push(`sem o controle no ${!b1 ? "SPO" : "kit"}`);
          if (semEfeito) falhas.push("o valor não muda nada no SPO");
          if (dif.length) falhas.push(`${dif.length} diferença(s) SPO × kit: ${dif.slice(0, 2).join(" || ")}`);
          registrar({ verificacao: "equivalencia", perfil, alvo: `${grupo} = ${valor}`, situacao: falhas.length ? "falha" : "ok", resumo: falhas.join("; "), detalhe: dif.slice(0, 12) });
        }
        await clicar(spo, null, sel, attr, padrao); await clicar(kit, "#kit-opcoes", sel, attr, padrao);
      }
      if (comPresets) {
        await abrirTela(spo, "atualizar/personalizar-spo/contrato-visual-protegido");
        for (const preset of ["default", "compact", "contrast", "mobile", "default"]) {
          etapa(`equivalencia ${perfil}`, `preset = ${preset}`);
          const f = (props) => { const b = document.querySelector('.contract-preset[data-contract-preset="compact"]'); const c = b && getComputedStyle(b); return { preset: document.documentElement.dataset.visualContractPreset, body: document.body.dataset.visualContractPreset, card: c ? props.map((p) => c.getPropertyValue(p)).join("|") : "" }; };
          const { a, b } = await comTempo((async () => {
            await spo.evaluate((p) => document.querySelector(`.contract-preset-grid [data-contract-preset="${p}"]`)?.click(), preset);
            await kit.evaluate((p) => document.querySelector(`#kit-opcoes [data-contract-preset="${p}"]`)?.click(), preset);
            await spo.waitForTimeout(450); await kit.waitForTimeout(450);
            return { a: await spo.evaluate(f, PROPS), b: await kit.evaluate(f, PROPS) };
          })(), LIMITE_PASSO_S, `equivalência preset = ${preset} (${perfil})`);
          const igual = JSON.stringify(a) === JSON.stringify(b);
          registrar({ verificacao: "equivalencia", perfil, alvo: `preset = ${preset}`, situacao: igual ? "ok" : "falha", resumo: igual ? "" : `SPO ${JSON.stringify(a).slice(0, 80)} × kit ${JSON.stringify(b).slice(0, 80)}` });
        }
      }
      await kit.close();
    } catch (e) {
      // um passo estourou o tempo ou deu erro: o resto deste perfil não é medido, e a bateria segue
      registrar({ verificacao: "equivalencia", perfil, alvo: "interrompida", situacao: "falha", resumo: `${String(e.message).slice(0, 160)} (o restante deste perfil não foi medido)` });
    } finally {
      await comTempo(nav.close(), 30, "fechar o navegador").catch(() => {});
    }
    if (comPresets) {
      // tema "Automático" com o sistema no escuro: os dois ficam escuros
      const { nav: n2, ctx: c2, pag: s2 } = await abrirPerfil(perfil, { colorScheme: "dark" });
      etapa(`equivalencia ${perfil}`, "tema Automático, sistema escuro");
      try {
        const { a, b } = await comTempo((async () => {
          await abrirTela(s2, "atualizar/personalizar-spo/temas");
          const k2 = await c2.newPage();
          await k2.goto(KIT, { waitUntil: "load" });
          await k2.waitForTimeout(2500);
          return { a: await s2.evaluate(() => document.body.classList.contains("theme-dark")), b: await k2.evaluate(() => document.body.classList.contains("theme-dark")) };
        })(), LIMITE_PASSO_S, `equivalência tema Automático com o sistema escuro (${perfil})`);
        registrar({ verificacao: "equivalencia", perfil, alvo: "tema Automático, sistema escuro", situacao: a && b ? "ok" : "falha", resumo: a && b ? "" : `SPO escuro=${a}, kit escuro=${b}` });
      } catch (e) {
        registrar({ verificacao: "equivalencia", perfil, alvo: "tema Automático, sistema escuro", situacao: "falha", resumo: String(e.message).slice(0, 160) });
      } finally {
        await comTempo(n2.close(), 30, "fechar o navegador").catch(() => {});
      }
    }
  }
}
