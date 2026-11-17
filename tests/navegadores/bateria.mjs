// Bateria SPO-AA+ (tests/navegadores): o SPO e o kit em Chromium, Firefox e WebKit, em três níveis.
//   node bateria.mjs basico          (~3 min; a cada PR)
//   node bateria.mjs intermediario   (~7 min; a cada lote)
//   node bateria.mjs completo        (~30 min; antes de enviar um lote ou numa mudança grande)
//   opções: --so=telas,contraste,equivalencia,recursos,travas   (só essas verificações do nível)
// Relatório em resultados/<data-hora>/relatorio.md (+ resultados.json e capturas das falhas). Sai com código 1 se
// houver falha. Só abre telas, clica em preferências (gravadas no navegador), digita em filtros e escolhe opções:
// nada é gravado no banco. Uma tela por vez (o banco de desenvolvimento é remoto e tem poucas conexões).
import { DESKTOP, APARELHOS, RAPIDOS, TELAS_BASICAS, TELAS_PRINCIPAIS, TELAS_TODAS, BASE, garantirSessao } from "./lib/ambiente.mjs";
import { escreverRelatorio } from "./lib/registro.mjs";
import { travas } from "./verificacoes/travas.mjs";
import { telas } from "./verificacoes/telas.mjs";
import { contraste, PREFERENCIAS } from "./verificacoes/contraste.mjs";
import { equivalencia, GRUPOS, GRUPOS_BASICOS } from "./verificacoes/equivalencia.mjs";
import { recursos, teclas } from "./verificacoes/recursos.mjs";

const TODOS_GRUPOS = GRUPOS.map(([g]) => g);
const TELAS_CONTRASTE = ["", "atualizar/personalizar-spo/temas", "relatorios/fip613", "cadastrar/dotacao/formulario",
  "atualizar/governanca-resultados/mapa", "paineis-dashboards/teto-orcamentario", "institucional/diretrizes"];

const NIVEIS = {
  basico: [
    ["travas", () => travas()],
    ["telas", () => telas([...DESKTOP, "M-s26-360"], TELAS_BASICAS)],
    ["equivalencia", () => equivalencia(["D-chromium-1366"], GRUPOS_BASICOS, false)],
  ],
  intermediario: [
    ["travas", () => travas()],
    ["telas", () => telas([...DESKTOP, "M-s26-360", "M-iphone16-393", "T-ipad11-834"], TELAS_PRINCIPAIS)],
    ["equivalencia", () => equivalencia(["D-chromium-1366"], TODOS_GRUPOS, true)],
    ["contraste", () => contraste(["D-chromium-1366"], TELAS_CONTRASTE.slice(0, 4), ["claro-azul", "escuro-amarelo-reforcado"])],
    ["recursos", async () => { await recursos(["D-chromium-1366", "M-s26-360", "M-iphone16-393"]); await teclas("D-chromium-1366"); }],
  ],
  completo: [
    ["travas", () => travas()],
    ["telas", async () => {
      await telas(["D-chromium-1366", "D-firefox-1366", "M-s26-360", "M-iphone16-393"], ["", ...TELAS_TODAS]);
      await telas(APARELHOS.filter((p) => !["D-chromium-1366", "D-firefox-1366", "M-s26-360", "M-iphone16-393"].includes(p)), TELAS_PRINCIPAIS);
      await telas(RAPIDOS, TELAS_BASICAS);
    }],
    ["equivalencia", () => equivalencia(DESKTOP, TODOS_GRUPOS, true)],
    ["contraste", () => contraste(["D-chromium-1366", "M-s26-360"], TELAS_CONTRASTE, Object.keys(PREFERENCIAS))],
    ["recursos", async () => { await recursos(APARELHOS); for (const p of DESKTOP) await teclas(p); }],
  ],
};

const nivel = process.argv[2];
if (!NIVEIS[nivel]) {
  console.log("uso: node bateria.mjs basico|intermediario|completo [--so=telas,contraste,equivalencia,recursos,travas]");
  process.exit(2);
}
const so = (process.argv.find((a) => a.startsWith("--so=")) || "").slice(5).split(",").filter(Boolean);
const inicio = Date.now();
console.log(`Bateria SPO-AA+, nível ${nivel}, em ${BASE}`);
await garantirSessao();
for (const [nome, rodar] of NIVEIS[nivel]) {
  if (so.length && !so.includes(nome)) continue;
  console.log(`\n== ${nome}`);
  await rodar();
}
const r = escreverRelatorio(nivel, inicio);
console.log(`\n${r.ok} ok, ${r.falhas} falha(s), ${r.avisos} aviso(s). Relatório: ${r.arquivo}`);
process.exit(r.falhas ? 1 : 0);
