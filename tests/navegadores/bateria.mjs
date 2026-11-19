// Bateria SPO-AA+ (tests/navegadores): o SPO e o kit em Chromium, Firefox e WebKit, em três níveis.
//   node bateria.mjs basico          (~3 min; a cada PR)
//   node bateria.mjs intermediario   (~7 min; a cada lote)
//   node bateria.mjs completo        (~30 min; antes de enviar um lote ou numa mudança grande)
//   opções: --so=telas,contraste,equivalencia,recursos,travas   (só essas verificações do nível)
// Relatório em resultados/<data-hora>/relatorio.md (+ resultados.json e capturas das falhas). Sai com código 1 se
// houver falha. Só abre telas, clica em preferências (gravadas no navegador), digita em filtros e escolhe opções:
// nada é gravado no banco. Uma tela por vez (o banco de desenvolvimento é remoto e tem poucas conexões).
import { DESKTOP, APARELHOS, RAPIDOS, TELAS_BASICAS, TELAS_PRINCIPAIS, TELAS_TODAS, BASE, garantirSessao } from "./lib/ambiente.mjs";
import { escreverRelatorio, registrar, progresso, etapa } from "./lib/registro.mjs";
import { LIMITE_SEM_PROGRESSO_MIN } from "./lib/tempo.mjs";
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
// vigia: sem nenhum resultado novo por LIMITE_SEM_PROGRESSO_MIN minutos (algo travou fora de um passo com limite),
// registra a falha dizendo onde parou, grava o relatório e sai com código 1, em vez de ficar parada em silêncio
const vigia = setInterval(() => {
  const parado = (Date.now() - progresso.ultimo) / 60000;
  if (parado < LIMITE_SEM_PROGRESSO_MIN) return;
  registrar({ verificacao: "bateria", alvo: "travou", situacao: "falha", resumo: `sem progresso há ${parado.toFixed(1)} min; parou em: ${progresso.etapa}${progresso.alvo ? " — " + progresso.alvo : ""}` });
  const rel = escreverRelatorio(nivel, inicio);
  console.log(`\nBateria interrompida pelo vigia (${LIMITE_SEM_PROGRESSO_MIN} min sem progresso). Relatório: ${rel.arquivo}`);
  process.exit(1);
}, 15000);
// o vigia segura o processo vivo de propósito (sem unref): se só sobrar uma espera parada, sem ele o Node sairia com o
// código 13, sem relatório; o clearInterval no fim libera a saída normal
etapa("sessão do usuário de teste");
await garantirSessao();
// BATERIA_TESTE_TRAVA=1: um passo que nunca termina, só para conferir o vigia (ver o README)
const passos = process.env.BATERIA_TESTE_TRAVA ? [["trava-de-teste", () => new Promise(() => {})], ...NIVEIS[nivel]] : NIVEIS[nivel];
for (const [nome, rodar] of passos) {
  if (so.length && !so.includes(nome)) continue;
  console.log(`\n== ${nome}`);
  etapa(nome);
  await rodar();
}
clearInterval(vigia);
const r = escreverRelatorio(nivel, inicio);
console.log(`\n${r.ok} ok, ${r.falhas} falha(s), ${r.avisos} aviso(s). Relatório: ${r.arquivo}`);
process.exit(r.falhas ? 1 : 0);
