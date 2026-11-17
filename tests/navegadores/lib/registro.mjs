// Bateria SPO-AA+ — registro dos resultados e relatório (Markdown + JSON) em resultados/<data-hora>/.
import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { PASTA } from "./ambiente.mjs";

const carimbo = new Date().toISOString().slice(0, 16).replace(/[:T]/g, "-");
export const PASTA_RESULTADO = resolve(PASTA, "resultados", carimbo);
mkdirSync(resolve(PASTA_RESULTADO, "capturas"), { recursive: true });

export const resultados = [];
// situacao: "ok" | "falha" | "aviso" (aviso = abaixo da meta AA+ mas dentro do mínimo, ou algo a olhar sem reprovar)
export function registrar(r) {
  resultados.push({ ...r, quando: new Date().toISOString() });
  const marca = { ok: "ok   ", falha: "FALHA", aviso: "aviso" }[r.situacao] || r.situacao;
  console.log(`${marca} ${r.verificacao.padEnd(14)} ${(r.perfil || "").padEnd(24)} ${(r.alvo || "").padEnd(40)} ${r.resumo || ""}`);
}
export async function capturar(pag, nome) {
  const arq = resolve(PASTA_RESULTADO, "capturas", nome.replace(/[^\w.-]+/g, "_") + ".png");
  try { await pag.screenshot({ path: arq }); return arq; } catch (e) { return null; }
}

export function escreverRelatorio(nivel, inicio) {
  const conta = (s) => resultados.filter((r) => r.situacao === s).length;
  const porVerificacao = {};
  for (const r of resultados) {
    const v = (porVerificacao[r.verificacao] ||= { ok: 0, falha: 0, aviso: 0 });
    v[r.situacao] = (v[r.situacao] || 0) + 1;
  }
  const minutos = ((Date.now() - inicio) / 60000).toFixed(1);
  const linhas = [
    `# Bateria SPO-AA+ — nível ${nivel}`,
    "",
    `${new Date().toLocaleString("pt-BR")} · ${minutos} min · ${resultados.length} verificações: **${conta("ok")} ok**, **${conta("falha")} falha(s)**, ${conta("aviso")} aviso(s).`,
    "",
    "| Verificação | ok | falha | aviso |",
    "|---|---|---|---|",
    ...Object.entries(porVerificacao).map(([v, n]) => `| ${v} | ${n.ok} | ${n.falha} | ${n.aviso} |`),
    "",
  ];
  const problemas = resultados.filter((r) => r.situacao !== "ok");
  if (problemas.length) {
    linhas.push("## Falhas e avisos", "", "| Situação | Verificação | Perfil | Alvo | Detalhe |", "|---|---|---|---|---|");
    for (const r of problemas) linhas.push(`| ${r.situacao} | ${r.verificacao} | ${r.perfil || ""} | ${r.alvo || ""} | ${String(r.resumo || "").replace(/\|/g, "/")}${r.captura ? " (captura)" : ""} |`);
  } else {
    linhas.push("Nenhuma falha e nenhum aviso.");
  }
  writeFileSync(resolve(PASTA_RESULTADO, "relatorio.md"), linhas.join("\n") + "\n");
  writeFileSync(resolve(PASTA_RESULTADO, "resultados.json"), JSON.stringify({ nivel, inicio: new Date(inicio).toISOString(), resultados }, null, 1));
  return { ok: conta("ok"), falhas: conta("falha"), avisos: conta("aviso"), arquivo: resolve(PASTA_RESULTADO, "relatorio.md") };
}
