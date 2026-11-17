// Contraste do texto em combinações de preferências (baterias 1–3 do AA+): reprova abaixo do AA (4,5:1; 3:1 grande);
// avisa abaixo da meta AA+ (7:1; 4,5:1 grande).
import { abrirPerfil, abrirTela, gravarPreferencias, nomeDaTela } from "../lib/ambiente.mjs";
import { medirContraste } from "../lib/medidas.mjs";
import { registrar, capturar } from "../lib/registro.mjs";

// combinações exigentes: o Amarelo no escuro e o Azul no claro; contraste reforçado; fontes, tamanho e leitura AAA
export const PREFERENCIAS = {
  "claro-azul": { tema: "light", destaque: "blue" },
  "escuro-amarelo-reforcado": { tema: "dark", destaque: "yellow", comuns: { contrast: "enhanced", diagrams: "high" }, fontes: { focus: "strong" } },
  "claro-atkinson-150-leitura": { tema: "light", destaque: "blue", fontes: { ui: "atkinson", size: "xlarge", line: "larger", reading: "strict", cvd: "safe", focus: "strong", links: "underline", brightness: "reduced", motion: "reduce" } },
  "escuro-azul-amplo": { tema: "dark", destaque: "blue", comuns: { density: "spacious", cards: "outlined" }, fontes: { ui: "mulish", data: "atkinson-mono", table: "legible", cvd: "safe", links: "underline", motion: "reduce" } },
  "claro-roxo-compacto": { tema: "light", destaque: "purple", comuns: { density: "compact", contrast: "medium", cards: "flat" }, fontes: { ui: "source", size: "large", table: "compact", data: "plex-sans" } },
};

export async function contraste(perfis, rotas, nomesPreferencias) {
  for (const perfil of perfis) {
    for (const nomePref of nomesPreferencias) {
      const { nav, ctx, pag } = await abrirPerfil(perfil);
      await gravarPreferencias(ctx, PREFERENCIAS[nomePref]);
      try {
        for (const rota of rotas) {
          const alvo = `${nomeDaTela(rota)} [${nomePref}]`;
          try {
            await abrirTela(pag, rota);
            await pag.waitForTimeout(400); // fim das transições de cor
            const m = await pag.evaluate(medirContraste, null);
            const ex = (a) => a.map(([x, t, c]) => `${x} «${t}» ${c}`).join(", ");
            if (m.abaixoAA) {
              registrar({ verificacao: "contraste", perfil, alvo, situacao: "falha", resumo: `${m.abaixoAA} texto(s) abaixo do AA: ${ex(m.abaixoAAexemplos)}`, captura: await capturar(pag, `contraste-${perfil}-${nomeDaTela(rota)}-${nomePref}`), medida: m });
            } else if (m.abaixoAAmais) {
              registrar({ verificacao: "contraste", perfil, alvo, situacao: "aviso", resumo: `${m.abaixoAAmais} de ${m.textos} abaixo da meta AA+ (7:1): ${ex(m.abaixoAAmaisExemplos)}`, medida: m });
            } else {
              registrar({ verificacao: "contraste", perfil, alvo, situacao: "ok", resumo: `${m.textos} textos ≥ 7:1` });
            }
          } catch (e) {
            registrar({ verificacao: "contraste", perfil, alvo, situacao: "falha", resumo: String(e.message).slice(0, 160) });
          }
        }
      } finally {
        await nav.close();
      }
    }
  }
}
