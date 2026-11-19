// Varredura de telas: abre cada tela pelo menu, uma por vez, e mede rolagem lateral, fora da tela, texto cortado,
// alvos < 24 px, avisos abertos sozinhos e erros de script. Cada tela tem um tempo-limite (lib/tempo.mjs): se
// estourar, a falha é registrada e o navegador é reaberto para a tela seguinte.
import { abrirPerfil, abrirTela, nomeDaTela } from "../lib/ambiente.mjs";
import { medirTela } from "../lib/medidas.mjs";
import { registrar, capturar, etapa } from "../lib/registro.mjs";
import { comTempo, LIMITE_PASSO_S, TempoEsgotado } from "../lib/tempo.mjs";

export async function telas(perfis, rotas) {
  for (const perfil of perfis) {
    let sessao = await abrirPerfil(perfil);
    try {
      for (const rota of rotas) {
        const alvo = nomeDaTela(rota);
        etapa(`telas ${perfil}`, alvo);
        const { pag, erros } = sessao;
        const antes = erros.length;
        try {
          const m = await comTempo((async () => { await abrirTela(pag, rota); return pag.evaluate(medirTela); })(), LIMITE_PASSO_S, `tela ${alvo} (${perfil})`);
          const errosTela = erros.slice(antes);
          const falhas = [];
          if (m.rolagemLateral) falhas.push(`rolagem lateral ${m.rolagemLateral}px`);
          if (m.foraDaTela) falhas.push(`fora da tela: ${m.foraExemplos.join(", ")}`);
          if (m.textoCortado) falhas.push(`texto cortado: ${m.cortadoExemplos.join(", ")}`);
          if (m.alvosMenor24) falhas.push(`alvos < 24 px: ${m.alvo24Exemplos.join(", ")}`);
          if (m.avisosAbertos.length) falhas.push(`aviso aberto sozinho: ${m.avisosAbertos.join(", ")}`);
          if (errosTela.length) falhas.push(`erros de script: ${errosTela.slice(0, 2).join(" | ")}`);
          const captura = falhas.length ? await capturar(pag, `telas-${perfil}-${alvo}`) : null;
          registrar({ verificacao: "telas", perfil, alvo, situacao: falhas.length ? "falha" : "ok", resumo: falhas.join("; ") || `${m.janela.join("×")}`, captura, medida: m });
          if (m.avisosAbertos.length) await pag.keyboard.press("Escape").catch(() => {});
        } catch (e) {
          registrar({ verificacao: "telas", perfil, alvo, situacao: "falha", resumo: String(e.message).slice(0, 160), captura: e instanceof TempoEsgotado ? null : await capturar(pag, `telas-${perfil}-${alvo}-erro`) });
          if (e instanceof TempoEsgotado) {
            // o navegador pode ter travado: fecha (com limite) e reabre para a tela seguinte
            await comTempo(sessao.nav.close(), 30, "fechar o navegador").catch(() => {});
            sessao = await abrirPerfil(perfil);
          }
        }
      }
    } finally {
      await comTempo(sessao.nav.close(), 30, "fechar o navegador").catch(() => {});
    }
  }
}
