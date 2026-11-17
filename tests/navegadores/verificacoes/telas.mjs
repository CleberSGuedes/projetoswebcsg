// Varredura de telas: abre cada tela pelo menu, uma por vez, e mede rolagem lateral, fora da tela, texto cortado,
// alvos < 24 px, avisos abertos sozinhos e erros de script.
import { abrirPerfil, abrirTela, nomeDaTela } from "../lib/ambiente.mjs";
import { medirTela } from "../lib/medidas.mjs";
import { registrar, capturar } from "../lib/registro.mjs";

export async function telas(perfis, rotas) {
  for (const perfil of perfis) {
    const { nav, pag, erros } = await abrirPerfil(perfil);
    try {
      for (const rota of rotas) {
        const alvo = nomeDaTela(rota);
        const antes = erros.length;
        try {
          await abrirTela(pag, rota);
          const m = await pag.evaluate(medirTela);
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
          registrar({ verificacao: "telas", perfil, alvo, situacao: "falha", resumo: String(e.message).slice(0, 160), captura: await capturar(pag, `telas-${perfil}-${alvo}-erro`) });
        }
      }
    } finally {
      await nav.close();
    }
  }
}
