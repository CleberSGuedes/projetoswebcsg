// Bateria SPO-AA+ — tempos-limite. Algumas chamadas do Playwright não têm limite próprio (page.evaluate, por
// exemplo): se o navegador para de responder, a bateria ficaria parada em silêncio (aconteceu uma vez no Firefox,
// no completo, em 17/11). Daqui em diante cada passo tem um limite e falha com mensagem.

// limite de cada passo (uma tela, um valor do Personalizar, uma checagem de recurso), em segundos
export const LIMITE_PASSO_S = Number(process.env.BATERIA_LIMITE_PASSO_S || 180);
// sem nenhum resultado registrado por este tempo, a bateria para, grava o relatório e sai com falha (minutos)
export const LIMITE_SEM_PROGRESSO_MIN = Number(process.env.BATERIA_LIMITE_SEM_PROGRESSO_MIN || 6);

export class TempoEsgotado extends Error {}

// corre a promessa contra o relógio; ao estourar, rejeita com uma mensagem que diz o que estava rodando
export function comTempo(promessa, segundos, oQue) {
  let relogio;
  const limite = new Promise((_, rejeitar) => {
    relogio = setTimeout(() => rejeitar(new TempoEsgotado(`passou de ${segundos} s sem terminar: ${oQue}`)), segundos * 1000);
  });
  return Promise.race([Promise.resolve(promessa), limite]).finally(() => clearTimeout(relogio));
}
