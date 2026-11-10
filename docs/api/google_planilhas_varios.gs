// ============================================================
//  SPO -> Google Planilhas  (fila em cascata, carga em lotes)
// ============================================================

// ============ CONFIGURE AQUI ============
const BASE = "https://projetoswebcsg.life";

// A ORDEM É A DA CASCATA: os relatórios pequenos primeiro (ficam prontos logo)
// e os grandes por último. Um item por relatório.
//   relatorio: emp | nob | ped | est-emp | fip613 | plan51
//   aba:       nome da aba que recebe os dados (criada se não existir; um nome diferente em cada linha)
//   filtros:   "" = tudo que a chave enxerga | ex.: "exercicio=2026"
const RELATORIOS = [
  { relatorio: "est-emp", aba: "EST EMP", filtros: "" },
  { relatorio: "ped",     aba: "PED",     filtros: "" },
  { relatorio: "fip613",  aba: "FIP613",  filtros: "" },
  { relatorio: "plan51",  aba: "PLAN 51", filtros: "" },
  { relatorio: "emp",     aba: "EMP",     filtros: "exercicio=2026" },
  { relatorio: "nob",     aba: "NOB",     filtros: "exercicio=2026" },
];

// Linhas baixadas por vez. Se aparecer "Exceeded maximum execution time" com
// frequência, diminua (ex.: 5000). Não precisa passar de 20000.
const LINHAS_POR_LOTE = 10000;
// ========================================

const LIMITE_MS = 5 * 60 * 1000;       // o Google corta em 6 min: paramos antes e continuamos depois
const CICLO_VELHO_MS = 3 * 60 * 60 * 1000; // ciclo parado há mais de 3 h é descartado

// ---------- menu da planilha ----------
function onOpen() {
  SpreadsheetApp.getUi().createMenu("SPO")
    .addItem("Atualizar agora", "atualizarAgora")
    .addItem("Refazer tudo (baixar de novo)", "refazerTudo")
    .addItem("Ver andamento", "verAndamento")
    .addToUi();
}

// ---------- pontos de entrada ----------
// Agende ESTA função em Acionadores (ex.: de hora em hora): só baixa o que o SPO atualizou.
function verificarAtualizacoes() { iniciar_(false, false); }
// Menu SPO > Atualizar agora: faz a mesma verificação na hora.
function atualizarAgora() { iniciar_(false, true); }
// Menu SPO > Refazer tudo: baixa todos os relatórios de novo, mesmo sem carga nova.
function refazerTudo() { iniciar_(true, true); }

function verAndamento() {
  const p = PropertiesService.getScriptProperties();
  const ciclo = lerCiclo_();
  const linhas = RELATORIOS.map((c) => {
    const ts = p.getProperty("ULTIMA_" + c.aba);
    const quando = ts ? Utilities.formatDate(new Date(Number(ts)), Session.getScriptTimeZone(), "dd/MM HH:mm") : "nunca";
    let estado = "";
    if (ciclo) {
      const pos = ciclo.fila.findIndex((f) => RELATORIOS[f.i].aba === c.aba);
      if (pos === 0) estado = ciclo.fila[0].job ? "  (baixando: " + (ciclo.fila[0].job.escritas || 0) + " linhas)" : "  (baixando)";
      else if (pos > 0) estado = "  (na fila)";
    }
    return c.aba + ": atualizado em " + quando + estado;
  });
  const cab = ciclo ? "Atualização em andamento.\n\n" : "Nenhuma atualização em andamento.\n\n";
  SpreadsheetApp.getUi().alert("SPO", cab + linhas.join("\n"), SpreadsheetApp.getUi().ButtonSet.OK);
}

// ---------- 1) verifica o que mudou e monta a fila ----------
function iniciar_(forcar, avisar) {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(15000)) { aviso_("Já há uma atualização em andamento.", avisar); return; }
  let comecar = false;
  try {
    const atual = lerCiclo_();
    if (atual && Date.now() - atual.inicio < CICLO_VELHO_MS) {
      aviso_("Já há uma atualização em andamento (use SPO > Ver andamento).", avisar);
      return;
    }
    const p = PropertiesService.getScriptProperties();
    const h = login_();
    const fila = [];
    const erros = [];
    RELATORIOS.forEach((c, i) => {
      try {
        const carga = cargaAtual_(c, h);
        if (forcar || p.getProperty("CARGA_" + c.aba) !== carga) fila.push({ i: i, carga: carga, tentativas: 0 });
      } catch (e) {
        Logger.log("ERRO ao consultar " + c.aba + ": " + e.message);
        erros.push(c.aba + ": " + e.message);
      }
    });
    if (!fila.length) {
      aviso_(erros.length ? "Nada para baixar, com erros: " + erros.join(" | ") : "Tudo já está atualizado.", avisar);
      if (erros.length) throw new Error("Falha ao consultar -> " + erros.join(" | "));
      return;
    }
    gravarCiclo_({ inicio: Date.now(), fila: fila, erros: erros });
    aviso_("Atualização iniciada: " + fila.map((f) => RELATORIOS[f.i].aba).join(", ") + ".", avisar);
    comecar = true;
  } finally {
    lock.releaseLock();
  }
  if (comecar) continuarAtualizacao();
}

// ---------- 2) baixa em cascata e em lotes (continua sozinho em novas execuções) ----------
function continuarAtualizacao() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(15000)) return; // outra execução em andamento
  try {
    apagarGatilhos_();
    const ciclo = lerCiclo_();
    if (!ciclo) return;
    const t0 = Date.now();
    try {
      const h = login_();
      while (ciclo.fila.length) {
        const item = ciclo.fila[0];
        const c = RELATORIOS[item.i];
        let r;
        try {
          r = passo_(c, item, h, t0);
        } catch (e) {
          Logger.log("ERRO em " + c.aba + ": " + e.message);
          ciclo.erros.push(c.aba + ": " + e.message);
          descartarTemporaria_(c);
          ciclo.fila.shift();
          gravarCiclo_(ciclo);
          continue;
        }
        if (r === "pronto") ciclo.fila.shift();
        gravarCiclo_(ciclo);
        if (r === "tempo") { agendar_(); return; }
      }
    } catch (e) {
      // erro inesperado (ex.: login): descarta o ciclo para a próxima verificação recomeçar
      PropertiesService.getScriptProperties().deleteProperty("CICLO");
      throw e;
    }
    PropertiesService.getScriptProperties().deleteProperty("CICLO");
    if (ciclo.erros.length) {
      aviso_("Concluído com erros: " + ciclo.erros.join(" | "), true);
      throw new Error("Alguns relatórios falharam -> " + ciclo.erros.join(" | "));
    }
    aviso_("Atualização concluída.", true);
    Logger.log("Atualização concluída.");
  } finally {
    lock.releaseLock();
  }
}

// Um passo do relatório da vez. Devolve "pronto", "tempo" (acabou o tempo desta execução) ou "continuar".
function passo_(c, item, h, t0) {
  const planilha = SpreadsheetApp.getActive();
  if (!item.job) item.job = novoJob_(c, planilha);
  const job = item.job;
  const p = PropertiesService.getScriptProperties();

  if (job.fase === "baixando") {
    const tmp = planilha.getSheetByName(job.tmp);
    if (!tmp) throw new Error("a aba temporária '" + job.tmp + "' foi removida durante a atualização");
    for (;;) {
      if (acabouTempo_(t0, job.ultimaMs)) return "tempo";
      const ini = Date.now();
      const q = (c.filtros ? c.filtros + "&" : "") + "limit=" + LINHAS_POR_LOTE + (job.after ? "&after_id=" + job.after : "");
      const resp = buscar_(BASE + "/api/v1/dados/" + c.relatorio + "/export?" + q, h);

      // a carga mudou no meio do caminho? (linha de outra carga, ou contagem diferente)
      const ups = cabecalho_(resp, "X-Upload-Id").split(",").filter(Boolean);
      if (!job.uploads) job.uploads = ups.join(",");
      const conhecidos = job.uploads.split(",");
      if (ups.some((u) => conhecidos.indexOf(u) < 0)) return reiniciar_(c, item, "o SPO recebeu carga nova durante a baixa", h);

      const dados = Utilities.parseCsv(resp.getContentText());
      const cab = dados.shift();
      if (!job.colunas) {
        job.colunas = cab.length;
        const total = parseInt(cabecalho_(resp, "X-Total"), 10);
        job.total = isNaN(total) ? null : total;
        garantirGrade_(tmp, 1, job.colunas);
        tmp.getRange(1, 1, 1, job.colunas).setValues([cab]);
      }
      if (dados.length) {
        const linhas = dados.map(converterLinha_);
        garantirGrade_(tmp, job.escritas + 1 + linhas.length, job.colunas);
        tmp.getRange(job.escritas + 2, 1, linhas.length, job.colunas).setValues(linhas);
        job.escritas += linhas.length;
      }
      job.after = cabecalho_(resp, "X-Next-After-Id");
      job.ultimaMs = Date.now() - ini;
      gravarItem_(item);
      if (!job.after || !dados.length) break;
    }
    if (job.total !== null && job.escritas !== job.total) return reiniciar_(c, item, "contagem difere (" + job.escritas + " x " + job.total + ")", h);
    if (cargaAtual_(c, h) !== item.carga) return reiniciar_(c, item, "o SPO recebeu carga nova durante a baixa", h);
    job.fase = "trocando";
    job.copiadas = 0;
    gravarItem_(item);
  }

  if (job.fase === "trocando") {
    // só agora a aba final é substituída (de uma vez), para o usuário nunca vê-la pela metade por muito tempo
    const tmp = planilha.getSheetByName(job.tmp);
    if (!tmp) throw new Error("a aba temporária '" + job.tmp + "' foi removida durante a atualização");
    let dest = planilha.getSheetByName(c.aba);
    if (!dest) dest = planilha.insertSheet(c.aba);
    const total = job.escritas + 1; // com o cabeçalho
    if (job.copiadas === 0) dest.clearContents();
    garantirGrade_(dest, total, job.colunas);
    while (job.copiadas < total) {
      if (acabouTempo_(t0, job.ultimaMs)) return "tempo";
      const ini = Date.now();
      const n = Math.min(20000, total - job.copiadas);
      tmp.getRange(job.copiadas + 1, 1, n, job.colunas)
        .copyTo(dest.getRange(job.copiadas + 1, 1), SpreadsheetApp.CopyPasteType.PASTE_VALUES, false);
      job.copiadas += n;
      job.ultimaMs = Date.now() - ini;
      gravarItem_(item);
    }
    planilha.deleteSheet(tmp);
    p.setProperty("CARGA_" + c.aba, item.carga);
    p.setProperty("ULTIMA_" + c.aba, String(Date.now()));
    Logger.log(c.aba + ": " + job.escritas + " linhas.");
    return "pronto";
  }
  throw new Error("estado desconhecido: " + job.fase);
}

function reiniciar_(c, item, motivo, h) {
  item.tentativas = (item.tentativas || 0) + 1;
  Logger.log(c.aba + ": " + motivo + " -> recomeçando (tentativa " + item.tentativas + ").");
  descartarTemporaria_(c);
  if (item.tentativas > 3) throw new Error(motivo + "; desisti após 3 tentativas");
  item.job = null;
  item.carga = cargaAtual_(c, h);
  gravarItem_(item);
  return "continuar";
}

// ---------- utilidades ----------
function login_() {
  const p = PropertiesService.getScriptProperties();
  const r = UrlFetchApp.fetch(BASE + "/api/auth/token", {
    method: "post", contentType: "application/json", muteHttpExceptions: true,
    payload: JSON.stringify({ client_id: p.getProperty("CLIENT_ID"), client_secret: p.getProperty("CLIENT_SECRET") }),
  });
  if (r.getResponseCode() !== 200) throw new Error("Falha no login: " + r.getContentText());
  return { headers: { Authorization: "Bearer " + JSON.parse(r.getContentText()).access_token }, muteHttpExceptions: true };
}

// Identifica a carga do SPO + o filtro: se não mudou, não há o que baixar.
function cargaAtual_(c, h) {
  const meta = buscar_(BASE + "/api/v1/dados/" + c.relatorio + "/meta" + (c.filtros ? "?" + c.filtros : ""), h);
  return JSON.parse(meta.getContentText()).upload_id.join(",") + "|" + c.filtros;
}

// Uma nova tentativa em caso de falha de rede ou erro 5xx.
function buscar_(url, h) {
  let ultimo = "";
  for (let t = 1; t <= 2; t++) {
    try {
      const r = UrlFetchApp.fetch(url, h);
      const cod = r.getResponseCode();
      if (cod === 200) return r;
      ultimo = cod + " " + String(r.getContentText()).slice(0, 200);
      if (cod < 500) break; // 4xx: repetir não adianta
    } catch (e) {
      ultimo = e.message;
    }
    if (t === 1) Utilities.sleep(3000);
  }
  throw new Error(ultimo);
}

function cabecalho_(resp, nome) {
  const hs = resp.getHeaders();
  const k = Object.keys(hs).filter((x) => x.toLowerCase() === nome.toLowerCase())[0];
  return k ? String(hs[k]) : "";
}

// Números chegam como texto com ponto decimal; vira número, exceto códigos com zero à esquerda ou muito longos.
function converterLinha_(linha) {
  return linha.map((v) => (/^-?(0|[1-9]\d{0,14})(\.\d+)?$/.test(v) ? Number(v) : v));
}

function acabouTempo_(t0, ultimaMs) {
  const gasto = Date.now() - t0;
  if (gasto < 10000) return false; // sempre dá ao menos um passo por execução
  return gasto + 1.5 * (ultimaMs || 0) > LIMITE_MS;
}

function garantirGrade_(aba, linhas, colunas) {
  if (aba.getMaxRows() < linhas) aba.insertRowsAfter(aba.getMaxRows(), linhas - aba.getMaxRows());
  if (aba.getMaxColumns() < colunas) aba.insertColumnsAfter(aba.getMaxColumns(), colunas - aba.getMaxColumns());
}

function novoJob_(c, planilha) {
  const nome = c.aba + " (novo)";
  const velha = planilha.getSheetByName(nome);
  if (velha) planilha.deleteSheet(velha);
  const tmp = planilha.insertSheet(nome);
  tmp.hideSheet();
  return { fase: "baixando", tmp: nome, after: "", escritas: 0, total: null, uploads: "", colunas: 0, ultimaMs: 0, copiadas: 0 };
}

function descartarTemporaria_(c) {
  try {
    const planilha = SpreadsheetApp.getActive();
    const tmp = planilha.getSheetByName(c.aba + " (novo)");
    if (tmp) planilha.deleteSheet(tmp);
  } catch (e) { Logger.log("não consegui apagar a aba temporária de " + c.aba + ": " + e.message); }
}

function lerCiclo_() {
  const t = PropertiesService.getScriptProperties().getProperty("CICLO");
  return t ? JSON.parse(t) : null;
}
function gravarCiclo_(ciclo) { PropertiesService.getScriptProperties().setProperty("CICLO", JSON.stringify(ciclo)); }
// grava o andamento do item da vez sem perder o resto do ciclo
function gravarItem_(item) {
  const ciclo = lerCiclo_();
  if (ciclo && ciclo.fila.length) { ciclo.fila[0] = item; gravarCiclo_(ciclo); }
}

function agendar_() { ScriptApp.newTrigger("continuarAtualizacao").timeBased().after(60 * 1000).create(); }
function apagarGatilhos_() {
  ScriptApp.getProjectTriggers().forEach((g) => {
    if (g.getHandlerFunction() === "continuarAtualizacao") ScriptApp.deleteTrigger(g);
  });
}

function aviso_(msg, avisar) {
  Logger.log(msg);
  if (!avisar) return;
  try { SpreadsheetApp.getActive().toast(msg, "SPO", 10); } catch (e) { /* sem tela ativa */ }
}
