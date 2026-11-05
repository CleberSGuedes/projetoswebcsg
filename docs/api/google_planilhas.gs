// ============ CONFIGURE AQUI ============
const BASE = "https://projetoswebcsg.life";
const RELATORIO = "est-emp";   // emp | nob | ped | est-emp | fip613 | plan51
const ABA = "EST EMP";         // nome da aba onde os dados serão gravados
const FILTROS = "";            // ex.: "exercicio=2026"  (vazio = tudo que a sua chave enxerga)
// ========================================

function atualizarDados() {
  const p = PropertiesService.getScriptProperties();

  // 1) login: gera um token de 60 minutos
  const login = UrlFetchApp.fetch(BASE + "/api/auth/token", {
    method: "post", contentType: "application/json", muteHttpExceptions: true,
    payload: JSON.stringify({ client_id: p.getProperty("CLIENT_ID"), client_secret: p.getProperty("CLIENT_SECRET") }),
  });
  if (login.getResponseCode() !== 200) throw new Error("Falha no login: " + login.getContentText());
  const h = { headers: { Authorization: "Bearer " + JSON.parse(login.getContentText()).access_token }, muteHttpExceptions: true };
  const q = FILTROS ? "?" + FILTROS : "";

  // 2) só baixa se o SPO recebeu carga nova
  const meta = UrlFetchApp.fetch(BASE + "/api/v1/dados/" + RELATORIO + "/meta" + q, h);
  if (meta.getResponseCode() !== 200) throw new Error("Falha ao consultar: " + meta.getContentText());
  const carga = JSON.parse(meta.getContentText()).upload_id.join(",") + "|" + FILTROS;
  if (p.getProperty("CARGA_" + RELATORIO) === carga) { Logger.log("Sem dados novos."); return; }

  // 3) baixa o CSV e grava na aba
  const csv = UrlFetchApp.fetch(BASE + "/api/v1/dados/" + RELATORIO + "/export" + q, h);
  if (csv.getResponseCode() !== 200) throw new Error("Falha ao baixar: " + csv.getContentText());
  const dados = Utilities.parseCsv(csv.getContentText()).map((linha, i) =>
    i === 0 ? linha : linha.map((v) => (/^-?(0|[1-9]\d{0,14})(\.\d+)?$/.test(v) ? Number(v) : v))
  );
  const planilha = SpreadsheetApp.getActive();
  const aba = planilha.getSheetByName(ABA) || planilha.insertSheet(ABA);
  aba.clearContents();
  aba.getRange(1, 1, dados.length, dados[0].length).setValues(dados);
  p.setProperty("CARGA_" + RELATORIO, carga);
  Logger.log("Atualizado: " + (dados.length - 1) + " linhas.");
}
