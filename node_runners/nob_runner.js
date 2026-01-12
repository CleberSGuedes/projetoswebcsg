const path = require("path");
const fs = require("fs");
const ExcelJS = require("exceljs");
const { connect, bulkInsert } = require("./db");
const {
  ensureDir,
  cleanHistorico,
  corrigirCaracteres,
  normalizeSimple,
  parseBrNumber,
  formatNumberPtBr,
  formatDatePtBr,
  parseDataDb,
  parseValorDb,
  updateStatusFields,
  readCancelFlag,
} = require("./util");


const OUTPUT_HEADER_MAP = {
  "n nob": "N\u00ba NOB",
  "n nob estorno estornado": "N\u00ba NOB Estorno/Estornado",
  "n liq": "N\u00ba LIQ",
  "n emp": "N\u00ba EMP",
  "n ped": "N\u00ba PED",
  "devolucao gcv": "Devolu\u00e7\u00e3o GCV",
  "dotacao orcamentaria": "Dota\u00e7\u00e3o Or\u00e7ament\u00e1ria",
  "exercicio": "Exerc\u00edcio",
  "situacao": "Situa\u00e7\u00e3o",
  "situacao obn": "Situa\u00e7\u00e3o OBN",
  "situacao funcional": "Situa\u00e7\u00e3o Funcional",
  "tipo de vinculo": "Tipo de V\u00ednculo",
  "data credito": "Data Cr\u00e9dito",
  "data ocorrencia": "Data de Ocorr\u00eancia",
  "periodo competencia": "Per\u00edodo/Compet\u00eancia",
};

function normalizeHeaderKey(label) {
  let key = normalizeSimple(label);
  key = key.replace(/\b(n|no)\b/g, "numero");
  key = key.replace(/\bnumero\b/g, "numero");
  key = key.replace(/[^a-z0-9]+/g, " ").trim();
  key = key.replace(/^numero\s+/, "n ");
  return key;
}

function fixOutputHeader(label) {
  const key = normalizeHeaderKey(label);
  return OUTPUT_HEADER_MAP[key] || label;
}
const BATCH_SIZE = 1000;
const INPUT_DIR = path.resolve(__dirname, "..", "upload", "nob");
const OUTPUT_DIR = path.resolve(__dirname, "..", "outputs", "td_nob");
const SAVE_NOB_SHEET = false;

const COLUMN_ALIASES = {
  "n nob": "N\u00ba NOB",
  "n nob estorno estornado": "N\u00ba NOB Estorno/Estornado",
  "n liq": "N\u00ba LIQ",
  "n emp": "N\u00ba EMP",
  "n ped": "N\u00ba PED",
  "devolucao gcv": "Devolu\u00e7\u00e3o GCV",
  "dotacao orcamentaria": "Dota\u00e7\u00e3o Or\u00e7ament\u00e1ria",
  "exercicio": "Exerc\u00edcio",
  "situacao": "Situa\u00e7\u00e3o",
  "situacao obn": "Situa\u00e7\u00e3o OBN",
  "situacao funcional": "Situa\u00e7\u00e3o Funcional",
  "tipo de vinculo": "Tipo de V\u00ednculo",
  "data credito": "Data Cr\u00e9dito",
  "data ocorrencia": "Data de Ocorr\u00eancia",
  "periodo competencia": "Per\u00edodo/Compet\u00eancia",
  "descricao do tipo de ob": "Descri\u00e7\u00e3o do Tipo de OB",
  "observacao obn": "Observa\u00e7\u00e3o OBN",
  "data debito": "Data D\u00e9bito",
  "usuario inclusao": "Usu\u00e1rio (Inclus\u00e3o)",
  "modalidade de licitacao": "Modalidade de Licita\u00e7\u00e3o",
  "numero licitacao": "N\u00famero Licita\u00e7\u00e3o",
  "ano da licitacao": "Ano da Licita\u00e7\u00e3o",
  "elemento": "Elemento",
  "nome do elemento da despesa": "Nome do Elemento da Despesa",
  "fonte": "Fonte",
  "historico liq": "Hist\u00f3rico LIQ",
  "numero c c dv debito": "N\u00ba C/C+DV (D\u00e9bito)",
  "numero c c dv credito": "N\u00ba C/C+DV (Cr\u00e9dito)",
  "numero conta str credito": "N\u00ba Conta STR (Cr\u00e9dito)",
  "banco debito": "Banco (D\u00e9bito)",
  "banco credito": "Banco (Cr\u00e9dito)",
  "agencia debito": "Ag\u00eancia (D\u00e9bito)",
  "agencia credito": "Ag\u00eancia (Cr\u00e9dito)",
  "cartao pagto governo": "N\u00ba Cart\u00e3o Pagto Governo",
  "conta cartao": "N\u00ba Conta Cart\u00e3o",
  "convenio ingresso": "N\u00ba Conv\u00eanio (Ingresso)",
  "convenio repasse": "N\u00ba Conv\u00eanio (Repasse)",
  "autenticacao bancaria": "N\u00ba Autentica\u00e7\u00e3o Banc\u00e1ria",
  "referencia": "N\u00ba Refer\u00eancia",
};

const COLUMNS_TO_DROP = [
  "UO Extinta",
  "N\u00ba Proc Orc Pagto",
  "N\u00ba Proc Fin Pagto",
  "N\u00ba PAC",
  "N\u00ba NOBLIST",
  "Tipo Pagto",
  "CBA",
  "Natureza",
  "Banco (D\u00e9bito)",
  "Ag\u00eancia (D\u00e9bito)",
  "N\u00ba C/C+DV (D\u00e9bito)",
  "Subconta",
  "Forma de Recebimento",
  "Banco (Cr\u00e9dito)",
  "Ag\u00eancia (Cr\u00e9dito)",
  "N\u00ba C/C+DV (Cr\u00e9dito)",
  "N\u00ba Conta STR (Cr\u00e9dito)",
  "DEPJU (Cr\u00e9dito)",
  "Identificador DEPJU (Cr\u00e9dito)",
  "Nome do Ordenador de Despesa",
  "Nome do Liberador de Pagamento",
  "Situa\u00e7\u00e3o",
  "REG",
  "Per\u00edodo/Compet\u00eancia",
  "UO SEAP",
  "Exe Anterior Folha",
  "Exerc\u00edcio da Folha",
  "M\u00eas da Folha",
  "Tipo de Folha",
  "N\u00ba Folha",
  "Situa\u00e7\u00e3o Funcional",
  "Tipo de V\u00ednculo",
  "Indicativo de NOB/Fatura Fato 54",
  "Tipo de Transmiss\u00e3o",
  "Transmiss\u00e3o",
  "Situa\u00e7\u00e3o OBN",
  "Tipo OB",
  "Descri\u00e7\u00e3o do Tipo de OB",
  "N\u00ba RE OBN",
  "N\u00ba Lote OBN",
  "Data de Ocorr\u00eancia",
  "N\u00ba Retorno OBN",
  "Retorno OBN",
  "Retorno CNAB240",
  "Observa\u00e7\u00e3o OBN",
  "Data Cr\u00e9dito",
  "N\u00ba NEX(s)",
  "Valor LIQ",
  "N\u00ba Conv\u00eanio (Ingresso)",
  "N\u00ba Conv\u00eanio (Repasse)",
  "Modalidade de Licita\u00e7\u00e3o",
  "N\u00famero Licita\u00e7\u00e3o",
  "Ano da Licita\u00e7\u00e3o",
  "Fundamento Legal",
  "Entrega Imediata",
  "NEX/NOB/OBF na RE",
  "Tipo de Fatura",
  "Subtipo de Fatura",
  "Tributo Federal",
  "N\u00ba Refer\u00eancia",
  "Valor da Fatura",
  "Valor da Multa",
  "Valor dos Juros/Encargos",
  "N\u00ba Autentica\u00e7\u00e3o Banc\u00e1ria",
  "CPF/CNPJ do Credor (Premia\u00e7\u00e3o - Nota MT)",
  "Nome do Credor (Premia\u00e7\u00e3o - Nota MT)",
  "N\u00ba Proc Judicial RPV",
  "N\u00ba ABJ",
  "N\u00ba Proc Sequestro Judicial",
  "N\u00ba Emenda (EP)",
  "Autor da Emenda (EP)",
  "Quantidade de Dias/Efici\u00eancia",
  "N\u00edvel de Efici\u00eancia",
  "Justificativa (Altera\u00e7\u00e3o da Ordem Cronol\u00f3gica)",
  "N\u00ba RDR",
  "N\u00ba RDE",
  "N\u00ba CAD",
  "N\u00ba Cart\u00e3o Pagto Governo",
  "N\u00ba Conta Cart\u00e3o",
  "Situa\u00e7\u00e3o VIPF",
  "N\u00ba Lote VIPF",
  "Data Ocorr\u00eancia VIPF",
  "N\u00ba Arquivo Retorno VIPF",
  "Data Retorno VIPF",
  "Cod Retorno VIPF",
  "Observa\u00e7\u00e3o VIPF",
  "Tipo de Transmissao",
  "Data D\u00e9bito",
  "Usu\u00e1rio (Inclus\u00e3o)",
  "N\u00ba DAR Virtual (Registros Intraor\u00e7ament\u00e1rios)",
];

function formatDateOutput(value) {
  if (value instanceof Date) {
    const hh = String(value.getHours()).padStart(2, "0");
    const mm = String(value.getMinutes()).padStart(2, "0");
    const ss = String(value.getSeconds()).padStart(2, "0");
    const hasTime = hh !== "00" || mm !== "00" || ss !== "00";
    const dateStr = formatDatePtBr(value);
    return hasTime ? `${dateStr} ${hh}:${mm}:${ss}` : dateStr;
  }
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value)) return formatDatePtBr(value);
  return value;
}

const REQUIRED_COLS_RAW = [
  "N\u00ba NOB",
  "N\u00ba NOB Estorno/Estornado",
  "N\u00ba LIQ",
  "N\u00ba EMP",
  "N\u00ba PED",
  "Valor NOB",
  "Devolu\u00e7\u00e3o GCV",
  "Data NOB",
  "Data Cadastro NOB",
  "Data/Hora de Cadastro da LIQ",
  "Dota\u00e7\u00e3o Or\u00e7ament\u00e1ria",
  "Natureza de Despesa",
  "Nome da Fonte de Recurso",
  "Exerc\u00edcio",
  "UG",
  "UO",
  "Nome do Credor Principal",
  "CPF/CNPJ do Credor Principal",
  "Credor",
  "Nome do Credor",
  "CPF/CNPJ do Credor",
  "Hist\u00f3rico LIQ",
  "Elemento",
  "Nome do Elemento da Despesa",
  "Fonte",
];

const COL_MAP = {
  exercicio: "exercicio",
  n_nob: "numero_nob",
  no_nob: "numero_nob",
  n_nob_estorno_estornado: "numero_nob_estorno",
  no_nob_estorno_estornado: "numero_nob_estorno",
  n_liq: "numero_liq",
  no_liq: "numero_liq",
  n_emp: "numero_emp",
  no_emp: "numero_emp",
  n_ped: "numero_ped",
  no_ped: "numero_ped",
  valor_nob: "valor_nob",
  devolucao_gcv: "devolucao_gcv",
  valor_nob_gcv: "valor_nob_gcv",
  data_nob: "data_nob",
  data_cadastro_nob: "data_cadastro_nob",
  data_hora_de_cadastro_da_liq: "data_hora_cadastro_liq",
  dotacao_orcamentaria: "dotacao_orcamentaria",
  natureza_de_despesa: "natureza_despesa",
  nome_da_fonte_de_recurso: "nome_fonte_recurso",
  ug: "ug",
  uo: "uo",
  nome_do_credor_principal: "nome_credor_principal",
  cpf_cnpj_do_credor_principal: "cpf_cnpj_credor_principal",
  credor: "credor",
  nome_do_credor: "nome_credor",
  cpf_cnpj_do_credor: "cpf_cnpj_credor",
  historico_liq: "historico_liq",
  empenho_atual: "empenho_atual",
  empenho_rp: "empenho_rp",
  funcao: "funcao",
  subfuncao: "subfuncao",
  programa_de_governo: "programa_governo",
  paoe: "paoe",
  cat_econ: "cat_econ",
  grupo: "grupo",
  modalidade: "modalidade",
  iduso: "iduso",
  elemento: "elemento",
  nome_do_elemento_da_despesa: "nome_elemento_despesa",
  fonte: "fonte",
};

const INSERT_COLS = [
  "upload_id",
  "exercicio",
  "numero_nob",
  "numero_nob_estorno",
  "numero_liq",
  "numero_emp",
  "numero_ped",
  "valor_nob",
  "devolucao_gcv",
  "valor_nob_gcv",
  "data_nob",
  "data_cadastro_nob",
  "data_hora_cadastro_liq",
  "dotacao_orcamentaria",
  "natureza_despesa",
  "nome_fonte_recurso",
  "ug",
  "uo",
  "nome_credor_principal",
  "cpf_cnpj_credor_principal",
  "credor",
  "nome_credor",
  "cpf_cnpj_credor",
  "historico_liq",
  "empenho_atual",
  "empenho_rp",
  "funcao",
  "subfuncao",
  "programa_governo",
  "paoe",
  "cat_econ",
  "grupo",
  "modalidade",
  "elemento",
  "nome_elemento_despesa",
  "fonte",
  "iduso",
  "raw_payload",
  "data_atualizacao",
  "data_arquivo",
  "user_email",
];

function normalizeColumns(columns) {
  return columns.map((col) => {
    const key = normalizeHeaderKey(col);
    return COLUMN_ALIASES[key] || col;
  });
}

function normalizeColName(name) {
  let text = String(name || "");
  text = text.replace(/[\u00ba\u00b0]/g, "o");
  try {
    text = text.normalize("NFKD");
  } catch {
    text = String(text);
  }
  text = text.replace(/\p{M}/gu, "");
  text = text.replace(/[^\w\s]/g, " ");
  text = text.replace(/\s+/g, "_").replace(/^_+|_+$/g, "").toLowerCase();
  return text;
}

function dedupeColumns(columns) {
  const seen = new Set();
  return columns.filter((col) => {
    if (seen.has(col)) return false;
    seen.add(col);
    return true;
  });
}

function moveColumnsAfter(columns, reference, colsToMove) {
  const existing = colsToMove.filter((col) => columns.includes(col));
  if (!existing.length) return columns;
  const remaining = columns.filter((col) => !existing.includes(col));
  const idx = remaining.indexOf(reference);
  if (idx === -1) return remaining.concat(existing);
  return [...remaining.slice(0, idx + 1), ...existing, ...remaining.slice(idx + 1)];
}

function insertColumnsAfter(columns, reference, colsToInsert) {
  const existing = colsToInsert.filter((col) => !columns.includes(col));
  if (!existing.length) return columns;
  const idx = columns.indexOf(reference);
  if (idx === -1) return columns.concat(existing);
  return [...columns.slice(0, idx + 1), ...existing, ...columns.slice(idx + 1)];
}

function buildFinalColumns(baseColumns) {
  let columns = baseColumns.filter((col) => !COLUMNS_TO_DROP.includes(col));
  columns = dedupeColumns(columns);

  if (!columns.includes("Valor NOB - GCV")) {
    columns.push("Valor NOB - GCV");
  }

  columns = moveColumnsAfter(columns, "N\u00ba NOB", [
    "N\u00ba NOB Estorno/Estornado",
    "N\u00ba LIQ",
    "N\u00ba EMP",
    "N\u00ba PED",
    "Valor NOB",
  ]);

  columns = moveColumnsAfter(columns, "Valor NOB", ["Devolu\u00e7\u00e3o GCV", "Valor NOB - GCV"]);

  if (columns.includes("Data Cadastro NOB") && columns.includes("Data NOB")) {
    columns = columns.filter((col) => col !== "Data NOB");
    const idx = columns.indexOf("Data Cadastro NOB");
    columns.splice(idx, 0, "Data NOB");
  }

  const credorCols = [
    "Nome do Credor Principal",
    "CPF/CNPJ do Credor Principal",
    "Credor",
    "Nome do Credor",
    "CPF/CNPJ do Credor",
  ].filter((col) => columns.includes(col));

  if (columns.includes("Data NOB")) {
    columns = columns.filter((col) => !credorCols.includes(col));
    const idx = columns.indexOf("Data NOB");
    columns.splice(idx + 1, 0, ...credorCols);
  }

  if (columns.includes("N\u00ba EMP")) {
    columns = insertColumnsAfter(columns, "N\u00ba EMP", ["Empenho Atual", "Empenho RP"]);
  }

  columns = insertColumnsAfter(columns, "Dota\u00e7\u00e3o Or\u00e7ament\u00e1ria", [
    "Fun\u00e7\u00e3o",
    "Subfun\u00e7\u00e3o",
    "Programa de Governo",
    "PAOE",
    "Natureza de Despesa",
  ]);

  columns = insertColumnsAfter(columns, "Natureza de Despesa", ["Cat.Econ", "Grupo", "Modalidade"]);

  columns = insertColumnsAfter(columns, "Modalidade", ["Elemento", "Nome do Elemento da Despesa", "Fonte"]);

  columns = insertColumnsAfter(columns, "Nome da Fonte de Recurso", ["Iduso"]);

  return columns;
}

function parseDateFlexible(value) {
  if (!value) return null;
  const text = String(value).trim();
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    const [y, m, d] = text.split("-").map(Number);
    return new Date(y, m - 1, d);
  }
  if (/^\d{2}\/\d{2}\/\d{4}$/.test(text)) {
    const [d, m, y] = text.split("/").map(Number);
    return new Date(y, m - 1, d);
  }
  const asDate = new Date(text);
  return Number.isNaN(asDate.getTime()) ? null : asDate;
}

function cleanTextFields(row) {
  const cleaned = { ...row };
  for (const key of Object.keys(cleaned)) {
    if (cleaned[key] === null || cleaned[key] === undefined) continue;
    cleaned[key] = String(cleaned[key]);
  }
  for (const key of Object.keys(cleaned)) {
    let value = cleaned[key];
    if (typeof value !== "string") continue;
    value = value.replace(/_x000D_/g, "");
    value = value.replace(/[^\S\r\n]+/g, " ");
    value = value.replace(/\s+/g, " ");
    value = value.replace(/\*/g, "|");
    if (value === "" || value === "nan" || value.toUpperCase() === "NAO INFORMADO") {
      value = "NÃO INFORMADO";
    }
    cleaned[key] = value;
  }
  return cleaned;
}

function computeEmpenho(row) {
  const nEmp = String(row["N\u00ba EMP"] || "");
  const exercicio = String(row["Exerc\u00edcio"] || "");
  let anoEmp = "";
  const parts = nEmp.split(".");
  if (parts.length >= 3 && /^\d+$/.test(parts[2])) {
    anoEmp = parts[2].slice(-2);
  }
  const anoEx = exercicio.slice(-2);
  const atual = anoEmp && anoEx && anoEmp === anoEx ? nEmp : "";
  const rp = anoEmp && anoEx && anoEmp !== anoEx ? nEmp : "";
  row["Empenho Atual"] = atual || "NÃO INFORMADO";
  row["Empenho RP"] = rp || "NÃO INFORMADO";
}

function addDotacao(row) {
  const dotacao = String(row["Dota\u00e7\u00e3o Or\u00e7ament\u00e1ria"] || "");
  const parts = dotacao.split(".");
  const get = (idx) => (parts.length > idx ? parts[idx] : "NÃO INFORMADO");
  row["Fun\u00e7\u00e3o"] = get(2);
  row["Subfun\u00e7\u00e3o"] = get(3);
  row["Programa de Governo"] = get(4);
  row["PAOE"] = get(5);
  row["Natureza de Despesa"] = get(7);
}

function addNatureza(row) {
  const natureza = String(row["Natureza de Despesa"] || "").trim();
  row["Cat.Econ"] = natureza.length >= 1 ? natureza.slice(0, 1) : "NÃO INFORMADO";
  row["Grupo"] = natureza.length >= 2 ? natureza.slice(1, 2) : "NÃO INFORMADO";
  row["Modalidade"] = natureza.length >= 4 ? natureza.slice(2, 4) : "NÃO INFORMADO";
}

function addIduso(row) {
  const dotacao = String(row["Dota\u00e7\u00e3o Or\u00e7ament\u00e1ria"] || "");
  const parts = dotacao.split(".");
  row["Iduso"] = parts.length > 9 ? parts[9] : "NÃO INFORMADO";
}

function buildDbPayload(row, uploadId, dataArquivo, userEmail) {
  const payload = {};
  for (const [col, val] of Object.entries(row)) {
    const key = normalizeColName(col);
    const dbCol = COL_MAP[key];
    if (!dbCol) continue;
    payload[dbCol] = val === "-" ? null : val;
  }

  for (const col of ["valor_nob", "devolucao_gcv", "valor_nob_gcv"]) {
    if (col in payload) payload[col] = parseValorDb(payload[col]);
  }
  for (const col of ["data_nob", "data_cadastro_nob", "data_hora_cadastro_liq"]) {
    if (col in payload) payload[col] = parseDataDb(payload[col]);
  }

  payload.raw_payload = JSON.stringify(row);
  payload.upload_id = uploadId;
  payload.data_atualizacao = new Date();
  payload.data_arquivo = dataArquivo || null;
  payload.user_email = userEmail;
  return payload;
}

async function loadSheetData(filePath) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(filePath);
  const worksheet = workbook.worksheets[0];
  if (!worksheet) throw new Error("Planilha sem conteudo.");

  const cachedRows = [];
  const maxScan = Math.min(400, worksheet.rowCount);
  for (let idx = 1; idx <= maxScan; idx += 1) {
    const row = worksheet.getRow(idx);
    const values = row.values.slice(1).map((cell) => (cell === null ? "" : cell));
    cachedRows.push(values);
  }

  let headerRowIdx = null;
  let headerValues = [];
  if (cachedRows.length >= 5) {
    const row5 = cachedRows[4];
    if (row5.some((c) => typeof c === "string" && c.toLowerCase().includes("exerc"))) {
      headerRowIdx = 5;
      headerValues = row5.map((c) => (c === null ? "" : String(c).trim()));
    }
  }

  if (!headerRowIdx) {
    let candidateSingle = null;
    for (let idx = 0; idx < cachedRows.length; idx += 1) {
      const row = cachedRows[idx];
      const nonempty = row.filter((c) => c !== null && c !== "").length;
      const hasExerc = row.some((c) => typeof c === "string" && c.toLowerCase().includes("exerc"));
      if (hasExerc && nonempty >= 3) {
        headerRowIdx = idx + 1;
        headerValues = row.map((c) => (c === null ? "" : String(c).trim()));
        break;
      }
      if (hasExerc && nonempty === 1 && candidateSingle === null) {
        candidateSingle = idx + 1;
      }
    }
    if (!headerRowIdx && candidateSingle) {
      const nextIdx = candidateSingle + 1;
      if (nextIdx <= cachedRows.length) {
        headerRowIdx = nextIdx;
        headerValues = cachedRows[nextIdx - 1].map((c) => (c === null ? "" : String(c).trim()));
      }
    }
  }

  if (!headerRowIdx) {
    throw new Error("Cabecalho com 'Exercicio' nao encontrado nas primeiras 400 linhas.");
  }

  const headerNorm = normalizeColumns(headerValues);
  const requiredCanon = new Set(normalizeColumns(REQUIRED_COLS_RAW));
  const keepIndices = [];
  const keepNames = [];
  headerNorm.forEach((name, pos) => {
    if (requiredCanon.has(name)) {
      keepIndices.push(pos);
      keepNames.push(name);
    }
  });

  if (!keepIndices.length) {
    throw new Error(`Nenhuma coluna necessaria encontrada. Header detectado: ${headerValues.join(" | ")}`);
  }

  const rows = [];
  for (let idx = headerRowIdx + 1; idx <= worksheet.rowCount; idx += 1) {
    const row = worksheet.getRow(idx);
    const values = row.values.slice(1);
    const selected = keepIndices.map((pos) => {
      const val = pos < values.length ? values[pos] : null;
      if (val === null || val === undefined) return "";
      if (val instanceof Date) return formatDateOutput(val);
      return String(val);
    });
    const obj = {};
    keepNames.forEach((name, i) => {
      obj[name] = selected[i];
    });
    rows.push(obj);
  }

  return { columns: keepNames, rows };
}

async function processNob(filePath, dataArquivo, userEmail, uploadId) {
  ensureDir(OUTPUT_DIR);
  const outputFile = path.join(OUTPUT_DIR, `${path.basename(filePath, path.extname(filePath))}_tratado.xlsx`);
  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: outputFile });
  const sheet = workbook.addWorksheet("nob_tratado");

  const db = await connect();
  if (db.kind === "mssql") {
    await db.pool.request().query("UPDATE nob SET ativo = 0 WHERE ativo = 1");
  } else {
    await db.pool.query("UPDATE nob SET ativo = 0 WHERE ativo = 1");
  }

  const batch = [];
  let totalInserted = 0;
  let processed = 0;
  let outputColumns = null;
  let keepIndices = null;
  let keepNames = null;
  let headerRowIdx = null;

  const bufferRows = [];

  const detectHeader = (cachedRows) => {
    let foundRowIdx = null;
    let foundValues = [];

    if (cachedRows.length >= 5) {
      const row5 = cachedRows[4];
      if (row5.some((c) => typeof c === "string" && c.toLowerCase().includes("exerc"))) {
        foundRowIdx = 5;
        foundValues = row5.map((c) => (c === null ? "" : String(c).trim()));
        return { foundRowIdx, foundValues };
      }
    }

    let candidateSingle = null;
    for (let idx = 0; idx < cachedRows.length; idx += 1) {
      const row = cachedRows[idx];
      const nonempty = row.filter((c) => c !== null && c !== "").length;
      const hasExerc = row.some((c) => typeof c === "string" && c.toLowerCase().includes("exerc"));
      if (hasExerc && nonempty >= 3) {
        foundRowIdx = idx + 1;
        foundValues = row.map((c) => (c === null ? "" : String(c).trim()));
        return { foundRowIdx, foundValues };
      }
      if (hasExerc && nonempty === 1 && candidateSingle === null) {
        candidateSingle = idx + 1;
      }
    }
    if (!foundRowIdx && candidateSingle) {
      const nextIdx = candidateSingle + 1;
      if (nextIdx <= cachedRows.length) {
        foundRowIdx = nextIdx;
        foundValues = cachedRows[nextIdx - 1].map((c) => (c === null ? "" : String(c).trim()));
      }
    }
    return foundRowIdx ? { foundRowIdx, foundValues } : null;
  };

  const initHeader = (foundIdx, foundValues) => {
    headerRowIdx = foundIdx;
    const headerNorm = normalizeColumns(foundValues);
    const requiredCanon = new Set(normalizeColumns(REQUIRED_COLS_RAW));
    keepIndices = [];
    keepNames = [];
    headerNorm.forEach((name, pos) => {
      if (requiredCanon.has(name)) {
        keepIndices.push(pos);
        keepNames.push(name);
      }
    });
    if (!keepIndices.length) {
      throw new Error(`Nenhuma coluna necessaria encontrada. Header detectado: ${foundValues.join(" | ")}`);
    }
    outputColumns = buildFinalColumns(keepNames);
    sheet.columns = outputColumns.map((col) => ({ header: fixOutputHeader(col), key: col }));
  };

  const processRecord = async (values) => {
    processed += 1;
    const selected = keepIndices.map((pos) => {
      const val = pos < values.length ? values[pos] : null;
      if (val === null || val === undefined) return "";
      if (val instanceof Date) return formatDateOutput(val);
      return String(val);
    });
    let record = {};
    keepNames.forEach((name, i) => {
      record[name] = selected[i];
    });

    for (const col of COLUMNS_TO_DROP) {
      delete record[col];
    }

    record = cleanTextFields(record);

    const valNob = record["Valor NOB"] ? parseBrNumber(record["Valor NOB"]) : 0.0;
    const valGcv = record["Devolução GCV"] ? parseBrNumber(record["Devolução GCV"]) : 0.0;
    record["Valor NOB"] = formatNumberPtBr(valNob);
    record["Devolução GCV"] = formatNumberPtBr(valGcv);
    record["Valor NOB - GCV"] = formatNumberPtBr(valNob - valGcv);

    if ("Data NOB" in record) {
      record["Data NOB"] = formatDateOutput(record["Data NOB"]);
    }
    if ("Data Cadastro NOB" in record) {
      record["Data Cadastro NOB"] = formatDateOutput(record["Data Cadastro NOB"]);
    }

    if ("Nº NOB Estorno/Estornado" in record) {
      const rawEstorno = String(record["Nº NOB Estorno/Estornado"] ?? "").trim();
      const statusEstorno = rawEstorno.toUpperCase();
      const vazioOuNaoInformado = !rawEstorno || new Set(["NAO INFORMADO", "NÃO INFORMADO", "NÇO INFORMADO"]).has(statusEstorno);
      if (!vazioOuNaoInformado) {
        return;
      }
      if (vazioOuNaoInformado) {
        record["Nº NOB Estorno/Estornado"] = "0";
      }
    }

    computeEmpenho(record);
    addDotacao(record);
    addNatureza(record);
    addIduso(record);

    const outputRow = outputColumns.map((col) => (col in record ? record[col] : "NÃO INFORMADO"));
    sheet.addRow(outputRow).commit();

    const payload = buildDbPayload(record, uploadId, dataArquivo, userEmail);
    batch.push(payload);

    if (batch.length >= BATCH_SIZE) {
      if (readCancelFlag("nob", uploadId)) {
        throw new Error("PROCESSAMENTO_CANCELADO");
      }
      await bulkInsert(db, "nob", INSERT_COLS, batch);
      totalInserted += batch.length;
      updateStatusFields("nob", uploadId, {
        message: `Gravando registros no banco (${totalInserted}).`,
      });
      batch.length = 0;
    }
  };

  const workbookReader = new ExcelJS.stream.xlsx.WorkbookReader(filePath);
  let worksheetFound = false;
  for await (const worksheetReader of workbookReader) {
    if (worksheetFound) break;
    worksheetFound = true;
    for await (const row of worksheetReader) {
      const values = row.values ? row.values.slice(1) : [];
      if (!headerRowIdx) {
        if (bufferRows.length < 400) {
          bufferRows.push({ number: row.number, values });
          const cached = bufferRows.map((r) => r.values);
          const detected = detectHeader(cached);
          if (detected) {
            initHeader(detected.foundRowIdx, detected.foundValues);
            for (const buffered of bufferRows) {
              if (buffered.number > headerRowIdx) {
                await processRecord(buffered.values);
              }
            }
          }
        } else {
          throw new Error("Cabecalho com 'Exercicio' nao encontrado nas primeiras 400 linhas.");
        }
        continue;
      }
      if (row.number <= headerRowIdx) continue;
      await processRecord(values);
    }
  }
  if (!headerRowIdx) {
    throw new Error("Cabecalho com 'Exercicio' nao encontrado nas primeiras 400 linhas.");
  }

  if (batch.length) {
    if (readCancelFlag("nob", uploadId)) {
      throw new Error("PROCESSAMENTO_CANCELADO");
    }
    await bulkInsert(db, "nob", INSERT_COLS, batch);
    totalInserted += batch.length;
    updateStatusFields("nob", uploadId, {
      progress: 100,
      message: `Gravando registros no banco (${totalInserted}).`,
    });
  }

  await workbook.commit();
  if (db.kind === "mssql") {
    await db.pool.close();
  } else {
    await db.pool.end();
  }

  return { total: totalInserted, outputPath: outputFile };
}

module.exports = {
  processNob,
};
