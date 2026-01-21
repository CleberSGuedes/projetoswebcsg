
const path = require("path");
const ExcelJS = require("exceljs");
const { connect, bulkInsert } = require("./db");
const {
  ensureDir,
  readJsonWithBom,
  normalizeSimple,
  cleanHistorico,
  corrigirCaracteres,
  normalizeForComparison,
  cleanForEmpSheet,
  canonicalizarChave,
  parseValorDb,
  parseDataDb,
  parseAno,
  formatNumberPtBr,
  formatDatePtBr,
  formatDateIso,
  updateStatusFields,
  readCancelFlag,
} = require("./util");

const BATCH_SIZE = 1000;
const OUTPUT_DIR = path.resolve(__dirname, "..", "outputs", "td_emp");
const BASE_DIR = path.resolve(__dirname, "..");
const JSON_CHAVES_PATH = path.join(BASE_DIR, "static", "js", "chaves_planejamento.json");
const JSON_CASOS_PATH = path.join(BASE_DIR, "static", "js", "chave_arrumar.json");
const JSON_FORCAR_PATH = path.join(BASE_DIR, "static", "js", "forcar_chave.json");

const OUTPUT_HEADER_LIST = [
  "Chave de Planejamento",
  "Regi\u00e3o",
  "Subfun\u00e7\u00e3o + UG",
  "ADJ",
  "Macropol\u00edtica",
  "Pilar",
  "Eixo",
  "Pol\u00edtica_Decreto",
  "Exerc\u00edcio",
  "N\u00ba EMP",
  "N\u00ba PED",
  "N\u00ba Processo Or\u00e7ament\u00e1rio de Pagamento",
  "Data emiss\u00e3o",
  "Data cria\u00e7\u00e3o",
  "UO",
  "Nome da Unidade Or\u00e7ament\u00e1ria",
  "UG",
  "Nome da Unidade Gestora",
  "Hist\u00f3rico",
  "N\u00ba NOBLIST",
  "N\u00ba OS",
  "Dota\u00e7\u00e3o Or\u00e7ament\u00e1ria",
  "Fun\u00e7\u00e3o",
  "Subfun\u00e7\u00e3o",
  "Programa de Governo",
  "PAOE",
  "Natureza de Despesa",
  "Cat.Econ",
  "Grupo",
  "Modalidade",
  "Elemento",
  "N\u00ba Emenda (EP)",
  "Fonte",
  "Iduso",
  "Tipo de Despesa",
  "N\u00ba ABJ",
  "N\u00ba Processo do Sequestro Judicial",
  "N\u00ba Conv\u00eanio",
  "Tipo Conta Banc\u00e1ria",
  "Credor",
  "Nome do Credor",
  "CPF/CNPJ do Credor",
  "Categoria do Credor",
  "N\u00ba Contrato",
  "Tipo Empenho",
  "N\u00ba Licita\u00e7\u00e3o",
  "Ano Licita\u00e7\u00e3o",
  "Situa\u00e7\u00e3o",
  "Finalidade de Aplica\u00e7\u00e3o FUNDEB (EMP)",
  "Modalidade de Aplica\u00e7\u00e3o",
  "Nome da Modalidade de Aplica\u00e7\u00e3o",
  "Usu\u00e1rio Respons\u00e1vel",
  "N\u00ba da Licita\u00e7\u00e3o",
  "Ano da Licita\u00e7\u00e3o",
  "N\u00ba NEX",
  "Situa\u00e7\u00e3o NEX",
  "N\u00ba RPV",
  "Devolu\u00e7\u00e3o GCV",
  "N\u00ba CAD",
  "N\u00ba NLA",
  "Valor EMP",
];

function normalizeHeaderKey(label) {
  let key = normalizeSimple(label);
  key = key.replace(/[\u00ba\u00b0]/g, "o");
  key = key.replace(/\b(no|n)\b/g, "numero");
  key = key.replace(/\bnumero\b/g, "numero");
  key = key.replace(/[^a-z0-9]+/g, " ").trim();
  return key;
}

function normalizeHeaderKeyLoose(label) {
  let key = normalizeSimple(label);
  key = key.replace(/[\u00ba\u00b0]/g, "o");
  key = key.replace(/\?/g, "");
  key = key.replace(/\b(no|n)\b/g, "numero");
  key = key.replace(/\bnumero\b/g, "numero");
  key = key.replace(/[^a-z0-9]+/g, "");
  return key;
}

const OUTPUT_HEADER_MAP = Object.fromEntries(
  OUTPUT_HEADER_LIST.map((label) => [normalizeHeaderKey(label), label])
);

const OUTPUT_HEADER_MAP_LOOSE = Object.fromEntries(
  OUTPUT_HEADER_LIST.map((label) => [normalizeHeaderKeyLoose(label), label])
);

function fixOutputHeader(label) {
  if (typeof label !== "string") return label;
  const key = normalizeHeaderKey(label);
  if (OUTPUT_HEADER_MAP[key]) return OUTPUT_HEADER_MAP[key];
  const looseKey = normalizeHeaderKeyLoose(label);
  if (OUTPUT_HEADER_MAP_LOOSE[looseKey]) return OUTPUT_HEADER_MAP_LOOSE[looseKey];
  for (const [mapKey, value] of Object.entries(OUTPUT_HEADER_MAP_LOOSE)) {
    if (mapKey && (looseKey.startsWith(mapKey) || mapKey.startsWith(looseKey))) {
      return value;
    }
  }
  return label.normalize("NFC");
}

function buildTratadoColumns(columns) {
  const keyToCol = new Map();
  for (const col of columns) {
    keyToCol.set(normalizeHeaderKeyLoose(col), col);
  }
  const used = new Set();
  const ordered = [];
  for (const label of OUTPUT_HEADER_LIST) {
    const key = normalizeHeaderKeyLoose(label);
    let match = keyToCol.get(key);
    if (!match) {
      for (const [colKey, colName] of keyToCol.entries()) {
        if (colKey && (key.startsWith(colKey) || colKey.startsWith(key))) {
          match = colName;
          break;
        }
      }
    }
    if (match && !used.has(match)) {
      ordered.push({ header: label, key: match });
      used.add(match);
    } else if (!match) {
      ordered.push({ header: label, key: label });
    }
  }
  for (const col of columns) {
    if (!used.has(col)) {
      ordered.push({ header: fixOutputHeader(col), key: col });
    }
  }
  return ordered;
}


const COL_MAP = {
  chave: "chave",
  chave_de_planejamento: "chave_planejamento",
  regiao: "regiao",
  subfuncao_ug: "subfuncao_ug",
  adj: "adj",
  macropolitica: "macropolitica",
  pilar: "pilar",
  eixo: "eixo",
  politica_decreto: "politica_decreto",
  exercicio: "exercicio",
  situacao: "situacao",
  historico: "historico",
  no_emp: "numero_emp",
  numero_emp: "numero_emp",
  no_ped: "numero_ped",
  numero_ped: "numero_ped",
  no_contrato: "numero_contrato",
  no_convenio: "numero_convenio",
  dotacao_orcamentaria: "dotacao_orcamentaria",
  funcao: "funcao",
  subfuncao: "subfuncao",
  programa_de_governo: "programa_governo",
  paoe: "paoe",
  natureza_de_despesa: "natureza_despesa",
  cat_econ: "cat_econ",
  grupo: "grupo",
  modalidade: "modalidade",
  fonte: "fonte",
  iduso: "iduso",
  elemento: "elemento",
  uo: "uo",
  nome_da_unidade_orcamentaria: "nome_unidade_orcamentaria",
  nome_unidade_orcamentaria: "nome_unidade_orcamentaria",
  ug: "ug",
  nome_da_unidade_gestora: "nome_unidade_gestora",
  nome_unidade_gestora: "nome_unidade_gestora",
  data_emissao: "data_emissao",
  data_criacao: "data_criacao",
  valor_emp: "valor_emp",
  devolucao_gcv: "devolucao_gcv",
  valor_emp_devolucao_gcv: "valor_emp_devolucao_gcv",
  tipo_empenho: "tipo_empenho",
  tipo_de_despesa: "tipo_despesa",
  credor: "credor",
  nome_do_credor: "nome_credor",
  nome_credor: "nome_credor",
  cpf_cnpj_do_credor: "cpf_cnpj_credor",
  cpf_cnpj_credor: "cpf_cnpj_credor",
  categoria_do_credor: "categoria_credor",
  categoria_credor: "categoria_credor",
};



const COLUNAS_NORMALIZACAO = {
  "Exercicio": "Exerc\u00edcio",
  "Exerc?cio": "Exerc\u00edcio",
  "Situacao": "Situa\u00e7\u00e3o",
  "Situação": "Situa\u00e7\u00e3o",
  "Historico": "Hist\u00f3rico",
  "Hist?rico": "Hist\u00f3rico",
  "Numero EMP": "N\u00ba EMP",
  "N? EMP": "N\u00ba EMP",
  "Numero PED": "N\u00ba PED",
  "N? PED": "N\u00ba PED",
  "Numero Contrato": "N\u00ba Contrato",
  "N? Contrato": "N\u00ba Contrato",
  "Numero Convenio": "N\u00ba Conv\u00eanio",
  "N? Conv?nio": "N\u00ba Conv\u00eanio",
  "Dotacao Orcamentaria": "Dota\u00e7\u00e3o Or\u00e7ament\u00e1ria",
  "Dotação Orçamentária": "Dota\u00e7\u00e3o Or\u00e7ament\u00e1ria",
  "Data emissao": "Data emiss\u00e3o",
  "Data emiss?o": "Data emiss\u00e3o",
  "Data criacao": "Data cria\u00e7\u00e3o",
  "Data criação": "Data cria\u00e7\u00e3o",
  "Devolucao GCV": "Devolu\u00e7\u00e3o GCV",
  "Devolução GCV": "Devolu\u00e7\u00e3o GCV",
  "Valor EMP-Devolucao GCV": "Valor EMP-Devolu\u00e7\u00e3o GCV",
  "Valor EMP-Devolução GCV": "Valor EMP-Devolu\u00e7\u00e3o GCV",
  "Regiao": "Regi\u00e3o",
  "Regi?o": "Regi\u00e3o",
  "Subfuncao + UG": "Subfun\u00e7\u00e3o + UG",
  "Subfunção + UG": "Subfun\u00e7\u00e3o + UG",
  "Macropolitica": "Macropol\u00edtica",
  "Macropol?tica": "Macropol\u00edtica",
  "Politica_Decreto": "Pol\u00edtica_Decreto",
  "Pol?tica_Decreto": "Pol\u00edtica_Decreto",
  "Subfuncao": "Subfun\u00e7\u00e3o",
  "Subfunção": "Subfun\u00e7\u00e3o",
  "Funcao": "Fun\u00e7\u00e3o",
  "Função": "Fun\u00e7\u00e3o",
  "Tipo Conta Bancaria": "Tipo Conta Banc\u00e1ria",
  "Tipo Conta Banc?ria": "Tipo Conta Banc\u00e1ria",
  "Numero Processo Orcamentario de Pagamento": "N\u00ba Processo Or\u00e7ament\u00e1rio de Pagamento",
  "N? Processo Or?ament?rio de Pagamento": "N\u00ba Processo Or\u00e7ament\u00e1rio de Pagamento",
  "Numero NOBLIST": "N\u00ba NOBLIST",
  "N? NOBLIST": "N\u00ba NOBLIST",
  "Numero DOTLIST": "N\u00ba DOTLIST",
  "N? DOTLIST": "N\u00ba DOTLIST",
  "Numero OS": "N\u00ba OS",
  "N? OS": "N\u00ba OS",
  "Numero Emenda (EP)": "N\u00ba Emenda (EP)",
  "N? Emenda (EP)": "N\u00ba Emenda (EP)",
  "Numero ABJ": "N\u00ba ABJ",
  "N? ABJ": "N\u00ba ABJ",
  "Numero Processo do Sequestro Judicial": "N\u00ba Processo do Sequestro Judicial",
  "N? Processo do Sequestro Judicial": "N\u00ba Processo do Sequestro Judicial",
  "Numero Licitacao": "N\u00ba Licita\u00e7\u00e3o",
  "Nº Licitação": "N\u00ba Licita\u00e7\u00e3o",
  "Ano Licitacao": "Ano Licita\u00e7\u00e3o",
  "Ano Licitação": "Ano Licita\u00e7\u00e3o",
  "Finalidade de Aplicacao FUNDEB (EMP)": "Finalidade de Aplica\u00e7\u00e3o FUNDEB (EMP)",
  "Finalidade de Aplicação FUNDEB (EMP)": "Finalidade de Aplica\u00e7\u00e3o FUNDEB (EMP)",
  "Modalidade de Aplicacao": "Modalidade de Aplica\u00e7\u00e3o",
  "Modalidade de Aplicação": "Modalidade de Aplica\u00e7\u00e3o",
  "Nome da Modalidade de Aplicacao": "Nome da Modalidade de Aplica\u00e7\u00e3o",
  "Nome da Modalidade de Aplicação": "Nome da Modalidade de Aplica\u00e7\u00e3o",
  "Usuario Responsavel": "Usu\u00e1rio Respons\u00e1vel",
  "Usu?rio Respons?vel": "Usu\u00e1rio Respons\u00e1vel",
  "Numero da Licitacao": "N\u00ba da Licita\u00e7\u00e3o",
  "Nº da Licitação": "N\u00ba da Licita\u00e7\u00e3o",
  "Ano da Licitacao": "Ano da Licita\u00e7\u00e3o",
  "Ano da Licitação": "Ano da Licita\u00e7\u00e3o",
  "Justificativa para despesa sem contrato(Sim/Nao)": "Justificativa para despesa sem contrato(Sim/N\u00e3o)",
  "Justificativa para despesa sem contrato(Sim/N?o)": "Justificativa para despesa sem contrato(Sim/N\u00e3o)",
  "Situacao NEX": "Situa\u00e7\u00e3o NEX",
  "Situação NEX": "Situa\u00e7\u00e3o NEX",
  "Numero NEX": "N\u00ba NEX",
  "N? NEX": "N\u00ba NEX",
  "Numero RPV": "N\u00ba RPV",
  "N? RPV": "N\u00ba RPV",
  "Numero CAD": "N\u00ba CAD",
  "N? CAD": "N\u00ba CAD",
  "Numero NLA": "N\u00ba NLA",
  "N? NLA": "N\u00ba NLA",
};

const CORRECOES_FORCAR = {
  "GESTAO_INOVACAO": "GEST\u00c3O_INOVA\u00c7\u00c3O",
  "P_GESTAO_": "P_GEST\u00c3O_",
  "E_GESTAO_ESCOLAR": "E_GEST\u00c3O_ESCOLAR",
  "E_GESTAO_DO_PATRIM": "E_GEST\u00c3O_DO_PATRIM",
  "E_VALORIZACAO_PROF": "E_VALORIZA\u00c7\u00c3O_PROF",
  "VALORIZACAO_PRO": "VALORIZA\u00c7\u00c3O_PRO",
  "_GESTAO_ESCOLAR": "_GEST\u00c3O_ESCOLAR",
  "_GESTAO_PATRIM": "_GEST\u00c3O_PATRIM",
  "_ALFABETIZACAO": "_ALFABETIZA\u00c7\u00c3O",
  "E_ENSINO_MEDIO": "E_ENSINO_M\u00c9DIO",
  "_NOVO_ENSINO_MED": "_NOVO_ENSINO_M\u00c9D",
  "CURRICULO": "CURR\u00cdCULO",
};

const INSERT_COLS = [
  "upload_id",
  "chave",
  "chave_planejamento",
  "regiao",
  "subfuncao_ug",
  "adj",
  "macropolitica",
  "pilar",
  "eixo",
  "politica_decreto",
  "exercicio",
  "situacao",
  "historico",
  "numero_emp",
  "numero_ped",
  "numero_contrato",
  "numero_convenio",
  "dotacao_orcamentaria",
  "funcao",
  "subfuncao",
  "programa_governo",
  "paoe",
  "natureza_despesa",
  "cat_econ",
  "grupo",
  "modalidade",
  "fonte",
  "iduso",
  "elemento",
  "uo",
  "nome_unidade_orcamentaria",
  "ug",
  "nome_unidade_gestora",
  "data_emissao",
  "data_criacao",
  "valor_emp",
  "devolucao_gcv",
  "valor_emp_devolucao_gcv",
  "tipo_empenho",
  "tipo_despesa",
  "credor",
  "nome_credor",
  "cpf_cnpj_credor",
  "categoria_credor",
  "raw_payload",
  "data_atualizacao",
  "data_arquivo",
  "user_email",
];
function canonicalizarNomeColuna(coluna) {
  const texto = String(coluna || "").replace(/\s+/g, " ").trim();
  return COLUNAS_NORMALIZACAO[texto] || texto;
}

function normalizeColumns(columns) {
  return columns.map((col) => {
    const canonical = canonicalizarNomeColuna(col);
    const normalized = normalizeSimple(canonical);
    if (normalized === "exercicio") return "Exerc\u00edcio";
    if (normalized === "dotacao orcamentaria") return "Dota\u00e7\u00e3o Or\u00e7ament\u00e1ria";
    if (normalized === "situacao") return "Situa\u00e7\u00e3o";
    return canonical;
  });
}

function normalizeColName(name) {
  let texto = String(name || "");
  texto = texto.replace(/[\u00ba\u00b0]/g, "o");
  texto = texto.normalize("NFKD");
  texto = texto.replace(/\p{M}/gu, "");
  texto = texto.replace(/[^a-zA-Z0-9]+/g, "_").replace(/^_+|_+$/g, "").toLowerCase();
  return texto;
}

function corrigirTermosCorrompidos(texto) {
  if (typeof texto !== "string") return texto;
  let out = texto;
  for (const [errado, correto] of Object.entries(CORRECOES_FORCAR)) {
    out = out.replaceAll(errado, correto);
  }
  return out;
}

function removerEmpenhosEstornados(dataset) {
  let colunaSituacao = null;
  for (const col of dataset.columns) {
    if (normalizeSimple(col) === "situacao") {
      colunaSituacao = col;
      break;
    }
  }
  if (!colunaSituacao) return { dataset, removidos: 0 };
  const rows = dataset.rows.filter((row) => {
    const value = normalizeSimple(row[colunaSituacao]);
    return !value.includes("empenho emp com estorno total");
  });
  const removidos = dataset.rows.length - rows.length;
  return { dataset: { columns: dataset.columns, rows }, removidos };
}

function obterExercicio(dataset) {
  let colName = null;
  for (const col of dataset.columns) {
    const norm = normalizeSimple(col);
    if (norm === "exercicio" || norm.startsWith("exerc")) {
      colName = col;
      break;
    }
  }
  if (!colName) return null;
  const values = dataset.rows
    .map((row) => Number(String(row[colName] || "").replace(/[^\d]/g, "")))
    .filter((num) => Number.isFinite(num) && num > 0);
  if (!values.length) return null;
  const counts = new Map();
  for (const value of values) {
    counts.set(value, (counts.get(value) || 0) + 1);
  }
  let mode = null;
  let max = 0;
  for (const [value, count] of counts.entries()) {
    if (count > max) {
      max = count;
      mode = value;
    }
  }
  return mode;
}

function converterTipos(dataset) {
  const colunasMonetarias = ["Valor EMP", "Devolu\u00e7\u00e3o GCV", "Valor EMP-Devolu\u00e7\u00e3o GCV"];
  const colunasNumericas = ["Exerc\u00edcio", "UO", "UG", "Elemento"];

  for (const row of dataset.rows) {
    for (const col of dataset.columns) {
      if (row[col] === "" || row[col] === null || row[col] === undefined) {
        row[col] = "NÃO INFORMADO";
      }
    }

    for (const col of colunasMonetarias) {
      if (!(col in row)) continue;
      let text = String(row[col] ?? "").trim();
      text = text.replace(/\.(?=\d{3})/g, "");
      text = text.replace(/,/g, ".");
      const num = Number(text);
      const formatted = Number.isFinite(num) ? num.toFixed(2) : "0.00";
      row[col] = formatted;
    }

    for (const col of colunasNumericas) {
      if (!(col in row)) continue;
      const raw = String(row[col] ?? "").trim();
      if (!raw || raw === "N\u00c3O INFORMADO" || raw === "-") {
        row[col] = 0;
        continue;
      }
      const digits = raw.replace(/[^\d]/g, "");
      const num = Number(digits);
      row[col] = Number.isFinite(num) ? num : 0;
    }
  }

  return dataset;
}

function formatarParaSaidaPtBr(dataset) {
  const output = { columns: dataset.columns.slice(), rows: [] };
  const colunasMonetarias = ["Valor EMP", "Devolu\u00e7\u00e3o GCV", "Valor EMP-Devolu\u00e7\u00e3o GCV"];
  const colunasDatas = ["Data emiss\u00e3o", "Data cria\u00e7\u00e3o"];

  for (const row of dataset.rows) {
    const outRow = { ...row };
    for (const col of colunasMonetarias) {
      if (!(col in outRow)) continue;
      const num = Number(outRow[col]);
      outRow[col] = formatNumberPtBr(Number.isFinite(num) ? num : 0.0);
    }
    for (const col of colunasDatas) {
      if (!(col in outRow)) continue;
      const value = outRow[col];
      if (value instanceof Date) {
        outRow[col] = formatDatePtBr(value);
        continue;
      }
      if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value)) {
        outRow[col] = formatDatePtBr(value);
      }
    }
    output.rows.push(outRow);
  }
  return output;
}
function carregarChavesPlanejamento(jsonPath) {
  try {
    const chaves = readJsonWithBom(jsonPath);
    const normalized = [];
    for (const chave of chaves) {
      let bruto = corrigirTermosCorrompidos(String(chave));
      bruto = bruto.replace(/\*/g, " * ");
      bruto = bruto.replace(/\s+/g, " ");
      const parts = bruto.split("*").map((p) => p.trim()).filter(Boolean);
      if (parts.length) {
        normalized.push(`* ${parts.join(" * ")} *`);
      }
    }
    return normalized;
  } catch {
    return [];
  }
}

function carregarCasosEspecificos(jsonPath) {
  try {
    const casos = readJsonWithBom(jsonPath);
    const out = {};
    for (const [k, v] of Object.entries(casos)) {
      const chaveSaida = canonicalizarChave(corrigirTermosCorrompidos(String(v)));
      const kCorrigido = corrigirTermosCorrompidos(k);
      const kNorm = normalizeSimple(kCorrigido);
      out[kNorm] = chaveSaida;
    }
    console.error(`[emp] chave_arrumar carregado: ${Object.keys(out).length} itens`);
    return out;
  } catch {
    console.error("[emp] chave_arrumar nao carregado");
    return {};
  }
}

function carregarForcarChaves(jsonPath) {
  try {
    const bruto = readJsonWithBom(jsonPath);
    const normalized = {};
    for (const [num, chave] of Object.entries(bruto)) {
      let chaveTexto = String(chave).replace(/\|/g, "*");
      chaveTexto = corrigirTermosCorrompidos(chaveTexto);
      const chaveLimpa = canonicalizarChave(chaveTexto);
      normalized[String(num).trim()] = chaveLimpa;
    }
    return normalized;
  } catch {
    return {};
  }
}

function extrairChaveValidaDoHistorico(hist, chaves) {
  for (const chave of chaves) {
    if (hist.includes(chave)) return chave;
  }
  return null;
}

function contarPartesChave(chave) {
  if (typeof chave !== "string") return 0;
  const texto = chave.trim();
  if (!texto || ["-", "NÃO INFORMADO", "NÃO IDENTIFICADO", "NÇO INFORMADO", "NÇO IDENTIFICADO"].includes(texto)) {
    return 0;
  }
  return texto.split("*").map((p) => p.trim()).filter(Boolean).length;
}

function extrairChaveDotDoHistorico(hist) {
  if (typeof hist !== "string") return null;
  let histLimpo = String(hist).trim();
  histLimpo = histLimpo.replace(/\*/g, " * ");
  histLimpo = histLimpo.replace(/\s+\*\s+/g, " * ");
  histLimpo = histLimpo.replace(/\s+/g, " ").trim();
  if (!histLimpo.startsWith("*")) histLimpo = `* ${histLimpo}`;
  if (!histLimpo.endsWith("*")) histLimpo = `${histLimpo} *`;
  histLimpo = histLimpo.replace(/\s*\*\s*/g, " * ");
  const partes = histLimpo.split("*").map((p) => p.trim()).filter(Boolean);
  for (let i = 0; i <= partes.length - 4; i += 1) {
    if (String(partes[i]).toUpperCase() !== "DOT") continue;
    const adj = partes[i + 1];
    const ano = partes[i + 2];
    const idDot = partes[i + 3];
    if (/^\d{4}$/.test(String(ano)) && /^\d+$/.test(String(idDot))) {
      return `* DOT * ${adj} * ${ano} * ${idDot} *`;
    }
  }
  return null;
}

function identificarChavePlanejamento(dataset, chavesPlanejamento, jsonCasosPath, keyColName, partesChave) {
  const casosEspecificos = carregarCasosEspecificos(jsonCasosPath);
  const chavesNorm = chavesPlanejamento.map((c) => canonicalizarChave(c));

  const obterExercicioLinha = (row) => {
    for (const key of Object.keys(row || {})) {
      const norm = normalizeSimple(key);
      if (norm === "exercicio" || norm.startsWith("exerc")) {
        const ano = parseAno(row[key]);
        if (ano) return ano;
      }
    }
    return null;
  };

  const paraPipe = (chave) => {
    if (typeof chave !== "string") return "";
    let texto = chave.replace(/\*/g, "|");
    texto = texto.replace(/\s+/g, " ").trim();
    if (!texto.startsWith("|")) texto = `| ${texto}`;
    if (!texto.endsWith("|")) texto = `${texto} |`;
    return texto.replace(/\s+/g, " ");
  };

  const resultados = [];

  for (const row of dataset.rows) {
    const anoLinha = obterExercicioLinha(row);
    const partesLinha = anoLinha && anoLinha >= 2026 ? 8 : partesChave;
    const chavesBase = chavesNorm.filter((c) => contarPartesChave(c) === partesLinha);
    const chavesSetBase = new Set(chavesBase);
    const chavesPipeSetBase = new Set(chavesBase.map((c) => paraPipe(c)).filter(Boolean));

    const hist = row["Hist\u00f3rico"] || "";
    if (hist === "NÃO INFORMADO") {
      resultados.push("NÃO IDENTIFICADO");
      continue;
    }
    let histLimpo = String(hist).trim();
    histLimpo = histLimpo.replace(/\*/g, " * ");
    histLimpo = histLimpo.replace(/\s+\*\s+/g, " * ");
    histLimpo = histLimpo.replace(/\s+/g, " ").trim();
    histLimpo = corrigirTermosCorrompidos(histLimpo);
    if (!histLimpo.startsWith("*")) histLimpo = `* ${histLimpo}`;
    if (!histLimpo.endsWith("*")) histLimpo = `${histLimpo} *`;
    histLimpo = histLimpo.replace(/\s*\*\s*/g, " * ");

    const histCanon = canonicalizarChave(histLimpo);
    const histPipe = paraPipe(histCanon);
    const histPipeComp = normalizeSimple(histPipe);

    const chaveDireta = extrairChaveValidaDoHistorico(histCanon, chavesBase);
    if (chaveDireta) {
      resultados.push(canonicalizarChave(chaveDireta));
      continue;
    }
    if (chavesPipeSetBase.has(histPipe)) {
      const chaveStar = canonicalizarChave(histPipe.replace(/\|/g, "*"));
      resultados.push(chaveStar);
      continue;
    }

    let casoEncontrado = null;
    const histComp = normalizeSimple(histCanon);
    for (const [casoNorm, chave] of Object.entries(casosEspecificos)) {
      if (casoNorm && (histComp.includes(casoNorm) || histPipeComp.includes(casoNorm))) {
        if (contarPartesChave(chave) === partesLinha) {
          casoEncontrado = chave;
          break;
        }
      }
    }
    if (casoEncontrado) {
      console.error(`[emp] chave_arrumar aplicada: ${casoEncontrado}`);
      resultados.push(canonicalizarChave(casoEncontrado));
      continue;
    }

    const partes = histCanon.split("*").map((p) => p.trim()).filter(Boolean);
    let chaveJanela = "NÃO IDENTIFICADO";
    const tamanhoJanela = Math.max(1, partesLinha);
    if (partes.length >= tamanhoJanela) {
      for (let i = 0; i <= partes.length - tamanhoJanela; i += 1) {
        const janela = `* ${partes.slice(i, i + tamanhoJanela).join(" * ")} *`;
        if (chavesSetBase.has(janela)) {
          chaveJanela = janela;
          break;
        }
        const janelaPipe = paraPipe(janela);
        if (chavesPipeSetBase.has(janelaPipe)) {
          chaveJanela = canonicalizarChave(janelaPipe.replace(/\|/g, "*"));
          break;
        }
      }
    }
    resultados.push(chaveJanela === "NÃO IDENTIFICADO" ? chaveJanela : canonicalizarChave(chaveJanela));
  }

  dataset.columns = [keyColName, ...dataset.columns];
  dataset.rows = dataset.rows.map((row, idx) => {
    const value = resultados[idx];
    const out = { [keyColName]: value, ...row };
    if (typeof out[keyColName] === "string" && !["NÃO IDENTIFICADO", "IGNORADO"].includes(out[keyColName])) {
      out[keyColName] = canonicalizarChave(out[keyColName]);
    }
    return out;
  });

  return dataset;
}

function forcarChavesManualmente(dataset, keyColName) {
  const substituicoes = carregarForcarChaves(JSON_FORCAR_PATH);
  if (!Object.keys(substituicoes).length) return dataset;
  if (!dataset.columns.includes("N\u00ba EMP")) return dataset;
  for (const row of dataset.rows) {
    const numEmp = String(row["N\u00ba EMP"] || "").trim();
    if (substituicoes[numEmp]) {
      row[keyColName] = canonicalizarChave(substituicoes[numEmp]);
      row.__forcar_chave = true;
    }
  }
  return dataset;
}

function adicionarNovasColunas(dataset, keyColName, planejamentoAtivo) {
  const novasColunasPlanejamento = [
    "Regi\u00e3o",
    "Subfun\u00e7\u00e3o + UG",
    "ADJ",
    "Macropol\u00edtica",
    "Pilar",
    "Eixo",
    "Pol\u00edtica_Decreto",
  ];
  const novasColunasOrcamentarias = [
    "Fun\u00e7\u00e3o",
    "Subfun\u00e7\u00e3o",
    "Programa de Governo",
    "PAOE",
    "Natureza de Despesa",
    "Cat.Econ",
    "Grupo",
    "Modalidade",
    "Fonte",
    "Iduso",
    "Elemento",
    "Nome do Elemento",
  ];

  if (!dataset.columns.includes("Exerc\u00edcio")) {
    return null;
  }

  if (planejamentoAtivo) {
    const idx = dataset.columns.indexOf("Exerc\u00edcio");
    for (let i = novasColunasPlanejamento.length - 1; i >= 0; i -= 1) {
      const col = novasColunasPlanejamento[i];
      if (!dataset.columns.includes(col)) {
        dataset.columns.splice(idx, 0, col);
        for (const row of dataset.rows) {
          row[col] = "NÃO INFORMADO";
        }
      }
    }
  }

  if (!dataset.columns.includes("Dota\u00e7\u00e3o Or\u00e7ament\u00e1ria")) {
    return null;
  }

  const insertPos = dataset.columns.indexOf("Dota\u00e7\u00e3o Or\u00e7ament\u00e1ria") + 1;
  let pos = insertPos;
  for (const col of novasColunasOrcamentarias) {
    if (!dataset.columns.includes(col)) {
      dataset.columns.splice(pos, 0, col);
      for (const row of dataset.rows) {
        row[col] = "NÃO INFORMADO";
      }
      pos += 1;
    }
  }

  const colunas = dataset.columns.slice();
  if (colunas.includes("Fonte") && colunas.includes("Iduso") && colunas.includes("Nome do Elemento")) {
    const filtered = colunas.filter((c) => c !== "Fonte" && c !== "Iduso");
    const idxNome = filtered.indexOf("Nome do Elemento");
    filtered.splice(idxNome + 1, 0, "Fonte", "Iduso");
    dataset.columns = filtered;
  }

  if (dataset.columns.includes("Hist\u00f3rico") && dataset.columns.includes("Iduso") && dataset.columns.includes("Credor")) {
    const filtered = dataset.columns.filter((c) => c !== "Hist\u00f3rico");
    const idxIduso = filtered.indexOf("Iduso");
    filtered.splice(idxIduso + 1, 0, "Hist\u00f3rico");
    dataset.columns = filtered;
  }

  return dataset;
}

function preencherNovasColunas(dataset, keyColName, planejamentoAtivo, partesChave) {
  if (planejamentoAtivo && dataset.columns.includes(keyColName)) {
    for (const row of dataset.rows) {
      const chave = row[keyColName];
      let partes = [];
      if (typeof chave === "string" && !["", "NÃO IDENTIFICADO", "NÃO INFORMADO"].includes(chave.trim())) {
        if (chave.trim() === "#") {
          partes = Array(partesChave).fill("#");
        } else {
          partes = chave.split("*").map((p) => p.trim()).filter(Boolean);
        }
      }
      if (partes.length < partesChave) {
        while (partes.length < partesChave) partes.push("NÃO INFORMADO");
      }
      if (partes.length > partesChave) {
        partes = partes.slice(0, partesChave);
      }
      const cols = ["Regi\u00e3o", "Subfun\u00e7\u00e3o + UG", "ADJ", "Macropol\u00edtica", "Pilar", "Eixo", "Pol\u00edtica_Decreto"];
      cols.forEach((col, idx) => {
        row[col] = partes[idx] || "NÃO INFORMADO";
      });
    }
  } else {
    dataset.columns = dataset.columns.filter(
      (col) =>
        !["Regi\u00e3o", "Subfun\u00e7\u00e3o + UG", "ADJ", "Macropol\u00edtica", "Pilar", "Eixo", "Pol\u00edtica_Decreto"].includes(col)
    );
    for (const row of dataset.rows) {
      delete row["Regi\u00e3o"];
      delete row["Subfun\u00e7\u00e3o + UG"];
      delete row["ADJ"];
      delete row["Macropol\u00edtica"];
      delete row["Pilar"];
      delete row["Eixo"];
      delete row["Pol\u00edtica_Decreto"];
    }
  }

  for (const row of dataset.rows) {
    const dotacao = String(row["Dota\u00e7\u00e3o Or\u00e7ament\u00e1ria"] || "").trim();
    let partes = dotacao.split(".").map((p) => p.trim()).filter(Boolean);
    if (partes.length < 11) {
      while (partes.length < 11) partes.push("NÃO INFORMADO");
    }
    row["Fun\u00e7\u00e3o"] = partes[2] || "NÃO INFORMADO";
    row["Subfun\u00e7\u00e3o"] = partes[3] || "NÃO INFORMADO";
    row["Programa de Governo"] = partes[4] || "NÃO INFORMADO";
    row["PAOE"] = partes[5] || "NÃO INFORMADO";
    row["Natureza de Despesa"] = partes[7] || "NÃO INFORMADO";
    row["Fonte"] = partes[8] || "NÃO INFORMADO";
    row["Iduso"] = partes[9] || "NÃO INFORMADO";

    const natureza = String(row["Natureza de Despesa"] || "");
    if (natureza.length < 4) {
      row["Cat.Econ"] = "NÃO INFORMADO";
      row["Grupo"] = "NÃO INFORMADO";
      row["Modalidade"] = "NÃO INFORMADO";
    } else {
      row["Cat.Econ"] = natureza.slice(0, 1);
      row["Grupo"] = natureza.slice(1, 2);
      row["Modalidade"] = natureza.slice(2, 4);
    }
  }

  const colunasRemover = [
    "N\u00ba Processo Or\u00e7ament\u00e1rio de Pagamento",
    "N\u00ba NOBLIST",
    "N\u00ba DOTLIST",
    "N\u00ba OS",
    "N\u00ba Emenda (EP)",
    "Autor da Emenda (EP)",
    "N\u00ba ABJ",
    "N\u00ba Processo do Sequestro Judicial",
    "CBA",
    "Tipo Conta Banc\u00e1ria",
    "N\u00ba Licita\u00e7\u00e3o",
    "Ano Licita\u00e7\u00e3o",
    "Nome do Elemento",
    "RP",
    "Ordenador",
    "Nome do Ordenador de Despesa",
    "Finalidade de Aplica\u00e7\u00e3o FUNDEB (EMP)",
    "Grupo Despesa",
    "Nome do Grupo Despesa",
    "Modalidade de Aplica\u00e7\u00e3o",
    "Nome da Modalidade de Aplica\u00e7\u00e3o",
    "Usu\u00e1rio Respons\u00e1vel",
    "N\u00famero da Licita\u00e7\u00e3o",
    "Ano da Licita\u00e7\u00e3o",
    "Fundamento Legal(Amparo Legal)",
    "Justificativa para despesa sem contrato(Sim/N\u00e3o)",
    "Despesa em Processamento",
    "UO Extinta",
    "N\u00ba NEX",
    "Situa\u00e7\u00e3o NEX",
    "Valor da NEX",
    "N\u00ba RPV",
    "RPV Vencido",
    "N\u00ba CAD",
    "N\u00ba NLA",
  ];

  dataset.columns = dataset.columns.filter((col) => !colunasRemover.includes(col));
  for (const row of dataset.rows) {
    for (const col of colunasRemover) {
      delete row[col];
    }
  }

  return dataset;
}
function moverColunas(columns, colsToMove, referencia, moverParaFim) {
  const existentes = colsToMove.filter((c) => columns.includes(c));
  if (!existentes.length) return columns;
  const restantes = columns.filter((c) => !existentes.includes(c));
  if (!moverParaFim && restantes.includes(referencia)) {
    const idx = restantes.indexOf(referencia);
    return [...restantes.slice(0, idx + 1), ...existentes, ...restantes.slice(idx + 1)];
  }
  return [...restantes, ...existentes];
}

function moverColunasParaDireita(columns, colsToMove, referencia) {
  const existentes = colsToMove.filter((c) => columns.includes(c));
  if (!existentes.length) return columns;
  const restantes = columns.filter((c) => !existentes.includes(c));
  if (!restantes.includes(referencia)) {
    return [...restantes, ...existentes];
  }
  const idx = restantes.indexOf(referencia);
  return [...restantes.slice(0, idx + 1), ...existentes, ...restantes.slice(idx + 1)];
}

function atualizarTipoDespesa(dataset) {
  if (!dataset.columns.includes("Hist\u00f3rico") || !dataset.columns.includes("Tipo de Despesa")) {
    return dataset;
  }
  for (const row of dataset.rows) {
    const hist = String(row["Hist\u00f3rico"] || "").toLowerCase();
    if (/\bbolsas?\b/.test(hist)) {
      row["Tipo de Despesa"] = "Bolsa";
    }
  }
  return dataset;
}

function calcularValorLiquido(dataset) {
  if (!dataset.columns.includes("Valor EMP") || !dataset.columns.includes("Devolu\u00e7\u00e3o GCV")) {
    return dataset;
  }
  for (const row of dataset.rows) {
    const emp = Number(String(row["Valor EMP"] || "0").replace(/,/g, ".")) || 0;
    const gcv = Number(String(row["Devolu\u00e7\u00e3o GCV"] || "0").replace(/,/g, ".")) || 0;
    row["Valor EMP-Devolu\u00e7\u00e3o GCV"] = (emp - gcv).toFixed(2);
  }
  if (!dataset.columns.includes("Valor EMP-Devolu\u00e7\u00e3o GCV")) {
    dataset.columns.push("Valor EMP-Devolu\u00e7\u00e3o GCV");
  }
  const idx = dataset.columns.indexOf("Devolu\u00e7\u00e3o GCV");
  if (idx !== -1) {
    dataset.columns = dataset.columns.filter((c) => c !== "Valor EMP-Devolu\u00e7\u00e3o GCV");
    dataset.columns.splice(idx + 1, 0, "Valor EMP-Devolu\u00e7\u00e3o GCV");
  }
  return dataset;
}

function montarRegistrosParaDb(dataset, dataArquivo, userEmail, uploadId) {
  const registros = [];
  for (const row of dataset.rows) {
    const payload = {};
    for (const [col, val] of Object.entries(row)) {
      const key = normalizeColName(col);
      const dbCol = COL_MAP[key];
      if (!dbCol) continue;
      payload[dbCol] = val === "-" ? null : val;
    }

    const ano = parseAno(payload.exercicio);
    if (row.__forcar_chave) {
      payload.chave = payload.chave || null;
      payload.chave_planejamento = payload.chave_planejamento || null;
    } else {
      const partesPlanejamento = ano && ano >= 2026 ? 8 : 7;
      const chavePartes = contarPartesChave(payload.chave);
      const planejamentoPartes = contarPartesChave(payload.chave_planejamento);
      payload.chave = chavePartes === 4 ? payload.chave : null;
      payload.chave_planejamento = planejamentoPartes === partesPlanejamento ? payload.chave_planejamento : null;
    }

    for (const col of ["valor_emp", "devolucao_gcv", "valor_emp_devolucao_gcv"]) {
      if (col in payload) payload[col] = parseValorDb(payload[col]);
    }
    for (const col of ["data_emissao", "data_criacao"]) {
      if (col in payload) payload[col] = parseDataDb(payload[col]);
    }

    const rawRow = { ...row };
    delete rawRow.__forcar_chave;
    payload.raw_payload = JSON.stringify(rawRow);
    payload.upload_id = uploadId;
    payload.data_atualizacao = new Date();
    payload.data_arquivo = dataArquivo || null;
    payload.user_email = userEmail;
    registros.push(payload);
  }
  return registros;
}

async function carregarPlanilha(filePath) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(filePath);
  const sheet = workbook.worksheets.find((ws) => String(ws.name || "").trim().toLowerCase() === "planilha1");
  const worksheet = sheet || workbook.worksheets[0];
  if (!worksheet) throw new Error("Planilha nao encontrada.");

  let headerRowIdx = null;
  for (let idx = 1; idx <= worksheet.rowCount; idx += 1) {
    const cellValue = worksheet.getRow(idx).getCell(1).value;
    if (typeof cellValue === "string" && cellValue.trim().toLowerCase().startsWith("exerc")) {
      headerRowIdx = idx;
      break;
    }
  }
  if (!headerRowIdx) throw new Error("Não foi possível localizar a linha de cabeçalho (Exercício).");

  const headerRow = worksheet.getRow(headerRowIdx).values.slice(1);
  const header = headerRow.map((c) => String(c ?? "").trim());
  const columns = normalizeColumns(header).map((c) => c.trim());

  const rows = [];
  for (let idx = headerRowIdx + 1; idx <= worksheet.rowCount; idx += 1) {
    const rowValues = worksheet.getRow(idx).values.slice(1);
    const row = {};
    columns.forEach((col, colIdx) => {
      const val = colIdx < rowValues.length ? rowValues[colIdx] : null;
      if (val === null || val === undefined) {
        row[col] ?? "";
      } else if (val instanceof Date) {
        row[col] = formatDatePtBr(val);
      } else {
        row[col] = String(val).trim();
      }
    });
    row.__linha = idx;
    rows.push(row);
  }

  return { columns, rows };
}

async function processEmp(filePath, dataArquivo, userEmail, uploadId) {
  ensureDir(OUTPUT_DIR);

  const raw = await carregarPlanilha(filePath);
  const dfEmpBase = {
    columns: raw.columns.slice(),
    rows: raw.rows.map((row) => {
      const out = { ...row };
      for (const key of Object.keys(out)) {
        out[key] = cleanForEmpSheet(out[key]);
      }
      return out;
    }),
  };

  let df = { columns: raw.columns.slice(), rows: raw.rows.map((row) => ({ ...row })) };

  df = removerEmpenhosEstornados(df).dataset;
  dfEmpBase.rows = removerEmpenhosEstornados(dfEmpBase).dataset.rows;

  if (df.columns.includes("Hist\u00f3rico")) {
    for (const row of df.rows) {
      row["Hist\u00f3rico"] = cleanHistorico(row["Hist\u00f3rico"]);
    }
  }

  for (const row of df.rows) {
    for (const col of df.columns) {
      if (typeof row[col] === "string") {
        row[col] = corrigirCaracteres(row[col]);
      }
    }
  }

  df = converterTipos(df);

  const exercicioVal = obterExercicio(df);
  const partesPlanejamento = exercicioVal !== null && exercicioVal >= 2026 ? 8 : 7;
  const keyColName = "Chave de Planejamento";
  const planejamentoAtivo = true;

  const chavesPlanejamento = carregarChavesPlanejamento(JSON_CHAVES_PATH);
  df = identificarChavePlanejamento(df, chavesPlanejamento, JSON_CASOS_PATH, keyColName, partesPlanejamento);
  df = forcarChavesManualmente(df, keyColName);

  df = adicionarNovasColunas(df, keyColName, planejamentoAtivo);
  if (!df) throw new Error("Falha ao adicionar novas colunas.");
  df = preencherNovasColunas(df, keyColName, planejamentoAtivo, partesPlanejamento);

  for (const row of df.rows) {
    const chaveDot = extrairChaveDotDoHistorico(row["Hist\u00f3rico"] || "");
    if (chaveDot) {
      row.Chave = canonicalizarChave(chaveDot);
    }
  }

  df = removerEmpenhosEstornados(df).dataset;
  dfEmpBase.rows = removerEmpenhosEstornados(dfEmpBase).dataset.rows;

  const missingPlanejamentoLines = [];
  for (let i = 0; i < df.rows.length; i += 1) {
    const row = df.rows[i];
    const value = String(row[keyColName] || "").trim();
    if (!value || ["NÇO IDENTIFICADO", "NÃO IDENTIFICADO", "NÇO INFORMADO", "NÃO INFORMADO", "-"].includes(value)) {
      missingPlanejamentoLines.push(i + 2); // header na linha 1, dados a partir da linha 2
    }
  }
  updateStatusFields("emp", uploadId, {
    planejamento_missing_lines: missingPlanejamentoLines,
  });

  df = atualizarTipoDespesa(df);
  df.columns = moverColunas(
    df.columns,
    ["Data emiss\u00e3o", "Data cria\u00e7\u00e3o", "N\u00ba Contrato", "N\u00ba Conv\u00eanio"],
    "Situa\u00e7\u00e3o",
    false
  );
  df.columns = moverColunasParaDireita(df.columns, ["Valor EMP", "Devolu\u00e7\u00e3o GCV"], "N\u00ba PED");
  df = calcularValorLiquido(df);
  df = converterTipos(df);

  const dfSaida = formatarParaSaidaPtBr(df);
  for (const row of dfSaida.rows) {
    for (const col of dfSaida.columns) {
      if (row[col] === "NÃO IDENTIFICADO") row[col] = "NÃO IDENTIFICADO";
      if (row[col] === "NÃO INFORMADO") row[col] = "-";
    }
  }

  const outputFile = path.join(
    OUTPUT_DIR,
    `${path.basename(filePath, path.extname(filePath))}_Tratado_${Date.now()}.xlsx`
  );
  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: outputFile });
  const sheetEmp = workbook.addWorksheet("emp");
  const sheetTratado = workbook.addWorksheet("emp_tratado");
  sheetEmp.columns = dfEmpBase.columns.map((col) => ({ header: fixOutputHeader(col), key: col }));
  sheetTratado.columns = buildTratadoColumns(dfSaida.columns);

  for (const row of dfEmpBase.rows) {
    const values = dfEmpBase.columns.map((col) => row[col] ?? "");
    sheetEmp.addRow(values).commit();
  }
  for (const row of dfSaida.rows) {
    sheetTratado.addRow(row).commit();
  }

  await workbook.commit();

  const db = await connect();
  if (db.kind === "mssql") {
    await db.pool.request().query("UPDATE emp SET ativo = 0 WHERE ativo = 1");
  } else {
    await db.pool.query("UPDATE emp SET ativo = 0 WHERE ativo = 1");
  }

  const registros = montarRegistrosParaDb(dfSaida, dataArquivo, userEmail, uploadId);
  let total = 0;
  const batch = [];
  for (const registro of registros) {
    if (readCancelFlag("emp", uploadId)) {
      throw new Error("PROCESSAMENTO_CANCELADO");
    }
    batch.push(registro);
    if (batch.length >= BATCH_SIZE) {
      await bulkInsert(db, "emp", INSERT_COLS, batch);
      total += batch.length;
      updateStatusFields("emp", uploadId, {
        progress: Math.min(100, Math.floor((total / registros.length) * 100)),
        message: `Gravando registros no banco (${total}/${registros.length}).`,
      });
      batch.length = 0;
    }
  }
  if (batch.length) {
    await bulkInsert(db, "emp", INSERT_COLS, batch);
    total += batch.length;
    updateStatusFields("emp", uploadId, {
      progress: 100,
      message: `Gravando registros no banco (${total}/${registros.length}).`,
    });
  }

  if (db.kind === "mssql") {
    await db.pool.close();
  } else {
    await db.pool.end();
  }

  return { total, outputPath: outputFile };
}

module.exports = {
  processEmp,
};
