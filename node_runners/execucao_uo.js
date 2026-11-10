// UO nos módulos de execução (NOB e EMP) - espelho de services/uo.py e
// services/execucao_uo.py (docs/plano_implementacao_novas_uos.md, Fase 1).
// As listas de UO vêm do Python (worker.py) pelas variáveis de ambiente
// UOS_EXECUCAO e UOS_EXECUCAO_LIBERADAS, para existir uma fonte só.
const ExcelJS = require("exceljs");

function listaEnv(nome, padrao) {
  const bruto = String(process.env[nome] || "").trim();
  const itens = (bruto || padrao).split(",").map((s) => s.trim()).filter(Boolean);
  return new Set(itens);
}

// Padrão conservador (só a secretaria) quando o runner roda sem o worker.
const SEDUC = "14101,14601";

function uoLiberadas() {
  return listaEnv("UOS_EXECUCAO_LIBERADAS", SEDUC);
}

function uoCadastradas() {
  return listaEnv("UOS_EXECUCAO", SEDUC);
}

// "14.101 - SECRETARIA ...", "14101" ou "1101" -> "14101" / "01101".
function uoCodigo(valor) {
  const s = String(valor ?? "").trim();
  const codigo = s.includes(" - ") ? s.split(" - ", 1)[0] : s;
  const digitos = codigo.replace(/\D/g, "");
  return digitos ? digitos.padStart(5, "0") : "";
}

// Só valores que parecem código de UO (5 dígitos, com ponto opcional): evita
// que um rodapé numérico vire "UO".
function pareceUo(valor) {
  return /^\s*\d{2}\.?\d{3}(\s|$)/.test(String(valor ?? ""));
}

// "26", "2026", 2026 e "2026.0" -> "2026"; inválido -> "".
function exercicio4d(valor) {
  const m = String(valor ?? "").match(/^\s*(\d+)/);
  if (!m) return "";
  let n = parseInt(m[1], 10);
  if (n < 100) n += 2000;
  return n >= 2000 && n <= 2100 ? String(n) : "";
}

// Valida a UO lida do CONTEÚDO do relatório (nunca do nome do arquivo) e
// devolve o código único. Mesmas mensagens do services/uo.py.
function validarUoExecucao(uos) {
  const liberadas = uoLiberadas();
  const encontradas = [...new Set([...uos].map(uoCodigo).filter(Boolean))].sort();
  const aceitas = [...liberadas].sort().join(", ");
  if (!encontradas.length) {
    throw new Error("Não foi possível identificar a UO no relatório. Nada foi gravado.");
  }
  if (encontradas.length > 1) {
    throw new Error(`O arquivo tem mais de uma UO (${encontradas.join(", ")}). Envie um relatório por UO. Nada foi gravado.`);
  }
  const uo = encontradas[0];
  if (liberadas.has(uo)) return uo;
  if (uoCadastradas().has(uo)) {
    throw new Error(`A UO ${uo} ainda não está liberada neste módulo (liberadas: ${aceitas}). Nada foi gravado.`);
  }
  throw new Error(`UO ${uo} não cadastrada (liberadas: ${aceitas}). Nada foi gravado.`);
}

// Se as regras de Chave de Planejamento valem para a UO (decisão D2): só as
// UOs da secretaria. Espelho de uo_usa_chave() em services/uo.py.
function uoUsaChave(valor) {
  return listaEnv("UOS_SEDUC", SEDUC).has(uoCodigo(valor));
}

// UO e exercícios das linhas já carregadas em memória (EMP): mesma validação
// do lerUoExercicios, sem segunda leitura do arquivo.
function uoExercicioDeLinhas(rows, colUo = "UO", colEx = "Exercício") {
  const uos = new Set();
  const exercicios = new Set();
  for (const row of rows) {
    if (pareceUo(row[colUo])) uos.add(uoCodigo(row[colUo]));
    const ex = exercicio4d(row[colEx]);
    if (ex) exercicios.add(ex);
  }
  if (!exercicios.size) {
    throw new Error("Não foi possível identificar o exercício no relatório. Nada foi gravado.");
  }
  return { uo: validarUoExecucao(uos), exercicios: [...exercicios].sort() };
}

// Lê em streaming só as colunas UO e Exercício (antes de tocar no banco).
// detectHeader(linhas) -> { foundRowIdx, foundValues } | null, e
// normalizeColumns(valores) -> nomes canônicos ("UO", "Exercício"), são os
// do próprio runner, para a detecção do cabeçalho ser idêntica.
async function lerUoExercicios(filePath, detectHeader, normalizeColumns) {
  const reader = new ExcelJS.stream.xlsx.WorkbookReader(filePath);
  const uos = new Set();
  const exercicios = new Set();
  const buffer = [];
  let headerRowIdx = null;
  let uoIdx = -1;
  let exIdx = -1;

  const coletar = (values) => {
    if (uoIdx >= 0) {
      const v = values[uoIdx];
      if (pareceUo(v)) uos.add(uoCodigo(v));
    }
    if (exIdx >= 0 && values[exIdx] !== null && values[exIdx] !== undefined) {
      const ex = exercicio4d(values[exIdx]);
      if (ex) exercicios.add(ex);
    }
  };

  for await (const worksheetReader of reader) {
    for await (const row of worksheetReader) {
      const values = row.values ? row.values.slice(1) : [];
      if (!headerRowIdx) {
        if (buffer.length >= 15) {
          throw new Error("Cabecalho com colunas necessarias nao encontrado nas primeiras 15 linhas.");
        }
        buffer.push({ number: row.number, values });
        const detected = detectHeader(buffer.map((r) => r.values));
        if (detected) {
          headerRowIdx = detected.foundRowIdx;
          const norm = normalizeColumns(detected.foundValues);
          uoIdx = norm.indexOf("UO");
          exIdx = norm.indexOf("Exercício");
          if (uoIdx < 0) throw new Error("Coluna UO não encontrada no relatório. Nada foi gravado.");
          for (const b of buffer) {
            if (b.number > headerRowIdx) coletar(b.values);
          }
        }
        continue;
      }
      if (row.number <= headerRowIdx) continue;
      coletar(values);
    }
    break; // só a primeira aba, como o processamento
  }

  if (!headerRowIdx) {
    throw new Error("Cabecalho com colunas necessarias nao encontrado nas primeiras 15 linhas.");
  }
  if (!exercicios.size) {
    throw new Error("Não foi possível identificar o exercício no relatório. Nada foi gravado.");
  }
  return { uo: validarUoExecucao(uos), exercicios: [...exercicios].sort() };
}

// Desativa só as linhas ativas da MESMA UO e dos MESMOS exercícios do
// arquivo (antes: a tabela inteira). Compara por número: cargas antigas
// guardaram a UO sem o zero à esquerda. Só inteiros entram no SQL.
async function desativarVersaoAnterior(db, tabela, uo, exercicios, colunaExercicio = "exercicio") {
  if (db.kind === "mssql") {
    throw new Error("Desativação por UO não suportada no engine mssql.");
  }
  const uoNum = parseInt(uoCodigo(uo), 10);
  const anos = new Set();
  for (const e of exercicios) {
    const n = parseInt(exercicio4d(e), 10);
    if (Number.isInteger(n)) {
      anos.add(n);
      anos.add(n % 100);
    }
  }
  if (!Number.isInteger(uoNum) || !anos.size) {
    throw new Error("UO ou exercício inválido para a desativação da versão anterior.");
  }
  const sql =
    `UPDATE ${tabela} SET ativo = 0 WHERE ativo = 1 ` +
    `AND CAST(REPLACE(SUBSTRING_INDEX(uo, ' - ', 1), '.', '') AS UNSIGNED) = ${uoNum} ` +
    `AND CAST(${colunaExercicio} AS UNSIGNED) IN (${[...anos].sort((a, b) => a - b).join(", ")})`;
  const [res] = await db.pool.query(sql);
  return res && res.affectedRows ? res.affectedRows : 0;
}

module.exports = {
  uoCodigo,
  pareceUo,
  exercicio4d,
  validarUoExecucao,
  uoUsaChave,
  uoExercicioDeLinhas,
  lerUoExercicios,
  desativarVersaoAnterior,
};
