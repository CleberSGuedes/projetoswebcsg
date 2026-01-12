const fs = require("fs");
const path = require("path");

const MISSING_INFO = "NÃO INFORMADO";
const MISSING_ID = "NÃO IDENTIFICADO";

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function readJsonWithBom(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const text = raw.charCodeAt(0) === 0xfeff ? raw.slice(1) : raw;
  return JSON.parse(text);
}

function sanitizeString(value) {
  if (value === null || value === undefined) return "";
  let text = String(value);
  text = text.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, "");
  return text;
}

function normalizeSimple(value) {
  let text = sanitizeString(value);
  text = text.replace(/\u00a0/g, " ");
  try {
    text = text.normalize("NFKD");
  } catch {
    text = sanitizeString(text);
  }
  text = text.replace(/\p{M}/gu, "");
  text = text.replace(/\s+/g, " ");
  return text.trim().toLowerCase();
}

function cleanHistorico(value) {
  if (typeof value !== "string") return MISSING_INFO;
  let text = value.replace(/_x000D_/g, " ").replace(/\n/g, " ").replace(/\r/g, " ");
  text = text.replace(/\s+\*\s+/g, " * ");
  text = text.replace(/\s+/g, " ").trim();
  return text || MISSING_INFO;
}

function corrigirCaracteres(value) {
  if (typeof value !== "string") return MISSING_INFO;
  let text = value.replace(/[^\p{L}\p{N}\s_.,/\-|*]/gu, "");
  text = text.replace(/\s+/g, " ").trim();
  return text || MISSING_INFO;
}

function normalizeForComparison(value) {
  if (typeof value !== "string") return "";
  let text = cleanHistorico(value);
  text = corrigirCaracteres(text);
  text = text.replace(/\s+\*\s+/g, " * ");
  text = text.replace(/\s+/g, " ").trim();
  return text.toLowerCase();
}

function cleanForEmpSheet(value) {
  if (typeof value !== "string") return value;
  let text = value.replace(/[\u0000-\u001f\u007f]/g, "");
  text = text.replace(/\s+/g, " ").trim();
  return text;
}

function canonicalizarChave(value) {
  if (typeof value !== "string") return value;
  const parts = value.split("*").map((p) => p.trim()).filter(Boolean);
  if (!parts.length) return value;
  return `* ${parts.join(" * ")} *`;
}

function parseValorDb(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  if (["", "-", MISSING_INFO, "NAO INFORMADO", MISSING_ID, "NAO IDENTIFICADO"].includes(text)) {
    return null;
  }
  let cleaned = text.replace(/[^\d,.-]/g, "");
  if (cleaned.includes(",")) {
    cleaned = cleaned.replace(/\./g, "").replace(/,/g, ".");
  }
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
}

function parseDataDb(value) {
  if (value === null || value === undefined) return null;
  if (value instanceof Date) return value;
  const text = String(value).trim();
  if (!text || ["-", "00/00/0000", "00/00/0000 00:00:00"].includes(text)) return null;
  const candidate = text.replace(/-/g, "/");
  const formats = [
    /^\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}:\d{2}$/,
    /^\d{2}\/\d{2}\/\d{4}$/,
    /^\d{4}-\d{2}-\d{2}$/,
  ];
  if (formats[0].test(candidate)) {
    const [datePart, timePart] = candidate.split(" ");
    const [d, m, y] = datePart.split("/").map(Number);
    const [hh, mm, ss] = timePart.split(":").map(Number);
    return new Date(y, m - 1, d, hh, mm, ss);
  }
  if (formats[1].test(candidate)) {
    const [d, m, y] = candidate.split("/").map(Number);
    return new Date(y, m - 1, d);
  }
  if (formats[2].test(text)) {
    const [y, m, d] = text.split("-").map(Number);
    return new Date(y, m - 1, d);
  }
  return null;
}

function parseAno(value) {
  if (!value) return null;
  const match = String(value).match(/(\d{4})/);
  if (!match) return null;
  const num = Number(match[1]);
  return Number.isFinite(num) ? num : null;
}

function formatNumberPtBr(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "0,00";
  return num
    .toFixed(2)
    .replace(/\B(?=(\d{3})+(?!\d))/g, ".")
    .replace(/\.(?=\d{2}$)/, ",");
}

function parseBrNumber(value) {
  if (value === null || value === undefined) return 0.0;
  let text = String(value).trim();
  if (!text || ["NAO INFORMADO", MISSING_INFO].includes(text.toUpperCase())) return 0.0;
  text = text.replace(/\u00a0/g, " ");
  text = text.replace(/R\$/g, "").replace(/\s+/g, "");
  if (text.includes(",")) {
    text = text.replace(/\./g, "").replace(/,/g, ".");
  }
  text = text.replace(/[^0-9.-]/g, "");
  const num = Number(text);
  return Number.isFinite(num) ? num : 0.0;
}

function formatDatePtBr(value) {
  if (!value) return MISSING_INFO;
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return MISSING_INFO;
  const dd = String(date.getDate()).padStart(2, "0");
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const yyyy = String(date.getFullYear());
  return `${dd}/${mm}/${yyyy}`;
}

function formatDateIso(value) {
  if (!value) return "0000-00-00";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "0000-00-00";
  const dd = String(date.getDate()).padStart(2, "0");
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const yyyy = String(date.getFullYear());
  return `${yyyy}-${mm}-${dd}`;
}

function statusDir() {
  return path.resolve(__dirname, "..", "outputs", "status");
}

function statusPath(kind, uploadId) {
  const safeKind = String(kind || "").trim().toLowerCase();
  return path.join(statusDir(), `${safeKind}_${uploadId}.json`);
}

function cancelPath(kind, uploadId) {
  const safeKind = String(kind || "").trim().toLowerCase();
  return path.join(statusDir(), `${safeKind}_${uploadId}.cancel`);
}

function writeStatus(kind, uploadId, payload) {
  ensureDir(statusDir());
  const now = new Date().toISOString();
  const data = {
    kind: String(kind || "").trim().toLowerCase(),
    upload_id: Number(uploadId),
    updated_at: now,
    ...payload,
  };
  fs.writeFileSync(statusPath(kind, uploadId), JSON.stringify(data), "utf8");
}

function updateStatusFields(kind, uploadId, fields) {
  ensureDir(statusDir());
  const pathStatus = statusPath(kind, uploadId);
  let current = {};
  if (fs.existsSync(pathStatus)) {
    try {
      current = JSON.parse(fs.readFileSync(pathStatus, "utf8"));
    } catch {
      current = {};
    }
  }
  const now = new Date().toISOString();
  const data = {
    kind: String(kind || "").trim().toLowerCase(),
    upload_id: Number(uploadId),
    ...current,
    ...fields,
    updated_at: now,
  };
  fs.writeFileSync(pathStatus, JSON.stringify(data), "utf8");
}

function readCancelFlag(kind, uploadId) {
  return fs.existsSync(cancelPath(kind, uploadId));
}

module.exports = {
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
  parseBrNumber,
  formatDatePtBr,
  formatDateIso,
  writeStatus,
  updateStatusFields,
  readCancelFlag,
};
