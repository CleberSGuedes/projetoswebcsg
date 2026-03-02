const fs = require("fs");
const path = require("path");

const args = process.argv.slice(2);
const defaultTargets = [
  path.join("..", "static", "js", "main.js"),
  path.join("..", "templates"),
  path.join("..", "rotas"),
  path.join("..", "services"),
  path.join("..", "models"),
];
const targets = args.length ? args : defaultTargets;

const wordWithQ = /[A-Za-zÀ-ÖØ-öø-ÿ]+\?[A-Za-zÀ-ÖØ-öø-ÿ]+/g;
const urlPattern = /https?:\/\/\S+/g;
const unicodeEscape = /\\u00[0-9a-fA-F]{2}/g;

let hasIssues = false;
const allowedExts = new Set([
  ".js",
  ".py",
  ".html",
  ".htm",
  ".jinja",
  ".jinja2",
  ".txt",
  ".md",
  ".css",
  ".json",
]);

const checkFile = (filePath) => {
  const rel = path.relative(process.cwd(), filePath);
  let content;
  try {
    content = fs.readFileSync(filePath, "utf8");
  } catch (err) {
    console.error(`[utf8-check] Falha ao ler ${rel}: ${err.message}`);
    hasIssues = true;
    return;
  }

  const contentNoUrls = content.replace(urlPattern, "");
  const wordMatches = contentNoUrls.match(wordWithQ) || [];
  const ext = path.extname(filePath).toLowerCase();
  const escapeMatches = ext === ".py" ? [] : (content.match(unicodeEscape) || []);

  if (wordMatches.length || escapeMatches.length) {
    hasIssues = true;
    console.error(`[utf8-check] Problemas em ${rel}`);
    if (wordMatches.length) {
      const uniq = Array.from(new Set(wordMatches)).slice(0, 20);
      console.error(`  Palavras com '?': ${uniq.join(", ")}`);
    }
    if (escapeMatches.length) {
      const uniq = Array.from(new Set(escapeMatches)).slice(0, 20);
      console.error(`  Escapes Unicode: ${uniq.join(", ")}`);
    }
  }
};

const resolveCandidate = (target) => {
  const byCwd = path.resolve(process.cwd(), target);
  if (fs.existsSync(byCwd)) return byCwd;
  const byScript = path.resolve(__dirname, target);
  if (fs.existsSync(byScript)) return byScript;
  // if default target and still missing, try repo root relative
  if (defaultTargets.includes(target)) {
    const repoGuess = path.resolve(__dirname, "..", "..", "static", "js", "main.js");
    if (fs.existsSync(repoGuess)) return repoGuess;
  }
  return null;
};

const shouldCheckFile = (filePath) => {
  const ext = path.extname(filePath).toLowerCase();
  return allowedExts.has(ext);
};

const scanDir = (dirPath) => {
  const entries = fs.readdirSync(dirPath, { withFileTypes: true });
  for (const entry of entries) {
    const filePath = path.join(dirPath, entry.name);
    if (entry.isDirectory()) {
      scanDir(filePath);
      continue;
    }
    if (entry.isFile() && shouldCheckFile(filePath)) {
      checkFile(filePath);
    }
  }
};

for (const target of targets) {
  const resolved = resolveCandidate(target);
  if (!resolved) {
    console.error(`[utf8-check] Caminho inválido: ${target}`);
    hasIssues = true;
    continue;
  }
  if (fs.statSync(resolved).isFile()) {
    checkFile(resolved);
    continue;
  }
  if (fs.statSync(resolved).isDirectory()) {
    scanDir(resolved);
    continue;
  }
}

if (hasIssues) {
  process.exit(1);
}
console.log("[utf8-check] OK");
