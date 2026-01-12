const path = require("path");
const { processEmp } = require("./emp_runner");
const { processNob } = require("./nob_runner");
const { writeStatus } = require("./util");

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    if (!key.startsWith("--")) continue;
    const value = argv[i + 1];
    args[key.slice(2)] = value;
    i += 1;
  }
  return args;
}

async function main() {
  const args = parseArgs(process.argv);
  const kind = args.kind;
  const filePath = args.file;
  const uploadId = Number(args["upload-id"] || 0);
  const userEmail = args["user-email"] || "desconhecido";
  const dataArquivoRaw = args["data-arquivo"] || "";
  const parsedDate = dataArquivoRaw ? new Date(dataArquivoRaw) : null;
  const dataArquivo = parsedDate && !Number.isNaN(parsedDate.getTime()) ? parsedDate : null;

  if (!kind || !filePath || !uploadId) {
    throw new Error("Parametros obrigatorios ausentes.");
  }

  writeStatus(kind, uploadId, {
    state: "em processamento",
    message: "Processamento iniciado (node).",
    progress: 0,
    pid: process.pid,
  });

  if (kind === "emp") {
    return await processEmp(filePath, dataArquivo, userEmail, uploadId);
  }
  if (kind === "nob") {
    return await processNob(filePath, dataArquivo, userEmail, uploadId);
  }
  throw new Error(`Tipo nao suportado: ${kind}`);
}

main()
  .then((result) => {
    const payload = {
      ok: true,
      total: result.total,
      output_filename: path.basename(result.outputPath),
      output_path: result.outputPath,
    };
    process.stdout.write(JSON.stringify(payload));
  })
  .catch((err) => {
    const payload = { ok: false, error: err.message || String(err) };
    process.stderr.write(JSON.stringify(payload));
    process.exit(1);
  });
