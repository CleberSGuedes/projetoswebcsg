const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const TARGET_DIRS = [path.join(ROOT, "upload"), path.join(ROOT, "outputs")];

const isTmpDir = (dirPath) => path.basename(dirPath).toLowerCase() === "tmp";

async function listDir(dirPath) {
  try {
    return await fs.promises.readdir(dirPath, { withFileTypes: true });
  } catch (err) {
    if (err && err.code === "ENOENT") return [];
    throw err;
  }
}

async function getXlsxFiles(dirPath) {
  const entries = await listDir(dirPath);
  const files = [];
  for (const entry of entries) {
    if (!entry.isFile()) continue;
    if (!entry.name.toLowerCase().endsWith(".xlsx")) continue;
    const fullPath = path.join(dirPath, entry.name);
    try {
      const stat = await fs.promises.stat(fullPath);
      files.push({ path: fullPath, mtimeMs: stat.mtimeMs });
    } catch (err) {
      if (err && err.code === "ENOENT") continue;
      throw err;
    }
  }
  return files.sort((a, b) => b.mtimeMs - a.mtimeMs);
}

async function cleanupDir(dirPath, keepCount) {
  const files = await getXlsxFiles(dirPath);
  const toDelete = files.slice(keepCount);
  for (const item of toDelete) {
    try {
      await fs.promises.unlink(item.path);
      // eslint-disable-next-line no-console
      console.log(`Removido: ${item.path}`);
    } catch (err) {
      if (err && err.code === "ENOENT") continue;
      throw err;
    }
  }
}

async function walk(dirPath) {
  if (isTmpDir(dirPath)) {
    await cleanupDir(dirPath, 2);
    return;
  }

  await cleanupDir(dirPath, 1);

  const entries = await listDir(dirPath);
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const nextPath = path.join(dirPath, entry.name);
    await walk(nextPath);
  }
}

async function main() {
  for (const baseDir of TARGET_DIRS) {
    await walk(baseDir);
  }
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error("Falha na limpeza de XLSX:", err);
  process.exit(1);
});
