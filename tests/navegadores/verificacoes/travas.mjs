// Travas do padrão (pytest): cor e tamanho de letra escritos à mão, style= nos templates e no JS, kit igual ao SPO.
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { RAIZ } from "../lib/ambiente.mjs";
import { registrar } from "../lib/registro.mjs";

const TESTES = ["tests/test_cor_literal.py", "tests/test_tamanho_fonte.py", "tests/test_style_inline.py", "tests/test_kit_mockup.py"];

export async function travas() {
  const python = [resolve(RAIZ, ".venv", "Scripts", "python.exe"), resolve(RAIZ, ".venv", "bin", "python")].find(existsSync) || "python";
  const r = spawnSync(python, ["-m", "pytest", "-q", "-p", "no:cacheprovider", ...TESTES], { cwd: RAIZ, encoding: "utf-8" });
  const fim = (r.stdout || "").trim().split(/\r?\n/).pop() || r.stderr || "sem saída";
  registrar({ verificacao: "travas", alvo: TESTES.length + " arquivos de teste", situacao: r.status === 0 ? "ok" : "falha", resumo: fim });
}
