// Travas do padrão (pytest): cor e tamanho de letra escritos à mão, style= nos templates e no JS, kit igual ao SPO.
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { RAIZ } from "../lib/ambiente.mjs";
import { registrar } from "../lib/registro.mjs";

const TESTES = ["tests/test_cor_literal.py", "tests/test_tamanho_fonte.py", "tests/test_style_inline.py", "tests/test_kit_mockup.py"];

export async function travas() {
  const python = [resolve(RAIZ, ".venv", "Scripts", "python.exe"), resolve(RAIZ, ".venv", "bin", "python")].find(existsSync) || "python";
  // limite de 5 min (as travas levam menos de 1 s): sem ele, um pytest parado deixaria a bateria parada
  const r = spawnSync(python, ["-m", "pytest", "-q", "-p", "no:cacheprovider", ...TESTES], { cwd: RAIZ, encoding: "utf-8", timeout: 300000 });
  const fim = r.error && r.error.code === "ETIMEDOUT" ? "o pytest passou de 5 min sem terminar"
    : (r.stdout || "").trim().split(/\r?\n/).pop() || r.stderr || "sem saída";
  registrar({ verificacao: "travas", alvo: TESTES.length + " arquivos de teste", situacao: r.status === 0 ? "ok" : "falha", resumo: fim });
}
