"""node_runners/execucao_uo.js (NOB e EMP) - espelho do services/uo.py.

Roda o módulo Node num subprocesso. A desativação usa um "db" falso que só
captura o SQL: nenhuma conexão com o banco.
"""

import json
import subprocess
from pathlib import Path

import pytest

NODE_DIR = Path(__file__).resolve().parents[1] / "node_runners"

SCRIPT = r"""
const m = require("./execucao_uo.js");
const out = {};
out.codigo = [m.uoCodigo("1101"), m.uoCodigo("14.101 - SECRETARIA"), m.uoCodigo("26101"), m.uoCodigo("")];
out.exercicio = [m.exercicio4d("26"), m.exercicio4d("2026"), m.exercicio4d(2026), m.exercicio4d("2026.0"), m.exercicio4d("x")];
out.parece = [m.pareceUo("14101"), m.pareceUo("14.101 - X"), m.pareceUo("1.234"), m.pareceUo("Total"), m.pareceUo("")];
const tenta = (uos) => { try { return m.validarUoExecucao(uos); } catch (e) { return "ERRO: " + e.message; } };
out.validar = [
  tenta(["14101", "14.101"]),
  tenta(["14101", "14601"]),
  tenta([]),
  tenta(["99999"]),
  tenta(["26201"]),
];
const sqls = [];
const db = { kind: "mysql", pool: { query: async (sql) => { sqls.push(sql); return [{ affectedRows: 3 }]; } } };
(async () => {
  out.afetadas = await m.desativarVersaoAnterior(db, "nob", "14101", ["2026"]);
  out.sql = sqls[0];
  try { await m.desativarVersaoAnterior(db, "nob", "", ["2026"]); } catch (e) { out.semUo = e.message; }
  console.log(JSON.stringify(out));
})();
"""


def _rodar(env_extra):
    import os

    env = {**os.environ, **env_extra}
    res = subprocess.run(["node", "-e", SCRIPT], cwd=NODE_DIR, capture_output=True, text=True, encoding="utf-8", env=env)
    assert res.returncode == 0, res.stderr
    return json.loads(res.stdout)


def test_helpers_e_validacao_so_secretaria_liberada():
    r = _rodar(
        {
            "UOS_EXECUCAO": "14101,14601,26101,26201,26202",
            "UOS_EXECUCAO_LIBERADAS": "14101,14601",
        }
    )
    assert r["codigo"] == ["01101", "14101", "26101", ""]
    assert r["exercicio"] == ["2026", "2026", "2026", "2026", ""]
    assert r["parece"] == [True, True, False, False, False]
    assert r["validar"][0] == "14101"
    assert "mais de uma UO" in r["validar"][1]
    assert "identificar a UO" in r["validar"][2]
    assert "não cadastrada" in r["validar"][3]
    assert "ainda não está liberada" in r["validar"][4]


def test_liberando_as_novas_uos():
    r = _rodar(
        {
            "UOS_EXECUCAO": "14101,14601,26101,26201,26202",
            "UOS_EXECUCAO_LIBERADAS": "14101,14601,26101,26201,26202",
        }
    )
    assert r["validar"][4] == "26201"


def test_sql_da_desativacao_por_uo_e_exercicio():
    r = _rodar({})
    assert r["afetadas"] == 3
    assert "AND CAST(REPLACE(SUBSTRING_INDEX(uo, ' - ', 1), '.', '') AS UNSIGNED) = 14101" in r["sql"]
    assert "IN (26, 2026)" in r["sql"]
    assert "WHERE ativo = 1 AND" in r["sql"]
    assert "inválido" in r["semUo"]
