"""Bateria SPO-AA+: as diferenças da comparação antes × depois (tests/navegadores/lib/diferencas.mjs).

Roda os testes do Node (`node --test`) com retratos montados à mão: sem navegador, sem servidor e sem banco. Garante
que a comparação alinha os irmãos (um bloco novo no meio de uma seção não gera diferenças falsas nos seguintes),
condição do dev. Cleber para as etapas 3 e 4 da modularização.
"""

import subprocess
from pathlib import Path

TESTE = Path(__file__).resolve().parent / "navegadores" / "lib" / "diferencas.test.mjs"


def test_diferencas_da_comparacao():
    res = subprocess.run(["node", "--test", str(TESTE)], capture_output=True, text=True, encoding="utf-8")
    assert res.returncode == 0, res.stdout + res.stderr
    assert "# fail 0" in res.stdout or "ℹ fail 0" in res.stdout, res.stdout
