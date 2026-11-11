"""Fila de processamento pesado: um worker por vez.

A hospedagem limita os processos/threads da conta inteira (NPROC). Cada worker
abre um processo Python e, no EMP/NOB, um Node com varias threads; varios ao
mesmo tempo (mais o servidor web e a automacao) estouravam o limite e o sistema
operacional recusava novos processos ("Resource temporarily unavailable").

O worker pede a "vez" antes de comecar: um arquivo de trava (flock) garante que
so um rode por vez; os demais esperam dormindo (quase sem custo). A trava
e liberada pelo sistema operacional quando o processo termina, mesmo se morrer.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

LOCK_PATH = Path("outputs/status/fila_pesada.lock")
POLL_S = 5


def _habilitada() -> bool:
    return os.getenv("FILA_PESADA", "1").strip().lower() not in {"0", "false", "no", "nao"}


def _tentar_travar(handle) -> bool:
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError:
        return False


def aguardar_vez(timeout_s: float | None = None, on_wait=None, lock_path: Path | None = None):
    """Espera a vez na fila e devolve o arquivo da trava (mantenha-o aberto).

    Levanta TimeoutError se esperar mais que `timeout_s` (padrao 60 min, ou
    FILA_PESADA_TIMEOUT_S). `on_wait` e chamado uma vez, quando precisa esperar.
    Devolve None se a fila estiver desligada (FILA_PESADA=0).
    """
    if not _habilitada():
        return None
    if timeout_s is None:
        try:
            timeout_s = float(os.getenv("FILA_PESADA_TIMEOUT_S", "3600"))
        except ValueError:
            timeout_s = 3600.0
    caminho = lock_path or LOCK_PATH
    caminho.parent.mkdir(parents=True, exist_ok=True)
    handle = open(caminho, "a+b")
    limite = time.monotonic() + timeout_s
    avisou = False
    while not _tentar_travar(handle):
        if not avisou:
            avisou = True
            if on_wait:
                try:
                    on_wait()
                except Exception:
                    pass
        if time.monotonic() >= limite:
            handle.close()
            raise TimeoutError(
                "Tempo esgotado aguardando a vez na fila de processamento. Tente novamente mais tarde."
            )
        time.sleep(POLL_S)
    return handle
