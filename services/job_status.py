from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STATUS_DIR = Path("outputs/status")
_CANCEL_SUFFIX = ".cancel"

EM_PROCESSAMENTO = "em processamento"
# Segundo clique em "Cancelar" depois deste prazo sem resposta do worker encerra o
# processamento direto no status (o worker pode estar morto sem que o pid mostre isso).
FORCAR_CANCELAMENTO_APOS_S = 300
# Status gravado ha menos que isto conta como em andamento mesmo com o pid parecendo
# morto: cobre a partida do worker e impede um segundo EMP/NOB logo em seguida
# (EMP 256 aceito ~1 min depois de outro EMP, 12/11/2026).
CARENCIA_INICIO_S = 180


def status_path(kind: str, upload_id: int) -> Path:
    safe_kind = (kind or "").strip().lower()
    return STATUS_DIR / f"{safe_kind}_{upload_id}.json"


def write_status(
    kind: str,
    upload_id: int,
    state: str,
    message: str | None = None,
    output_filename: str | None = None,
    progress: int | None = None,
    pid: int | None = None,
) -> None:
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "kind": (kind or "").strip().lower(),
        "upload_id": int(upload_id),
        "state": state,
        "message": message or "",
        "output_filename": output_filename,
        "progress": progress,
        "pid": pid,
        "updated_at": datetime.utcnow().isoformat(),
    }
    status_path(kind, upload_id).write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def read_status(kind: str, upload_id: int) -> dict[str, Any] | None:
    path = status_path(kind, upload_id)
    if not path.exists():
        return None
    try:
        raw = path.read_text(encoding="utf-8")
        return json.loads(raw) if raw else None
    except Exception:
        return None


def update_status_fields(kind: str, upload_id: int, renovar: bool = True, **fields: Any) -> None:
    """Altera campos do status. `renovar=False` mantem o updated_at (nao renova a trava)."""
    current = read_status(kind, upload_id) or {}
    if not current:
        current = {
            "kind": (kind or "").strip().lower(),
            "upload_id": int(upload_id),
        }
    current.update(fields)
    if renovar or not current.get("updated_at"):
        current["updated_at"] = datetime.utcnow().isoformat()
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    status_path(kind, upload_id).write_text(json.dumps(current, ensure_ascii=True), encoding="utf-8")


def parse_utc(valor) -> datetime | None:
    """Horario do status em UTC sem fuso (naive), ou None.

    O Python grava sem fuso (utcnow) e o runner Node grava com "Z" (toISOString).
    Comparar os dois dava TypeError, e a trava 409 liberava o envio (12/11/2026).
    """
    try:
        dt = datetime.fromisoformat(str(valor))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _args_worker_spo(cmdline: bytes) -> list[str] | None:
    """Argumentos se a linha de comando e a do worker.py do SPO, senao None.

    Compara o NOME EXATO do arquivo: o `pedidos_worker.py` da automacao roda na mesma
    conta e casava com uma busca por "worker.py".
    """
    args = [a.decode("utf-8", "replace") for a in cmdline.split(b"\0") if a]
    for i, arg in enumerate(args):
        if Path(arg).name == "worker.py":
            return args[i + 1:]
    return None


def processos_worker_spo() -> dict[tuple[str, int], int] | None:
    """{(tipo, id): pid} dos workers do SPO rodando agora. None se nao der para saber (sem /proc)."""
    proc = Path("/proc")
    if not (proc / "self").exists():
        return None
    encontrados: dict[tuple[str, int], int] = {}
    for item in proc.iterdir():
        if not item.name.isdigit():
            continue
        try:
            cmdline = (item / "cmdline").read_bytes()
            estado = (item / "stat").read_text(encoding="utf-8", errors="replace").rsplit(")", 1)[-1].split()[:1]
        except OSError:
            continue
        if estado and estado[0] in {"Z", "X"}:
            continue
        args = _args_worker_spo(cmdline)
        if args is None:
            continue
        try:
            tipo = args[args.index("--kind") + 1]
            upload_id = int(args[args.index("--upload-id") + 1])
        except (ValueError, IndexError):
            continue
        encontrados[(tipo, upload_id)] = int(item.name)
    return encontrados


def _pid_vivo_windows(pid: int) -> bool:
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return ctypes.get_last_error() == 5  # acesso negado: existe
        try:
            codigo = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(codigo)):
                return True
            return codigo.value == 259  # STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)
    except Exception:
        return True


def _pid_vivo(pid) -> bool:
    """True se o worker com este pid ainda roda. Na duvida, True.

    No Linux tambem trata como morto o processo zumbi (o worker terminou, mas o
    servidor web que o iniciou nunca o recolheu: os.kill(pid, 0) ainda funciona)
    e o pid reaproveitado por outro programa que nao e o worker.py do SPO.
    """
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return True  # sem pid conhecido: nao da para afirmar que morreu
    if pid <= 0:
        return True
    if os.name == "nt":
        return _pid_vivo_windows(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        pass  # sem permissao para sinalizar: o processo existe
    proc = Path("/proc")
    if not (proc / "self").exists():
        return True
    try:
        stat = (proc / str(pid) / "stat").read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return False
    except OSError:
        return True
    estado = stat.rsplit(")", 1)[-1].split()[:1]
    if estado and estado[0] in {"Z", "X"}:
        return False
    try:
        cmdline = (proc / str(pid) / "cmdline").read_bytes()
    except OSError:
        return True
    if cmdline and _args_worker_spo(cmdline) is None:
        return False  # pid reaproveitado por outro programa
    return True


def _parado_ha_mais_de(dados: dict[str, Any], minutos: float, agora: datetime | None = None) -> bool:
    atualizado = parse_utc(dados.get("updated_at"))
    if atualizado is None:
        return False
    return ((agora or datetime.utcnow()) - atualizado).total_seconds() > minutos * 60


def encerrar_se_orfao(
    kind: str,
    upload_id: int,
    dados: dict[str, Any] | None = None,
    max_idade_min: int = 90,
) -> dict[str, Any] | None:
    """Marca como falha o status "em processamento" cujo worker nao existe mais.

    Orfao = pid conhecido e morto, ou sem pid e sem atualizacao ha mais de
    `max_idade_min` minutos. Devolve o status atual (alterado ou nao).
    """
    if dados is None:
        dados = read_status(kind, upload_id)
    if not dados or dados.get("state") != EM_PROCESSAMENTO:
        return dados
    if not _parado_ha_mais_de(dados, CARENCIA_INICIO_S / 60):
        return dados
    if dados.get("pid") in (None, ""):
        if not _parado_ha_mais_de(dados, max_idade_min):
            return dados
        motivo = f"sem atualização há mais de {max_idade_min} minutos"
    elif _pid_vivo(dados.get("pid")):
        return dados
    else:
        motivo = "o processo do worker não está mais em execução"
    pediu_cancelamento = read_cancel_flag(kind, upload_id)
    clear_cancel_flag(kind, upload_id)
    if pediu_cancelamento:
        estado, mensagem = "processamento cancelado", f"Cancelado pelo usuário ({motivo})."
    else:
        estado = "falha no processamento"
        mensagem = f"Processamento interrompido: {motivo}. Envie o arquivo novamente ou use Reprocessar."
    update_status_fields(kind, upload_id, state=estado, message=mensagem)
    return read_status(kind, upload_id)


def upload_em_andamento(kind: str, max_idade_min: int = 90) -> int | None:
    """Id do upload (EMP/NOB) ainda em processamento, ou None.

    Considera parado (nao em andamento) o status sem atualizacao ha mais de
    `max_idade_min` minutos ou cujo processo ja morreu, para um registro
    esquecido nao bloquear novos envios. O status de worker morto e gravado
    como falha (encerrar_se_orfao), para nao ficar "em processamento" na tela.
    """
    prefixo = (kind or "").strip().lower()
    if not STATUS_DIR.exists():
        return None
    for path in STATUS_DIR.glob(f"{prefixo}_*.json"):
        sufixo = path.stem[len(prefixo) + 1:]
        if not sufixo.isdigit():
            continue
        try:
            dados = json.loads(path.read_text(encoding="utf-8") or "{}")
        except Exception:
            continue
        if dados.get("state") != EM_PROCESSAMENTO:
            continue
        if parse_utc(dados.get("updated_at")) is None:
            continue
        dados = encerrar_se_orfao(prefixo, int(sufixo), dados, max_idade_min) or {}
        if dados.get("state") != EM_PROCESSAMENTO:
            continue
        if _parado_ha_mais_de(dados, max_idade_min):
            continue
        return int(sufixo)
    return None


def cancel_path(kind: str, upload_id: int) -> Path:
    safe_kind = (kind or "").strip().lower()
    return STATUS_DIR / f"{safe_kind}_{upload_id}{_CANCEL_SUFFIX}"


def set_cancel_flag(kind: str, upload_id: int) -> None:
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    cancel_path(kind, upload_id).write_text(datetime.utcnow().isoformat(), encoding="utf-8")


def clear_cancel_flag(kind: str, upload_id: int) -> None:
    try:
        cancel_path(kind, upload_id).unlink()
    except FileNotFoundError:
        pass


def read_cancel_flag(kind: str, upload_id: int) -> bool:
    return cancel_path(kind, upload_id).exists()


def _cancelamento_pedido_em(kind: str, upload_id: int) -> datetime | None:
    try:
        return parse_utc(cancel_path(kind, upload_id).read_text(encoding="utf-8").strip())
    except OSError:
        return None


def solicitar_cancelamento(kind: str, upload_id: int, agora: datetime | None = None) -> tuple[bool, str]:
    """Cancelar do EMP/NOB. Devolve (aceito, mensagem).

    - ja finalizado: (False, ...);
    - worker morto: encerra na hora como cancelado;
    - primeiro pedido: grava o pedido para o worker parar na proxima checagem;
    - novo pedido depois de FORCAR_CANCELAMENTO_APOS_S sem resposta: encerra
      direto no status (o pedido continua gravado, entao um worker que ainda
      estiver vivo para na proxima checagem).
    O pedido nao renova o updated_at, para nao estender a trava de 90 minutos.
    """
    agora = agora or datetime.utcnow()
    pediu_antes = _cancelamento_pedido_em(kind, upload_id)
    dados = read_status(kind, upload_id)
    if not dados or dados.get("state") != EM_PROCESSAMENTO:
        return False, "O processamento já foi finalizado."
    if pediu_antes is None:
        set_cancel_flag(kind, upload_id)
    dados = encerrar_se_orfao(kind, upload_id, dados) or {}
    if dados.get("state") != EM_PROCESSAMENTO:
        return True, "Processamento encerrado: o worker não estava mais em execução."
    if pediu_antes is not None and (agora - pediu_antes).total_seconds() >= FORCAR_CANCELAMENTO_APOS_S:
        update_status_fields(
            kind, upload_id, state="processamento cancelado",
            message="Cancelado pelo usuário (o worker não respondeu ao pedido de cancelamento).",
        )
        return True, "Processamento encerrado como cancelado."
    update_status_fields(kind, upload_id, renovar=False, message="Cancelamento solicitado.")
    minutos = FORCAR_CANCELAMENTO_APOS_S // 60
    return True, (
        "Cancelamento solicitado. Se o processamento não parar em "
        f"{minutos} minutos, clique em Cancelar de novo para encerrá-lo."
    )
