from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

STATUS_DIR = Path("outputs/status")
_CANCEL_SUFFIX = ".cancel"


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


def update_status_fields(kind: str, upload_id: int, **fields: Any) -> None:
    current = read_status(kind, upload_id) or {}
    if not current:
        current = {
            "kind": (kind or "").strip().lower(),
            "upload_id": int(upload_id),
        }
    current.update(fields)
    current["updated_at"] = datetime.utcnow().isoformat()
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    status_path(kind, upload_id).write_text(json.dumps(current, ensure_ascii=True), encoding="utf-8")


def _pid_vivo(pid) -> bool:
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return True  # sem pid conhecido: nao da para afirmar que morreu
    if pid <= 0 or os.name == "nt":
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True
    return True


def upload_em_andamento(kind: str, max_idade_min: int = 90) -> int | None:
    """Id do upload (EMP/NOB) ainda em processamento, ou None.

    Considera parado (nao em andamento) o status sem atualizacao ha mais de
    `max_idade_min` minutos ou cujo processo ja morreu, para um registro
    esquecido nao bloquear novos envios.
    """
    prefixo = (kind or "").strip().lower()
    if not STATUS_DIR.exists():
        return None
    agora = datetime.utcnow()
    for path in STATUS_DIR.glob(f"{prefixo}_*.json"):
        sufixo = path.stem[len(prefixo) + 1:]
        if not sufixo.isdigit():
            continue
        try:
            dados = json.loads(path.read_text(encoding="utf-8") or "{}")
        except Exception:
            continue
        if dados.get("state") != "em processamento":
            continue
        try:
            atualizado = datetime.fromisoformat(dados.get("updated_at"))
        except (TypeError, ValueError):
            continue
        if (agora - atualizado).total_seconds() > max_idade_min * 60:
            continue
        if not _pid_vivo(dados.get("pid")):
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
