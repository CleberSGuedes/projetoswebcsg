from __future__ import annotations

import json
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
