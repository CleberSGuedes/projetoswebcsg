from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path

from app import create_app
from models import db, EmpUpload, NobUpload
from sqlalchemy.exc import SQLAlchemyError
from services.job_status import clear_cancel_flag, update_status_fields, write_status

EMP_INPUT_DIR = Path("upload/emp")
NOB_INPUT_DIR = Path("upload/nob")
NODE_RUNNER = Path(__file__).resolve().parent / "node_runners" / "run.js"
NODE_EXE = os.getenv("NODE_EXE", "node")


def _find_upload_path(base_dir: Path, stored_filename: str) -> Path | None:
    if not stored_filename:
        return None
    candidate = (base_dir / stored_filename).resolve()
    if candidate.exists():
        return candidate
    tmp_dir = (base_dir / "tmp").resolve()
    if not tmp_dir.exists():
        return None
    stem = Path(stored_filename).stem
    matches = sorted(tmp_dir.glob(f"{stem}_*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _run_node(kind: str, file_path: Path, user_email: str, data_arquivo, upload_id: int) -> dict:
    args = [
        NODE_EXE,
        str(NODE_RUNNER),
        "--kind",
        kind,
        "--file",
        str(file_path),
        "--upload-id",
        str(upload_id),
        "--user-email",
        user_email or "desconhecido",
    ]
    if data_arquivo:
        try:
            args.extend(["--data-arquivo", data_arquivo.isoformat()])
        except Exception:
            args.extend(["--data-arquivo", str(data_arquivo)])
    proc = subprocess.run(args, capture_output=True, text=True, cwd=str(NODE_RUNNER.parent))
    if proc.stderr:
        print(proc.stderr, file=sys.stderr)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"Node runner falhou: {err or 'erro desconhecido'}")
    raw = (proc.stdout or "").strip()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Resposta invalida do Node: {exc}") from exc
    if not payload.get("ok"):
        raise RuntimeError(f"Node runner falhou: {payload.get('error')}")
    return payload


def _commit_upload_filename(model_cls, upload_id: int, output_filename: str | None) -> None:
    def _do_commit() -> None:
        upload = db.session.get(model_cls, upload_id)
        if not upload:
            raise RuntimeError(f"Upload nao encontrado: {upload_id}")
        upload.output_filename = str(output_filename or "")
        db.session.commit()

    try:
        _do_commit()
    except SQLAlchemyError as exc:
        msg = str(exc)
        if "MySQL server has gone away" in msg or "Packet sequence number wrong" in msg:
            db.session.rollback()
            db.session.remove()
            try:
                db.engine.dispose()
            except Exception:
                pass
            _do_commit()
        else:
            raise


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Background worker for heavy uploads.")
    parser.add_argument("--kind", choices=["emp", "nob"], required=True)
    parser.add_argument("--upload-id", type=int, required=True)
    return parser.parse_args()


def _run_emp(upload_id: int) -> None:
    upload = db.session.get(EmpUpload, upload_id)
    if not upload:
        raise RuntimeError(f"Upload EMP nao encontrado: {upload_id}")
    file_path = _find_upload_path(Path(EMP_INPUT_DIR), upload.stored_filename)
    if not file_path:
        raise RuntimeError(f"Arquivo EMP nao encontrado: {Path(EMP_INPUT_DIR) / upload.stored_filename}")
    payload = _run_node("emp", file_path, upload.user_email, upload.data_arquivo, upload.id)
    _commit_upload_filename(EmpUpload, upload_id, payload.get("output_filename"))
    update_status_fields(
        "emp",
        upload_id,
        state="processamento finalizado",
        message=f"Processado com sucesso. Registros: {payload.get('total')}.",
        output_filename=payload.get("output_filename"),
    )


def _run_nob(upload_id: int) -> None:
    upload = db.session.get(NobUpload, upload_id)
    if not upload:
        raise RuntimeError(f"Upload NOB nao encontrado: {upload_id}")
    file_path = _find_upload_path(Path(NOB_INPUT_DIR), upload.stored_filename)
    if not file_path:
        raise RuntimeError(f"Arquivo NOB nao encontrado: {Path(NOB_INPUT_DIR) / upload.stored_filename}")
    payload = _run_node("nob", file_path, upload.user_email, upload.data_arquivo, upload.id)
    _commit_upload_filename(NobUpload, upload_id, payload.get("output_filename"))
    write_status(
        "nob",
        upload_id,
        "processamento finalizado",
        f"Processado com sucesso. Registros: {payload.get('total')}.",
        payload.get("output_filename"),
    )


def main() -> int:
    args = _parse_args()
    app = create_app()
    with app.app_context():
        try:
            clear_cancel_flag(args.kind, args.upload_id)
            write_status(
                args.kind,
                args.upload_id,
                "em processamento",
                "Processamento iniciado.",
                progress=0,
                pid=os.getpid(),
            )
            if args.kind == "emp":
                _run_emp(args.upload_id)
            else:
                _run_nob(args.upload_id)
        except Exception as exc:
            msg = f"{type(exc).__name__}: {exc}"
            if "PROCESSAMENTO_CANCELADO" in msg:
                write_status(args.kind, args.upload_id, "processamento cancelado", "Cancelado pelo usuario.")
            else:
                write_status(args.kind, args.upload_id, "falha no processamento", msg)
            traceback.print_exc()
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
