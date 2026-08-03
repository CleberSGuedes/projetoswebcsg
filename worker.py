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
from models import db, EmpUpload, NobUpload, Fip613Upload, PedUpload, EstEmpUpload, ReceitaAnexo10Upload, ProcessamentoJob
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from services.emp_record import update_emp_record_from_status
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
    node_env = os.environ.copy()
    node_max_old_space_mb = os.getenv("NODE_MAX_OLD_SPACE_MB", "4096").strip()
    if node_max_old_space_mb.isdigit() and int(node_max_old_space_mb) > 0:
        extra_opt = f"--max-old-space-size={int(node_max_old_space_mb)}"
        existing_opts = str(node_env.get("NODE_OPTIONS") or "").strip()
        if extra_opt not in existing_opts:
            node_env["NODE_OPTIONS"] = f"{existing_opts} {extra_opt}".strip()

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
    proc = subprocess.run(
        args,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(NODE_RUNNER.parent),
        env=node_env,
    )
    if proc.stderr:
        print(proc.stderr, file=sys.stderr)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"Node runner falhou: {err or 'erro desconhecido'}")
    raw = (proc.stdout or "").strip()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        parsed = None
        for line in reversed(raw.splitlines()):
            text = line.strip()
            if not text:
                continue
            try:
                parsed = json.loads(text)
                break
            except Exception:
                continue
        if parsed is None:
            raise RuntimeError(f"Resposta invalida do Node: {exc}") from exc
        payload = parsed
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
    parser.add_argument("--kind", choices=["fip613", "ped", "emp", "est_emp", "nob", "see", "receita_anexo10"], required=True)
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
        progress=100,
    )
    try:
        update_emp_record_from_status(upload_id)
    except Exception as exc:
        print(f"Falha ao atualizar recorde EMP: {exc}", file=sys.stderr)


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
        progress=100,
    )


def _run_managed_job(job_id: int) -> None:
    from services.processamento_jobs import check_cancelled, finish_job, update_job

    job = db.session.get(ProcessamentoJob, job_id)
    if not job:
        raise RuntimeError(f"Job não encontrado: {job_id}")
    configs = {
        "fip613": (Fip613Upload, Path("upload/fip_613"), "fip613"),
        "ped": (PedUpload, Path("upload/ped"), "ped"),
        "est_emp": (EstEmpUpload, Path("upload/est_emp"), "est_emp"),
        "receita_anexo10": (ReceitaAnexo10Upload, Path("upload/receita_anexo10"), "receita_anexo10_registros"),
    }
    model_cls, input_dir, table_name = configs[job.tipo]
    upload = db.session.get(model_cls, job.upload_id)
    if not upload:
        raise RuntimeError(f"Upload {job.tipo.upper()} não encontrado: {job.upload_id}")
    file_path = _find_upload_path(input_dir, upload.stored_filename)
    if not file_path and job.tipo != "receita_anexo10":
        raise RuntimeError(f"Arquivo de entrada não encontrado: {upload.stored_filename}")

    update_job(job, status="em_processamento", etapa="iniciando", mensagem="Worker iniciado.",
               progresso=1, iniciado_em=datetime.utcnow(), worker_pid=os.getpid())

    def progress(etapa: str, percentual: int, mensagem: str) -> None:
        check_cancelled(job)
        update_job(job, status="em_processamento", etapa=etapa, mensagem=mensagem, progresso=percentual)

    def cancel_check() -> None:
        check_cancelled(job)

    try:
        if job.tipo == "fip613":
            from services.fip613_runner import run_fip613
            total, output = run_fip613(file_path, upload.data_arquivo, upload.user_email, upload.id,
                                       progress, cancel_check)
            alerts = None
        elif job.tipo == "ped":
            from services.ped_runner import run_ped
            total, output, missing_dotacao, missing_planejamento = run_ped(
                file_path, upload.data_arquivo, upload.user_email, upload.id, progress, cancel_check
            )
            alerts = {"chaves_dotacao": missing_dotacao, "linhas_planejamento": missing_planejamento}
        elif job.tipo == "est_emp":
            from services.est_emp_runner import run_est_emp
            total, output = run_est_emp(file_path, upload.data_arquivo, upload.user_email, upload.id,
                                        progress, cancel_check)
            alerts = None
        else:
            from services.receita_anexo10_runner import run_receita_anexo10
            total, output, alerts = run_receita_anexo10(input_dir, upload.id, progress, cancel_check)
        cancel_check()
        upload.output_filename = output.name if output else None
        alert_count = sum(len(v) for v in alerts.values()) if alerts else 0
        job.detalhes_alertas = json.dumps(alerts, ensure_ascii=False) if alerts else None
        finish_job(job, "finalizado_com_alertas" if alert_count else "finalizado",
                   f"Processamento concluído. {total} registros inseridos.",
                   arquivo_saida=output.name if output else None, caminho_saida=str(output) if output else None, total_registros=total,
                   registros_processados=total, total_alertas=alert_count)
        db.session.commit()
    except Exception:
        db.session.rollback()
        if job.tipo == "receita_anexo10":
            db.session.execute(
                text("UPDATE receita_anexo10_registros SET ativo = 0 WHERE upload_id = :upload_id"),
                {"upload_id": upload.id},
            )
        else:
            # Remove somente a carga provisoria; a carga ativa anterior permanece disponivel.
            db.session.execute(
                text(f"DELETE FROM {table_name} WHERE upload_id = :upload_id AND ativo = 0"),
                {"upload_id": upload.id},
            )
        db.session.commit()
        raise


def main() -> int:
    args = _parse_args()
    app = create_app()
    with app.app_context():
        try:
            if args.kind in {"fip613", "ped", "est_emp", "receita_anexo10"}:
                _run_managed_job(args.upload_id)
            else:
                clear_cancel_flag(args.kind, args.upload_id)
                write_status(args.kind, args.upload_id, "em processamento", "Processamento iniciado.",
                             progress=0, pid=os.getpid())
                if args.kind == "emp":
                    _run_emp(args.upload_id)
                elif args.kind == "nob":
                    _run_nob(args.upload_id)
                else:
                    from rotas.home_routes import _run_see_processamento
                    _run_see_processamento(app, args.upload_id)
        except Exception as exc:
            msg = f"{type(exc).__name__}: {exc}"
            if args.kind in {"fip613", "ped", "est_emp", "receita_anexo10"}:
                from services.processamento_jobs import finish_job
                job = db.session.get(ProcessamentoJob, args.upload_id)
                if job:
                    if "PROCESSAMENTO_CANCELADO" in msg:
                        finish_job(job, "cancelado", "Processamento cancelado pelo usuário.")
                    else:
                        job.erro_tecnico = msg
                        job.total_erros = 1
                        finish_job(job, "falha", "Falha durante o processamento.")
            else:
                if "PROCESSAMENTO_CANCELADO" in msg:
                    write_status(args.kind, args.upload_id, "processamento cancelado", "Cancelado pelo usuario.")
                else:
                    write_status(args.kind, args.upload_id, "falha no processamento", msg)
            traceback.print_exc()
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
