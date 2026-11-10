from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

from models import ProcessamentoEvento, ProcessamentoJob, db

ACTIVE_STATUSES = {"aguardando", "em_processamento", "cancelamento_solicitado"}
TERMINAL_STATUSES = {"finalizado", "finalizado_com_alertas", "falha", "cancelado"}


class ProcessamentoCancelado(RuntimeError):
    pass


def create_job(tipo: str, upload: Any, usuario_id: int | None = None) -> ProcessamentoJob:
    active = (
        ProcessamentoJob.query.filter_by(tipo=tipo)
        .filter(ProcessamentoJob.status.in_(ACTIVE_STATUSES))
        .order_by(ProcessamentoJob.id.desc())
        .first()
    )
    if active:
        raise RuntimeError(f"Já existe um processamento {tipo.upper()} em andamento.")
    job = ProcessamentoJob(
        tipo=tipo,
        upload_id=upload.id,
        usuario_id=usuario_id,
        user_email=upload.user_email,
        arquivo_original=upload.original_filename,
        arquivo_armazenado=upload.stored_filename,
        status="aguardando",
        etapa_atual="arquivo_recebido",
        mensagem_atual="Arquivo recebido e aguardando o worker.",
        progresso=Decimal("0.00"),
    )
    db.session.add(job)
    db.session.flush()
    add_event(job, "informacao", "Arquivo recebido e validado.", "arquivo_recebido", 0)
    db.session.commit()
    return job


def add_event(job: ProcessamentoJob, tipo: str, mensagem: str, etapa: str | None = None,
              progresso: float | int | None = None, detalhes: Any = None) -> None:
    db.session.add(ProcessamentoEvento(
        processamento_id=job.id,
        tipo_evento=tipo,
        etapa=etapa,
        mensagem=str(mensagem)[:2000],
        progresso=progresso,
        detalhes=json.dumps(detalhes, ensure_ascii=False) if detalhes is not None else None,
    ))


def update_job(job: ProcessamentoJob, *, status: str | None = None, etapa: str | None = None,
               mensagem: str | None = None, progresso: float | int | None = None,
               event_type: str = "progresso", commit: bool = True, **fields: Any) -> None:
    if status is not None:
        job.status = status
    if etapa is not None:
        job.etapa_atual = etapa
    if mensagem is not None:
        job.mensagem_atual = str(mensagem)[:1000]
    if progresso is not None:
        job.progresso = max(0, min(100, Decimal(str(progresso))))
    for name, value in fields.items():
        if hasattr(job, name):
            setattr(job, name, value)
    if mensagem:
        add_event(job, event_type, mensagem, etapa, progresso)
    if commit:
        db.session.commit()


def check_cancelled(job: ProcessamentoJob) -> None:
    db.session.refresh(job)
    if job.cancelamento_solicitado:
        raise ProcessamentoCancelado("PROCESSAMENTO_CANCELADO")


def serialize_job(job: ProcessamentoJob) -> dict[str, Any]:
    return {
        "id": job.id, "tipo": job.tipo, "upload_id": job.upload_id,
        "user_email": job.user_email, "original_filename": job.arquivo_original,
        "output_filename": job.arquivo_saida, "status": job.status,
        "status_message": job.mensagem_atual, "status_progress": float(job.progresso or 0),
        "status_pid": job.worker_pid, "status_updated_at": job.atualizado_em.isoformat() if job.atualizado_em else None,
        "uploaded_at": job.solicitado_em.isoformat() if job.solicitado_em else None,
        "started_at": job.iniciado_em.isoformat() if job.iniciado_em else None,
        "finished_at": job.finalizado_em.isoformat() if job.finalizado_em else None,
        "duration_seconds": float(job.duracao_segundos) if job.duracao_segundos is not None else None,
        "total_records": int(job.total_registros or 0), "processed_records": int(job.registros_processados or 0),
        "total_alerts": int(job.total_alertas or 0), "total_errors": int(job.total_erros or 0),
        "alerts": json.loads(job.detalhes_alertas) if job.detalhes_alertas else None,
        "error": job.erro_tecnico,
    }


def finish_job(job: ProcessamentoJob, status: str, message: str, **fields: Any) -> None:
    now = datetime.utcnow()
    duration = (now - job.iniciado_em).total_seconds() if job.iniciado_em else None
    update_job(job, status=status, etapa=status, mensagem=message,
               progresso=100 if status.startswith("finalizado") else job.progresso,
               event_type="erro" if status == "falha" else "informacao",
               finalizado_em=now, duracao_segundos=duration, **fields)
