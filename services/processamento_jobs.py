from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import bindparam, text

from models import ProcessamentoEvento, ProcessamentoJob, db

ACTIVE_STATUSES = {"aguardando", "em_processamento", "cancelamento_solicitado"}
TERMINAL_STATUSES = {"finalizado", "finalizado_com_alertas", "falha", "cancelado"}


class ProcessamentoCancelado(RuntimeError):
    pass


def create_job(tipo: str, upload: Any, usuario_id: int | None = None) -> ProcessamentoJob:
    active = job_ativo(tipo)  # encerra antes os jobs com worker morto
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


# --- Job orfao (worker morto) e Cancelar -------------------------------------------
# Antes, um job ativo com o worker morto travava o tipo para sempre: a trava 409 so
# olhava o status, e o Cancelar so marcava um pedido que ninguem lia.
CARENCIA_S = 180  # partida do worker: nao julga antes disso
SEM_PID_LIMITE_S = 600  # sem pid registrado (spawn nunca confirmado)
FORCAR_CANCELAMENTO_APOS_S = 300  # 2o Cancelar sem resposta do worker encerra o job


def motivo_orfao(worker_pid, idade_s, pid_vivo, ignorar_carencia: bool = False) -> str | None:
    """Motivo para encerrar um job ativo, ou None se ele pode estar rodando (funcao pura).

    `idade_s`: segundos desde a ultima atualizacao do job. `pid_vivo`: funcao(pid) -> bool.
    """
    idade_s = float(idade_s or 0)
    if worker_pid:
        if not ignorar_carencia and idade_s < CARENCIA_S:
            return None
        return None if pid_vivo(worker_pid) else "o processo do worker não está mais em execução"
    if idade_s >= SEM_PID_LIMITE_S:
        return f"nenhum worker registrado há mais de {SEM_PID_LIMITE_S // 60} minutos"
    return None


def _jobs_ativos(tipo: str | None = None, job_id: int | None = None):
    sql = (
        "SELECT id, worker_pid, cancelamento_solicitado, "
        "TIMESTAMPDIFF(SECOND, atualizado_em, NOW()) AS idade_s "
        "FROM processamento_jobs WHERE status IN :ativos"
    )
    params: dict[str, Any] = {"ativos": list(ACTIVE_STATUSES)}
    if tipo:
        sql += " AND tipo = :tipo"
        params["tipo"] = tipo
    if job_id:
        sql += " AND id = :id"
        params["id"] = job_id
    stmt = text(sql).bindparams(bindparam("ativos", expanding=True))
    return db.session.execute(stmt, params).fetchall()


def _encerrar(job: ProcessamentoJob, motivo: str, cancelado: bool) -> None:
    if cancelado:
        finish_job(job, "cancelado", f"Cancelado pelo usuário ({motivo}).",
                   cancelamento_solicitado=True)
    else:
        job.erro_tecnico = f"Job órfão: {motivo}."
        finish_job(job, "falha",
                   f"Processamento interrompido: {motivo}. Envie o arquivo novamente ou use Reprocessar.")


def encerrar_orfaos(tipo: str | None = None, job_id: int | None = None) -> list[int]:
    """Marca como falha (ou cancelado, se pedido) os jobs ativos com o worker morto."""
    from services.job_status import _pid_vivo

    encerrados = []
    for row in _jobs_ativos(tipo, job_id):
        motivo = motivo_orfao(row.worker_pid, row.idade_s, _pid_vivo)
        if not motivo:
            continue
        job = db.session.get(ProcessamentoJob, row.id)
        if job is None or job.status not in ACTIVE_STATUSES:
            continue
        _encerrar(job, motivo, bool(row.cancelamento_solicitado))
        encerrados.append(row.id)
    return encerrados


def job_ativo(tipo: str) -> ProcessamentoJob | None:
    """Job ativo do tipo depois de encerrar os orfaos (base da trava 409)."""
    encerrar_orfaos(tipo)
    return (
        ProcessamentoJob.query.filter_by(tipo=tipo)
        .filter(ProcessamentoJob.status.in_(ACTIVE_STATUSES))
        .order_by(ProcessamentoJob.id.desc())
        .first()
    )


def _segundos_desde_pedido_de_cancelamento(job_id: int) -> float | None:
    valor = db.session.execute(
        text(
            "SELECT TIMESTAMPDIFF(SECOND, MIN(created_at), NOW()) FROM processamento_eventos "
            "WHERE processamento_id = :id AND tipo_evento = 'cancelamento'"
        ),
        {"id": job_id},
    ).scalar()
    return float(valor) if valor is not None else None


def solicitar_cancelamento_job(job: ProcessamentoJob) -> tuple[bool, str]:
    """Cancelar dos jobs gerenciados. Devolve (aceito, mensagem).

    Worker morto: encerra na hora. Primeiro pedido: grava para o worker parar na
    proxima checagem. Novo pedido depois de FORCAR_CANCELAMENTO_APOS_S sem resposta:
    encerra como cancelado (o pedido continua gravado; um worker vivo para sozinho).
    """
    from services.job_status import _pid_vivo

    db.session.refresh(job)
    if job.status not in ACTIVE_STATUSES:
        return False, "O processamento já foi finalizado."
    if job.worker_pid and not _pid_vivo(job.worker_pid):
        _encerrar(job, "o worker não estava mais em execução", cancelado=True)
        return True, "Processamento encerrado: o worker não estava mais em execução."
    if job.cancelamento_solicitado:
        decorrido = _segundos_desde_pedido_de_cancelamento(job.id)
        if decorrido is not None and decorrido >= FORCAR_CANCELAMENTO_APOS_S:
            _encerrar(job, "o worker não respondeu ao pedido de cancelamento", cancelado=True)
            return True, "Processamento encerrado como cancelado."
    else:
        update_job(job, status="cancelamento_solicitado", etapa="cancelamento",
                   mensagem="Cancelamento solicitado pelo usuário.", event_type="cancelamento",
                   cancelamento_solicitado=True)
    minutos = FORCAR_CANCELAMENTO_APOS_S // 60
    return True, (
        "Cancelamento solicitado. Se o processamento não parar em "
        f"{minutos} minutos, clique em Cancelar de novo para encerrá-lo."
    )
