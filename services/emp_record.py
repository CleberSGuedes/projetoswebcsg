from __future__ import annotations

from datetime import datetime

from models import EmpStatusDiario, EmpUpload, db
from services.job_status import read_status


def _has_emp_alert(status_data: dict | None) -> bool:
    data = status_data or {}
    missing_lines = data.get("planejamento_missing_lines") or []
    missing_dot = data.get("dotacao_missing_keys") or []
    return bool(missing_lines) or bool(missing_dot)


def update_emp_record_from_status(upload_id: int) -> None:
    upload = db.session.get(EmpUpload, upload_id)
    if not upload:
        return

    status_data = read_status("emp", upload_id) or {}
    has_alert = _has_emp_alert(status_data)

    upload.alerta_emp = bool(has_alert)

    dia = (upload.uploaded_at or datetime.utcnow()).date()
    status = EmpStatusDiario.query.filter_by(dia=dia).first()

    prev = (
        EmpStatusDiario.query.filter(EmpStatusDiario.dia < dia)
        .order_by(EmpStatusDiario.dia.desc())
        .first()
    )
    prev_streak = prev.dias_sem_erro if prev and not prev.houve_alerta else 0
    prev_record = prev.recorde if prev else 0

    if has_alert:
        dias_sem_erro = 0
    else:
        dias_sem_erro = prev_streak + 1

    recorde = max(prev_record, dias_sem_erro)

    if not status:
        status = EmpStatusDiario(dia=dia)
        db.session.add(status)

    status.houve_alerta = bool(has_alert)
    status.dias_sem_erro = dias_sem_erro
    status.recorde = recorde

    last_two = EmpUpload.query.order_by(EmpUpload.uploaded_at.desc()).limit(2).all()
    status.ult_upload_at = last_two[0].uploaded_at if len(last_two) > 0 else None
    status.penult_upload_at = last_two[1].uploaded_at if len(last_two) > 1 else None

    db.session.commit()


def get_emp_record_snapshot() -> dict:
    status = EmpStatusDiario.query.order_by(EmpStatusDiario.dia.desc()).first()
    dias = status.dias_sem_erro if status else 0
    recorde = status.recorde if status else 0
    ult = status.ult_upload_at if status else None
    penult = status.penult_upload_at if status else None

    if not ult or not penult:
        last_two = EmpUpload.query.order_by(EmpUpload.uploaded_at.desc()).limit(2).all()
        if last_two:
            ult = last_two[0].uploaded_at
        if len(last_two) > 1:
            penult = last_two[1].uploaded_at

    return {
        "dias_sem_erro": dias,
        "recorde": recorde,
        "ult_upload_at": ult,
        "penult_upload_at": penult,
    }
