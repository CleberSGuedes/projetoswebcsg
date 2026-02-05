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

    base_dt = upload.data_arquivo or upload.uploaded_at or datetime.utcnow()
    dia = base_dt.date()
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

    status.ult_upload_at, status.penult_upload_at = _get_last_two_distinct_days()

    db.session.commit()


def get_emp_record_snapshot() -> dict:
    status = EmpStatusDiario.query.order_by(EmpStatusDiario.dia.desc()).first()
    dias = status.dias_sem_erro if status else 0
    recorde = status.recorde if status else 0
    ult = status.ult_upload_at if status else None
    penult = status.penult_upload_at if status else None

    if not ult or not penult:
        ult, penult = _get_last_two_distinct_days()

    return {
        "dias_sem_erro": dias,
        "recorde": recorde,
        "ult_upload_at": ult,
        "penult_upload_at": penult,
    }


def _get_last_two_distinct_days() -> tuple[datetime | None, datetime | None]:
    registros = (
        EmpUpload.query.filter(EmpUpload.data_arquivo.isnot(None))
        .order_by(EmpUpload.data_arquivo.desc())
        .all()
    )
    vistos: set[str] = set()
    datas: list[datetime] = []
    for reg in registros:
        dt = reg.data_arquivo or reg.uploaded_at
        if not dt:
            continue
        chave = dt.date().isoformat()
        if chave in vistos:
            continue
        vistos.add(chave)
        datas.append(dt)
        if len(datas) >= 2:
            break
    ult = datas[0] if len(datas) > 0 else None
    penult = datas[1] if len(datas) > 1 else None
    return ult, penult
