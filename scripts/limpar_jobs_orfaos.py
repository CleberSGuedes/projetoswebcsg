"""Encerra jobs orfaos: "em processamento" sem nenhum worker do SPO rodando.

Uso (na pasta da aplicacao, com o virtualenv ativo):
    python scripts/limpar_jobs_orfaos.py            # so lista (nao altera nada)
    python scripts/limpar_jobs_orfaos.py --aplicar  # encerra os orfaos listados

Cobre os dois mecanismos de status:
- EMP/NOB: arquivos outputs/status/<tipo>_<id>.json com state "em processamento";
- FIP 613, PED, EST EMP e Receita Anexo 10: tabela processamento_jobs (status ativo).

Seguranca: um job so e encerrado se NAO houver processo do worker.py do SPO para ele
(nome exato do arquivo + --kind/--upload-id; o pedidos_worker.py da automacao nao casa),
se o pid gravado nao estiver vivo e se a ultima atualizacao tiver mais de 3 minutos.
Idempotente: rodar de novo nao encontra nada. Equivale a checagem manual
    ps -fu "$USER" | grep "projetoswebcsg_app" | grep "[w]orker.py"
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
os.chdir(RAIZ)  # outputs/status e relativo a pasta da aplicacao
sys.path.insert(0, str(RAIZ))
for _var in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(_var, "1")

from services import job_status  # noqa: E402

CARENCIA_S = 180
MENSAGEM = "Encerrado pelo script de limpeza de órfãos: nenhum worker do SPO em execução."


def _rodando(workers, tipo: str, job_id: int, pid) -> bool:
    if workers is not None and (tipo, job_id) in workers:
        return True
    if pid in (None, ""):
        return False
    return job_status._pid_vivo(pid)


def orfaos_em_arquivo(workers) -> list[dict]:
    encontrados = []
    if not job_status.STATUS_DIR.exists():
        return encontrados
    for tipo in ("emp", "nob"):
        for path in sorted(job_status.STATUS_DIR.glob(f"{tipo}_*.json")):
            sufixo = path.stem[len(tipo) + 1:]
            if not sufixo.isdigit():
                continue
            try:
                dados = json.loads(path.read_text(encoding="utf-8") or "{}")
            except Exception:
                continue
            if dados.get("state") != job_status.EM_PROCESSAMENTO:
                continue
            if not job_status._parado_ha_mais_de(dados, CARENCIA_S / 60):
                continue
            job_id, pid = int(sufixo), dados.get("pid")
            if _rodando(workers, tipo, job_id, pid):
                continue
            encontrados.append({"origem": "arquivo", "tipo": tipo, "id": job_id, "pid": pid,
                                "atualizado": dados.get("updated_at"), "mensagem": dados.get("message")})
    return encontrados


def orfaos_no_banco(workers) -> list[dict]:
    from sqlalchemy import bindparam, text

    from models import db
    from services.processamento_jobs import ACTIVE_STATUSES

    stmt = text(
        "SELECT id, tipo, worker_pid, status, atualizado_em, "
        "TIMESTAMPDIFF(SECOND, atualizado_em, NOW()) AS idade_s, mensagem_atual "
        "FROM processamento_jobs WHERE status IN :ativos ORDER BY id"
    ).bindparams(bindparam("ativos", expanding=True))
    encontrados = []
    for row in db.session.execute(stmt, {"ativos": list(ACTIVE_STATUSES)}):
        if float(row.idade_s or 0) < CARENCIA_S:
            continue
        if _rodando(workers, row.tipo, int(row.id), row.worker_pid):
            continue
        encontrados.append({"origem": "banco", "tipo": row.tipo, "id": int(row.id), "pid": row.worker_pid,
                            "atualizado": str(row.atualizado_em), "mensagem": row.mensagem_atual})
    return encontrados


def encerrar(item: dict) -> None:
    if item["origem"] == "arquivo":
        pediu_cancelamento = job_status.read_cancel_flag(item["tipo"], item["id"])
        job_status.clear_cancel_flag(item["tipo"], item["id"])
        estado = "processamento cancelado" if pediu_cancelamento else "falha no processamento"
        job_status.update_status_fields(item["tipo"], item["id"], state=estado, message=MENSAGEM)
        return
    from models import ProcessamentoJob, db
    from services.processamento_jobs import ACTIVE_STATUSES, finish_job

    job = db.session.get(ProcessamentoJob, item["id"])
    if job is None or job.status not in ACTIVE_STATUSES:
        return
    job.erro_tecnico = MENSAGEM
    finish_job(job, "cancelado" if job.cancelamento_solicitado else "falha", MENSAGEM)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--aplicar", action="store_true", help="encerra os orfaos (sem isso, so lista)")
    parser.add_argument("--somente-arquivos", action="store_true", help="nao consulta o banco (so EMP/NOB)")
    args = parser.parse_args(argv)

    workers = job_status.processos_worker_spo()
    if workers is None:
        print("Aviso: sem /proc (fora do Linux); a checagem usa so o pid gravado no status.")
    else:
        print(f"Workers do SPO em execucao: {len(workers)} " + (str(sorted(workers)) if workers else ""))

    orfaos = orfaos_em_arquivo(workers)
    app = None
    if not args.somente_arquivos:
        from app import create_app

        app = create_app()
        with app.app_context():
            orfaos += orfaos_no_banco(workers)

    if not orfaos:
        print("Nenhum job orfao encontrado.")
        return 0
    print(f"Jobs orfaos: {len(orfaos)}")
    for item in orfaos:
        print(f"  [{item['origem']}] {item['tipo']} {item['id']}  pid={item['pid']}  "
              f"atualizado={item['atualizado']}  {item['mensagem'] or ''}")
    if not args.aplicar:
        print("Nada foi alterado. Para encerrar, rode de novo com --aplicar.")
        return 0
    if app is not None:
        with app.app_context():
            for item in orfaos:
                encerrar(item)
    else:
        for item in orfaos:
            encerrar(item)
    print(f"Encerrados: {len(orfaos)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
