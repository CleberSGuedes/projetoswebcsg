"""Atualizar > Execução > Limpeza de Histórico (docs/claude.md, seção 23).

Rotas num arquivo próprio (e não em home_routes.py) para não aumentar o
arquivo monolítico e facilitar a sincronização com as outras branches.
Acesso pela feature "atualizar/limpeza-historico" (painel de permissões);
o administrador (nível 1) passa pela regra geral de require_feature.
"""

from __future__ import annotations

import json
import threading
import uuid

from flask import Blueprint, current_app, jsonify, render_template, request, session
from sqlalchemy import bindparam, text

from models import LimpezaHistoricoLog, db
from rotas.home_routes import _now_local, require_feature
from services import limpeza_historico as lh
from services.auth import login_required
from services.job_status import read_status, update_status_fields, write_status

limpeza_bp = Blueprint("limpeza_historico", __name__)

FEATURE = "atualizar/limpeza-historico"
KIND = "limpeza_historico"
ESTADO_FIM = "processamento finalizado"
ESTADO_FALHA = "falha no processamento"


def _user_email() -> str:
    return (session.get("user") or {}).get("email") or "desconhecido"


def _parametros(payload: dict, exigir_periodo: bool = True):
    tabelas = payload.get("tabelas") or []
    if not exigir_periodo:
        tabelas = [t for t in lh.TABELAS if t in set(tabelas)]
        if not tabelas:
            raise ValueError("Selecione ao menos uma tabela.")
        return None, None, None, tabelas
    return lh.validar_parametros(
        payload.get("exercicio"), payload.get("ref_mes"), payload.get("ref_ano"), tabelas
    )


def _fmt_num(valor: float, casas: int = 0) -> str:
    """1234567.8 -> "1.234.567,8" (padrão pt-BR das mensagens)."""
    return f"{valor:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _novo_job() -> int:
    return (uuid.uuid4().int % 2_000_000_000) + 1


def _iniciar(job_id: int, alvo, *args) -> None:
    app = current_app._get_current_object()

    def _runner():
        with app.app_context():
            try:
                alvo(job_id, *args)
            except Exception as exc:  # erro inesperado: registra e avisa a tela
                app.logger.exception("Falha na Limpeza de Histórico (job %s).", job_id)
                write_status(KIND, job_id, ESTADO_FALHA, f"Falha: {exc}", progress=100)
            finally:
                db.session.remove()

    threading.Thread(target=_runner, daemon=True).start()


def _progresso(job_id: int):
    def _cb(mensagem: str, pct: int) -> None:
        write_status(KIND, job_id, "em processamento", mensagem, progress=pct)

    return _cb


# ---------------------------------------------------------------- jobs


def _job_simular(job_id, exercicio, ref_mes, ref_ano, tabelas):
    with db.engine.connect() as conn:
        planos = lh.simular(conn, exercicio, ref_mes, ref_ano, tabelas, _progresso(job_id))
    write_status(KIND, job_id, ESTADO_FIM, "Simulação concluída.", progress=100)
    update_status_fields(KIND, job_id, resultado=[p.resumo() for p in planos])


def _novo_log(acao, exercicio, ref_mes, ref_ano, tabelas, email) -> int:
    log = LimpezaHistoricoLog(
        acao=acao,
        status="em andamento",
        user_email=email,
        exercicio=exercicio,
        ref_mes=ref_mes,
        ref_ano=ref_ano,
        tabelas=",".join(tabelas),
        iniciado_em=_now_local(),  # fuso da aplicação (America/Manaus), não o do banco
    )
    db.session.add(log)
    db.session.commit()
    return log.id


def _fechar_log(log_id, status, linhas, detalhes) -> None:
    log = db.session.get(LimpezaHistoricoLog, log_id)
    if not log:
        return
    log.status = status
    log.linhas_removidas = linhas
    log.detalhes = json.dumps(detalhes, ensure_ascii=False, default=str)
    log.finalizado_em = _now_local()
    db.session.commit()


def _job_executar(job_id, exercicio, ref_mes, ref_ano, tabelas, email):
    with db.engine.connect() as conn:
        if not lh.obter_trava(conn):
            write_status(
                KIND, job_id, ESTADO_FALHA,
                "Já existe uma limpeza ou otimização em andamento. Aguarde a conclusão.",
                progress=100,
            )
            return
        log_id = _novo_log("limpeza", exercicio, ref_mes, ref_ano, tabelas, email)
        resultado, total = [], 0
        try:
            # Reanalisa no momento da execução (não confia na simulação da
            # tela - pode ter entrado carga nova desde então).
            def _analise(msg, pct):
                write_status(KIND, job_id, "em processamento", msg, progress=int(pct * 0.3))

            planos = lh.simular(conn, exercicio, ref_mes, ref_ano, tabelas, _analise)
            for i, plano in enumerate(planos):
                base = 30 + 70 * i / len(planos)
                passo = 70 / len(planos)
                label = lh.TABELAS[plano.tabela]["label"]

                def _cb(pct, base=base, passo=passo, label=label):
                    write_status(
                        KIND, job_id, "em processamento",
                        f"Removendo cargas antigas de {label}... {pct}%",
                        progress=int(base + passo * pct / 100),
                    )

                removidas = lh.remover_cargas(conn, plano, _cb)
                total += removidas
                resumo = plano.resumo()
                resumo["linhas_removidas"] = removidas
                resultado.append(resumo)
            _fechar_log(log_id, "concluida", total, resultado)
        except Exception as exc:
            _fechar_log(log_id, "falha", total, {"erro": str(exc), "parcial": resultado})
            raise
        finally:
            lh.liberar_trava(conn)
    write_status(
        KIND, job_id, ESTADO_FIM,
        f"Limpeza concluída: {_fmt_num(total)} registros removidos.",
        progress=100,
    )
    update_status_fields(KIND, job_id, resultado=resultado)


def _job_otimizar(job_id, tabelas, email):
    with db.engine.connect() as conn:
        if not lh.obter_trava(conn):
            write_status(
                KIND, job_id, ESTADO_FALHA,
                "Já existe uma limpeza ou otimização em andamento. Aguarde a conclusão.",
                progress=100,
            )
            return
        log_id = _novo_log("otimizacao", None, None, None, tabelas, email)
        resultado = []
        try:
            antes = {t: e["disco"] for t, e in lh.espaco_mb(conn, tabelas).items()}
            for i, tabela in enumerate(tabelas):
                write_status(
                    KIND, job_id, "em processamento",
                    f"Otimizando {lh.TABELAS[tabela]['label']} ({i + 1}/{len(tabelas)})... "
                    "As tabelas grandes podem levar vários minutos.",
                    progress=int(5 + 90 * i / len(tabelas)),
                )
                lh.otimizar_tabela(conn, tabela)
                depois = lh.espaco_mb(conn, [tabela]).get(tabela, {}).get("disco", 0.0)
                resultado.append(
                    {
                        "tabela": tabela,
                        "label": lh.TABELAS[tabela]["label"],
                        "antes_mb": round(antes.get(tabela, 0.0), 1),
                        "depois_mb": round(depois, 1),
                    }
                )
            _fechar_log(log_id, "concluida", 0, resultado)
        except Exception as exc:
            _fechar_log(log_id, "falha", 0, {"erro": str(exc), "parcial": resultado})
            raise
        finally:
            lh.liberar_trava(conn)
    liberado = sum(r["antes_mb"] - r["depois_mb"] for r in resultado)
    mensagem = (
        f"Otimização concluída: {_fmt_num(liberado, 1)} MB liberados."
        if liberado >= 0.1
        else "Otimização concluída, sem redução de tamanho. A otimização só libera espaço "
        "depois de uma limpeza que remova boa parte da tabela."
    )
    write_status(
        KIND, job_id, ESTADO_FIM,
        mensagem,
        progress=100,
    )
    update_status_fields(KIND, job_id, resultado=resultado)


# ---------------------------------------------------------------- rotas


@limpeza_bp.route("/partial/atualizar/limpeza-historico")
@login_required
@require_feature(FEATURE)
def partial_limpeza_historico():
    return render_template(
        "partials/atualizar_limpeza_historico.html",
        tabelas=[(k, v["label"]) for k, v in lh.TABELAS.items()],
    )


@limpeza_bp.route("/api/limpeza-historico/info", methods=["GET"])
@login_required
@require_feature(FEATURE)
def api_limpeza_info():
    tabelas = list(lh.TABELAS)
    with db.engine.connect() as conn:
        espacos = lh.espaco_mb(conn, tabelas)
        linhas = dict(
            conn.execute(
                text(
                    "SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN :t"
                ).bindparams(bindparam("t", expanding=True)),
                {"t": tabelas},
            ).fetchall()
        )
    logs = (
        LimpezaHistoricoLog.query.order_by(LimpezaHistoricoLog.id.desc()).limit(10).all()
    )
    return jsonify(
        {
            "ok": True,
            "tabelas": [
                {
                    "tabela": t,
                    "label": lh.TABELAS[t]["label"],
                    "tamanho_mb": round(espacos.get(t, {}).get("disco", 0.0), 1),
                    "livre_mb": round(espacos.get(t, {}).get("livre", 0.0), 1),
                    "linhas_aprox": int(linhas.get(t) or 0),
                }
                for t in tabelas
            ],
            "historico": [
                {
                    "acao": l.acao,
                    "status": l.status,
                    "user_email": l.user_email,
                    "exercicio": l.exercicio,
                    "ref_mes": l.ref_mes,
                    "ref_ano": l.ref_ano,
                    "tabelas": l.tabelas,
                    "linhas_removidas": l.linhas_removidas,
                    "iniciado_em": l.iniciado_em.isoformat() if l.iniciado_em else None,
                    "finalizado_em": l.finalizado_em.isoformat() if l.finalizado_em else None,
                }
                for l in logs
            ],
        }
    )


def _disparar(alvo, payload_args):
    job_id = _novo_job()
    write_status(KIND, job_id, "em processamento", "Iniciando...", progress=0)
    _iniciar(job_id, alvo, *payload_args)
    return jsonify({"ok": True, "job_id": job_id})


@limpeza_bp.route("/api/limpeza-historico/simular", methods=["POST"])
@login_required
@require_feature(FEATURE)
def api_limpeza_simular():
    try:
        exercicio, ref_mes, ref_ano, tabelas = _parametros(request.get_json(silent=True) or {})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return _disparar(_job_simular, (exercicio, ref_mes, ref_ano, tabelas))


@limpeza_bp.route("/api/limpeza-historico/executar", methods=["POST"])
@login_required
@require_feature(FEATURE)
def api_limpeza_executar():
    payload = request.get_json(silent=True) or {}
    if payload.get("confirmado") is not True:
        return jsonify({"error": "Confirme que a remoção é definitiva."}), 400
    try:
        exercicio, ref_mes, ref_ano, tabelas = _parametros(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return _disparar(_job_executar, (exercicio, ref_mes, ref_ano, tabelas, _user_email()))


@limpeza_bp.route("/api/limpeza-historico/otimizar", methods=["POST"])
@login_required
@require_feature(FEATURE)
def api_limpeza_otimizar():
    payload = request.get_json(silent=True) or {}
    if payload.get("confirmado") is not True:
        return jsonify({"error": "Confirme a otimização."}), 400
    try:
        _, _, _, tabelas = _parametros(payload, exigir_periodo=False)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return _disparar(_job_otimizar, (tabelas, _user_email()))


@limpeza_bp.route("/api/limpeza-historico/status/<int:job_id>", methods=["GET"])
@login_required
@require_feature(FEATURE)
def api_limpeza_status(job_id: int):
    data = read_status(KIND, job_id)
    if not data:
        return jsonify({"error": "Processamento não encontrado."}), 404
    return jsonify({"ok": True, **data})
