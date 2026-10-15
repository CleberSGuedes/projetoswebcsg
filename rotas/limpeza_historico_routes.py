"""Atualizar > Execução > Limpeza de Histórico (docs/claude.md, seção 23).

Rotas num arquivo próprio (e não em home_routes.py) para não aumentar o
arquivo monolítico e facilitar a sincronização com as outras branches.
Acesso pela feature "atualizar/limpeza-historico" (painel de permissões);
o administrador (nível 1) passa pela regra geral de require_feature.

Processamento em etapas curtas, conduzidas pela tela (seção 23.9): iniciar
só cria a linha do limpeza_historico_log (o id é o job_id da tela); cada
POST em /passo/<id> faz no máximo lh.PRAZO_PASSO segundos de trabalho, grava
na coluna `estado` onde parou e responde. Nada roda depois da resposta - no
servidor online (LiteSpeed) uma thread em segundo plano é reciclada ou
congelada junto com o processo (seções 23.7 e 23.9).
"""

from __future__ import annotations

import json
import time

from flask import Blueprint, current_app, jsonify, render_template, request, session
from sqlalchemy import bindparam, text

from models import LimpezaHistoricoLog, db
from rotas.home_routes import _now_local, require_feature
from services import limpeza_historico as lh
from services.auth import login_required

limpeza_bp = Blueprint("limpeza_historico", __name__)

FEATURE = "atualizar/limpeza-historico"
ESTADO_FIM = "processamento finalizado"
ESTADO_FALHA = "falha no processamento"
ESTADO_ANDAMENTO = "em processamento"
ACOES_HISTORICO = ("limpeza", "otimizacao")  # a simulação não aparece em "Últimas execuções"


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


def _json(valor) -> str:
    return json.dumps(valor, ensure_ascii=False, default=str)


def _pausada(log: LimpezaHistoricoLog) -> bool:
    return lh.pausada(log.status, log.atualizado_em or log.iniciado_em, _now_local())


def _mensagem_final(estado: dict) -> str:
    acao = estado["acao"]
    if acao == "simulacao":
        return "Simulação concluída."
    if acao == "limpeza":
        return f"Limpeza concluída: {_fmt_num(estado['linhas'])} registros removidos."
    liberado = sum(r["antes_mb"] - r["depois_mb"] for r in estado["resultado"])
    if liberado >= 0.1:
        return f"Otimização concluída: {_fmt_num(liberado, 1)} MB liberados."
    return (
        "Otimização concluída, sem redução de tamanho. A otimização só libera espaço "
        "depois de uma limpeza que remova boa parte da tabela."
    )


def _resposta(log: LimpezaHistoricoLog, **extra):
    """Mesmo formato de status para /passo e /status."""
    if log.status == "em andamento":
        corpo = {
            "state": ESTADO_ANDAMENTO,
            "message": log.mensagem or "",
            "progress": log.progresso or 0,
            "pausada": _pausada(log),
        }
    elif log.status == "concluida":
        try:
            resultado = json.loads(log.detalhes) if log.detalhes else []
        except ValueError:
            resultado = []
        corpo = {"state": ESTADO_FIM, "message": log.mensagem or "", "progress": 100, "resultado": resultado}
    else:  # falha, interrompida (versão com thread) ou abandonada
        corpo = {"state": ESTADO_FALHA, "message": log.mensagem or "Falha no processamento.", "progress": 100}
    return jsonify({"ok": True, "job_id": log.id, "acao": log.acao, **corpo, **extra})


def _novo_log(acao, exercicio, ref_mes, ref_ano, tabelas) -> LimpezaHistoricoLog:
    agora = _now_local()  # fuso da aplicação (America/Manaus), não o do banco
    estado = lh.novo_estado(acao, tabelas, exercicio, ref_mes, ref_ano)
    mensagem, progresso = lh.progresso_estado(estado)
    log = LimpezaHistoricoLog(
        acao=acao,
        status="em andamento",
        user_email=_user_email(),
        exercicio=exercicio,
        ref_mes=ref_mes,
        ref_ano=ref_ano,
        tabelas=",".join(tabelas),
        iniciado_em=agora,
        atualizado_em=agora,
        progresso=progresso,
        mensagem=mensagem,
        estado=_json(estado),
    )
    db.session.add(log)
    db.session.commit()
    return log


def _iniciar_exclusivo(acao, exercicio, ref_mes, ref_ano, tabelas):
    """Limpeza e otimização: uma por vez.

    A trava GET_LOCK dura só esta requisição (checa e cria a linha). Uma
    execução pausada há mais de lh.LIMITE_PAUSA segundos não bloqueia: é
    encerrada como "abandonada" (o que já foi removido fica registrado).
    """
    with db.engine.connect() as conn:
        if not conn.execute(text("SELECT GET_LOCK(:n, 5)"), {"n": lh.NOME_TRAVA}).scalar():
            return jsonify({"error": "Outra execução está sendo iniciada. Tente novamente."}), 409
        try:
            abertas = LimpezaHistoricoLog.query.filter(
                LimpezaHistoricoLog.status == "em andamento",
                LimpezaHistoricoLog.acao.in_(ACOES_HISTORICO),
            ).all()
            for aberta in abertas:
                if not _pausada(aberta):
                    inicio = aberta.iniciado_em.strftime("%d/%m/%Y %H:%M") if aberta.iniciado_em else ""
                    return (
                        jsonify(
                            {
                                "error": "Já existe uma limpeza ou otimização em andamento "
                                f"({aberta.user_email}, iniciada em {inicio}). Aguarde a conclusão."
                            }
                        ),
                        409,
                    )
                aberta.status = "abandonada"
                aberta.mensagem = "Execução pausada e substituída por uma nova execução."
                aberta.finalizado_em = _now_local()
            log = _novo_log(acao, exercicio, ref_mes, ref_ano, tabelas)
        finally:
            conn.execute(text("SELECT RELEASE_LOCK(:n)"), {"n": lh.NOME_TRAVA})
    return jsonify({"ok": True, "job_id": log.id})


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
    # Simulação abandonada (tela fechada): não há o que continuar.
    for log in LimpezaHistoricoLog.query.filter_by(acao="simulacao", status="em andamento").all():
        if _pausada(log):
            log.status, log.finalizado_em = "abandonada", _now_local()
    db.session.commit()
    logs = (
        LimpezaHistoricoLog.query.filter(LimpezaHistoricoLog.acao.in_(ACOES_HISTORICO))
        .order_by(LimpezaHistoricoLog.id.desc())
        .limit(10)
        .all()
    )
    pendente = next((l for l in logs if l.status == "em andamento"), None)
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
                    "status": "pausada" if _pausada(l) else l.status,
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
            # Limpeza/otimização não concluída: a tela oferece "Continuar".
            "pendente": (
                {
                    "job_id": pendente.id,
                    "acao": pendente.acao,
                    "progress": pendente.progresso or 0,
                    "message": pendente.mensagem or "",
                    "pausada": _pausada(pendente),
                    "user_email": pendente.user_email,
                    "iniciado_em": pendente.iniciado_em.isoformat() if pendente.iniciado_em else None,
                }
                if pendente
                else None
            ),
        }
    )


@limpeza_bp.route("/api/limpeza-historico/simular", methods=["POST"])
@login_required
@require_feature(FEATURE)
def api_limpeza_simular():
    try:
        exercicio, ref_mes, ref_ano, tabelas = _parametros(request.get_json(silent=True) or {})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    log = _novo_log("simulacao", exercicio, ref_mes, ref_ano, tabelas)
    return jsonify({"ok": True, "job_id": log.id})


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
    return _iniciar_exclusivo("limpeza", exercicio, ref_mes, ref_ano, tabelas)


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
    return _iniciar_exclusivo("otimizacao", None, None, None, tabelas)


@limpeza_bp.route("/api/limpeza-historico/passo/<int:job_id>", methods=["POST"])
@login_required
@require_feature(FEATURE)
def api_limpeza_passo(job_id: int):
    """Executa a próxima etapa (até lh.PRAZO_PASSO segundos) e responde."""
    log = db.session.get(LimpezaHistoricoLog, job_id)
    if not log:
        return jsonify({"error": "Processamento não encontrado."}), 404
    if log.status != "em andamento" or not log.estado:
        return _resposta(log)

    trava = f"limpeza_passo_{job_id}"
    with db.engine.connect() as conn:
        # Duas abas na mesma execução: só uma faz a etapa; a outra espera.
        if not conn.execute(text("SELECT GET_LOCK(:n, 0)"), {"n": trava}).scalar():
            return _resposta(log, ocupado=True)
        try:
            # Pode ter mudado antes da trava. O commit encerra a transação
            # (REPEATABLE READ) para o refresh enxergar o valor atual.
            db.session.commit()
            db.session.refresh(log)
            if log.status != "em andamento":
                return _resposta(log)
            estado = json.loads(log.estado)
            try:
                lh.executar_passo(conn, estado, time.monotonic() + lh.PRAZO_PASSO)
            except Exception as exc:
                # A etapa é refeita na próxima chamada a partir do último
                # estado gravado: cada lote removido já foi commitado e o
                # DELETE seguinte simplesmente não encontra mais nada.
                current_app.logger.exception("Falha numa etapa da Limpeza de Histórico (log %s).", job_id)
                log.atualizado_em = _now_local()
                db.session.commit()
                return jsonify({"error": f"Falha nesta etapa: {exc}", "transitorio": True}), 500
            mensagem, progresso = lh.progresso_estado(estado)
            log.estado = _json(estado)
            log.linhas_removidas = estado["linhas"]
            log.atualizado_em = _now_local()
            if estado["fase"] == "fim":
                log.status = "concluida"
                log.mensagem = _mensagem_final(estado)[:500]
                log.progresso = 100
                log.detalhes = _json(estado["resultado"])
                log.finalizado_em = log.atualizado_em
            else:
                log.mensagem, log.progresso = mensagem[:500], progresso
            db.session.commit()
        finally:
            conn.execute(text("SELECT RELEASE_LOCK(:n)"), {"n": trava})
    return _resposta(log)


@limpeza_bp.route("/api/limpeza-historico/status/<int:job_id>", methods=["GET"])
@login_required
@require_feature(FEATURE)
def api_limpeza_status(job_id: int):
    log = db.session.get(LimpezaHistoricoLog, job_id)
    if not log:
        return jsonify({"error": "Processamento não encontrado."}), 404
    return _resposta(log)
