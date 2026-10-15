"""Atualizar > Execução > Limpeza de Histórico (docs/claude.md, seção 23).

Rotas num arquivo próprio (e não em home_routes.py) para não aumentar o
arquivo monolítico e facilitar a sincronização com as outras branches.
Acesso pela feature "atualizar/limpeza-historico" (painel de permissões);
o administrador (nível 1) passa pela regra geral de require_feature.

O andamento de cada processamento fica na linha do limpeza_historico_log
(o id da linha é o job_id da tela), e não em arquivo: o servidor online
roda vários processos da aplicação ao mesmo tempo e recicla esses processos
em poucos minutos (seção 23.7).
"""

from __future__ import annotations

import json
import threading
from datetime import timedelta

from flask import Blueprint, current_app, jsonify, render_template, request, session
from sqlalchemy import bindparam, or_, text

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

_LOG = LimpezaHistoricoLog.__table__


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


# ---------------------------------------------------------------- andamento


class Andamento:
    """Andamento de um processamento, gravado na linha do log.

    Toda gravação é um UPDATE próprio, com commit (engine.begin), fora da
    sessão do ORM - pode ser chamada da thread do processamento e da thread
    do sinal de vida. Os horários usam _now_local() (fuso da aplicação).
    """

    def __init__(self, log_id: int):
        self.log_id = log_id
        self.engine = db.engine  # capturado no contexto da requisição
        self.linhas = 0
        self.resultado: list[dict] = []
        self._ultimo = None
        self._parar = threading.Event()

    def _gravar(self, onde_andamento: bool = False, **campos) -> None:
        campos["atualizado_em"] = _now_local()
        stmt = _LOG.update().where(_LOG.c.id == self.log_id)
        if onde_andamento:
            stmt = stmt.where(_LOG.c.status == "em andamento")
        with self.engine.begin() as conn:
            conn.execute(stmt.values(**campos))

    def progresso(self, mensagem: str, pct: int) -> None:
        if (mensagem, pct) == self._ultimo:
            return  # a remoção chama a cada lote; só grava quando muda
        self._ultimo = (mensagem, pct)
        self._gravar(mensagem=mensagem[:500], progresso=pct)

    def parcial(self, linhas: int, resultado: list[dict]) -> None:
        """Guarda o que já foi feito - fica no log se o processo morrer."""
        self.linhas, self.resultado = linhas, resultado
        self._gravar(linhas_removidas=linhas, detalhes=_json(resultado))

    def iniciar_sinal_de_vida(self) -> None:
        def _loop():
            while not self._parar.wait(lh.INTERVALO_SINAL):
                try:
                    self._gravar(onde_andamento=True)
                except Exception:  # banco indisponível por um instante: tenta no próximo
                    pass

        threading.Thread(target=_loop, daemon=True).start()

    def _fechar(self, status: str, mensagem: str, detalhes) -> None:
        self._parar.set()
        self._gravar(
            status=status,
            mensagem=mensagem[:500],
            progresso=100,
            linhas_removidas=self.linhas,
            detalhes=_json(detalhes),
            finalizado_em=_now_local(),
        )

    def concluir(self, mensagem: str, resultado) -> None:
        self._fechar("concluida", mensagem, resultado)

    def falhar(self, mensagem: str) -> None:
        self._fechar("falha", mensagem, {"erro": mensagem, "parcial": self.resultado})


def _novo_log(acao, exercicio, ref_mes, ref_ano, tabelas, email) -> int:
    agora = _now_local()  # fuso da aplicação (America/Manaus), não o do banco
    log = LimpezaHistoricoLog(
        acao=acao,
        status="em andamento",
        user_email=email,
        exercicio=exercicio,
        ref_mes=ref_mes,
        ref_ano=ref_ano,
        tabelas=",".join(tabelas),
        iniciado_em=agora,
        atualizado_em=agora,
        progresso=0,
        mensagem="Iniciando...",
    )
    db.session.add(log)
    db.session.commit()
    return log.id


def _mensagem_interrompido(acao: str) -> str:
    mensagem = "Processamento interrompido pelo servidor. Rode novamente."
    if acao == "limpeza":
        mensagem += (
            " O que já foi removido continua removido; o restante segue desativado e intacto,"
            " e a nova execução continua de onde parou."
        )
    return mensagem


def _verificar_sinal(log: LimpezaHistoricoLog) -> None:
    """Marca como "interrompida" a linha em andamento sem sinal de vida.

    O UPDATE só vale se a linha continuar em andamento e parada - se o
    processamento gravou algo nesse meio tempo, nada muda.
    """
    agora = _now_local()
    ultimo = log.atualizado_em or log.iniciado_em
    if not lh.sem_sinal_de_vida(log.status, ultimo, agora):
        return
    limite = agora - timedelta(seconds=lh.LIMITE_SEM_SINAL)
    mensagem = _mensagem_interrompido(log.acao)
    resultado = db.session.execute(
        _LOG.update()
        .where(_LOG.c.id == log.id)
        .where(_LOG.c.status == "em andamento")
        .where(or_(_LOG.c.atualizado_em.is_(None), _LOG.c.atualizado_em < limite))
        .values(status="interrompida", mensagem=mensagem, finalizado_em=agora)
    )
    db.session.commit()
    if resultado.rowcount:
        log.status, log.mensagem, log.finalizado_em = "interrompida", mensagem, agora


def _iniciar(log_id: int, alvo, *args) -> None:
    app = current_app._get_current_object()
    andamento = Andamento(log_id)

    def _runner():
        with app.app_context():
            andamento.iniciar_sinal_de_vida()
            try:
                alvo(andamento, *args)
            except Exception as exc:  # erro inesperado: registra e avisa a tela
                app.logger.exception("Falha na Limpeza de Histórico (log %s).", log_id)
                try:
                    andamento.falhar(f"Falha: {exc}")
                except Exception:
                    app.logger.exception("Não foi possível registrar a falha (log %s).", log_id)
            finally:
                andamento._parar.set()
                db.session.remove()

    threading.Thread(target=_runner, daemon=True).start()


# ---------------------------------------------------------------- jobs


def _job_simular(andamento: Andamento, exercicio, ref_mes, ref_ano, tabelas):
    with db.engine.connect() as conn:
        planos = lh.simular(conn, exercicio, ref_mes, ref_ano, tabelas, andamento.progresso)
    andamento.concluir("Simulação concluída.", [p.resumo() for p in planos])


def _job_executar(andamento: Andamento, exercicio, ref_mes, ref_ano, tabelas):
    with db.engine.connect() as conn:
        if not lh.obter_trava(conn):
            andamento.falhar("Já existe uma limpeza ou otimização em andamento. Aguarde a conclusão.")
            return
        try:
            # Reanalisa no momento da execução (não confia na simulação da
            # tela - pode ter entrado carga nova desde então).
            def _analise(msg, pct):
                andamento.progresso(msg, int(pct * 0.3))

            planos = lh.simular(conn, exercicio, ref_mes, ref_ano, tabelas, _analise)
            resultado, total = [], 0
            for i, plano in enumerate(planos):
                base = 30 + 70 * i / len(planos)
                passo = 70 / len(planos)
                label = lh.TABELAS[plano.tabela]["label"]

                def _cb(pct, base=base, passo=passo, label=label):
                    andamento.progresso(
                        f"Removendo cargas antigas de {label}... {pct}%",
                        int(base + passo * pct / 100),
                    )

                removidas = lh.remover_cargas(conn, plano, _cb)
                total += removidas
                resumo = plano.resumo()
                resumo["linhas_removidas"] = removidas
                resultado.append(resumo)
                andamento.parcial(total, resultado)
        finally:
            lh.liberar_trava(conn)
    andamento.concluir(f"Limpeza concluída: {_fmt_num(total)} registros removidos.", resultado)


def _job_otimizar(andamento: Andamento, tabelas):
    with db.engine.connect() as conn:
        if not lh.obter_trava(conn):
            andamento.falhar("Já existe uma limpeza ou otimização em andamento. Aguarde a conclusão.")
            return
        try:
            antes = {t: e["disco"] for t, e in lh.espaco_mb(conn, tabelas).items()}
            resultado = []
            for i, tabela in enumerate(tabelas):
                andamento.progresso(
                    f"Otimizando {lh.TABELAS[tabela]['label']} ({i + 1}/{len(tabelas)})... "
                    "As tabelas grandes podem levar vários minutos.",
                    int(5 + 90 * i / len(tabelas)),
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
                andamento.parcial(0, resultado)
        finally:
            lh.liberar_trava(conn)
    liberado = sum(r["antes_mb"] - r["depois_mb"] for r in resultado)
    mensagem = (
        f"Otimização concluída: {_fmt_num(liberado, 1)} MB liberados."
        if liberado >= 0.1
        else "Otimização concluída, sem redução de tamanho. A otimização só libera espaço "
        "depois de uma limpeza que remova boa parte da tabela."
    )
    andamento.concluir(mensagem, resultado)


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
    # Execução cujo processo morreu sem ninguém acompanhando na tela: sem
    # isso ficaria "em andamento" para sempre em "Últimas execuções".
    for log in LimpezaHistoricoLog.query.filter_by(status="em andamento").all():
        _verificar_sinal(log)
    logs = (
        LimpezaHistoricoLog.query.filter(LimpezaHistoricoLog.acao.in_(ACOES_HISTORICO))
        .order_by(LimpezaHistoricoLog.id.desc())
        .limit(10)
        .all()
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


def _disparar(acao, alvo, exercicio, ref_mes, ref_ano, tabelas, *extra):
    log_id = _novo_log(acao, exercicio, ref_mes, ref_ano, tabelas, _user_email())
    _iniciar(log_id, alvo, *extra)
    return jsonify({"ok": True, "job_id": log_id})


@limpeza_bp.route("/api/limpeza-historico/simular", methods=["POST"])
@login_required
@require_feature(FEATURE)
def api_limpeza_simular():
    try:
        exercicio, ref_mes, ref_ano, tabelas = _parametros(request.get_json(silent=True) or {})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return _disparar(
        "simulacao", _job_simular, exercicio, ref_mes, ref_ano, tabelas,
        exercicio, ref_mes, ref_ano, tabelas,
    )


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
    return _disparar(
        "limpeza", _job_executar, exercicio, ref_mes, ref_ano, tabelas,
        exercicio, ref_mes, ref_ano, tabelas,
    )


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
    return _disparar("otimizacao", _job_otimizar, None, None, None, tabelas, tabelas)


@limpeza_bp.route("/api/limpeza-historico/status/<int:job_id>", methods=["GET"])
@login_required
@require_feature(FEATURE)
def api_limpeza_status(job_id: int):
    log = db.session.get(LimpezaHistoricoLog, job_id)
    if not log:
        return jsonify({"error": "Processamento não encontrado."}), 404
    _verificar_sinal(log)
    if log.status == "em andamento":
        return jsonify(
            {"ok": True, "state": ESTADO_ANDAMENTO, "message": log.mensagem or "", "progress": log.progresso}
        )
    if log.status == "concluida":
        try:
            resultado = json.loads(log.detalhes) if log.detalhes else []
        except ValueError:
            resultado = []
        return jsonify(
            {"ok": True, "state": ESTADO_FIM, "message": log.mensagem or "", "progress": 100, "resultado": resultado}
        )
    # "falha" ou "interrompida"
    return jsonify(
        {"ok": True, "state": ESTADO_FALHA, "message": log.mensagem or "Falha no processamento.", "progress": 100}
    )
