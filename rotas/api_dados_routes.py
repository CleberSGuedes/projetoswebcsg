"""API de dados (somente leitura) para as equipes atualizarem planilhas sozinhas.

Complementa as rotas paginadas `/api/v1/dados/<relatorio>` de home_routes.py (que
continuam como estavam) com:

* `/export`  carga completa em CSV (ou JSON), com o cabeçalho do Excel por padrão;
* `/meta`    última atualização (para só baixar quando houver dado novo);
* `/schema`  colunas, rótulos e tipos;
* `/api/v1/dados/plan51` (paginada, como as demais) e as três rotas acima para o PLAN 51.

Mesmo token e mesmos escopos (`dados.<relatorio>.read`). Sem parâmetro `uo` os
relatórios com coluna UO devolvem só a SEDUC; `uo=all` devolve todas. O PLAN 51
é do estado inteiro (não tem coluna UO).
"""

from __future__ import annotations

import csv
import io
from datetime import date, datetime
from decimal import Decimal

from flask import Blueprint, Response, jsonify, request
from sqlalchemy import Date, DateTime, Float, Integer, Numeric, func, inspect

from models import (
    EmpRegistro,
    EmpUpload,
    EstEmpRegistro,
    EstEmpUpload,
    Fip613Registro,
    Fip613Upload,
    NobRegistro,
    NobUpload,
    PedRegistro,
    PedUpload,
    Plan51Registro,
    Plan51Upload,
    db,
)
from rotas.home_routes import _api_query_model, require_api_scope
from services import api_dados_rotulos as rot
from services.execucao_uo import cond_uo_em
from services.plan51_runner import PLAN51_COLUNAS
from services.uo import UOS_SEDUC, uo_codigo

api_dados_bp = Blueprint("api_dados", __name__)

# Colunas técnicas que nunca saem pela API.
_TECNICAS = {"raw_payload", "estilo"}
# Colunas que só aparecem no modo `colunas=banco`.
_META = {"id", "upload_id", "chave", "chave_planejamento", "data_atualizacao", "data_arquivo", "user_email", "ativo", "created_at"}
_RESERVADOS = {"formato", "colunas", "fields", "uo"}
_LOTE = 5000

RELATORIOS = {
    "emp": {"modelo": EmpRegistro, "upload": EmpUpload, "rotulos": rot.ROTULOS_EMP},
    "nob": {"modelo": NobRegistro, "upload": NobUpload, "rotulos": rot.ROTULOS_NOB},
    "ped": {"modelo": PedRegistro, "upload": PedUpload, "rotulos": rot.ROTULOS_PED},
    "est-emp": {"modelo": EstEmpRegistro, "upload": EstEmpUpload, "rotulos": rot.ROTULOS_EST_EMP},
    "fip613": {"modelo": Fip613Registro, "upload": Fip613Upload, "rotulos": rot.ROTULOS_FIP613},
    "plan51": {"modelo": Plan51Registro, "upload": Plan51Upload, "rotulos": {c[1]: c[0] for c in PLAN51_COLUNAS}},
}


def _agora() -> datetime:
    return datetime.now()


def _chaves(modelo) -> list[str]:
    return [c.key for c in inspect(modelo).columns]


def _colunas_modelo(cfg: dict, modo: str) -> list[tuple[str, str]]:
    """(coluna_do_banco, nome_no_cabecalho) na ordem de saída."""
    modelo = cfg["modelo"]
    chaves = _chaves(modelo)
    if modo == "banco":
        return [(k, k) for k in chaves if k not in _TECNICAS]
    rotulos = cfg["rotulos"]
    cols = [(k, rotulos[k]) for k in rotulos if k in chaves]
    # coluna nova do banco que ainda não tem rótulo: sai ao final, sem sumir
    conhecidas = {k for k, _ in cols}
    for k in chaves:
        if k not in conhecidas and k not in _TECNICAS and k not in _META:
            cols.append((k, k.replace("_", " ").title()))
    return cols


def _filtro_base(cfg: dict):
    """Query com `ativo`, UO e filtros por coluna. Devolve (query, uo_filtrada) ou (None, resposta_de_erro)."""
    modelo = cfg["modelo"]
    chaves = _chaves(modelo)
    query = db.session.query(modelo).filter(modelo.ativo == True)  # noqa: E712
    uo_filtrada = "todas"
    if "uo" in chaves:
        uo_param = str(request.args.get("uo", "") or "").strip()
        uo_filtrada = "seduc"
        if not uo_param:
            query = query.filter(cond_uo_em(modelo, UOS_SEDUC))
        elif uo_param.lower() == "all":
            uo_filtrada = "all"
        else:
            codigos = [c for c in (uo_codigo(v) for v in uo_param.split(",")) if c]
            if not codigos:
                return None, (jsonify({"error": "Parâmetro uo inválido."}), 400)
            query = query.filter(cond_uo_em(modelo, codigos))
            uo_filtrada = ",".join(sorted(set(codigos)))
    # filtros por coluna: aceita o nome do banco ou o rótulo do Excel
    por_rotulo = {v: k for k, v in cfg["rotulos"].items()}
    for chave, valor in request.args.items():
        if chave in _RESERVADOS:
            continue
        coluna = chave if chave in chaves else por_rotulo.get(chave)
        if not coluna or coluna in _TECNICAS:
            continue
        texto = str(valor or "").strip()
        if not texto:
            continue
        if "," in texto:
            valores = [v.strip() for v in texto.split(",") if v.strip()]
            if valores:
                query = query.filter(getattr(modelo, coluna).in_(valores))
        else:
            query = query.filter(getattr(modelo, coluna) == texto)
    return query, uo_filtrada


def _colunas_pedidas(cfg: dict, modo: str) -> list[tuple[str, str]]:
    todas = _colunas_modelo(cfg, modo)
    pedido = str(request.args.get("fields") or "").strip()
    if not pedido:
        return todas
    nomes = [n.strip() for n in pedido.split(",") if n.strip()]
    mapa = {}
    for k, cab in todas:
        mapa[k] = (k, cab)
        mapa[cab] = (k, cab)
    escolhidas, vistas = [], set()
    for n in nomes:
        par = mapa.get(n)
        if par and par[0] not in vistas:
            escolhidas.append(par)
            vistas.add(par[0])
    return escolhidas or todas


def _texto(valor) -> str:
    if valor is None:
        return ""
    if isinstance(valor, Decimal):
        return format(valor, "f")
    if isinstance(valor, bool):
        return "1" if valor else "0"
    if isinstance(valor, datetime):
        return valor.isoformat()
    if isinstance(valor, date):
        return valor.isoformat()
    return str(valor)


def _json(valor):
    if isinstance(valor, Decimal):
        return float(valor)
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()
    return valor


def _linhas(cfg: dict, query, colunas: list[tuple[str, str]]):
    """Percorre a consulta em lotes por `id` (sem OFFSET, que degrada com 70 mil linhas)."""
    modelo = cfg["modelo"]
    attrs = [getattr(modelo, k) for k, _ in colunas]
    base = query.with_entities(modelo.id, modelo.upload_id, *attrs).order_by(modelo.id.asc())
    ultimo = 0
    while True:
        lote = base.filter(modelo.id > ultimo).limit(_LOTE).all()
        if not lote:
            return
        for r in lote:
            yield r[1], r[2:]
        ultimo = lote[-1][0]
        if len(lote) < _LOTE:
            return


def _exportar(rel: str):
    cfg = RELATORIOS[rel]
    formato = str(request.args.get("formato", "csv") or "csv").lower()
    if formato not in ("csv", "json"):
        return jsonify({"error": "formato deve ser csv ou json."}), 400
    modo = str(request.args.get("colunas", "rotulo") or "rotulo").lower()
    if modo not in ("rotulo", "banco"):
        return jsonify({"error": "colunas deve ser rotulo ou banco."}), 400

    query, uo = _filtro_base(cfg)
    if query is None:
        return uo
    colunas = _colunas_pedidas(cfg, modo)
    cabecalho = [cab for _, cab in colunas]
    uploads: set[int] = set()
    gerado_em = _agora().isoformat(timespec="seconds")

    if formato == "csv":
        buf = io.StringIO(newline="")
        w = csv.writer(buf, lineterminator="\r\n")
        w.writerow(cabecalho)
        total = 0
        for upload_id, valores in _linhas(cfg, query, colunas):
            uploads.add(int(upload_id))
            w.writerow([_texto(v) for v in valores])
            total += 1
        resp = Response(buf.getvalue().encode("utf-8"), mimetype="text/csv")
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = f'attachment; filename="{rel}.csv"'
    else:
        dados = []
        for upload_id, valores in _linhas(cfg, query, colunas):
            uploads.add(int(upload_id))
            dados.append({cab: _json(v) for (_, cab), v in zip(colunas, valores)})
        total = len(dados)
        resp = jsonify(
            {
                "ok": True,
                "relatorio": rel,
                "uo": uo,
                "upload_id": sorted(uploads),
                "total": total,
                "gerado_em": gerado_em,
                "colunas": cabecalho,
                "data": dados,
            }
        )
    resp.headers["X-Total"] = str(total)
    resp.headers["X-Upload-Id"] = ",".join(str(u) for u in sorted(uploads))
    resp.headers["X-Gerado-Em"] = gerado_em
    resp.headers["X-UO"] = uo
    resp.headers["Cache-Control"] = "no-store"
    return resp


def _meta(rel: str):
    cfg = RELATORIOS[rel]
    modelo, upload = cfg["modelo"], cfg["upload"]
    query, uo = _filtro_base(cfg)
    if query is None:
        return uo
    grupos = (
        query.with_entities(modelo.upload_id, func.count(), func.max(modelo.data_arquivo))
        .group_by(modelo.upload_id)
        .order_by(modelo.upload_id.desc())
        .all()
    )
    ids = [int(g[0]) for g in grupos if g[0] is not None]
    envios = {}
    if ids:
        for u in db.session.query(upload).filter(upload.id.in_(ids)).all():
            envios[int(u.id)] = u
    cargas = []
    for upload_id, linhas, data_arquivo in grupos:
        u = envios.get(int(upload_id)) if upload_id is not None else None
        enviado = getattr(u, "uploaded_at", None)
        cargas.append(
            {
                "upload_id": int(upload_id) if upload_id is not None else None,
                "linhas": int(linhas),
                "data_arquivo": _json(data_arquivo or getattr(u, "data_arquivo", None)),
                "enviado_em": _json(enviado),
            }
        )
    enviados = [c["enviado_em"] for c in cargas if c["enviado_em"]]
    resp = jsonify(
        {
            "ok": True,
            "relatorio": rel,
            "uo": uo,
            "total": sum(c["linhas"] for c in cargas),
            "atualizado_em": max(enviados) if enviados else None,
            "upload_id": [c["upload_id"] for c in cargas if c["upload_id"] is not None],
            "cargas": cargas,
            "gerado_em": _agora().isoformat(timespec="seconds"),
        }
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


def _tipo(coluna) -> str:
    t = coluna.type
    if isinstance(t, DateTime):
        return "datahora"
    if isinstance(t, Date):
        return "data"
    if isinstance(t, (Numeric, Float, Integer)):
        return "numero"
    return "texto"


def _schema(rel: str):
    cfg = RELATORIOS[rel]
    mapper = inspect(cfg["modelo"])
    tipos = {c.key: _tipo(c) for c in mapper.columns}
    colunas = [
        {"coluna": k, "rotulo": cab, "tipo": tipos.get(k, "texto")}
        for k, cab in _colunas_modelo(cfg, "rotulo")
    ]
    return jsonify({"ok": True, "relatorio": rel, "total_colunas": len(colunas), "colunas": colunas})


def _registrar(rel: str, escopo: str):
    sufixo = rel.replace("-", "_")

    @require_api_scope(escopo)
    def export():
        return _exportar(rel)

    @require_api_scope(escopo)
    def meta():
        return _meta(rel)

    @require_api_scope(escopo)
    def schema():
        return _schema(rel)

    api_dados_bp.add_url_rule(f"/api/v1/dados/{rel}/export", f"export_{sufixo}", export, methods=["GET"])
    api_dados_bp.add_url_rule(f"/api/v1/dados/{rel}/meta", f"meta_{sufixo}", meta, methods=["GET"])
    api_dados_bp.add_url_rule(f"/api/v1/dados/{rel}/schema", f"schema_{sufixo}", schema, methods=["GET"])


for _rel in RELATORIOS:
    _registrar(_rel, f"dados.{_rel.replace('-', '_')}.read")


@api_dados_bp.route("/api/v1/dados/plan51", methods=["GET"])
@require_api_scope("dados.plan51.read")
def api_dados_plan51():
    return _api_query_model(Plan51Registro, "plan51")
