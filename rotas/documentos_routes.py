"""Documentos publicados do SPO (feat/padrao-spo-aa-plus, 2026-11-14).

Rota num arquivo próprio (e não em home_routes.py), como a Limpeza de Histórico.
GET /api/documentos/<nome> devolve o documento convertido em HTML para quem está
logado; hoje só o documento técnico do padrão SPO-AA+ (services/documentos.py).
"""

from __future__ import annotations

from flask import Blueprint, current_app, jsonify

from services import documentos
from services.auth import login_required

documentos_bp = Blueprint("documentos", __name__)


@documentos_bp.route("/api/documentos/<nome>")
@login_required
def api_documento(nome: str):
    try:
        doc = documentos.carregar(nome)
    except OSError:
        current_app.logger.exception("documento %s ilegível", nome)
        return jsonify({"error": "Documento indisponível no servidor."}), 503
    if doc is None:
        return jsonify({"error": "Documento não encontrado."}), 404
    resposta = jsonify(doc)
    resposta.headers["Cache-Control"] = "no-store"
    return resposta
