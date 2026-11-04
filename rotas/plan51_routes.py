"""Atualizar/Relatórios > Execução > PLAN 51 (Relação Detalhada de Créditos Adicionais).

Rotas num arquivo próprio (e não em home_routes.py) para não aumentar o
arquivo monolítico e facilitar a sincronização com as outras branches.
Funciona como o FIP 613/PED: upload do .xlsx em "Atualizar", tratamento pelo
script portado (services/plan51_runner.py), grava no banco só a versão ativa e
mostra o resultado em "Relatórios".

Diferente dos outros relatórios de execução, o PLAN 51 é do ESTADO inteiro:
a tela abre sem filtro de UO (o remanejamento entre UOs é o que ele mostra).
"""

from __future__ import annotations

import tempfile
from datetime import datetime
from io import BytesIO
from pathlib import Path

from flask import Blueprint, jsonify, render_template, request, session
from sqlalchemy import text

from models import Plan51Upload, db
from rotas.home_routes import _send_excel_bytes, _validate_upload_layout, require_feature
from services.auth import login_required
from services.plan51_runner import (
    OUTPUT_DIR,
    PLAN51_COLUNAS,
    UPLOAD_DIR,
    Plan51Recusado,
    gerar_excel_do_banco,
    run_plan51,
)

plan51_bp = Blueprint("plan51", __name__)


def _as_iso(value):
    if not value:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


@plan51_bp.route("/partial/atualizar/plan51")
@login_required
@require_feature("atualizar/plan51")
def partial_atualizar_plan51():
    return render_template("partials/atualizar_plan51.html")


@plan51_bp.route("/partial/relatorios/plan51")
@login_required
@require_feature("relatorios/plan51")
def partial_relatorios_plan51():
    return render_template("partials/relatorios_plan51.html")


@plan51_bp.route("/api/plan51/status", methods=["GET"])
@login_required
@require_feature("atualizar/plan51")
def api_plan51_status():
    last = Plan51Upload.query.order_by(Plan51Upload.uploaded_at.desc()).first()
    if not last:
        return jsonify({"ok": True, "last": None})
    return jsonify(
        {
            "ok": True,
            "last": {
                "user_email": last.user_email,
                "uploaded_at": _as_iso(last.uploaded_at),
                "data_arquivo": _as_iso(last.data_arquivo),
                "original_filename": last.original_filename,
                "output_filename": last.output_filename,
            },
        }
    )


@plan51_bp.route("/api/plan51/upload", methods=["POST"])
@login_required
@require_feature("atualizar/plan51")
def api_plan51_upload():
    if "arquivo" not in request.files:
        return jsonify({"error": "Arquivo é obrigatório."}), 400
    arquivo = request.files["arquivo"]
    data_arquivo_raw = request.form.get("data_arquivo")
    if not data_arquivo_raw:
        return jsonify({"error": "Data do download é obrigatória."}), 400
    try:
        data_arquivo = datetime.fromisoformat(data_arquivo_raw)
    except ValueError:
        return jsonify({"error": "Data do download inválida."}), 400

    if not arquivo.filename.lower().endswith(".xlsx"):
        return jsonify({"error": "Envie um arquivo .xlsx."}), 400

    user = session.get("user") or {}
    user_email = user.get("email") or "desconhecido"

    save_path = None
    registro = None
    try:
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        (UPLOAD_DIR / "tmp").mkdir(parents=True, exist_ok=True)
        for f in UPLOAD_DIR.glob("*.xlsx"):
            dest = UPLOAD_DIR / "tmp" / f"{f.stem}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}{f.suffix}"
            try:
                f.rename(dest)
            except OSError:
                pass
        stored_name = f"plan51_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        save_path = UPLOAD_DIR / stored_name
        arquivo.save(save_path)

        layout_error = _validate_upload_layout(save_path, "plan51", "PLAN51")
        if layout_error:
            save_path.unlink(missing_ok=True)
            return jsonify({"error": layout_error}), 400

        registro = Plan51Upload(
            user_email=user_email,
            original_filename=arquivo.filename,
            stored_filename=stored_name,
            data_arquivo=data_arquivo,
            uploaded_at=datetime.utcnow(),
        )
        db.session.add(registro)
        db.session.commit()

        try:
            total, output_path, conferencia = run_plan51(save_path, data_arquivo, user_email, registro.id)
        except Plan51Recusado as exc:
            # Recusado antes de gravar qualquer linha: não deixa o envio no histórico.
            db.session.rollback()
            db.session.delete(registro)
            db.session.commit()
            save_path.unlink(missing_ok=True)
            return jsonify({"error": str(exc)}), 400

        registro.output_filename = str(output_path.name)
        db.session.commit()
        return jsonify(
            {
                "ok": True,
                "message": f"Processado com sucesso. Registros inseridos: {total}. {conferencia}".strip(),
                "output": output_path.name,
            }
        )
    except Exception as exc:
        db.session.rollback()
        return jsonify({"error": f"Falha ao processar: {exc}"}), 500


def _linhas_ativas():
    """Linhas da versão ativa, na ordem da planilha; a última coluna é a máscara
    de cores (`estilo`) que o script de tratamento calculou para a linha."""
    cols = ", ".join(c for _, c, _ in PLAN51_COLUNAS)
    return db.session.execute(text(f"SELECT {cols}, estilo FROM plan51 WHERE ativo = 1 ORDER BY id")).fetchall()


@plan51_bp.route("/api/relatorios/plan51", methods=["GET"])
@login_required
@require_feature("relatorios/plan51")
def api_relatorio_plan51():
    """Linhas como LISTAS (cerca de 12 mil x 66 colunas): bem menor que um
    dicionário por linha. A tela monta os objetos."""
    try:
        numericas = [tipo == "num" for _, _, tipo in PLAN51_COLUNAS]
        linhas = _linhas_ativas()
        rows = [
            [None if v is None else (float(v) if num else v) for v, num in zip(linha, numericas)]
            for linha in linhas
        ]
        estilos = [int(linha[-1] or 0) for linha in linhas]
        columns = [{"key": c, "label": rotulo, "numeric": tipo == "num"} for rotulo, c, tipo in PLAN51_COLUNAS]
        exercicios = [
            int(r[0])
            for r in db.session.execute(
                text("SELECT DISTINCT exercicio FROM plan51 WHERE ativo = 1 ORDER BY exercicio")
            ).fetchall()
        ]
        last = Plan51Upload.query.order_by(Plan51Upload.uploaded_at.desc()).first()
        return jsonify(
            {
                "ok": True,
                "columns": columns,
                "rows": rows,
                "estilos": estilos,
                "exercicios": exercicios,
                "data_arquivo": _as_iso(last.data_arquivo) if last else None,
                "uploaded_at": _as_iso(last.uploaded_at) if last else None,
                "user_email": last.user_email if last else None,
            }
        )
    except Exception as exc:
        return jsonify({"error": f"Falha ao buscar dados: {exc}"}), 500


@plan51_bp.route("/api/relatorios/plan51/download", methods=["GET"])
@login_required
@require_feature("relatorios/plan51")
def api_relatorio_plan51_download():
    """A planilha tratada pelo script (cores, subtotais e rodapé). Vale o arquivo
    de saída gerado no upload da versão ativa; se ele não existir mais (limpeza
    de arquivos), é refeito do banco com a mesma função de formatação."""
    try:
        ativo = db.session.execute(text("SELECT MAX(upload_id) FROM plan51 WHERE ativo = 1")).scalar()
        if ativo is None:
            return jsonify({"error": "Nenhum dado para exportar."}), 404
        upload = db.session.get(Plan51Upload, int(ativo))
        if upload and upload.output_filename:
            arquivo = OUTPUT_DIR / upload.output_filename
            if arquivo.is_file():
                return _send_excel_bytes(BytesIO(arquivo.read_bytes()), arquivo.name)

        linhas = _linhas_ativas()
        db.session.close()
        with tempfile.TemporaryDirectory() as pasta:
            destino = Path(pasta) / "plan51.xlsx"
            quando = upload.uploaded_at.strftime("%d/%m/%Y %H:%M:%S") if upload and upload.uploaded_at else "-"
            gerar_excel_do_banco(
                str(destino),
                [linha[:-1] for linha in linhas],
                [int(linha[-1] or 0) for linha in linhas],
                upload.original_filename if upload else "-",
                quando,
            )
            conteudo = destino.read_bytes()
        return _send_excel_bytes(BytesIO(conteudo), f"plan51_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx")
    except Exception as exc:
        return jsonify({"error": f"Falha ao exportar: {exc}"}), 500
