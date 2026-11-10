"""Área UENs > SAGE > Notas SEE: DANFEs em PDF -> planilha consolidada.

Rotas num arquivo próprio (e não em home_routes.py), como o PLAN 51, para não aumentar o
arquivo monolítico. Fluxo: o usuário escolhe um catálogo (produtos de um exercício) e envia
as DANFEs em lotes (até 50 arquivos / 20 MB por requisição, por causa do limite de 1000
partes do Werkzeug); um worker (`worker.py --kind see`) extrai DRE, escola e as quantidades
de cada produto, confere com o cadastro de escolas (`see_escolas`) e gera o Excel.

A biblioteca pdfplumber é carregada só na extração (services/see_notes.py): se faltar no
servidor, só esta funcionalidade fica indisponível, e o resto do sistema segue normal.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import sys
import threading
import uuid
from datetime import datetime
from pathlib import Path

import pytz
from flask import Blueprint, current_app, g, jsonify, render_template, request, send_file, session
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from models import (
    SeeCatalogo,
    SeeCatalogoProduto,
    SeeEscola,
    SeeItemExtraido,
    SeeOcorrencia,
    SeeProcessamento,
    SeeProcessamentoArquivo,
    SeeProcessamentoEvento,
    Usuario,
    db,
)
from rotas.home_routes import _safe_session_rollback, require_feature
from services.auth import login_required
from services.see_escolas import Escola, EscolaIndex, identificar_escola
from services.see_notes import (
    PdfPlumberIndisponivel,
    extract_pdf as extract_see_pdf,
    generate_xlsx as generate_see_xlsx,
    pdfplumber_disponivel,
)

notas_see_bp = Blueprint("notas_see", __name__)


def _current_usuario_id() -> int | None:
    usuario_id = getattr(g, "user_id", None)
    if usuario_id:
        return int(usuario_id)
    user_session = session.get("user") or {}
    usuario_id = user_session.get("id")
    if usuario_id:
        g.user_id = int(usuario_id)
        return int(usuario_id)
    email = (user_session.get("email") or "").strip().lower()
    if not email:
        return None
    try:
        usuario_id = db.session.execute(
            select(Usuario.id).where(func.lower(func.trim(Usuario.email)) == email)
        ).scalar_one_or_none()
    except SQLAlchemyError:
        _safe_session_rollback()
        return None
    if usuario_id is not None:
        g.user_id = int(usuario_id)
        user_session["id"] = int(usuario_id)
        session["user"] = user_session
        return int(usuario_id)
    return None


SEE_UPLOAD_DIR = Path("upload") / "notas_see"
SEE_OUTPUT_DIR = Path("outputs") / "notas_see"


# Ordem de exibição dos produtos: alunos (al) antes de professores (pr); dentro de cada grupo,
# regular, EJA, especial e artes; e, em cada tipo, pela série (1º, 2º, ...).
_SEE_PRODUTO_RE = re.compile(r"^\s*(\d+)\D*?_?(reg|eja|esp|art)_(al|pr)\s*$", re.IGNORECASE)
_SEE_ORDEM_PUBLICO = {"al": 0, "pr": 1}
_SEE_ORDEM_TIPO = {"reg": 0, "eja": 1, "esp": 2, "art": 3}


def _see_produto_ordem(produto) -> tuple:
    match = _SEE_PRODUTO_RE.match(produto.nome or "")
    if not match:  # nomes fora do padrão vão para o fim, por código
        return (1, 0, 0, 0, str(produto.codigo))
    serie, tipo, publico = int(match.group(1)), match.group(2).lower(), match.group(3).lower()
    return (0, _SEE_ORDEM_PUBLICO[publico], _SEE_ORDEM_TIPO[tipo], serie, str(produto.codigo))


def _see_catalog_payload(catalogo: SeeCatalogo) -> dict:
    produtos = sorted(
        SeeCatalogoProduto.query.filter_by(catalogo_id=catalogo.id, ativo=True).all(),
        key=_see_produto_ordem,
    )
    return {
        "id": catalogo.id,
        "nome": catalogo.nome,
        "exercicio": catalogo.exercicio,
        "descricao": catalogo.descricao or "",
        "ativo": bool(catalogo.ativo),
        "produtos": [
            {
                "id": produto.id,
                "codigo": produto.codigo,
                "nome": produto.nome,
                "observacao": produto.observacao or "",
            }
            for produto in produtos
        ],
    }


def _see_parse_exercicio(value) -> int | None:
    try:
        exercicio = int(str(value or "").strip())
    except ValueError:
        return None
    return exercicio if 2000 <= exercicio <= 2100 else None


def _see_catalogo_duplicado(nome: str, exercicio: int, ignorar_id: int | None = None) -> bool:
    query = SeeCatalogo.query.filter(
        func.lower(SeeCatalogo.nome) == nome.lower(),
        SeeCatalogo.exercicio == exercicio,
    )
    if ignorar_id:
        query = query.filter(SeeCatalogo.id != ignorar_id)
    return query.first() is not None


@notas_see_bp.route("/partial/area-uens/sage/notas-see")
@login_required
@require_feature("area-uens/sage/notas-see")
def partial_notas_see():
    return render_template("partials/notas_see.html")


@notas_see_bp.route("/api/notas-see/catalogos", methods=["GET", "POST"])
@login_required
@require_feature("area-uens/sage/notas-see")
def api_notas_see_catalogos():
    if request.method == "GET":
        catalogos = SeeCatalogo.query.order_by(
            SeeCatalogo.ativo.desc(), SeeCatalogo.exercicio.desc(), SeeCatalogo.nome
        ).all()
        return jsonify({"catalogos": [_see_catalog_payload(catalogo) for catalogo in catalogos]})
    data = request.get_json() or {}
    nome = (data.get("nome") or "").strip()
    exercicio = _see_parse_exercicio(data.get("exercicio"))
    if not nome:
        return jsonify({"error": "Nome do catálogo é obrigatório."}), 400
    if not exercicio:
        return jsonify({"error": "Informe um exercício válido (ex.: 2026)."}), 400
    if _see_catalogo_duplicado(nome, exercicio):
        return jsonify({"error": f"Já existe um catálogo com esse nome no exercício {exercicio}."}), 400
    catalogo = SeeCatalogo(
        nome=nome,
        exercicio=exercicio,
        descricao=(data.get("descricao") or "").strip() or None,
        ativo=True,
        criado_por=_current_usuario_id(),
        atualizado_por=_current_usuario_id(),
    )
    db.session.add(catalogo)
    db.session.commit()
    return jsonify({"ok": True, "catalogo": _see_catalog_payload(catalogo)}), 201


@notas_see_bp.route("/api/notas-see/catalogos/<int:catalogo_id>", methods=["PUT", "DELETE"])
@login_required
@require_feature("area-uens/sage/notas-see")
def api_notas_see_catalogo(catalogo_id: int):
    catalogo = db.session.get(SeeCatalogo, catalogo_id)
    if not catalogo:
        return jsonify({"error": "Catálogo não encontrado."}), 404
    if request.method == "DELETE":
        catalogo.ativo = False
    else:
        data = request.get_json() or {}
        nome = (data.get("nome") or "").strip()
        exercicio = _see_parse_exercicio(data.get("exercicio"))
        if not nome:
            return jsonify({"error": "Nome do catálogo é obrigatório."}), 400
        if not exercicio:
            return jsonify({"error": "Informe um exercício válido (ex.: 2026)."}), 400
        if _see_catalogo_duplicado(nome, exercicio, catalogo.id):
            return jsonify({"error": f"Já existe um catálogo com esse nome no exercício {exercicio}."}), 400
        catalogo.nome = nome
        catalogo.exercicio = exercicio
        catalogo.descricao = (data.get("descricao") or "").strip() or None
        catalogo.ativo = bool(data.get("ativo", catalogo.ativo))
    catalogo.atualizado_por = _current_usuario_id()
    db.session.commit()
    return jsonify({"ok": True, "catalogo": _see_catalog_payload(catalogo)})


@notas_see_bp.route("/api/notas-see/catalogos/<int:catalogo_id>/produtos", methods=["POST"])
@login_required
@require_feature("area-uens/sage/notas-see")
def api_notas_see_produto_criar(catalogo_id: int):
    catalogo = db.session.get(SeeCatalogo, catalogo_id)
    if not catalogo or not catalogo.ativo:
        return jsonify({"error": "Catálogo não encontrado ou inativo."}), 404
    data = request.get_json() or {}
    codigo = re.sub(r"\D", "", str(data.get("codigo") or ""))  # só dígitos: "936.360" vira "936360"
    nome = (data.get("nome") or "").strip()
    if not codigo or not nome:
        return jsonify({"error": "Informe o código (números) e o nome do produto."}), 400
    existing = SeeCatalogoProduto.query.filter_by(catalogo_id=catalogo_id, codigo=codigo).first()
    if existing:
        if existing.ativo:
            return jsonify({"error": "Este código já está cadastrado no catálogo."}), 400
        existing.nome = nome
        existing.observacao = (data.get("observacao") or "").strip() or None
        existing.ativo = True
        produto = existing
    else:
        produto = SeeCatalogoProduto(
            catalogo_id=catalogo_id,
            codigo=codigo,
            nome=nome,
            observacao=(data.get("observacao") or "").strip() or None,
            ativo=True,
        )
        db.session.add(produto)
    db.session.commit()
    return jsonify({"ok": True, "catalogo": _see_catalog_payload(catalogo)}), 201


@notas_see_bp.route("/api/notas-see/produtos/<int:produto_id>", methods=["PUT", "DELETE"])
@login_required
@require_feature("area-uens/sage/notas-see")
def api_notas_see_produto(produto_id: int):
    produto = db.session.get(SeeCatalogoProduto, produto_id)
    if not produto:
        return jsonify({"error": "Produto não encontrado."}), 404
    if request.method == "DELETE":
        produto.ativo = False
    else:
        data = request.get_json() or {}
        codigo = re.sub(r"\D", "", str(data.get("codigo") or ""))  # só dígitos: "936.360" vira "936360"
        nome = (data.get("nome") or "").strip()
        if not codigo or not nome:
            return jsonify({"error": "Informe o código (números) e o nome do produto."}), 400
        duplicate = SeeCatalogoProduto.query.filter(
            SeeCatalogoProduto.catalogo_id == produto.catalogo_id,
            SeeCatalogoProduto.codigo == codigo,
            SeeCatalogoProduto.id != produto.id,
            SeeCatalogoProduto.ativo == True,  # noqa: E712
        ).first()
        if duplicate:
            return jsonify({"error": "Este código já está cadastrado no catálogo."}), 400
        produto.codigo = codigo
        produto.nome = nome
        produto.observacao = (data.get("observacao") or "").strip() or None
    db.session.commit()
    catalogo = db.session.get(SeeCatalogo, produto.catalogo_id)
    return jsonify({"ok": True, "catalogo": _see_catalog_payload(catalogo)})


def _see_add_event(job: SeeProcessamento, message: str, arquivo_id: int | None = None) -> None:
    db.session.add(
        SeeProcessamentoEvento(
            processamento_id=job.id,
            arquivo_id=arquivo_id,
            tipo="progresso",
            estado=job.status,
            mensagem=message,
            progresso=job.progresso,
            arquivos_processados=job.arquivos_processados,
            arquivos_sucesso=job.arquivos_sucesso,
            arquivos_alerta=job.arquivos_alerta,
            arquivos_erro=job.arquivos_erro,
        )
    )


def _see_server_datetime_iso(value):
    """Serializa datas sem fuso do MySQL como horário do servidor."""
    if not value:
        return None
    if value.tzinfo is not None:
        return value.isoformat()
    timezone_name = os.getenv("SEE_SERVER_TIMEZONE", "America/Sao_Paulo")
    try:
        return pytz.timezone(timezone_name).localize(value).isoformat()
    except (pytz.UnknownTimeZoneError, ValueError):
        current_app.logger.warning("Fuso SEE_SERVER_TIMEZONE inválido: %s", timezone_name)
        return pytz.timezone("America/Sao_Paulo").localize(value).isoformat()


def _run_see_processamento(app, processamento_id: int) -> None:
    with app.app_context():
        started = datetime.utcnow()
        try:
            job = db.session.get(SeeProcessamento, processamento_id)
            if not job:
                return
            job.status = "processando"
            job.etapa_atual = "extração"
            job.mensagem_atual = "Processamento dos PDFs iniciado."
            job.iniciado_em = started
            job.progresso = 1
            _see_add_event(job, job.mensagem_atual)
            db.session.commit()
            product_rows = sorted(
                SeeCatalogoProduto.query.filter_by(catalogo_id=job.catalogo_id, ativo=True).all(),
                key=_see_produto_ordem,
            )
            products = [
                {"id": row.id, "codigo": row.codigo, "nome": row.nome}
                for row in product_rows
            ]
            escola_index = EscolaIndex.build([
                Escola(codigo=row.codigo, nome=row.nome, municipio=row.municipio, dre=row.dre, tipo=row.tipo, ativo=bool(row.ativo))
                for row in SeeEscola.query.all()
            ])
            files = (
                SeeProcessamentoArquivo.query
                .filter_by(processamento_id=job.id, status="enviado")
                .order_by(SeeProcessamentoArquivo.ordem)
                .all()
            )
            for index, file_row in enumerate(files, start=1):
                db.session.refresh(job)
                if job.cancelamento_solicitado:
                    job.status = "cancelado"
                    job.mensagem_atual = "Processamento cancelado pelo usuário."
                    break
                file_started = datetime.utcnow()
                file_row.status = "processando"
                file_row.etapa_atual = "extração do PDF"
                file_row.iniciado_em = file_started
                job.mensagem_atual = f"Processando {index} de {len(files)}: {file_row.nome_original}"
                db.session.commit()

                def page_progress(page: int, total: int) -> None:
                    file_row.pagina_atual = page
                    file_row.total_paginas = total
                    file_row.progresso = round((page / max(total, 1)) * 100, 2)
                    db.session.commit()

                try:
                    result = extract_see_pdf(Path(file_row.caminho_armazenado), products, page_progress)
                    metadata = result["metadata"]
                    warnings = list(result["warnings"])
                    cadastro = identificar_escola(metadata, escola_index)
                    if cadastro["codigo"] and not metadata.get("codigo_escola"):
                        # Código completado pelo cadastro: deixa de ser um campo ausente.
                        warnings = [item for item in warnings if item["codigo"] != "CAMPO_CODIGO_ESCOLA_AUSENTE"]
                    warnings += cadastro["warnings"]
                    metadata["cadastro"] = {
                        "origem_codigo": cadastro["origem_codigo"],
                        "destaque": cadastro["destaque"],
                        "conferencia": cadastro["conferencia"],
                    }
                    file_row.total_paginas = result["total_pages"]
                    file_row.metodo_extracao = result["method"]
                    file_row.chave_acesso = metadata.get("chave_acesso")
                    file_row.numero_danfe = metadata.get("numero_danfe")
                    file_row.dre = cadastro["dre"]
                    file_row.codigo_escola = cadastro["codigo"]
                    file_row.nome_escola = cadastro["nome"]
                    file_row.confianca_metadados = metadata.get("confianca")
                    file_row.dados_extraidos_json = metadata
                    for item in result["items"]:
                        db.session.add(
                            SeeItemExtraido(
                                processamento_id=job.id,
                                arquivo_id=file_row.id,
                                catalogo_produto_id=item.get("catalogo_produto_id"),
                                codigo_produto=item["codigo"],
                                nome_produto=item["nome"],
                                unidade=item.get("unidade"),
                                quantidade=item["quantidade"],
                                pagina=item.get("pagina"),
                                estrategia_extracao=item.get("estrategia"),
                                confianca=item.get("confianca"),
                                texto_origem=item.get("texto_origem"),
                            )
                        )
                    for warning in warnings:
                        db.session.add(
                            SeeOcorrencia(
                                processamento_id=job.id,
                                arquivo_id=file_row.id,
                                severidade="alerta",
                                fase="extração",
                                codigo=warning["codigo"],
                                mensagem=warning["mensagem"],
                            )
                        )
                    file_row.total_produtos = len(result["items"])
                    file_row.total_alertas = len(warnings)
                    file_row.status = "finalizado_com_alerta" if warnings else "finalizado"
                    file_row.progresso = 100
                    if warnings:
                        job.arquivos_alerta += 1
                    else:
                        job.arquivos_sucesso += 1
                except Exception as exc:
                    file_row.status = "falha"
                    file_row.total_erros = 1
                    file_row.erro_resumido = f"{type(exc).__name__}: {exc}"[:2000]
                    job.arquivos_erro += 1
                    db.session.add(
                        SeeOcorrencia(
                            processamento_id=job.id,
                            arquivo_id=file_row.id,
                            severidade="erro",
                            fase="extração",
                            codigo=type(exc).__name__,
                            mensagem=f"Falha ao processar {file_row.nome_original}.",
                            detalhe_tecnico=str(exc),
                        )
                    )
                file_row.finalizado_em = datetime.utcnow()
                file_row.duracao_segundos = (file_row.finalizado_em - file_started).total_seconds()
                job.arquivos_processados += 1
                job.progresso = round(5 + (job.arquivos_processados / max(job.total_arquivos, 1)) * 85, 2)
                _see_add_event(job, job.mensagem_atual, file_row.id)
                db.session.commit()

            if job.status != "cancelado":
                job.status = "gerando_excel"
                job.etapa_atual = "geração do Excel"
                job.mensagem_atual = "Gerando arquivo Excel."
                job.progresso = 92
                db.session.commit()
                file_rows = SeeProcessamentoArquivo.query.filter_by(processamento_id=job.id).all()
                item_rows = SeeItemExtraido.query.filter_by(processamento_id=job.id).all()
                occurrence_rows = SeeOcorrencia.query.filter_by(processamento_id=job.id).all()
                file_map = {row.id: row for row in file_rows}

                def cadastro_info(row) -> dict:
                    data = row.dados_extraidos_json or {}
                    info = data.get("cadastro") or {}
                    return {
                        "escola_nota": data.get("nome_escola"),
                        "origem_codigo": info.get("origem_codigo"),
                        "destaque": info.get("destaque"),
                        "conferencia": info.get("conferencia"),
                    }

                output = SEE_OUTPUT_DIR / f"notas_see_{job.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                generate_see_xlsx(
                    output,
                    [{"Arquivo PDF": row.nome_original, "Status": row.status, "DANFE": row.numero_danfe, "DRE": row.dre, "Código Escola": row.codigo_escola, "Escola": row.nome_escola, "Escola (nota)": cadastro_info(row)["escola_nota"], "Origem do código": cadastro_info(row)["origem_codigo"], "Conferência": cadastro_info(row)["conferencia"], "Produtos": row.total_produtos, "Alertas": row.total_alertas, "Erros": row.total_erros} for row in file_rows],
                    [{"arquivo": file_map[row.arquivo_id].nome_original, "dre": file_map[row.arquivo_id].dre, "codigo_escola": file_map[row.arquivo_id].codigo_escola, "nome_escola": file_map[row.arquivo_id].nome_escola, "numero_danfe": file_map[row.arquivo_id].numero_danfe, "codigo": row.codigo_produto, "nome": row.nome_produto, "quantidade": row.quantidade, **cadastro_info(file_map[row.arquivo_id])} for row in item_rows],
                    [{"Arquivo PDF": file_map[row.arquivo_id].nome_original if row.arquivo_id in file_map else "", "Severidade": row.severidade, "Fase": row.fase, "Código": row.codigo, "Mensagem": row.mensagem, "Detalhe": row.detalhe_tecnico} for row in occurrence_rows],
                    products,
                )
                job.nome_arquivo_saida = output.name
                job.caminho_arquivo_saida = str(output.resolve())
                job.tamanho_arquivo_saida = output.stat().st_size
                job.status = "finalizado_com_alertas" if job.arquivos_alerta or job.arquivos_erro else "finalizado"
                job.etapa_atual = "concluído"
                job.mensagem_atual = "Processamento concluído."
                job.progresso = 100
                _see_limpar_pdfs(job)
            job.finalizado_em = datetime.utcnow()
            job.duracao_segundos = (job.finalizado_em - started).total_seconds()
            _see_add_event(job, job.mensagem_atual)
            db.session.commit()
        except Exception as exc:
            db.session.rollback()
            job = db.session.get(SeeProcessamento, processamento_id)
            if job:
                job.status = "falha"
                job.erro_geral = f"{type(exc).__name__}: {exc}"[:2000]
                job.mensagem_atual = "Falha geral no processamento."
                job.finalizado_em = datetime.utcnow()
                job.duracao_segundos = (job.finalizado_em - started).total_seconds()
                db.session.commit()
            current_app.logger.exception("Falha no processamento Notas SEE %s", processamento_id)
        finally:
            db.session.remove()


def _start_see_worker(job: SeeProcessamento) -> None:
    worker_path = Path(__file__).resolve().parents[1] / "worker.py"
    log_dir = Path("outputs") / "status"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_handle = open(log_dir / f"worker_see_{job.id}.log", "a", encoding="utf-8")
    creationflags = 0
    if sys.platform.startswith("win"):
        creationflags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(
            subprocess, "CREATE_NEW_PROCESS_GROUP", 0
        )
    try:
        process = subprocess.Popen(
            [sys.executable, str(worker_path), "--kind", "see", "--upload-id", str(job.id)],
            cwd=str(worker_path.parent),
            stdout=log_handle,
            stderr=log_handle,
            creationflags=creationflags,
        )
        job.worker_pid = process.pid
        db.session.commit()
    except Exception:
        log_handle.close()
        current_app.logger.exception("Falha ao iniciar worker SEE; usando thread.")
        app = current_app._get_current_object()
        threading.Thread(target=_run_see_processamento, args=(app, job.id), daemon=True).start()


@notas_see_bp.route("/api/notas-see/processamentos", methods=["POST"])
@login_required
@require_feature("area-uens/sage/notas-see")
def api_notas_see_processar():
    try:
        catalogo_id = int(request.form.get("catalogo_id") or 0)
    except ValueError:
        catalogo_id = 0
    catalogo = db.session.get(SeeCatalogo, catalogo_id)
    if not catalogo or not catalogo.ativo:
        return jsonify({"error": "Selecione um catálogo ativo."}), 400
    if not SeeCatalogoProduto.query.filter_by(catalogo_id=catalogo_id, ativo=True).first():
        return jsonify({"error": "O catálogo selecionado não possui produtos ativos."}), 400
    append_mode = (request.form.get("append_mode") or "novo").strip().lower()
    try:
        base_job_id = int(request.form.get("base_job_id") or 0)
    except ValueError:
        base_job_id = 0
    base_job = None
    base_files = []
    if append_mode == "acrescentar":
        base_job = db.session.get(SeeProcessamento, base_job_id)
        terminal_statuses = {"finalizado", "finalizado_com_alertas"}
        if (
            not base_job
            or base_job.usuario_id != _current_usuario_id()
            or base_job.catalogo_id != catalogo_id
            or base_job.status not in terminal_statuses
        ):
            return jsonify({"error": "O processamento anterior não pode ser atualizado."}), 400
        base_files = (
            SeeProcessamentoArquivo.query
            .filter_by(processamento_id=base_job.id)
            .order_by(SeeProcessamentoArquivo.ordem)
            .all()
        )
    user = session.get("user") or {}
    job = SeeProcessamento(
        usuario_id=_current_usuario_id(),
        user_email=(user.get("email") or "desconhecido"),
        catalogo_id=catalogo_id,
        status="enviando",
        etapa_atual="upload",
        total_arquivos=0,
    )
    db.session.add(job)
    db.session.flush()
    inherited = 0
    inherited_file_map = {}
    for old_file in base_files:
        copied_file = SeeProcessamentoArquivo(
            processamento_id=job.id,
            ordem=old_file.ordem,
            nome_original=old_file.nome_original,
            caminho_relativo=old_file.caminho_relativo,
            nome_armazenado=old_file.nome_armazenado,
            caminho_armazenado=old_file.caminho_armazenado,
            mime_type=old_file.mime_type,
            tamanho_bytes=old_file.tamanho_bytes,
            sha256=old_file.sha256,
            status=old_file.status,
            etapa_atual="herdado",
            progresso=100,
            total_paginas=old_file.total_paginas,
            pagina_atual=old_file.pagina_atual,
            metodo_extracao=old_file.metodo_extracao,
            chave_acesso=old_file.chave_acesso,
            numero_danfe=old_file.numero_danfe,
            dre=old_file.dre,
            codigo_escola=old_file.codigo_escola,
            nome_escola=old_file.nome_escola,
            confianca_metadados=old_file.confianca_metadados,
            total_produtos=old_file.total_produtos,
            total_alertas=old_file.total_alertas,
            total_erros=old_file.total_erros,
            dados_extraidos_json=old_file.dados_extraidos_json,
            erro_resumido=old_file.erro_resumido,
            iniciado_em=old_file.iniciado_em,
            finalizado_em=old_file.finalizado_em,
            duracao_segundos=old_file.duracao_segundos,
        )
        db.session.add(copied_file)
        db.session.flush()
        inherited_file_map[old_file.id] = copied_file.id
        inherited += 1
    if base_job:
        for old_item in SeeItemExtraido.query.filter_by(processamento_id=base_job.id).all():
            if old_item.arquivo_id not in inherited_file_map:
                continue
            db.session.add(SeeItemExtraido(
                processamento_id=job.id, arquivo_id=inherited_file_map[old_item.arquivo_id],
                catalogo_produto_id=old_item.catalogo_produto_id, codigo_produto=old_item.codigo_produto,
                nome_produto=old_item.nome_produto, unidade=old_item.unidade, quantidade=old_item.quantidade,
                pagina=old_item.pagina, linha_referencia=old_item.linha_referencia,
                estrategia_extracao=old_item.estrategia_extracao, confianca=old_item.confianca,
                texto_origem=old_item.texto_origem, revisado=old_item.revisado,
                corrigido_manualmente=old_item.corrigido_manualmente, valor_original=old_item.valor_original,
            ))
        for old_occurrence in SeeOcorrencia.query.filter_by(processamento_id=base_job.id).all():
            if old_occurrence.codigo == "ARQUIVO_IGNORADO":
                continue
            mapped_file_id = inherited_file_map.get(old_occurrence.arquivo_id)
            if old_occurrence.arquivo_id and not mapped_file_id:
                continue
            db.session.add(SeeOcorrencia(
                processamento_id=job.id, arquivo_id=mapped_file_id, severidade=old_occurrence.severidade,
                fase=old_occurrence.fase, codigo=old_occurrence.codigo, mensagem=old_occurrence.mensagem,
                detalhe_tecnico=old_occurrence.detalhe_tecnico, pagina=old_occurrence.pagina,
                contexto=old_occurrence.contexto, dados_contexto_json=old_occurrence.dados_contexto_json,
                resolvido=old_occurrence.resolvido, resolvido_por=old_occurrence.resolvido_por,
                resolvido_em=old_occurrence.resolvido_em, observacao_resolucao=old_occurrence.observacao_resolucao,
            ))
    job.total_arquivos = inherited
    job.arquivos_enviados = 0
    job.arquivos_processados = inherited
    job.arquivos_sucesso = sum(1 for row in base_files if row.status == "finalizado")
    job.arquivos_alerta = sum(1 for row in base_files if row.status == "finalizado_com_alerta")
    job.arquivos_erro = sum(1 for row in base_files if row.status == "falha")
    job.mensagem_atual = "Recebendo PDFs."
    db.session.commit()
    return jsonify({"ok": True, "job_id": job.id, "initial": inherited}), 201


def _see_upload_job(processamento_id: int):
    """Retorna o processamento em fase de upload do usuário atual, ou uma resposta de erro."""
    job = db.session.get(SeeProcessamento, processamento_id)
    if not job or job.usuario_id != _current_usuario_id():
        return None, (jsonify({"error": "Processamento não encontrado."}), 404)
    if job.status != "enviando":
        return None, (jsonify({"error": "Este processamento não está mais recebendo arquivos."}), 409)
    return job, None


def _see_limpar_pdfs(job: SeeProcessamento) -> None:
    """Apaga os PDFs de um processamento concluído (ficam o Excel e os dados extraídos).

    Os PDFs só servem para a extração: o "acrescentar" herda as linhas já extraídas e não
    relê arquivos. Sem isso, centenas de PDFs por bimestre se acumulam no servidor (o cron
    de limpeza só varre .xlsx). Para manter, defina SEE_MANTER_PDFS=1."""
    if os.getenv("SEE_MANTER_PDFS", "").strip() in {"1", "true", "TRUE", "sim"}:
        return
    try:
        shutil.rmtree(SEE_UPLOAD_DIR / str(job.id), ignore_errors=True)
    except Exception:
        current_app.logger.warning("Notas SEE: não foi possível apagar os PDFs do processamento %s", job.id)


def _see_discard_upload(job: SeeProcessamento, message: str) -> None:
    job.status = "descartado"
    job.mensagem_atual = message
    job.finalizado_em = datetime.utcnow()
    db.session.commit()
    shutil.rmtree(SEE_UPLOAD_DIR / str(job.id), ignore_errors=True)


@notas_see_bp.route("/api/notas-see/processamentos/<int:processamento_id>/arquivos", methods=["POST"])
@login_required
@require_feature("area-uens/sage/notas-see")
def api_notas_see_enviar_arquivos(processamento_id: int):
    job, error = _see_upload_job(processamento_id)
    if error:
        return error
    uploads = [file for file in request.files.getlist("pdfs") if file and file.filename]
    if not uploads:
        return jsonify({"error": "Nenhum arquivo recebido neste lote."}), 400
    existing = (
        db.session.query(SeeProcessamentoArquivo.sha256, SeeProcessamentoArquivo.etapa_atual, SeeProcessamentoArquivo.ordem)
        .filter(SeeProcessamentoArquivo.processamento_id == job.id)
        .all()
    )
    base_hashes = {sha for sha, etapa, _ in existing if sha and etapa == "herdado"}
    seen_hashes = {sha for sha, _, _ in existing if sha}
    order = max((ordem or 0 for _, _, ordem in existing), default=0)
    upload_dir = SEE_UPLOAD_DIR / str(job.id)
    upload_dir.mkdir(parents=True, exist_ok=True)
    saved = 0
    ignored = []
    accepted_names = []
    for upload in uploads:
        original = upload.filename.replace("\\", "/")
        display_name = Path(original).name
        if not original.lower().endswith(".pdf"):
            ignored.append({"name": display_name, "reason": "Extensão diferente de PDF."})
            continue
        data = upload.read()
        if not data.startswith(b"%PDF"):
            ignored.append({"name": display_name, "reason": "O conteúdo do arquivo não é um PDF válido."})
            continue
        digest = hashlib.sha256(data).hexdigest()
        if digest in seen_hashes:
            reason = "Arquivo já processado anteriormente." if digest in base_hashes else "Arquivo duplicado na seleção atual."
            ignored.append({"name": display_name, "reason": reason})
            continue
        seen_hashes.add(digest)
        order += 1
        stored = f"{order:05d}_{uuid.uuid4().hex}.pdf"
        path = upload_dir / stored
        path.write_bytes(data)
        db.session.add(
            SeeProcessamentoArquivo(
                processamento_id=job.id,
                ordem=order,
                nome_original=display_name,
                caminho_relativo=original,
                nome_armazenado=stored,
                caminho_armazenado=str(path.resolve()),
                mime_type=upload.mimetype or "application/pdf",
                tamanho_bytes=len(data),
                sha256=digest,
                status="enviado",
            )
        )
        saved += 1
        accepted_names.append(display_name)
    for item in ignored:
        db.session.add(SeeOcorrencia(
            processamento_id=job.id,
            severidade="informacao",
            fase="upload",
            codigo="ARQUIVO_IGNORADO",
            mensagem=f"{item['name']}: {item['reason']}",
            contexto=item["name"],
            dados_contexto_json=item,
        ))
    job.arquivos_enviados = (job.arquivos_enviados or 0) + saved
    job.mensagem_atual = f"{job.arquivos_enviados} PDF(s) recebido(s)."
    db.session.commit()
    return jsonify({"ok": True, "added": saved, "accepted": accepted_names, "ignored": ignored})


@notas_see_bp.route("/api/notas-see/processamentos/<int:processamento_id>/iniciar", methods=["POST"])
@login_required
@require_feature("area-uens/sage/notas-see")
def api_notas_see_iniciar(processamento_id: int):
    job, error = _see_upload_job(processamento_id)
    if error:
        return error
    if not pdfplumber_disponivel():
        message = "A leitura de PDFs não está disponível neste servidor (biblioteca pdfplumber não instalada). Avise o NGER."
        _see_discard_upload(job, message)
        return jsonify({"error": message}), 503
    saved = SeeProcessamentoArquivo.query.filter_by(processamento_id=job.id, status="enviado").count()
    inherited = SeeProcessamentoArquivo.query.filter_by(processamento_id=job.id, etapa_atual="herdado").count()
    ignored = [
        row.dados_contexto_json or {"name": row.contexto, "reason": row.mensagem}
        for row in SeeOcorrencia.query.filter_by(processamento_id=job.id, codigo="ARQUIVO_IGNORADO").all()
    ]
    if not saved:
        details = "; ".join(f"{item.get('name')}: {item.get('reason')}" for item in ignored[:5])
        suffix = f"; e mais {len(ignored) - 5}" if len(ignored) > 5 else ""
        message = f"Nenhum arquivo novo para processar. {details}{suffix}" if details else "Nenhum PDF válido foi recebido."
        _see_discard_upload(job, message)
        return jsonify({"error": message, "ignored_count": len(ignored)}), 400
    job.total_arquivos = inherited + saved
    job.arquivos_enviados = saved
    job.status = "na_fila"
    job.etapa_atual = "fila"
    job.mensagem_atual = (
        f"Atualização iniciada: {inherited} anterior(es) + {saved} novo(s), {len(ignored)} ignorado(s)."
        if inherited else f"{saved} PDF(s) recebido(s), {len(ignored)} ignorado(s)."
    )
    db.session.commit()
    _start_see_worker(job)
    return jsonify({"ok": True, "job_id": job.id, "message": job.mensagem_atual, "initial": inherited, "added": saved, "total": inherited + saved, "ignored_count": len(ignored)}), 202


@notas_see_bp.route("/api/notas-see/processamentos/<int:processamento_id>", methods=["GET"])
@login_required
@require_feature("area-uens/sage/notas-see")
def api_notas_see_status(processamento_id: int):
    job = db.session.get(SeeProcessamento, processamento_id)
    if not job:
        return jsonify({"error": "Processamento não encontrado."}), 404
    files = SeeProcessamentoArquivo.query.filter_by(processamento_id=job.id).order_by(SeeProcessamentoArquivo.ordem).all()
    ignored_occurrences = SeeOcorrencia.query.filter_by(processamento_id=job.id, codigo="ARQUIVO_IGNORADO").all()
    ignored_files = [
        {
            "name": (row.dados_contexto_json or {}).get("name") or row.contexto or "Arquivo",
            "reason": (row.dados_contexto_json or {}).get("reason") or row.mensagem,
        }
        for row in ignored_occurrences
    ]
    catalog = db.session.get(SeeCatalogo, job.catalogo_id)
    inherited = sum(1 for row in files if row.etapa_atual == "herdado")
    duration = float(job.duracao_segundos or 0)
    if not duration and job.iniciado_em:
        duration = max(0, (datetime.utcnow() - job.iniciado_em).total_seconds())
    return jsonify({
        "id": job.id,
        "catalog_id": job.catalogo_id,
        "catalog_name": catalog.nome if catalog else "Catálogo removido",
        "executed_by": job.user_email,
        "created_at": _see_server_datetime_iso(job.created_at),
        "initial_count": inherited,
        "added_count": max(0, job.total_arquivos - inherited),
        "ignored_count": len(ignored_files),
        "status": job.status,
        "etapa": job.etapa_atual,
        "message": job.mensagem_atual,
        "progress": float(job.progresso or 0),
        "total": job.total_arquivos,
        "processed": job.arquivos_processados,
        "success": job.arquivos_sucesso,
        "warnings": job.arquivos_alerta,
        "errors": job.arquivos_erro,
        "duration": duration,
        "download_ready": bool(job.caminho_arquivo_saida),
        "files": [
            {"name": row.nome_original, "status": row.status, "progress": float(row.progresso or 0), "error": row.erro_resumido or "", "warnings": row.total_alertas}
            for row in files
        ] + [
            {"name": item["name"], "status": "ignorado", "progress": 0, "error": item["reason"], "warnings": 0}
            for item in ignored_files
        ],
    })


@notas_see_bp.route("/api/notas-see/processamentos/ultimo", methods=["GET"])
@login_required
@require_feature("area-uens/sage/notas-see")
def api_notas_see_ultimo_processamento():
    try:
        catalogo_id = int(request.args.get("catalogo_id") or 0)
    except ValueError:
        catalogo_id = 0
    if not catalogo_id:
        return jsonify({"job_id": None})
    job = (
        SeeProcessamento.query
        .filter(
            SeeProcessamento.catalogo_id == catalogo_id,
            SeeProcessamento.status.notin_(["enviando", "descartado"]),
        )
        .order_by(SeeProcessamento.id.desc())
        .first()
    )
    return jsonify({"job_id": job.id if job else None})


@notas_see_bp.route("/api/notas-see/processamentos/historico", methods=["GET"])
@login_required
@require_feature("area-uens/sage/notas-see")
def api_notas_see_historico():
    try:
        catalogo_id = int(request.args.get("catalogo_id") or 0)
    except ValueError:
        catalogo_id = 0
    exercicio = _see_parse_exercicio(request.args.get("exercicio"))
    # Cada atualização gera um novo processamento acumulado; só o mais recente de cada
    # catálogo ativo (a versão atual) é exibido.
    latest_ids = (
        db.session.query(func.max(SeeProcessamento.id))
        .join(SeeCatalogo, SeeCatalogo.id == SeeProcessamento.catalogo_id)
        .filter(
            SeeCatalogo.ativo == True,  # noqa: E712
            SeeProcessamento.status.in_(["finalizado", "finalizado_com_alertas"]),
            SeeProcessamento.caminho_arquivo_saida.isnot(None),
        )
        .group_by(SeeProcessamento.catalogo_id)
    )
    if catalogo_id:
        latest_ids = latest_ids.filter(SeeProcessamento.catalogo_id == catalogo_id)
    if exercicio:
        latest_ids = latest_ids.filter(SeeCatalogo.exercicio == exercicio)
    jobs = (
        SeeProcessamento.query
        .filter(SeeProcessamento.id.in_(latest_ids.scalar_subquery()))
        .order_by(SeeProcessamento.id.desc())
        .all()
    )
    catalogs_by_id = {
        row.id: row
        for row in SeeCatalogo.query.filter(SeeCatalogo.id.in_({job.catalogo_id for job in jobs})).all()
    } if jobs else {}
    items = []
    for job in jobs:
        items.append({
            "id": job.id,
            "catalog_id": job.catalogo_id,
            "catalog_name": catalogs_by_id[job.catalogo_id].nome if job.catalogo_id in catalogs_by_id else "Catálogo removido",
            "exercicio": catalogs_by_id[job.catalogo_id].exercicio if job.catalogo_id in catalogs_by_id else None,
            "executed_by": job.user_email,
            "created_at": _see_server_datetime_iso(job.created_at),
            "status": job.status,
            "total": job.total_arquivos,
            "success": job.arquivos_sucesso,
            "warnings": job.arquivos_alerta,
            "errors": job.arquivos_erro,
        })
    return jsonify({"processamentos": items})


@notas_see_bp.route("/api/notas-see/processamentos/<int:processamento_id>/cancelar", methods=["POST"])
@login_required
@require_feature("area-uens/sage/notas-see")
def api_notas_see_cancelar(processamento_id: int):
    job = db.session.get(SeeProcessamento, processamento_id)
    if not job:
        return jsonify({"error": "Processamento não encontrado."}), 404
    if job.status == "enviando":
        _see_discard_upload(job, "Envio cancelado.")
        return jsonify({"ok": True, "message": "Envio cancelado."})
    job.cancelamento_solicitado = True
    db.session.commit()
    return jsonify({"ok": True, "message": "Cancelamento solicitado."})


@notas_see_bp.route("/api/notas-see/processamentos/<int:processamento_id>/download")
@login_required
@require_feature("area-uens/sage/notas-see")
def api_notas_see_download(processamento_id: int):
    job = db.session.get(SeeProcessamento, processamento_id)
    if not job or not job.caminho_arquivo_saida:
        return jsonify({"error": "Arquivo de saída não disponível."}), 404
    target = Path(job.caminho_arquivo_saida).resolve()
    output_root = SEE_OUTPUT_DIR.resolve()
    if output_root not in target.parents or not target.exists():
        return jsonify({"error": "Arquivo de saída não encontrado."}), 404
    return send_file(target, as_attachment=True, download_name=job.nome_arquivo_saida or target.name)
