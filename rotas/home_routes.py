from flask import Blueprint, jsonify, render_template, request, abort, g, session, send_file, current_app
from functools import wraps
from datetime import datetime, timedelta
from decimal import Decimal
import os
from io import BytesIO
import json
import unicodedata
import subprocess
import sys
import threading
import pandas as pd
from models import (
    Usuario,
    Perfil,
    PerfilPermissao,
    Fip613Upload,
    Fip613Registro,
    Plan20Upload,
    PedUpload,
    PedRegistro,
    EmpUpload,
    EmpRegistro,
    EstEmpUpload,
    EstEmpRegistro,
    NobUpload,
    NobRegistro,
    Plan21Nger,
    Adj,
    Dotacao,
    ActiveSession,
    db,
)
from sqlalchemy.exc import ProgrammingError, IntegrityError
from services.auth import login_required, role_required, current_user
from services.features import FEATURES, flatten_features, build_parent_map
from services.fip613_runner import run_fip613, UPLOAD_DIR
from services.plan20_runner import run_plan20
from services.ped_runner import (
    run_ped,
    move_existing_to_tmp,
    INPUT_DIR as PED_UPLOAD_DIR,
    OUTPUT_DIR as PED_OUTPUT_DIR,
)
from services.est_emp_runner import (
    run_est_emp,
    INPUT_DIR as EST_EMP_UPLOAD_DIR,
    OUTPUT_DIR as EST_EMP_OUTPUT_DIR,
    move_existing_to_tmp as move_est_emp_existing_to_tmp,
)
from services.job_status import read_status, set_cancel_flag, update_status_fields, write_status
from pathlib import Path
from sqlalchemy import text, func

home_bp = Blueprint("home", __name__)

EMP_UPLOAD_DIR = Path("upload/emp")
EMP_OUTPUT_DIR = Path("outputs/td_emp")
NOB_UPLOAD_DIR = Path("upload/nob")
NOB_OUTPUT_DIR = Path("outputs/td_nob")
NODE_RUNNER = Path(__file__).resolve().parents[1] / "node_runners" / "run.js"
NODE_EXE = os.getenv("NODE_EXE", "node")


def _find_upload_path(base_dir: Path, stored_filename: str) -> Path | None:
    if not stored_filename:
        return None
    candidate = base_dir / stored_filename
    if candidate.exists():
        return candidate
    tmp_dir = base_dir / "tmp"
    if not tmp_dir.exists():
        return None
    stem = Path(stored_filename).stem
    matches = sorted(tmp_dir.glob(f"{stem}_*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _run_node(kind: str, file_path: Path, user_email: str, data_arquivo, upload_id: int) -> dict:
    args = [
        NODE_EXE,
        str(NODE_RUNNER),
        "--kind",
        kind,
        "--file",
        str(file_path),
        "--upload-id",
        str(upload_id),
        "--user-email",
        user_email or "desconhecido",
    ]
    if data_arquivo:
        try:
            args.extend(["--data-arquivo", data_arquivo.isoformat()])
        except Exception:
            args.extend(["--data-arquivo", str(data_arquivo)])
    proc = subprocess.run(args, capture_output=True, text=True, cwd=str(NODE_RUNNER.parent))
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"Node runner falhou: {err or 'erro desconhecido'}")
    raw = (proc.stdout or "").strip()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Resposta invalida do Node: {exc}") from exc
    if not payload.get("ok"):
        raise RuntimeError(f"Node runner falhou: {payload.get('error')}")
    return payload


def _move_existing_to_tmp(base_dir: Path) -> None:
    tmp = base_dir / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    for f in base_dir.glob("*.xlsx"):
        dest = tmp / f"{f.stem}_{datetime.now().strftime('%Y%m%d%H%M%S')}{f.suffix}"
        try:
            f.rename(dest)
        except OSError:
            pass


def _next_pk(model) -> int:
    max_id = db.session.query(func.max(model.id)).scalar() or 0
    return int(max_id) + 1


def _process_emp_upload(upload_id: int) -> None:
    registro = db.session.get(EmpUpload, upload_id)
    if not registro:
        raise RuntimeError(f"Upload EMP nao encontrado: {upload_id}")
    file_path = _find_upload_path(EMP_UPLOAD_DIR, registro.stored_filename)
    if not file_path:
        raise RuntimeError(f"Arquivo EMP nao encontrado: {EMP_UPLOAD_DIR / registro.stored_filename}")
    payload = _run_node("emp", file_path, registro.user_email, registro.data_arquivo, registro.id)
    registro.output_filename = str(payload.get("output_filename") or "")
    db.session.commit()
    write_status(
        "emp",
        upload_id,
        "processamento finalizado",
        f"Processado com sucesso. Registros: {payload.get('total')}.",
        payload.get("output_filename"),
        progress=100,
    )


def _process_nob_upload(upload_id: int) -> None:
    registro = db.session.get(NobUpload, upload_id)
    if not registro:
        raise RuntimeError(f"Upload NOB nao encontrado: {upload_id}")
    file_path = _find_upload_path(NOB_UPLOAD_DIR, registro.stored_filename)
    if not file_path:
        raise RuntimeError(f"Arquivo NOB nao encontrado: {NOB_UPLOAD_DIR / registro.stored_filename}")
    payload = _run_node("nob", file_path, registro.user_email, registro.data_arquivo, registro.id)
    registro.output_filename = str(payload.get("output_filename") or "")
    db.session.commit()
    write_status(
        "nob",
        upload_id,
        "processamento finalizado",
        f"Processado com sucesso. Registros: {payload.get('total')}.",
        payload.get("output_filename"),
        progress=100,
    )


def _start_thread(kind: str, upload_id: int) -> None:
    app = current_app._get_current_object()

    def _runner() -> None:
        with app.app_context():
            try:
                write_status(
                    kind,
                    upload_id,
                    "em processamento",
                    "Processamento iniciado (thread).",
                    progress=0,
                )
                if kind == "emp":
                    _process_emp_upload(upload_id)
                else:
                    _process_nob_upload(upload_id)
            except Exception as exc:
                msg = f"{type(exc).__name__}: {exc}"
                if "PROCESSAMENTO_CANCELADO" in msg:
                    write_status(kind, upload_id, "processamento cancelado", "Cancelado pelo usuario.")
                else:
                    write_status(kind, upload_id, "falha no processamento", msg)
            finally:
                db.session.remove()

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()


def _start_worker(kind: str, upload_id: int) -> None:
    worker_path = Path(__file__).resolve().parents[1] / "worker.py"
    creationflags = 0
    if sys.platform.startswith("win"):
        creationflags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(
            subprocess, "CREATE_NEW_PROCESS_GROUP", 0
        )
    try:
        log_dir = Path("outputs") / "status"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"worker_{kind}_{upload_id}.log"
        log_handle = open(log_path, "a", encoding="utf-8")
        proc = subprocess.Popen(
            [sys.executable, str(worker_path), "--kind", kind, "--upload-id", str(upload_id)],
            cwd=str(worker_path.parent),
            stdout=log_handle,
            stderr=log_handle,
            creationflags=creationflags,
        )
        update_status_fields(
            kind,
            upload_id,
            message="Worker externo iniciado.",
            pid=proc.pid,
        )
    except Exception as exc:
        update_status_fields(
            kind,
            upload_id,
            message=f"Falha ao iniciar worker externo ({type(exc).__name__}: {exc}). Usando thread.",
        )
        _start_thread(kind, upload_id)


@home_bp.route("/")
@login_required
def index():
    # initial_content tells JS which partial to load first
    allowed = _permissoes_with_parents(getattr(g, "user_perfil_id", None))
    return render_template("base.html", initial_content="dashboard", initial_features=allowed)


@home_bp.route("/partial/dashboard")
@login_required
def partial_dashboard():
    nivel_raw = getattr(g, "user_nivel", 99)
    try:
        nivel_int = int(nivel_raw)
    except (TypeError, ValueError):
        nivel_int = 99
    can_view_sessions = nivel_int in (1, 2)
    active_sessions = []
    if can_view_sessions:
        cutoff = datetime.utcnow() - timedelta(hours=2)
        sessions = (
            ActiveSession.query.filter(ActiveSession.last_activity >= cutoff)
            .order_by(ActiveSession.last_activity.desc())
            .all()
        )
        emails = [s.email for s in sessions]
        usuarios = {u.email: u.nome for u in Usuario.query.filter(Usuario.email.in_(emails)).all()}
        for s in sessions:
            active_sessions.append(
                {
                    "email": s.email,
                    "nome": usuarios.get(s.email, s.email),
                    "last_activity": s.last_activity,
                }
            )
    return render_template(
        "partials/dashboard.html",
        can_view_sessions=can_view_sessions,
        active_sessions=active_sessions,
    )


def ensure_admin_nivel1():
    nivel = getattr(g, "user_nivel", None)
    if nivel != 1:
        abort(403)

def has_permission(feature: str) -> bool:
    if getattr(g, "user_nivel", None) == 1:
        return True
    perfil_id = getattr(g, "user_perfil_id", None)
    if not perfil_id:
        return False
    try:
        exists = (
            db.session.query(PerfilPermissao.id)
            .filter(PerfilPermissao.perfil_id == perfil_id, PerfilPermissao.feature == feature)
            .first()
        )
        return bool(exists)
    except ProgrammingError:
        db.session.rollback()
        return False


def _permissoes_with_parents(perfil_id: int | None):
    locked = [f["id"] for f in FEATURES if f.get("locked")]
    if perfil_id is None:
        return locked
    parent_map = build_parent_map()
    try:
        feats = []
        for pp in (
            PerfilPermissao.query.filter(
                PerfilPermissao.perfil_id == perfil_id,
                PerfilPermissao.ativo == True,  # noqa: E712
                PerfilPermissao.feature.isnot(None),
            ).all()
            or []
        ):
            feat_id = getattr(pp, "feature", None)
            if feat_id:
                feats.append(feat_id)
    except ProgrammingError:
        db.session.rollback()
        feats = []
    # include parents of children
    for feat in list(feats):
        parent = parent_map.get(feat)
        if parent and parent not in feats:
            feats.append(parent)
    # add locked always
    for l in locked:
        if l not in feats:
            feats.append(l)
    return feats


def require_feature(feature_id):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if getattr(g, "user_nivel", None) == 1:
                return view(*args, **kwargs)
            if has_permission(feature_id):
                return view(*args, **kwargs)
            abort(403)

        return wrapped

    return decorator


def _perfil_by_nome(nome: str | None):
    if not nome:
        return None
    nome_strip = nome.strip()
    return (
        Perfil.query.filter(
            func.lower(func.ltrim(func.rtrim(Perfil.nome))) == nome_strip.lower()
        ).first()
        or Perfil.query.filter(Perfil.nome.ilike(nome_strip)).first()
    )


def _is_nivel1(perfil_nome: str | None) -> bool:
    perfil_row = _perfil_by_nome(perfil_nome)
    if perfil_row:
        return perfil_row.nivel == 1
    # fallback se nao achar registro (ex: nome antigo hardcoded)
    return (perfil_nome or "").lower() == "admin"


@home_bp.route("/partial/usuarios")
@login_required
@require_feature("usuarios")
def partial_usuarios():
    return partial_usuarios_cadastrar()


@home_bp.route("/partial/usuarios/cadastrar")
@login_required
@require_feature("usuarios/cadastrar")
def partial_usuarios_cadastrar():
    usuarios = Usuario.query.order_by(Usuario.nome).all()
    perfis_query = Perfil.query.filter_by(ativo=True)
    caller_nivel = getattr(g, "user_nivel", None)
    # Somente nivel 1 enxerga perfis de nivel 1 (admin)
    if caller_nivel != 1:
        perfis_query = perfis_query.filter(Perfil.nivel != 1)
    perfis = perfis_query.order_by(Perfil.nivel, Perfil.nome).all()
    return render_template("partials/usuarios_cadastrar.html", usuarios=usuarios, perfis=perfis)


@home_bp.route("/partial/usuarios/editar")
@login_required
@require_feature("usuarios/editar")
def partial_usuarios_editar():
    usuarios = Usuario.query.order_by(Usuario.nome).all()
    perfis_query = Perfil.query.filter_by(ativo=True)
    caller_nivel = getattr(g, "user_nivel", None)
    if caller_nivel != 1:
        perfis_query = perfis_query.filter(Perfil.nivel != 1)
    perfis = perfis_query.order_by(Perfil.nivel, Perfil.nome).all()
    return render_template("partials/usuarios_editar.html", usuarios=usuarios, perfis=perfis)


@home_bp.route("/partial/usuarios/perfil")
@login_required
@role_required("admin")
def partial_usuarios_perfil():
    perfis = Perfil.query.order_by(Perfil.nivel, Perfil.nome).all()
    return render_template("partials/usuarios_perfil.html", perfis=perfis)


@home_bp.route("/partial/usuarios/senha")
@login_required
@require_feature("usuarios/senha")
def partial_usuarios_senha():
    return render_template("partials/usuarios_senha.html")


@home_bp.route("/partial/painel")
@login_required
def partial_painel():
    if not has_permission("painel"):
        abort(403)
    perfis = Perfil.query.order_by(Perfil.nivel, Perfil.nome).all()
    allowed_map: dict[int, list[str]] = {}
    try:
        permissoes = (
            db.session.query(PerfilPermissao.perfil_id, PerfilPermissao.feature)
            .filter(PerfilPermissao.feature.isnot(None), PerfilPermissao.ativo == True)  # noqa: E712
            .all()
        )
        for p_id, feat in permissoes:
            if feat:
                allowed_map.setdefault(p_id, []).append(feat)
    except ProgrammingError:
        db.session.rollback()
    features = FEATURES
    allowed = {}
    for perfil in perfis:
        feats = allowed_map.get(perfil.id) or []
        # inclui pais e locked
        allowed[perfil.id] = _permissoes_with_parents(perfil.id)
    return render_template(
        "partials/painel.html",
        perfis=perfis,
        features=features,
        allowed=allowed,
    )


@home_bp.route("/partial/atualizar/fip613")
@login_required
@require_feature("atualizar/fip613")
def partial_atualizar_fip613():
    return render_template("partials/atualizar_fip613.html")


@home_bp.route("/partial/atualizar/ped")
@login_required
@require_feature("atualizar/ped")
def partial_atualizar_ped():
    return render_template("partials/atualizar_ped.html")


@home_bp.route("/partial/atualizar/emp")
@login_required
@require_feature("atualizar/emp")
def partial_atualizar_emp():
    return render_template("partials/atualizar_emp.html")


@home_bp.route("/partial/atualizar/est-emp")
@login_required
@require_feature("atualizar/est-emp")
def partial_atualizar_est_emp():
    return render_template("partials/atualizar_est_emp.html")


@home_bp.route("/partial/atualizar/nob")
@login_required
@require_feature("atualizar/nob")
def partial_atualizar_nob():
    return render_template("partials/atualizar_nob.html")


@home_bp.route("/partial/atualizar/plan20-seduc")
@login_required
@require_feature("atualizar/plan20-seduc")
def partial_atualizar_plan20():
    return render_template("partials/atualizar_plan20.html")


@home_bp.route("/partial/cadastrar/dotacao")
@login_required
@require_feature("cadastrar/dotacao")
def partial_cadastrar_dotacao():
    rows = (
        db.session.query(Dotacao, Adj.abreviacao)
        .outerjoin(Adj, Dotacao.adj_id == Adj.id)
        .filter(Dotacao.ativo == True)  # noqa: E712
        .order_by(Dotacao.id.desc())
        .all()
    )
    dotacoes = []
    for dot, adj_abreviacao in rows:
        dotacoes.append(
            {
                "exercicio": dot.exercicio,
                "adj_abreviacao": adj_abreviacao or "",
                "chave_planejamento": dot.chave_planejamento,
                "uo": dot.uo,
                "regiao": dot.regiao,
                "subacao_entrega": dot.subacao_entrega,
                "etapa": dot.etapa,
                "natureza_despesa": dot.natureza_despesa,
                "elemento": dot.elemento,
                "fonte": dot.fonte,
                "iduso": dot.iduso,
                "valor_dotacao": dot.valor_dotacao,
                "justificativa_historico": dot.justificativa_historico,
            }
        )
    return render_template("partials/cadastrar_dotacao.html", dotacoes=dotacoes)


@home_bp.route("/partial/institucional/diretrizes")
@login_required
def partial_institucional_diretrizes():
    return render_template("partials/institucional_diretrizes.html")


@home_bp.route("/partial/institucional/repositorio")
@login_required
def partial_institucional_repositorio():
    return render_template("partials/institucional_repositorio.html")


@home_bp.route("/partial/institucional/legislacao")
@login_required
def partial_institucional_legislacao():
    return render_template("partials/institucional_legislacao.html")


@home_bp.route("/partial/institucional/parceiros")
@login_required
def partial_institucional_parceiros():
    return render_template("partials/institucional_parceiros.html")


@home_bp.route("/partial/relatorios/fip613")
@login_required
@require_feature("relatorios/fip613")
def partial_relatorios_fip613():
    return render_template("partials/relatorios_fip613.html")


@home_bp.route("/partial/relatorios/ped")
@login_required
@require_feature("relatorios/ped")
def partial_relatorios_ped():
    return render_template("partials/relatorios_ped.html")


@home_bp.route("/partial/relatorios/emp")
@login_required
@require_feature("relatorios/emp")
def partial_relatorios_emp():
    return render_template("partials/relatorios_emp.html")


@home_bp.route("/partial/relatorios/est-emp")
@login_required
@require_feature("relatorios/est-emp")
def partial_relatorios_est_emp():
    return render_template("partials/relatorios_est_emp.html")


@home_bp.route("/partial/relatorios/nob")
@login_required
@require_feature("relatorios/nob")
def partial_relatorios_nob():
    return render_template("partials/relatorios_nob.html")


@home_bp.route("/partial/relatorios/plan20-seduc")
@login_required
@require_feature("relatorios/plan20-seduc")
def partial_relatorios_plan20():
    return render_template("partials/relatorios_plan20.html")


@home_bp.route("/api/permissoes/<int:perfil_id>", methods=["GET", "POST"])
@login_required
def api_permissoes(perfil_id):
    perfil = db.session.get(Perfil, perfil_id)
    if not perfil:
        return jsonify({"error": "Perfil nao encontrado."}), 404

    if not (has_permission("painel") or getattr(g, "user_nivel", None) == 1):
        return jsonify({"error": "Sem permissao."}), 403

    if request.method == "GET":
        try:
            feats = [
                pp.feature
                for pp in PerfilPermissao.query.filter_by(perfil_id=perfil_id, ativo=True).all()
            ]
            for f in FEATURES:
                if f.get("locked") and f["id"] not in feats:
                    feats.append(f["id"])
            return jsonify({"features": feats})
        except ProgrammingError:
            db.session.rollback()
            return jsonify({"features": []})

    data = request.get_json() or {}
    feats = data.get("features") or []
    if not isinstance(feats, list):
        return jsonify({"error": "Formato invalido."}), 400
    # limpa valores vazios/None e remove duplicados
    clean_feats = []
    seen = set()
    for f in feats:
        if not isinstance(f, str):
            continue
        fid = f.strip()
        if not fid or fid in seen:
            continue
        seen.add(fid)
        clean_feats.append(fid)
    locked_feats = {f["id"] for f in FEATURES if f.get("locked")}
    clean_feats = [f for f in clean_feats if f not in locked_feats]
    try:
        # desativa anteriores
        PerfilPermissao.query.filter_by(perfil_id=perfil_id).update({"ativo": False, "updated_at": datetime.utcnow()})
        # remove qualquer registro sem feature
        PerfilPermissao.query.filter(PerfilPermissao.feature == None).delete(synchronize_session=False)  # noqa: E711
        for f in clean_feats:
            db.session.add(PerfilPermissao(perfil_id=perfil_id, feature=f, ativo=True))
        db.session.commit()
    except ProgrammingError:
        db.session.rollback()
        return jsonify({"error": "Tabela perfil_permissoes inexistente. Crie a tabela antes de salvar."}), 500
    return jsonify({"ok": True, "message": "Permissoes atualizadas."})


@home_bp.route("/api/permissoes/current", methods=["GET"])
@login_required
def api_permissoes_current():
    user_session = session.get("user") or {}
    perfil_id = getattr(g, "user_perfil_id", None) or user_session.get("perfil_id")
    if perfil_id is None:
        perfil_nome = user_session.get("perfil")
        perfil = _perfil_by_nome(perfil_nome)
        if perfil:
            perfil_id = perfil.id
            # atualiza session para futuras chamadas
            user_session["perfil_id"] = perfil.id
            session["user"] = user_session
    feats = _permissoes_with_parents(perfil_id)
    return jsonify({"features": feats})


def _parse_decimal(raw_val):
    if raw_val is None:
        return None
    raw = str(raw_val).strip()
    if not raw:
        return None
    cleaned = raw.replace(".", "").replace(",", ".")
    try:
        return Decimal(cleaned)
    except Exception:
        return None


def _dec_or_zero(value):
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    parsed = _parse_decimal(value)
    return parsed if parsed is not None else Decimal("0")


def _natureza_prefix(value: str) -> str:
    if not value:
        return ""
    parts = [p for p in str(value).split(".") if p]
    if len(parts) >= 3:
        return ".".join(parts[:3])
    return str(value).strip()


def _leading_token(value: str) -> str:
    if not value:
        return ""
    return str(value).strip().split(" ", 1)[0]


def _normalize_ug(value: str) -> str:
    token = _leading_token(value)
    if not token:
        return ""
    try:
        return str(int(token))
    except ValueError:
        return token


def _normalize_uo(value: str) -> str:
    if not value:
        return ""
    token = _leading_token(value)
    digits = "".join(ch for ch in token if ch.isdigit())
    return digits or token


def _normalize_chave(value: str) -> str:
    if not value:
        return ""
    value = str(value)
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return "".join(ch for ch in value if ch.isalnum() or ch == "*").upper()


@home_bp.route("/api/dotacao/options", methods=["GET"])
@login_required
@require_feature("cadastrar/dotacao")
def api_dotacao_options():
    fields = {
        "exercicio": Plan21Nger.exercicio,
        "chave_planejamento": Plan21Nger.chave_planejamento,
        "uo": Plan21Nger.uo,
        "programa": Plan21Nger.programa,
        "acao_paoe": Plan21Nger.acao_paoe,
        "produto": Plan21Nger.produto,
        "ug": Plan21Nger.ug,
        "regiao": Plan21Nger.regiao,
        "subacao_entrega": Plan21Nger.subacao_entrega,
        "etapa": Plan21Nger.etapa,
        "natureza_despesa": Plan21Nger.natureza,
        "fonte": Plan21Nger.fonte,
        "iduso": Plan21Nger.idu,
    }
    selected = {}
    for key in fields:
        val = (request.args.get(key) or "").strip()
        if val:
            selected[key] = val

    options = {}
    for key, col in fields.items():
        query = db.session.query(col).distinct()
        for s_key, s_val in selected.items():
            if s_key == key:
                continue
            if s_key == "natureza_despesa":
                query = query.filter(fields[s_key].like(f"{s_val}%"))
            else:
                query = query.filter(fields[s_key] == s_val)
        rows = query.all()
        values = []
        for (val,) in rows:
            if val is None:
                continue
            s = str(val).strip()
            if s == "":
                continue
            if key == "natureza_despesa":
                s = _natureza_prefix(s)
            values.append(s)
        values = sorted(set(values), key=lambda v: v.lower())
        options[key] = values

    adjs = Adj.query.order_by(Adj.abreviacao).all()
    adj_options = [{"id": a.id, "label": a.abreviacao} for a in adjs if a.abreviacao]
    return jsonify({"options": options, "adj": adj_options})


@home_bp.route("/api/dotacao", methods=["POST"])
@login_required
@require_feature("cadastrar/dotacao")
def api_dotacao_create():
    data = request.get_json() or {}
    exercicio = (data.get("exercicio") or "").strip()
    chave_planejamento = (data.get("chave_planejamento") or "").strip()
    uo = (data.get("uo") or "").strip()
    programa = (data.get("programa") or "").strip()
    acao_paoe = (data.get("acao_paoe") or "").strip()
    produto = (data.get("produto") or "").strip()
    ug = (data.get("ug") or "").strip()
    regiao = (data.get("regiao") or "").strip()
    subacao_entrega = (data.get("subacao_entrega") or "").strip()
    etapa = (data.get("etapa") or "").strip()
    natureza_despesa = (data.get("natureza_despesa") or "").strip()
    fonte = (data.get("fonte") or "").strip()
    iduso = (data.get("iduso") or "").strip()
    adj_raw = (data.get("adj_id") or "").strip()
    elemento_raw = (data.get("elemento") or "").strip()
    valor_raw = (data.get("valor_dotacao") or "").strip()
    justificativa = (data.get("justificativa_historico") or "").strip()

    required = {
        "exercicio": exercicio,
        "chave_planejamento": chave_planejamento,
        "uo": uo,
        "programa": programa,
        "acao_paoe": acao_paoe,
        "produto": produto,
        "ug": ug,
        "regiao": regiao,
        "subacao_entrega": subacao_entrega,
        "etapa": etapa,
        "natureza_despesa": natureza_despesa,
        "fonte": fonte,
        "iduso": iduso,
        "adj_id": adj_raw,
        "elemento": elemento_raw,
        "valor_dotacao": valor_raw,
        "justificativa_historico": justificativa,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        return jsonify({"error": f"Campos obrigatorios ausentes: {', '.join(missing)}."}), 400

    try:
        adj_id = int(adj_raw)
    except ValueError:
        return jsonify({"error": "Adjunta Responsavel invalida."}), 400
    if not db.session.get(Adj, adj_id):
        return jsonify({"error": "Adjunta Responsavel nao encontrada."}), 400

    try:
        elemento = int(elemento_raw)
    except ValueError:
        return jsonify({"error": "Elemento invalido."}), 400

    valor_dotacao = _parse_decimal(valor_raw)
    if valor_dotacao is None:
        return jsonify({"error": "Valor da dotacao invalido."}), 400

    query = Plan21Nger.query
    query = query.filter(Plan21Nger.exercicio == exercicio)
    query = query.filter(Plan21Nger.chave_planejamento == chave_planejamento)
    query = query.filter(Plan21Nger.uo == uo)
    query = query.filter(Plan21Nger.programa == programa)
    query = query.filter(Plan21Nger.acao_paoe == acao_paoe)
    query = query.filter(Plan21Nger.produto == produto)
    query = query.filter(Plan21Nger.ug == ug)
    query = query.filter(Plan21Nger.regiao == regiao)
    query = query.filter(Plan21Nger.subacao_entrega == subacao_entrega)
    query = query.filter(Plan21Nger.etapa == etapa)
    query = query.filter(Plan21Nger.natureza.like(f"{natureza_despesa}%"))
    query = query.filter(Plan21Nger.fonte == fonte)
    query = query.filter(Plan21Nger.idu == iduso)
    rows = query.limit(2).all()
    if not rows:
        return jsonify({"error": "Nenhum registro do plan21_nger encontrado para esta selecao."}), 400
    if len(rows) > 1:
        return jsonify({"error": "Selecao ambigua no plan21_nger. Ajuste os filtros."}), 400
    plan = rows[0]

    registro = Dotacao(
        plan21_nger_id=plan.id,
        exercicio=exercicio,
        adj_id=adj_id,
        chave_planejamento=chave_planejamento,
        uo=uo,
        programa=getattr(plan, "programa", None),
        acao_paoe=getattr(plan, "acao_paoe", None),
        produto=getattr(plan, "produto", None),
        ug=getattr(plan, "ug", None),
        regiao=regiao,
        subacao_entrega=subacao_entrega,
        etapa=etapa,
        natureza_despesa=natureza_despesa,
        elemento=elemento,
        fonte=fonte,
        iduso=iduso,
        valor_dotacao=valor_dotacao,
        justificativa_historico=justificativa,
        ativo=True,
    )
    db.session.add(registro)
    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        return jsonify({"error": f"Falha ao salvar dotacao: {exc}"}), 500

    return jsonify({"ok": True, "message": "Dotacao cadastrada."}), 201


@home_bp.route("/api/dotacao/saldo", methods=["GET"])
@login_required
@require_feature("cadastrar/dotacao")
def api_dotacao_saldo():
    exercicio = (request.args.get("exercicio") or "").strip()
    programa = (request.args.get("programa") or "").strip()
    acao_paoe = (request.args.get("acao_paoe") or "").strip()
    produto = (request.args.get("produto") or "").strip()
    ug = (request.args.get("ug") or "").strip()
    uo = (request.args.get("uo") or "").strip()
    regiao = (request.args.get("regiao") or "").strip()
    subacao_entrega = (request.args.get("subacao_entrega") or "").strip()
    etapa = (request.args.get("etapa") or "").strip()
    fonte = (request.args.get("fonte") or "").strip()
    iduso = (request.args.get("iduso") or "").strip()
    chave_planejamento = (request.args.get("chave_planejamento") or "").strip()

    if not exercicio or not chave_planejamento:
        return jsonify({"saldo": 0})

    plan21_filters = [Plan21Nger.ativo == True]  # noqa: E712
    if exercicio:
        plan21_filters.append(Plan21Nger.exercicio == exercicio)
    if programa:
        plan21_filters.append(Plan21Nger.programa == programa)
    if acao_paoe:
        plan21_filters.append(Plan21Nger.acao_paoe == acao_paoe)
    if produto:
        plan21_filters.append(Plan21Nger.produto == produto)
    if ug:
        plan21_filters.append(Plan21Nger.ug == ug)
    if uo:
        plan21_filters.append(Plan21Nger.uo == uo)
    if regiao:
        plan21_filters.append(Plan21Nger.regiao == regiao)
    if subacao_entrega:
        plan21_filters.append(Plan21Nger.subacao_entrega == subacao_entrega)
    if etapa:
        plan21_filters.append(Plan21Nger.etapa == etapa)
    if fonte:
        plan21_filters.append(Plan21Nger.fonte == fonte)
    if iduso:
        plan21_filters.append(Plan21Nger.idu == iduso)
    if chave_planejamento:
        plan21_filters.append(Plan21Nger.chave_planejamento == chave_planejamento)

    plan21_query = db.session.query(Plan21Nger.valor_atual).filter(*plan21_filters)
    plan21_count = plan21_query.count()
    valor_atual = (
        db.session.query(func.coalesce(func.sum(Plan21Nger.valor_atual), 0))
        .filter(*plan21_filters)
        .scalar()
    )
    valor_atual = _dec_or_zero(valor_atual)

    valor_dotacao = (
        db.session.query(func.coalesce(func.sum(Dotacao.valor_dotacao), 0))
        .filter(
            Dotacao.ativo == True,  # noqa: E712
            *( [Dotacao.exercicio == exercicio] if exercicio else [] ),
            *( [Dotacao.programa == programa] if programa else [] ),
            *( [Dotacao.acao_paoe == acao_paoe] if acao_paoe else [] ),
            *( [Dotacao.produto == produto] if produto else [] ),
            *( [Dotacao.ug == ug] if ug else [] ),
            *( [Dotacao.uo == uo] if uo else [] ),
            *( [Dotacao.regiao == regiao] if regiao else [] ),
            *( [Dotacao.fonte == fonte] if fonte else [] ),
            *( [Dotacao.iduso == iduso] if iduso else [] ),
            *( [Dotacao.chave_planejamento == chave_planejamento] if chave_planejamento else [] ),
        )
        .scalar()
    )
    valor_dotacao = _dec_or_zero(valor_dotacao)

    programa_key = _leading_token(programa)
    acao_paoe_key = _leading_token(acao_paoe)
    ug_norm = _normalize_ug(ug)
    uo_norm = _normalize_uo(uo)
    try:
        exercicio_int = int(str(exercicio).split(".")[0])
    except ValueError:
        exercicio_int = None
    chave_field = "chave_planejamento" if exercicio_int and exercicio_int <= 2025 else "chave"
    chave_norm = _normalize_chave(chave_planejamento)

    ped_base = [PedRegistro.ativo == True]  # noqa: E712
    if exercicio:
        ped_base.append(PedRegistro.exercicio == exercicio)
    if programa_key:
        ped_base.append(PedRegistro.programa_governo == programa_key)
    if acao_paoe_key:
        ped_base.append(PedRegistro.paoe == acao_paoe_key)
    if fonte:
        ped_base.append(PedRegistro.fonte == fonte)
    if iduso:
        ped_base.append(PedRegistro.iduso == iduso)
    if uo_norm:
        ped_base.append(PedRegistro.uo == uo_norm)
    if ug_norm:
        ped_base.append(PedRegistro.subfuncao_ug.like(f"%.{ug_norm}"))
    if regiao:
        ped_base.append(PedRegistro.regiao == regiao)
    if chave_planejamento:
        if chave_field == "chave_planejamento":
            ped_base.append(PedRegistro.chave_planejamento == chave_planejamento)
        else:
            ped_base.append(PedRegistro.chave == chave_planejamento)
    ped_rows = (
        PedRegistro.query.with_entities(
            PedRegistro.valor_ped, PedRegistro.chave_planejamento, PedRegistro.chave
        )
        .filter(*ped_base)
        .all()
    )
    ped_filtered = []
    for row in ped_rows:
        chave_val = row.chave_planejamento if chave_field == "chave_planejamento" else row.chave
        if _normalize_chave(chave_val) == chave_norm:
            ped_filtered.append(row)
    valor_ped = sum((_dec_or_zero(r.valor_ped) for r in ped_filtered), Decimal("0"))
    ped_count = len(ped_filtered)

    emp_base = [EmpRegistro.ativo == True]  # noqa: E712
    if exercicio:
        emp_base.append(EmpRegistro.exercicio == exercicio)
    if programa_key:
        emp_base.append(EmpRegistro.programa_governo == programa_key)
    if acao_paoe_key:
        emp_base.append(EmpRegistro.paoe == acao_paoe_key)
    if fonte:
        emp_base.append(EmpRegistro.fonte == fonte)
    if iduso:
        emp_base.append(EmpRegistro.iduso == iduso)
    if uo_norm:
        emp_base.append(EmpRegistro.uo == uo_norm)
    if ug_norm:
        emp_base.append(EmpRegistro.subfuncao_ug.like(f"%.{ug_norm}"))
    if regiao:
        emp_base.append(EmpRegistro.regiao == regiao)
    if chave_planejamento:
        if chave_field == "chave_planejamento":
            emp_base.append(EmpRegistro.chave_planejamento == chave_planejamento)
        else:
            emp_base.append(EmpRegistro.chave == chave_planejamento)
    emp_rows = (
        EmpRegistro.query.with_entities(
            EmpRegistro.numero_emp, EmpRegistro.chave_planejamento, EmpRegistro.chave
        )
        .filter(*emp_base)
        .all()
    )
    emp_nums = []
    for row in emp_rows:
        chave_val = row.chave_planejamento if chave_field == "chave_planejamento" else row.chave
        if _normalize_chave(chave_val) == chave_norm and row.numero_emp:
            emp_nums.append(row.numero_emp)
    emp_nums = list(dict.fromkeys(emp_nums))
    emp_count = len(emp_nums)
    if emp_nums:
        valor_emp_liquido = (
            db.session.query(func.coalesce(func.sum(EstEmpRegistro.valor_emp_liquido), 0))
            .filter(
                EstEmpRegistro.ativo == True,  # noqa: E712
                EstEmpRegistro.numero_emp.in_(emp_nums),
            )
            .scalar()
        )
    else:
        valor_emp_liquido = Decimal("0")
    valor_emp_liquido = _dec_or_zero(valor_emp_liquido)

    saldo = valor_atual - valor_dotacao - valor_ped - valor_emp_liquido
    return jsonify(
        {
            "saldo": float(saldo),
            "valor_atual": float(valor_atual),
            "valor_dotacao": float(valor_dotacao),
            "valor_ped": float(valor_ped),
            "valor_emp_liquido": float(valor_emp_liquido),
            "plan21_count": plan21_count,
            "dotacao_count": int(
                db.session.query(func.count(Dotacao.id))
                .filter(
                    Dotacao.ativo == True,  # noqa: E712
                    *( [Dotacao.exercicio == exercicio] if exercicio else [] ),
                    *( [Dotacao.programa == programa] if programa else [] ),
                    *( [Dotacao.acao_paoe == acao_paoe] if acao_paoe else [] ),
                    *( [Dotacao.produto == produto] if produto else [] ),
                    *( [Dotacao.ug == ug] if ug else [] ),
                    *( [Dotacao.uo == uo] if uo else [] ),
                    *( [Dotacao.regiao == regiao] if regiao else [] ),
                    *( [Dotacao.fonte == fonte] if fonte else [] ),
                    *( [Dotacao.iduso == iduso] if iduso else [] ),
                    *( [Dotacao.chave_planejamento == chave_planejamento] if chave_planejamento else [] ),
                )
                .scalar()
                or 0
            ),
            "ped_count": ped_count,
            "emp_count": emp_count,
        }
    )


@home_bp.route("/api/fip613/status", methods=["GET"])
@login_required
@require_feature("atualizar/fip613")
def api_fip613_status():
    def _as_iso(value):
        if not value:
            return None
        if isinstance(value, str) and value.startswith("0000-00-00"):
            return None
        if hasattr(value, "isoformat"):
            return value.isoformat()
        try:
            # tenta converter string para datetime
            from datetime import datetime

            return datetime.fromisoformat(str(value)).isoformat()
        except Exception:
            return str(value)

    last = Fip613Upload.query.order_by(Fip613Upload.uploaded_at.desc()).first()
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


@home_bp.route("/api/fip613/upload", methods=["POST"])
@login_required
@require_feature("atualizar/fip613")
def api_fip613_upload():
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

    try:
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        (UPLOAD_DIR / "tmp").mkdir(parents=True, exist_ok=True)
        for f in UPLOAD_DIR.glob("*.xlsx"):
            dest = UPLOAD_DIR / "tmp" / f"{f.stem}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}{f.suffix}"
            try:
                f.rename(dest)
            except OSError:
                pass
        stored_name = f"fip613_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        save_path = UPLOAD_DIR / stored_name
        arquivo.save(save_path)

        registro = Fip613Upload(
            user_email=user_email,
            original_filename=arquivo.filename,
            stored_filename=stored_name,
            data_arquivo=data_arquivo,
            uploaded_at=datetime.utcnow(),
        )
        db.session.add(registro)
        db.session.commit()

        total, output_path = run_fip613(save_path, data_arquivo, user_email, registro.id)

        registro.output_filename = str(output_path.name)
        db.session.commit()

        return jsonify(
            {
                "ok": True,
                "message": f"Processado com sucesso. Registros inseridos: {total}.",
                "output": output_path.name,
            }
        )
    except Exception as exc:
        db.session.rollback()
        return jsonify({"error": f"Falha ao processar: {exc}"}), 500


@home_bp.route("/api/relatorios/fip613", methods=["GET"])
@login_required
@require_feature("relatorios/fip613")
def api_relatorio_fip613():
    def _as_iso(value):
        if not value:
            return None
        if hasattr(value, "isoformat"):
            return value.isoformat()
        try:
            from datetime import datetime

            return datetime.fromisoformat(str(value)).isoformat()
        except Exception:
            return str(value)

    try:
        rows = Fip613Registro.query.filter_by(ativo=True).all()
        last_upload = (
            Fip613Registro.query.filter_by(ativo=True)
            .order_by(Fip613Registro.created_at.desc())
            .first()
        )
        data_arquivo = _as_iso(last_upload.data_arquivo) if last_upload else None
        uploaded_at = _as_iso(last_upload.created_at) if last_upload else None
        user_email = last_upload.user_email if last_upload else None
        data = []
        for r in rows:
            data.append(
                {
                    "uo": r.uo,
                    "ug": r.ug,
                    "funcao": r.funcao,
                    "subfuncao": r.subfuncao,
                    "programa": r.programa,
                    "projeto_atividade": r.projeto_atividade,
                    "regional": r.regional,
                    "natureza_despesa": str(r.natureza_despesa or ""),
                    "fonte_recurso": str(r.fonte_recurso or ""),
                    "iduso": r.iduso,
                    "tipo_recurso": r.tipo_recurso,
                    "dotacao_inicial": float(r.dotacao_inicial or 0),
                    "cred_suplementar": float(r.cred_suplementar or 0),
                    "cred_especial": float(r.cred_especial or 0),
                    "cred_extraordinario": float(r.cred_extraordinario or 0),
                    "reducao": float(r.reducao or 0),
                    "cred_autorizado": float(r.cred_autorizado or 0),
                    "bloqueado_conting": float(r.bloqueado_conting or 0),
                    "reserva_empenho": float(r.reserva_empenho or 0),
                    "saldo_destaque": float(r.saldo_destaque or 0),
                    "saldo_dotacao": float(r.saldo_dotacao or 0),
                    "empenhado": float(r.empenhado or 0),
                    "liquidado": float(r.liquidado or 0),
                    "a_liquidar": float(r.a_liquidar or 0),
                    "valor_pago": float(r.valor_pago or 0),
                    "valor_a_pagar": float(r.valor_a_pagar or 0),
                }
            )
        return jsonify({"ok": True, "data": data, "data_arquivo": data_arquivo, "uploaded_at": uploaded_at, "user_email": user_email})
    except Exception as exc:
        return jsonify({"error": f"Falha ao buscar dados: {exc}"}), 500


@home_bp.route("/api/relatorios/fip613/download", methods=["GET"])
@login_required
@require_feature("relatorios/fip613")
def api_relatorio_fip613_download():
    try:
        rows = Fip613Registro.query.filter_by(ativo=True).all()
        data = []
        for r in rows:
            data.append(
                {
                    "UO": r.uo,
                    "UG": r.ug,
                    "Função": r.funcao,
                    "Subfunção": r.subfuncao,
                    "Programa": r.programa,
                    "Projeto/Atividade": r.projeto_atividade,
                    "Regional": r.regional,
                    "Natureza de Despesa": str(r.natureza_despesa or ""),
                    "Fonte de Recurso": str(r.fonte_recurso or ""),
                    "Iduso": r.iduso,
                    "Tipo de Recurso": r.tipo_recurso,
                    "Dotação Inicial": float(r.dotacao_inicial or 0),
                    "Créd. Suplementar": float(r.cred_suplementar or 0),
                    "Créd. Especial": float(r.cred_especial or 0),
                    "Créd. Extraordinário": float(r.cred_extraordinario or 0),
                    "Redução": float(r.reducao or 0),
                    "Créd. Autorizado": float(r.cred_autorizado or 0),
                    "Bloqueado/Conting.": float(r.bloqueado_conting or 0),
                    "Reserva Empenho": float(r.reserva_empenho or 0),
                    "Saldo de Destaque": float(r.saldo_destaque or 0),
                    "Saldo Dotação": float(r.saldo_dotacao or 0),
                    "Empenhado": float(r.empenhado or 0),
                    "Liquidado": float(r.liquidado or 0),
                    "A liquidar": float(r.a_liquidar or 0),
                    "Valor Pago": float(r.valor_pago or 0),
                    "Valor a Pagar": float(r.valor_a_pagar or 0),
                }
            )
        df = None
        try:
            import pandas as pd
            from io import BytesIO
            from flask import send_file
            from openpyxl import load_workbook
            from openpyxl.styles import Font
            import unicodedata

            df = pd.DataFrame(data)

            invert_targets = {"reducao", "bloqueadoconting", "reservaempenho", "empenhado"}

            def _norm_col(name: str) -> str:
                base = unicodedata.normalize("NFKD", str(name or ""))
                ascii_only = "".join(ch for ch in base if not unicodedata.combining(ch))
                return (
                    ascii_only.lower()
                    .replace(" ", "")
                    .replace(".", "")
                    .replace("/", "")
                    .replace("_", "")
                )

            for col in list(df.columns):
                if _norm_col(col) in invert_targets:
                    df[col] = df[col].apply(lambda x: -(x or 0))
            output = BytesIO()
            df.to_excel(output, index=False)
            output.seek(0)

            # aplica fonte e formato numérico no Excel
            wb = load_workbook(output)
            ws = wb.active
            font = Font(name="Helvetica", size=8)
            number_format = "[Blue]#,##0.00;[Red]-#,##0.00;0"
            # colunas numéricas começam em 12 (1-based) até o final
            numeric_cols = set(range(12, ws.max_column + 1))
            for row in ws.iter_rows():
                for cell in row:
                    cell.font = font
                    if cell.col_idx in numeric_cols and isinstance(cell.value, (int, float)):
                        cell.number_format = number_format

            styled = BytesIO()
            wb.save(styled)
            styled.seek(0)

            filename = f"fip613_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
            return send_file(
                styled,
                as_attachment=True,
                download_name=filename,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as exc:
            return jsonify({"error": f"Falha ao preparar planilha: {exc}"}), 500
    except Exception as exc:
        return jsonify({"error": f"Falha ao exportar: {exc}"}), 500


# PED


@home_bp.route("/api/ped/status", methods=["GET"])
@login_required
@require_feature("atualizar/ped")
def api_ped_status():
    def _as_iso(value):
        if value in (None, ""):
            return None
        if isinstance(value, str) and value.startswith("0000-00-00"):
            return None
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        try:
            from datetime import datetime

            return datetime.fromisoformat(str(value)).isoformat()
        except Exception:
            return str(value)

    PED_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    PED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    last = PedUpload.query.order_by(PedUpload.uploaded_at.desc()).first()
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


@home_bp.route("/api/ped/upload", methods=["POST"])
@login_required
@require_feature("atualizar/ped")
def api_ped_upload():
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

    try:
        PED_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        move_existing_to_tmp(PED_UPLOAD_DIR)
        stored_name = f"ped_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        save_path = PED_UPLOAD_DIR / stored_name
        arquivo.save(save_path)

        registro = PedUpload(
            user_email=user_email,
            original_filename=arquivo.filename,
            stored_filename=stored_name,
            data_arquivo=data_arquivo,
            uploaded_at=datetime.utcnow(),
        )
        db.session.add(registro)
        db.session.commit()

        total, output_path = run_ped(save_path, data_arquivo, user_email, registro.id)

        registro.output_filename = str(output_path.name)
        db.session.commit()

        return jsonify(
            {
                "ok": True,
                "message": f"Processado com sucesso. Registros inseridos: {total}.",
                "output": output_path.name,
            }
        )
    except Exception as exc:
        db.session.rollback()
        return jsonify({"error": f"Falha ao processar: {exc}"}), 500


# EMP


@home_bp.route("/api/emp/status", methods=["GET"])
@login_required
@require_feature("atualizar/emp")
def api_emp_status():
    def _as_iso(value):
        if value in (None, ""):
            return None
        if isinstance(value, str) and value.startswith("0000-00-00"):
            return None
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        try:
            from datetime import datetime

            return datetime.fromisoformat(str(value)).isoformat()
        except Exception:
            return str(value)

    try:
        EMP_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        EMP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        last = EmpUpload.query.order_by(EmpUpload.uploaded_at.desc()).first()
        if not last:
            return jsonify({"ok": True, "last": None})
        status_data = read_status("emp", last.id)
        output_name = last.output_filename
        if not output_name and status_data:
            output_name = status_data.get("output_filename") or output_name
        return jsonify(
            {
                "ok": True,
                "last": {
                    "user_email": last.user_email,
                    "uploaded_at": _as_iso(last.uploaded_at),
                    "data_arquivo": _as_iso(last.data_arquivo),
                    "original_filename": last.original_filename,
                    "output_filename": output_name,
                    "status": status_data.get("state") if status_data else None,
                    "status_message": status_data.get("message") if status_data else None,
                    "status_updated_at": status_data.get("updated_at") if status_data else None,
                    "status_progress": status_data.get("progress") if status_data else None,
                    "status_pid": status_data.get("pid") if status_data else None,
                },
            }
        )
    except Exception:
        return jsonify({"ok": True, "last": None, "status_error": True})


@home_bp.route("/api/est-emp/status", methods=["GET"])
@login_required
@require_feature("atualizar/est-emp")
def api_est_emp_status():
    def _as_iso(value):
        if not value:
            return None
        if isinstance(value, str) and value.startswith("0000-00-00"):
            return None
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        try:
            from datetime import datetime

            return datetime.fromisoformat(str(value)).isoformat()
        except Exception:
            return str(value)

    EST_EMP_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    EST_EMP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    last = EstEmpUpload.query.order_by(EstEmpUpload.uploaded_at.desc()).first()
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


@home_bp.route("/api/nob/status", methods=["GET"])
@login_required
@require_feature("atualizar/nob")
def api_nob_status():
    def _as_iso(value):
        if not value:
            return None
        if isinstance(value, str) and value.startswith("0000-00-00"):
            return None
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        try:
            from datetime import datetime

            return datetime.fromisoformat(str(value)).isoformat()
        except Exception:
            return str(value)

    try:
        NOB_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        NOB_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        last = NobUpload.query.order_by(NobUpload.uploaded_at.desc()).first()
        if not last:
            return jsonify({"ok": True, "last": None})
        status_data = read_status("nob", last.id)
        output_name = last.output_filename
        if not output_name and status_data:
            output_name = status_data.get("output_filename") or output_name
        return jsonify(
            {
                "ok": True,
                "last": {
                    "user_email": last.user_email,
                    "uploaded_at": _as_iso(last.uploaded_at),
                    "data_arquivo": _as_iso(last.data_arquivo),
                    "original_filename": last.original_filename,
                    "output_filename": output_name,
                    "status": status_data.get("state") if status_data else None,
                    "status_message": status_data.get("message") if status_data else None,
                    "status_updated_at": status_data.get("updated_at") if status_data else None,
                    "status_progress": status_data.get("progress") if status_data else None,
                    "status_pid": status_data.get("pid") if status_data else None,
                },
            }
        )
    except Exception:
        return jsonify({"ok": True, "last": None, "status_error": True})


@home_bp.route("/api/emp/upload", methods=["POST"])
@login_required
@require_feature("atualizar/emp")
def api_emp_upload():
    if "arquivo" not in request.files:
        return jsonify({"error": "Arquivo obrigatorio."}), 400
    arquivo = request.files["arquivo"]
    data_arquivo_raw = request.form.get("data_arquivo")
    if not data_arquivo_raw:
        return jsonify({"error": "Data do download obrigatoria."}), 400
    try:
        data_arquivo = datetime.fromisoformat(data_arquivo_raw)
    except ValueError:
        return jsonify({"error": "Data do download invalida."}), 400

    if not arquivo.filename.lower().endswith(".xlsx"):
        return jsonify({"error": "Envie um arquivo .xlsx."}), 400

    user = session.get("user") or {}
    user_email = user.get("email") or "desconhecido"

    try:
        EMP_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        _move_existing_to_tmp(EMP_UPLOAD_DIR)
        stored_name = f"emp_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        save_path = EMP_UPLOAD_DIR / stored_name
        arquivo.save(save_path)

        registro = EmpUpload(
            user_email=user_email,
            original_filename=arquivo.filename,
            stored_filename=stored_name,
            data_arquivo=data_arquivo,
            uploaded_at=datetime.utcnow(),
        )
        db.session.add(registro)
        db.session.commit()

        write_status("emp", registro.id, "em processamento", "Arquivo recebido. Processamento em background.")
        _start_worker("emp", registro.id)
        return jsonify(
            {
                "ok": True,
                "message": "Arquivo recebido. O processamento ocorrerá em segundo plano.",
                "job_id": registro.id,
            }
        )
    except Exception as exc:
        db.session.rollback()
        return jsonify({"error": f"Falha ao processar: {exc}"}), 500


@home_bp.route("/api/emp/reprocess", methods=["POST"])
@login_required
@require_feature("atualizar/emp")
def api_emp_reprocess():
    payload = request.get_json(silent=True) or {}
    upload_id = payload.get("upload_id")
    if upload_id:
        registro = db.session.get(EmpUpload, upload_id)
    else:
        registro = EmpUpload.query.order_by(EmpUpload.uploaded_at.desc()).first()
    if not registro:
        return jsonify({"error": "Nenhum upload encontrado para reprocessar."}), 404
    try:
        file_path = _find_upload_path(EMP_UPLOAD_DIR, registro.stored_filename)
        if not file_path:
            return jsonify({"error": "Arquivo do upload nao encontrado."}), 404
        # garante o caminho correto para o worker
        if file_path.parent.name == "tmp":
            registro.stored_filename = f"tmp/{file_path.name}"
        registro.output_filename = None
        db.session.commit()
        write_status("emp", registro.id, "em processamento", "Reprocessamento iniciado.")
        _start_worker("emp", registro.id)
        return jsonify({"ok": True, "message": "Reprocessamento iniciado.", "job_id": registro.id})
    except Exception as exc:
        db.session.rollback()
        return jsonify({"error": f"Falha ao reprocessar: {exc}"}), 500


@home_bp.route("/api/emp/cancel", methods=["POST"])
@login_required
@require_feature("atualizar/emp")
def api_emp_cancel():
    payload = request.get_json(silent=True) or {}
    upload_id = payload.get("upload_id")
    if upload_id:
        registro = db.session.get(EmpUpload, upload_id)
    else:
        registro = EmpUpload.query.order_by(EmpUpload.uploaded_at.desc()).first()
    if not registro:
        return jsonify({"error": "Nenhum upload encontrado para cancelar."}), 404
    set_cancel_flag("emp", registro.id)
    update_status_fields("emp", registro.id, message="Cancelamento solicitado.")
    return jsonify({"ok": True, "message": "Cancelamento solicitado.", "job_id": registro.id})


@home_bp.route("/api/est-emp/upload", methods=["POST"])
@login_required
@require_feature("atualizar/est-emp")
def api_est_emp_upload():
    if "arquivo" not in request.files:
        return jsonify({"error": "Arquivo obrigatorio."}), 400
    arquivo = request.files["arquivo"]
    data_arquivo_raw = request.form.get("data_arquivo")
    if not data_arquivo_raw:
        return jsonify({"error": "Data do download obrigatoria."}), 400
    try:
        data_arquivo = datetime.fromisoformat(data_arquivo_raw)
    except ValueError:
        return jsonify({"error": "Data do download invalida."}), 400

    if not arquivo.filename.lower().endswith(".xlsx"):
        return jsonify({"error": "Envie um arquivo .xlsx."}), 400

    user = session.get("user") or {}
    user_email = user.get("email") or "desconhecido"

    try:
        EST_EMP_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        move_est_emp_existing_to_tmp(EST_EMP_UPLOAD_DIR)
        stored_name = f"est_emp_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        save_path = EST_EMP_UPLOAD_DIR / stored_name
        arquivo.save(save_path)

        registro = EstEmpUpload(
            user_email=user_email,
            original_filename=arquivo.filename,
            stored_filename=stored_name,
            data_arquivo=data_arquivo,
            uploaded_at=datetime.utcnow(),
        )
        db.session.add(registro)
        db.session.commit()

        total, output_path = run_est_emp(save_path, data_arquivo, user_email, registro.id)

        registro.output_filename = str(output_path.name)
        db.session.commit()

        return jsonify(
            {
                "ok": True,
                "message": f"Processado com sucesso. Registros inseridos: {total}.",
                "output": output_path.name,
            }
        )
    except Exception as exc:
        db.session.rollback()
        return jsonify({"error": f"Falha ao processar: {exc}"}), 500


@home_bp.route("/api/nob/upload", methods=["POST"])
@login_required
@require_feature("atualizar/nob")
def api_nob_upload():
    if "arquivo" not in request.files:
        return jsonify({"error": "Arquivo obrigatorio."}), 400
    arquivo = request.files["arquivo"]
    data_arquivo_raw = request.form.get("data_arquivo")
    if not data_arquivo_raw:
        return jsonify({"error": "Data do download obrigatoria."}), 400
    try:
        data_arquivo = datetime.fromisoformat(data_arquivo_raw)
    except ValueError:
        return jsonify({"error": "Data do download invalida."}), 400

    if not arquivo.filename.lower().endswith(".xlsx"):
        return jsonify({"error": "Envie um arquivo .xlsx."}), 400

    user = session.get("user") or {}
    user_email = user.get("email") or "desconhecido"

    try:
        NOB_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        _move_existing_to_tmp(NOB_UPLOAD_DIR)
        stored_name = f"nob_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        save_path = NOB_UPLOAD_DIR / stored_name
        arquivo.save(save_path)

        registro = NobUpload(
            user_email=user_email,
            original_filename=arquivo.filename,
            stored_filename=stored_name,
            data_arquivo=data_arquivo,
            uploaded_at=datetime.utcnow(),
        )
        db.session.add(registro)
        db.session.commit()

        write_status("nob", registro.id, "em processamento", "Arquivo recebido. Processamento em background.")
        _start_worker("nob", registro.id)
        return jsonify(
            {
                "ok": True,
                "message": "Arquivo recebido. O processamento ocorrerá em segundo plano.",
                "job_id": registro.id,
            }
        )
    except Exception as exc:
        db.session.rollback()
        return jsonify({"error": f"Falha ao processar: {exc}"}), 500


@home_bp.route("/api/nob/reprocess", methods=["POST"])
@login_required
@require_feature("atualizar/nob")
def api_nob_reprocess():
    payload = request.get_json(silent=True) or {}
    upload_id = payload.get("upload_id")
    if upload_id:
        registro = db.session.get(NobUpload, upload_id)
    else:
        registro = NobUpload.query.order_by(NobUpload.uploaded_at.desc()).first()
    if not registro:
        return jsonify({"error": "Nenhum upload encontrado para reprocessar."}), 404
    try:
        file_path = _find_upload_path(NOB_UPLOAD_DIR, registro.stored_filename)
        if not file_path:
            return jsonify({"error": "Arquivo do upload nao encontrado."}), 404
        if file_path.parent.name == "tmp":
            registro.stored_filename = f"tmp/{file_path.name}"
        registro.output_filename = None
        db.session.commit()
        write_status("nob", registro.id, "em processamento", "Reprocessamento iniciado.")
        _start_worker("nob", registro.id)
        return jsonify({"ok": True, "message": "Reprocessamento iniciado.", "job_id": registro.id})
    except Exception as exc:
        db.session.rollback()
        return jsonify({"error": f"Falha ao reprocessar: {exc}"}), 500


@home_bp.route("/api/nob/cancel", methods=["POST"])
@login_required
@require_feature("atualizar/nob")
def api_nob_cancel():
    payload = request.get_json(silent=True) or {}
    upload_id = payload.get("upload_id")
    if upload_id:
        registro = db.session.get(NobUpload, upload_id)
    else:
        registro = NobUpload.query.order_by(NobUpload.uploaded_at.desc()).first()
    if not registro:
        return jsonify({"error": "Nenhum upload encontrado para cancelar."}), 404
    set_cancel_flag("nob", registro.id)
    update_status_fields("nob", registro.id, message="Cancelamento solicitado.")
    return jsonify({"ok": True, "message": "Cancelamento solicitado.", "job_id": registro.id})


@home_bp.route("/api/ped/download/<path:filename>", methods=["GET"])
@login_required
@require_feature("atualizar/ped")
def api_ped_download(filename):
    target = PED_OUTPUT_DIR / filename
    if not target.exists():
        abort(404)
    return send_file(target, as_attachment=True)


@home_bp.route("/api/relatorios/ped", methods=["GET"])
@login_required
@require_feature("relatorios/ped")
def api_relatorio_ped():
    def _as_iso(value):
        if value in (None, ""):
            return None
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        try:
            from datetime import datetime

            return datetime.fromisoformat(str(value)).isoformat()
        except Exception:
            return str(value)

    def _to_float(val):
        try:
            if val in (None, ""):
                return 0.0
            if isinstance(val, str):
                cleaned = val.replace(".", "").replace(",", ".")
                return float(cleaned)
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    try:
        rows = (
            db.session.execute(
                text(
                    """
                    SELECT
                        chave,
                        chave_planejamento,
                        regiao,
                        subfuncao_ug,
                        adj,
                        macropolitica,
                        pilar,
                        eixo,
                        politica_decreto,
                        exercicio,
                        numero_ped,
                        numero_ped_estorno,
                        numero_emp,
                        numero_cad,
                        numero_noblist,
                        numero_os,
                        convenio,
                        numero_processo_orcamentario_pagamento,
                        valor_ped,
                        valor_estorno,
                        indicativo_licitacao_exercicios_anteriores,
                        data_licitacao,
                        liberado_fisco_estadual,
                        situacao,
                        uo,
                        nome_unidade_orcamentaria,
                        ug,
                        nome_unidade_gestora,
                        data_solicitacao,
                        data_criacao,
                        tipo_empenho,
                        dotacao_orcamentaria,
                        funcao,
                        subfuncao,
                        programa_governo,
                        paoe,
                        natureza_despesa,
                        cat_econ,
                        grupo,
                        modalidade,
                        elemento,
                        nome_elemento,
                        fonte,
                        iduso,
                        numero_emenda_ep,
                        autor_emenda_ep,
                        numero_cac,
                        licitacao,
                        usuario_responsavel,
                        historico,
                        credor,
                        nome_credor,
                        data_autorizacao,
                        data_hora_cadastro_autorizacao,
                        tipo_despesa,
                        numero_abj,
                        numero_processo_sequestro_judicial,
                        indicativo_entrega_imediata,
                        indicativo_contrato,
                        codigo_uo_extinta,
                        devolucao_gcv,
                        mes_competencia_folha_pagamento,
                        exercicio_competencia_folha,
                        obrigacao_patronal,
                        tipo_obrigacao_patronal,
                        numero_nla
                    FROM ped
                    WHERE ativo = 1
                    """
                )
            )
            .mappings()
            .all()
        )

        last_upload = PedUpload.query.order_by(PedUpload.uploaded_at.desc()).first()
        data_arquivo = _as_iso(getattr(last_upload, "data_arquivo", None)) if last_upload else None
        uploaded_at = _as_iso(getattr(last_upload, "uploaded_at", None)) if last_upload else None
        user_email = last_upload.user_email if last_upload else None

        data = []
        for r in rows:
            data.append(
                {
                    "chave": r.get("chave"),
                    "chave_planejamento": r.get("chave_planejamento"),
                    "regiao": r.get("regiao"),
                    "subfuncao_ug": r.get("subfuncao_ug"),
                    "adj": r.get("adj"),
                    "macropolitica": r.get("macropolitica"),
                    "pilar": r.get("pilar"),
                    "eixo": r.get("eixo"),
                    "politica_decreto": r.get("politica_decreto"),
                    "exercicio": r.get("exercicio"),
                    "numero_ped": r.get("numero_ped"),
                    "numero_ped_estorno": r.get("numero_ped_estorno"),
                    "numero_emp": r.get("numero_emp"),
                    "numero_cad": r.get("numero_cad"),
                    "numero_noblist": r.get("numero_noblist"),
                    "numero_os": r.get("numero_os"),
                    "convenio": r.get("convenio"),
                    "numero_processo_orcamentario_pagamento": r.get("numero_processo_orcamentario_pagamento"),
                    "valor_ped": _to_float(r.get("valor_ped")),
                    "valor_estorno": _to_float(r.get("valor_estorno")),
                    "indicativo_licitacao_exercicios_anteriores": r.get("indicativo_licitacao_exercicios_anteriores"),
                    "data_licitacao": r.get("data_licitacao"),
                    "liberado_fisco_estadual": r.get("liberado_fisco_estadual"),
                    "situacao": r.get("situacao"),
                    "uo": r.get("uo"),
                    "nome_unidade_orcamentaria": r.get("nome_unidade_orcamentaria"),
                    "ug": r.get("ug"),
                    "nome_unidade_gestora": r.get("nome_unidade_gestora"),
                    "data_solicitacao": r.get("data_solicitacao"),
                    "data_criacao": r.get("data_criacao"),
                    "tipo_empenho": r.get("tipo_empenho"),
                    "dotacao_orcamentaria": r.get("dotacao_orcamentaria"),
                    "funcao": r.get("funcao"),
                    "subfuncao": r.get("subfuncao"),
                    "programa_governo": r.get("programa_governo"),
                    "paoe": r.get("paoe"),
                    "natureza_despesa": r.get("natureza_despesa"),
                    "cat_econ": r.get("cat_econ"),
                    "grupo": r.get("grupo"),
                    "modalidade": r.get("modalidade"),
                    "elemento": r.get("elemento"),
                    "nome_elemento": r.get("nome_elemento"),
                    "fonte": r.get("fonte"),
                    "iduso": r.get("iduso"),
                    "numero_emenda_ep": r.get("numero_emenda_ep"),
                    "autor_emenda_ep": r.get("autor_emenda_ep"),
                    "numero_cac": r.get("numero_cac"),
                    "licitacao": r.get("licitacao"),
                    "usuario_responsavel": r.get("usuario_responsavel"),
                    "historico": r.get("historico"),
                    "credor": r.get("credor"),
                    "nome_credor": r.get("nome_credor"),
                    "data_autorizacao": r.get("data_autorizacao"),
                    "data_hora_cadastro_autorizacao": r.get("data_hora_cadastro_autorizacao"),
                    "tipo_despesa": r.get("tipo_despesa"),
                    "numero_abj": r.get("numero_abj"),
                    "numero_processo_sequestro_judicial": r.get("numero_processo_sequestro_judicial"),
                    "indicativo_entrega_imediata": r.get("indicativo_entrega_imediata"),
                    "indicativo_contrato": r.get("indicativo_contrato"),
                    "codigo_uo_extinta": r.get("codigo_uo_extinta"),
                    "devolucao_gcv": r.get("devolucao_gcv"),
                    "mes_competencia_folha_pagamento": r.get("mes_competencia_folha_pagamento"),
                    "exercicio_competencia_folha": r.get("exercicio_competencia_folha"),
                    "obrigacao_patronal": r.get("obrigacao_patronal"),
                    "tipo_obrigacao_patronal": r.get("tipo_obrigacao_patronal"),
                    "numero_nla": r.get("numero_nla"),
                }
            )

        return jsonify(
            {
                "ok": True,
                "data": data,
                "data_arquivo": data_arquivo,
                "uploaded_at": uploaded_at,
                "user_email": user_email,
            }
        )
    except Exception as exc:
        return jsonify({"error": f"Falha ao buscar dados do PED: {exc}"}), 500


@home_bp.route("/api/relatorios/ped/download", methods=["GET"])
@login_required
@require_feature("relatorios/ped")
def api_relatorio_ped_download():
    def _to_float(val):
        try:
            if val in (None, ""):
                return 0.0
            if isinstance(val, str):
                cleaned = val.replace(".", "").replace(",", ".")
                return float(cleaned)
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    try:
        rows = (
            db.session.execute(
                text(
                    """
                    SELECT
                        chave,
                        chave_planejamento,
                        regiao,
                        subfuncao_ug,
                        adj,
                        macropolitica,
                        pilar,
                        eixo,
                        politica_decreto,
                        exercicio,
                        numero_ped,
                        numero_ped_estorno,
                        numero_emp,
                        numero_cad,
                        numero_noblist,
                        numero_os,
                        convenio,
                        numero_processo_orcamentario_pagamento,
                        valor_ped,
                        valor_estorno,
                        indicativo_licitacao_exercicios_anteriores,
                        data_licitacao,
                        liberado_fisco_estadual,
                        situacao,
                        uo,
                        nome_unidade_orcamentaria,
                        ug,
                        nome_unidade_gestora,
                        data_solicitacao,
                        data_criacao,
                        tipo_empenho,
                        dotacao_orcamentaria,
                        funcao,
                        subfuncao,
                        programa_governo,
                        paoe,
                        natureza_despesa,
                        cat_econ,
                        grupo,
                        modalidade,
                        elemento,
                        nome_elemento,
                        fonte,
                        iduso,
                        numero_emenda_ep,
                        autor_emenda_ep,
                        numero_cac,
                        licitacao,
                        usuario_responsavel,
                        historico,
                        credor,
                        nome_credor,
                        data_autorizacao,
                        data_hora_cadastro_autorizacao,
                        tipo_despesa,
                        numero_abj,
                        numero_processo_sequestro_judicial,
                        indicativo_entrega_imediata,
                        indicativo_contrato,
                        codigo_uo_extinta,
                        devolucao_gcv,
                        mes_competencia_folha_pagamento,
                        exercicio_competencia_folha,
                        obrigacao_patronal,
                        tipo_obrigacao_patronal,
                        numero_nla
                    FROM ped
                    WHERE ativo = 1
                    """
                )
            )
            .mappings()
            .all()
        )
        if not rows:
            return jsonify({"error": "Nenhum dado para exportar."}), 404

        df = pd.DataFrame(rows)

        def _chave_display(row):
            try:
                ex = int(str(row.get("exercicio") or 0)[:4])
            except Exception:
                ex = 0
            chave = row.get("chave") or ""
            chave_plan = row.get("chave_planejamento") or ""
            if ex >= 2026 and chave:
                return chave
            return chave_plan or chave

        df["Chave / Chave de Planejamento"] = df.apply(_chave_display, axis=1)
        df.drop(columns=["chave", "chave_planejamento"], inplace=True, errors="ignore")
        if "valor_ped" in df.columns:
            df["valor_ped"] = df["valor_ped"].apply(_to_float)
        if "valor_estorno" in df.columns:
            df["valor_estorno"] = df["valor_estorno"].apply(_to_float)

        rename_map = {
            "regiao": "Região",
            "subfuncao_ug": "Subfunção + UG",
            "adj": "ADJ",
            "macropolitica": "Macropolítica",
            "pilar": "Pilar",
            "eixo": "Eixo",
            "politica_decreto": "Política_Decreto",
            "exercicio": "Exercício",
            "numero_ped": "Nº PED",
            "numero_ped_estorno": "Nº PED Estorno/Estornado",
            "numero_emp": "Nº EMP",
            "numero_cad": "Nº CAD",
            "numero_noblist": "Nº NOBLIST",
            "numero_os": "Nº OS",
            "convenio": "Convênio",
            "numero_processo_orcamentario_pagamento": "Nº Processo Orçamentário de Pagamento",
            "valor_ped": "Valor PED",
            "valor_estorno": "Valor do Estorno",
            "indicativo_licitacao_exercicios_anteriores": "Indicativo de Licitação de Exercícios Anteriores",
            "data_licitacao": "Data da Licitação",
            "liberado_fisco_estadual": "Liberado Fisco Estadual",
            "situacao": "Situação",
            "uo": "UO",
            "nome_unidade_orcamentaria": "Nome da Unidade Orçamentária",
            "ug": "UG",
            "nome_unidade_gestora": "Nome da Unidade Gestora",
            "data_solicitacao": "Data Solicitação",
            "data_criacao": "Data Criação",
            "tipo_empenho": "Tipo Empenho",
            "dotacao_orcamentaria": "Dotação Orçamentária",
            "funcao": "Função",
            "subfuncao": "Subfunção",
            "programa_governo": "Programa de Governo",
            "paoe": "PAOE",
            "natureza_despesa": "Natureza de Despesa",
            "cat_econ": "Cat.Econ",
            "grupo": "Grupo",
            "modalidade": "Modalidade",
            "elemento": "Elemento",
            "nome_elemento": "Nome do Elemento",
            "fonte": "Fonte",
            "iduso": "Iduso",
            "numero_emenda_ep": "Nº Emenda (EP)",
            "autor_emenda_ep": "Autor da Emenda (EP)",
            "numero_cac": "Nº CAC",
            "licitacao": "Licitação",
            "usuario_responsavel": "Usuário Responsável",
            "historico": "Histórico",
            "credor": "Credor",
            "nome_credor": "Nome do Credor",
            "data_autorizacao": "Data Autorização",
            "data_hora_cadastro_autorizacao": "Data/Hora Cadastro Autorização",
            "tipo_despesa": "Tipo de Despesa",
            "numero_abj": "Nº ABJ",
            "numero_processo_sequestro_judicial": "Nº Processo do Sequestro Judicial",
            "indicativo_entrega_imediata": "Indicativo de Entrega imediata - § 4º Art. 62 Lei 8.666",
            "indicativo_contrato": "Indicativo de contrato",
            "codigo_uo_extinta": "Código UO Extinta",
            "devolucao_gcv": "Devolução GCV",
            "mes_competencia_folha_pagamento": "Mês de Competência da Folha de Pagamento",
            "exercicio_competencia_folha": "Exercício de Competência da Folha de Pagamento",
            "obrigacao_patronal": "Obrigação Patronal",
            "tipo_obrigacao_patronal": "Tipo de Obrigação Patronal",
            "numero_nla": "Nº NLA",
        }
        df.rename(columns=rename_map, inplace=True)

        col_order = [
            "Chave / Chave de Planejamento",
            "Região",
            "Subfunção + UG",
            "ADJ",
            "Macropolítica",
            "Pilar",
            "Eixo",
            "Política_Decreto",
            "Exercício",
            "Nº PED",
            "Nº PED Estorno/Estornado",
            "Nº EMP",
            "Nº CAD",
            "Nº NOBLIST",
            "Nº OS",
            "Convênio",
            "Nº Processo Orçamentário de Pagamento",
            "Valor PED",
            "Valor do Estorno",
            "Indicativo de Licitação de Exercícios Anteriores",
            "Data da Licitação",
            "Liberado Fisco Estadual",
            "Situação",
            "UO",
            "Nome da Unidade Orçamentária",
            "UG",
            "Nome da Unidade Gestora",
            "Data Solicitação",
            "Data Criação",
            "Tipo Empenho",
            "Dotação Orçamentária",
            "Função",
            "Subfunção",
            "Programa de Governo",
            "PAOE",
            "Natureza de Despesa",
            "Cat.Econ",
            "Grupo",
            "Modalidade",
            "Elemento",
            "Nome do Elemento",
            "Fonte",
            "Iduso",
            "Nº Emenda (EP)",
            "Autor da Emenda (EP)",
            "Nº CAC",
            "Licitação",
            "Usuário Responsável",
            "Histórico",
            "Credor",
            "Nome do Credor",
            "Data Autorização",
            "Data/Hora Cadastro Autorização",
            "Tipo de Despesa",
            "Nº ABJ",
            "Nº Processo do Sequestro Judicial",
            "Indicativo de Entrega imediata - § 4º Art. 62 Lei 8.666",
            "Indicativo de contrato",
            "Código UO Extinta",
            "Devolução GCV",
            "Mês de Competência da Folha de Pagamento",
            "Exercício de Competência da Folha de Pagamento",
            "Obrigação Patronal",
            "Tipo de Obrigação Patronal",
            "Nº NLA",
        ]
        col_order = [c for c in col_order if c in df.columns]
        if col_order:
            df = df[col_order]

        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="PED", header=False, startrow=1)
            workbook = writer.book
            worksheet = writer.sheets["PED"]
            cell_fmt = workbook.add_format({"font_name": "Helvetica", "font_size": 8})
            header_fmt = workbook.add_format({"font_name": "Helvetica", "font_size": 8})
            worksheet.set_default_row(12, cell_fmt)
            if len(df.columns) > 0:
                worksheet.set_column(0, len(df.columns) - 1, None, cell_fmt)
                worksheet.write_row(0, 0, df.columns, header_fmt)
                worksheet.set_row(0, None, header_fmt)
        output.seek(0)
        filename = f"ped_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as exc:
        return jsonify({"error": f"Falha ao exportar: {exc}"}), 500


# Plan20 SEDUC
PLAN20_UPLOAD_DIR = Path("upload/plan20_seduc")
PLAN20_OUTPUT_DIR = Path("outputs/plan20_seduc")


@home_bp.route("/api/plan20/status", methods=["GET"])
@login_required
@require_feature("atualizar/plan20-seduc")
def api_plan20_status():
    def _as_iso(value):
        """
        Converte datetime ou string em isoformat; se já for string que não
        parseia, devolve a string mesmo.
        """
        if value in (None, ""):
            return None
        if isinstance(value, str) and value.startswith("0000-00-00"):
            return None
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        try:
            from datetime import datetime

            return datetime.fromisoformat(str(value)).isoformat()
        except Exception:
            return str(value)

    PLAN20_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    PLAN20_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    registro = Plan20Upload.query.order_by(Plan20Upload.uploaded_at.desc()).first()
    if not registro:
        return jsonify({"ok": True, "last": None})
    last = {
        "user_email": registro.user_email,
        "uploaded_at": _as_iso(registro.uploaded_at),
        "data_arquivo": _as_iso(registro.data_arquivo),
        "original_filename": registro.original_filename,
        "output_filename": registro.output_filename,
    }
    return jsonify({"ok": True, "last": last})


@home_bp.route("/api/plan20/upload", methods=["POST"])
@login_required
@require_feature("atualizar/plan20-seduc")
def api_plan20_upload():
    def _as_iso(value):
        """
        Converte datetime ou string em isoformat; se já for string que não
        parseia, devolve a string mesmo.
        """
        if value in (None, ""):
            return None
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        try:
            from datetime import datetime

            return datetime.fromisoformat(str(value)).isoformat()
        except Exception:
            return str(value)

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

    try:
        PLAN20_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        PLAN20_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        (PLAN20_UPLOAD_DIR / "tmp").mkdir(parents=True, exist_ok=True)
        (PLAN20_OUTPUT_DIR / "tmp").mkdir(parents=True, exist_ok=True)

        for f in PLAN20_UPLOAD_DIR.glob("*.xlsx"):
            dest = PLAN20_UPLOAD_DIR / "tmp" / f"{f.stem}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}{f.suffix}"
            try:
                f.rename(dest)
            except OSError:
                pass
        for f in PLAN20_OUTPUT_DIR.glob("*.xlsx"):
            dest = PLAN20_OUTPUT_DIR / "tmp" / f"{f.stem}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}{f.suffix}"
            try:
                f.rename(dest)
            except OSError:
                pass

        stored_name = f"plan20_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        save_path = PLAN20_UPLOAD_DIR / stored_name
        arquivo.save(save_path)

        output_path = run_plan20(save_path, PLAN20_OUTPUT_DIR)

        registro = Plan20Upload(
            user_email=user_email,
            original_filename=arquivo.filename,
            stored_filename=stored_name,
            output_filename=output_path.name if output_path else None,
            data_arquivo=data_arquivo,
            uploaded_at=datetime.utcnow(),
        )
        db.session.add(registro)
        db.session.commit()

        # Insere dados na tabela plan20_seduc a partir do arquivo processado
        try:
            df_out = pd.read_excel(output_path, sheet_name="Plan20_SEDUC")
            if not df_out.empty:
                col_map = {
                    "Exercício": "exercicio",
                    "Programa": "programa",
                    "Função": "funcao",
                    "Unidade Orçamentária": "unidade_orcamentaria",
                    "Ação (P/A/OE)": "acao_paoe",
                    "Subfunção": "subfuncao",
                    "Objetivo Específico": "objetivo_especifico",
                    "Esfera": "esfera",
                    "Responsável pela Ação": "responsavel_acao",
                    "Produto(s) da Ação": "produto_acao",
                    "Unidade de Medida do Produto": "unid_medida_produto",
                    "Região do Produto": "regiao_produto",
                    "Meta do Produto": "meta_produto",
                    "Saldo Meta do Produto": "saldo_meta_produto",
                    "Público Transversal": "publico_transversal",
                    "Subação/entrega": "subacao_entrega",
                    "Responsável": "responsavel",
                    "Prazo": "prazo",
                    "Unid. Gestora": "unid_gestora",
                    "Unidade Setorial de Planejamento": "unidade_setorial_planejamento",
                    "Produto da Subação": "produto_subacao",
                    "Unidade de Medida": "unidade_medida",
                    "Região da Subação": "regiao_subacao",
                    "Código": "codigo",
                    "Município(s) da entrega": "municipios_entrega",
                    "Meta da Subação": "meta_subacao",
                    "Detalhamento do produto": "detalhamento_produto",
                    "Etapa": "etapa",
                    "Responsável da Etapa": "responsavel_etapa",
                    "Prazo da Etapa": "prazo_etapa",
                    "Região da Etapa": "regiao_etapa",
                    "Natureza": "natureza",
                    "Fonte": "fonte",
                    "IDU": "idu",
                    "Descrição do Item de Despesa": "descricao_item_despesa",
                    "Unid. Medida": "unid_medida_item",
                    "Quantidade": "quantidade",
                    "Valor Unitário": "valor_unitario",
                    "Valor Total": "valor_total",
                    "Chave de Planejamento": "chave_planejamento",
                    "Região": "regiao",
                    "Subfunção + UG": "subfuncao_ug",
                    "ADJ": "adj",
                    "Macropolitica": "macropolitica",
                    "Pilar": "pilar",
                    "Eixo": "eixo",
                    "Politica_Decreto": "politica_decreto",
                    "Público Transversal (chave)": "publico_transversal_chave",
                    "Cat.Econ": "cat_econ",
                    "Grupo": "grupo",
                    "Modalidade": "modalidade",
                    "Elemento": "elemento",
                    "Subelemento": "subelemento",
                }

                def _norm_col(name: str) -> str:
                    base = unicodedata.normalize("NFKD", str(name or ""))
                    ascii_only = "".join(ch for ch in base if not unicodedata.combining(ch))
                    return ascii_only.lower().replace(" ", "").replace("_", "").replace(".", "").replace("/", "")

                norm_map = {_norm_col(src): dst for src, dst in col_map.items()}
                rename_dict = {}
                for col in df_out.columns:
                    norm = _norm_col(col)
                    if norm in norm_map:
                        rename_dict[col] = norm_map[norm]
                df_out = df_out.rename(columns=rename_dict)

                meta_cols = {
                    "data_atualizacao",
                    "ano",
                    "data_arquivo",
                    "user_email",
                    "ativo",
                }
                keep_cols = list(col_map.values()) + list(meta_cols)
                for col in keep_cols:
                    if col not in df_out.columns:
                        df_out[col] = None
                df_out = df_out[[c for c in keep_cols if c in df_out.columns]]

                # Converte colunas numericas para evitar erro de cast (usa formato pt-BR)
                def _to_numeric_br(series):
                    return pd.to_numeric(
                        series.astype(str)
                        .str.replace(".", "", regex=False)
                        .str.replace(",", ".", regex=False),
                        errors="coerce",
                    )

                # Apenas colunas realmente numéricas no banco
                numeric_cols = [
                    "exercicio",
                    "quantidade",
                    "valor_unitario",
                    "valor_total",
                ]
                for col in numeric_cols:
                    if col in df_out.columns:
                        df_out[col] = _to_numeric_br(df_out[col])

                now = datetime.utcnow()
                df_out["data_atualizacao"] = now
                df_out["data_arquivo"] = data_arquivo
                df_out["user_email"] = user_email
                df_out["ativo"] = True
                if "exercicio" in df_out.columns:
                    df_out["ano"] = pd.to_numeric(df_out["exercicio"], errors="coerce")
                else:
                    df_out["ano"] = None
                # Desativa somente registros do mesmo exercicio+unidade_orcamentaria
                combos = set()
                if "unidade_orcamentaria" in df_out.columns and "exercicio" in df_out.columns:
                    for _, uo, ex in df_out[["unidade_orcamentaria", "exercicio"]].dropna().itertuples():
                        try:
                            ex_int = int(ex)
                        except (TypeError, ValueError):
                            continue
                        combos.add((str(uo).strip(), ex_int))
                for uo, ex in combos:
                    db.session.execute(
                        text(
                            "UPDATE plan20_seduc SET ativo = 0 WHERE unidade_orcamentaria = :uo AND exercicio = :ex"
                        ),
                        {"uo": uo, "ex": ex},
                    )
                db.session.commit()
                df_out.to_sql("plan20_seduc", db.engine, if_exists="append", index=False)
        except Exception as exc:
            db.session.rollback()
            return jsonify({"error": f"Plan20 processado, mas falha ao gravar no banco: {exc}"}), 500

        return jsonify(
            {
                "ok": True,
                "message": "Plan20 processado com sucesso.",
                "output": output_path.name,
                "last": {
                    "user_email": user_email,
                    "uploaded_at": _as_iso(registro.uploaded_at),
                    "data_arquivo": _as_iso(data_arquivo),
                    "original_filename": arquivo.filename,
                    "output_filename": output_path.name,
                },
            }
        )
    except Exception as exc:
        db.session.rollback()
        return jsonify({"error": f"Falha ao processar: {exc}"}), 500


@home_bp.route("/api/plan20/download/<path:filename>", methods=["GET"])
@login_required
@require_feature("atualizar/plan20-seduc")
def api_plan20_download(filename):
    target = PLAN20_OUTPUT_DIR / filename
    if not target.exists():
        abort(404)
    return send_file(target, as_attachment=True, download_name=target.name)


@home_bp.route("/api/relatorios/plan20-seduc", methods=["GET"])
@login_required
@require_feature("relatorios/plan20-seduc")
def api_relatorio_plan20():
    def _as_iso(value):
        if value in (None, ""):
            return None
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        try:
            from datetime import datetime

            return datetime.fromisoformat(str(value)).isoformat()
        except Exception:
            return str(value)

    def _to_float(val):
        try:
            if val in (None, ""):
                return 0.0
            if isinstance(val, str):
                cleaned = val.replace(".", "").replace(",", ".")
                return float(cleaned)
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    try:
        rows = (
            db.session.execute(
                text(
                    """
                    SELECT
                        exercicio,
                        chave_planejamento,
                        regiao,
                        subfuncao_ug,
                        adj,
                        macropolitica,
                        pilar,
                        eixo,
                        politica_decreto,
                        publico_transversal_chave,
                        programa,
                        funcao,
                        unidade_orcamentaria,
                        acao_paoe,
                        subfuncao,
                        objetivo_especifico,
                        esfera,
                        responsavel_acao,
                        produto_acao,
                        unid_medida_produto,
                        regiao_produto,
                        meta_produto,
                        saldo_meta_produto,
                        publico_transversal,
                        subacao_entrega,
                        responsavel,
                        prazo,
                        unid_gestora,
                        unidade_setorial_planejamento,
                        produto_subacao,
                        unidade_medida,
                        regiao_subacao,
                        codigo,
                        municipios_entrega,
                        meta_subacao,
                        detalhamento_produto,
                        etapa,
                        responsavel_etapa,
                        prazo_etapa,
                        regiao_etapa,
                        natureza,
                        cat_econ,
                        grupo,
                        modalidade,
                        elemento,
                        subelemento,
                        fonte,
                        idu,
                        descricao_item_despesa,
                        unid_medida_item,
                        quantidade,
                        valor_unitario,
                        valor_total
                    FROM plan20_seduc
                    WHERE ativo = 1
                    """
                )
            )
            .mappings()
            .all()
        )

        last_upload = Plan20Upload.query.order_by(Plan20Upload.uploaded_at.desc()).first()
        data_arquivo = _as_iso(getattr(last_upload, "data_arquivo", None)) if last_upload else None
        uploaded_at = _as_iso(getattr(last_upload, "uploaded_at", None)) if last_upload else None
        user_email = last_upload.user_email if last_upload else None

        data = []
        for r in rows:
            data.append(
                {
                    "exercicio": r.get("exercicio"),
                    "chave_planejamento": r.get("chave_planejamento"),
                    "regiao": r.get("regiao"),
                    "subfuncao_ug": r.get("subfuncao_ug"),
                    "adj": r.get("adj"),
                    "macropolitica": r.get("macropolitica"),
                    "pilar": r.get("pilar"),
                    "eixo": r.get("eixo"),
                    "politica_decreto": r.get("politica_decreto"),
                    "publico_transversal_chave": r.get("publico_transversal_chave"),
                    "programa": r.get("programa"),
                    "funcao": r.get("funcao"),
                    "unidade_orcamentaria": r.get("unidade_orcamentaria"),
                    "acao_paoe": r.get("acao_paoe"),
                    "subfuncao": r.get("subfuncao"),
                    "objetivo_especifico": r.get("objetivo_especifico"),
                    "esfera": r.get("esfera"),
                    "responsavel_acao": r.get("responsavel_acao"),
                    "produto_acao": r.get("produto_acao"),
                    "unid_medida_produto": r.get("unid_medida_produto"),
                    "regiao_produto": r.get("regiao_produto"),
                    "meta_produto": r.get("meta_produto"),
                    "saldo_meta_produto": r.get("saldo_meta_produto"),
                    "publico_transversal": r.get("publico_transversal"),
                    "subacao_entrega": r.get("subacao_entrega"),
                    "responsavel": r.get("responsavel"),
                    "prazo": r.get("prazo"),
                    "unid_gestora": r.get("unid_gestora"),
                    "unidade_setorial_planejamento": r.get("unidade_setorial_planejamento"),
                    "produto_subacao": r.get("produto_subacao"),
                    "unidade_medida": r.get("unidade_medida"),
                    "regiao_subacao": r.get("regiao_subacao"),
                    "codigo": r.get("codigo"),
                    "municipios_entrega": r.get("municipios_entrega"),
                    "meta_subacao": r.get("meta_subacao"),
                    "detalhamento_produto": r.get("detalhamento_produto"),
                    "etapa": r.get("etapa"),
                    "responsavel_etapa": r.get("responsavel_etapa"),
                    "prazo_etapa": r.get("prazo_etapa"),
                    "regiao_etapa": r.get("regiao_etapa"),
                    "natureza": r.get("natureza"),
                    "cat_econ": r.get("cat_econ"),
                    "grupo": r.get("grupo"),
                    "modalidade": r.get("modalidade"),
                    "elemento": r.get("elemento"),
                    "subelemento": r.get("subelemento"),
                    "fonte": r.get("fonte"),
                    "idu": r.get("idu"),
                    "descricao_item_despesa": r.get("descricao_item_despesa"),
                    "unid_medida_item": r.get("unid_medida_item"),
                    "quantidade": _to_float(r.get("quantidade")),
                    "valor_unitario": _to_float(r.get("valor_unitario")),
                    "valor_total": _to_float(r.get("valor_total")),
                }
            )

        return jsonify(
            {
                "ok": True,
                "data": data,
                "data_arquivo": data_arquivo,
                "uploaded_at": uploaded_at,
                "user_email": user_email,
            }
        )
    except Exception as exc:
        return jsonify({"error": f"Falha ao buscar dados: {exc}"}), 500


@home_bp.route("/api/relatorios/emp", methods=["GET"])
@login_required
@require_feature("relatorios/emp")
def api_relatorio_emp():
    def _as_iso(value):
        if value in (None, ""):
            return None
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        try:
            from datetime import datetime

            return datetime.fromisoformat(str(value)).isoformat()
        except Exception:
            return str(value)

    def _to_float(val):
        try:
            if val in (None, ""):
                return 0.0
            if isinstance(val, str):
                cleaned = val.replace(".", "").replace(",", ".")
                return float(cleaned)
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    def _format_date(val):
        if not val:
            return None
        if hasattr(val, "strftime"):
            return val.strftime("%d/%m/%Y")
        return str(val)

    try:
        rows = (
            db.session.execute(
                text(
                    """
                    SELECT
                        chave,
                        chave_planejamento,
                        regiao,
                        subfuncao_ug,
                        adj,
                        macropolitica,
                        pilar,
                        eixo,
                        politica_decreto,
                        exercicio,
                        numero_emp,
                        numero_ped,
                        valor_emp,
                        devolucao_gcv,
                        valor_emp_devolucao_gcv,
                        uo,
                        nome_unidade_orcamentaria,
                        ug,
                        nome_unidade_gestora,
                        dotacao_orcamentaria,
                        funcao,
                        subfuncao,
                        programa_governo,
                        paoe,
                        natureza_despesa,
                        cat_econ,
                        grupo,
                        modalidade,
                        elemento,
                        fonte,
                        iduso,
                        historico,
                        tipo_despesa,
                        credor,
                        nome_credor,
                        cpf_cnpj_credor,
                        categoria_credor,
                        tipo_empenho,
                        situacao,
                        data_emissao,
                        data_criacao,
                        numero_contrato,
                        numero_convenio
                    FROM emp
                    WHERE ativo = 1
                    """
                )
            )
            .mappings()
            .all()
        )
        last_upload = (
            EmpRegistro.query.filter_by(ativo=True)
            .order_by(EmpRegistro.created_at.desc())
            .first()
        )
        data_arquivo = _as_iso(last_upload.data_arquivo) if last_upload else None
        uploaded_at = _as_iso(last_upload.created_at) if last_upload else None
        user_email = last_upload.user_email if last_upload else None
        data = []
        for r in rows:
            data.append(
                {
                    "chave": r.get("chave"),
                    "chave_planejamento": r.get("chave_planejamento"),
                    "regiao": r.get("regiao"),
                    "subfuncao_ug": r.get("subfuncao_ug"),
                    "adj": r.get("adj"),
                    "macropolitica": r.get("macropolitica"),
                    "pilar": r.get("pilar"),
                    "eixo": r.get("eixo"),
                    "politica_decreto": r.get("politica_decreto"),
                    "exercicio": r.get("exercicio"),
                    "numero_emp": r.get("numero_emp"),
                    "numero_ped": r.get("numero_ped"),
                    "valor_emp": _to_float(r.get("valor_emp")),
                    "devolucao_gcv": _to_float(r.get("devolucao_gcv")),
                    "valor_emp_devolucao_gcv": _to_float(r.get("valor_emp_devolucao_gcv")),
                    "uo": r.get("uo"),
                    "nome_unidade_orcamentaria": r.get("nome_unidade_orcamentaria"),
                    "ug": r.get("ug"),
                    "nome_unidade_gestora": r.get("nome_unidade_gestora"),
                    "dotacao_orcamentaria": r.get("dotacao_orcamentaria"),
                    "funcao": r.get("funcao"),
                    "subfuncao": r.get("subfuncao"),
                    "programa_governo": r.get("programa_governo"),
                    "paoe": r.get("paoe"),
                    "natureza_despesa": r.get("natureza_despesa"),
                    "cat_econ": r.get("cat_econ"),
                    "grupo": r.get("grupo"),
                    "modalidade": r.get("modalidade"),
                    "elemento": r.get("elemento"),
                    "fonte": r.get("fonte"),
                    "iduso": r.get("iduso"),
                    "historico": r.get("historico"),
                    "tipo_despesa": r.get("tipo_despesa"),
                    "credor": r.get("credor"),
                    "nome_credor": r.get("nome_credor"),
                    "cpf_cnpj_credor": r.get("cpf_cnpj_credor"),
                    "categoria_credor": r.get("categoria_credor"),
                    "tipo_empenho": r.get("tipo_empenho"),
                    "situacao": r.get("situacao"),
                    "data_emissao": _format_date(r.get("data_emissao")),
                    "data_criacao": _format_date(r.get("data_criacao")),
                    "numero_contrato": r.get("numero_contrato"),
                    "numero_convenio": r.get("numero_convenio"),
                }
            )
        return jsonify(
            {
                "ok": True,
                "data": data,
                "data_arquivo": data_arquivo,
                "uploaded_at": uploaded_at,
                "user_email": user_email,
            }
        )
    except Exception as exc:
        return jsonify({"error": f"Falha ao buscar dados do EMP: {exc}"}), 500


@home_bp.route("/api/relatorios/est-emp", methods=["GET"])
@login_required
@require_feature("relatorios/est-emp")
def api_relatorio_est_emp():
    def _as_iso(value):
        if value in (None, ""):
            return None
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        try:
            from datetime import datetime

            return datetime.fromisoformat(str(value)).isoformat()
        except Exception:
            return str(value)

    def _to_float(val):
        try:
            if val in (None, ""):
                return 0.0
            if isinstance(val, str):
                cleaned = val.replace(".", "").replace(",", ".")
                return float(cleaned)
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    def _format_date(val):
        if not val:
            return None
        if hasattr(val, "strftime"):
            return val.strftime("%d/%m/%Y")
        return str(val)

    try:
        rows = (
            db.session.execute(
                text(
                    """
                    SELECT
                        exercicio,
                        numero_est,
                        numero_emp,
                        empenho_atual,
                        empenho_rp,
                        numero_ped,
                        valor_emp,
                        valor_est_emp_sem_aqs,
                        valor_est_emp_com_aqs,
                        valor_emp_liquido,
                        uo,
                        nome_unidade_orcamentaria,
                        ug,
                        nome_unidade_gestora,
                        dotacao_orcamentaria,
                        historico,
                        credor,
                        nome_credor,
                        cpf_cnpj_credor,
                        data_criacao,
                        data_emissao,
                        situacao,
                        rp
                    FROM est_emp
                    WHERE ativo = 1
                    """
                )
            )
            .mappings()
            .all()
        )
        last_upload = EstEmpUpload.query.order_by(EstEmpUpload.uploaded_at.desc()).first()
        data_arquivo = _as_iso(getattr(last_upload, "data_arquivo", None)) if last_upload else None
        uploaded_at = _as_iso(getattr(last_upload, "uploaded_at", None)) if last_upload else None
        user_email = last_upload.user_email if last_upload else None
        data = []
        for r in rows:
            data.append(
                {
                    "exercicio": r.get("exercicio"),
                    "numero_est": r.get("numero_est"),
                    "numero_emp": r.get("numero_emp"),
                    "empenho_atual": r.get("empenho_atual"),
                    "empenho_rp": r.get("empenho_rp"),
                    "numero_ped": r.get("numero_ped"),
                    "valor_emp": _to_float(r.get("valor_emp")),
                    "valor_est_emp_sem_aqs": _to_float(r.get("valor_est_emp_sem_aqs")),
                    "valor_est_emp_com_aqs": _to_float(r.get("valor_est_emp_com_aqs")),
                    "valor_emp_liquido": _to_float(r.get("valor_emp_liquido")),
                    "uo": r.get("uo"),
                    "nome_unidade_orcamentaria": r.get("nome_unidade_orcamentaria"),
                    "ug": r.get("ug"),
                    "nome_unidade_gestora": r.get("nome_unidade_gestora"),
                    "dotacao_orcamentaria": r.get("dotacao_orcamentaria"),
                    "historico": r.get("historico"),
                    "credor": r.get("credor"),
                    "nome_credor": r.get("nome_credor"),
                    "cpf_cnpj_credor": r.get("cpf_cnpj_credor"),
                    "data_criacao": _format_date(r.get("data_criacao")),
                    "data_emissao": _format_date(r.get("data_emissao")),
                    "situacao": r.get("situacao"),
                    "rp": r.get("rp"),
                }
            )
        return jsonify(
            {
                "ok": True,
                "data": data,
                "data_arquivo": data_arquivo,
                "uploaded_at": uploaded_at,
                "user_email": user_email,
            }
        )
    except Exception as exc:
        return jsonify({"error": f"Falha ao buscar dados do Est EMP: {exc}"}), 500


@home_bp.route("/api/relatorios/nob", methods=["GET"])
@login_required
@require_feature("relatorios/nob")
def api_relatorio_nob():
    def _as_iso(value):
        if not value:
            return None
        if isinstance(value, str) and value.startswith("0000-00-00"):
            return None
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        try:
            return datetime.fromisoformat(str(value)).isoformat()
        except Exception:
            return str(value)

    def _to_float(val):
        try:
            if val in (None, ""):
                return 0.0
            if isinstance(val, str):
                cleaned = val.replace(".", "").replace(",", ".")
                return float(cleaned)
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    def _format_date(val):
        if not val:
            return None
        if hasattr(val, "strftime"):
            return val.strftime("%d/%m/%Y")
        return str(val)

    def _format_datetime(val):
        if not val:
            return None
        if hasattr(val, "strftime"):
            return val.strftime("%d/%m/%Y %H:%M:%S")
        return str(val)

    try:
        rows = (
            db.session.execute(
                text(
                    """
                    SELECT
                        exercicio,
                        numero_nob,
                        numero_nob_estorno,
                        numero_liq,
                        numero_emp,
                        empenho_atual,
                        empenho_rp,
                        numero_ped,
                        valor_nob,
                        devolucao_gcv,
                        valor_nob_gcv,
                        uo,
                        ug,
                        dotacao_orcamentaria,
                        funcao,
                        subfuncao,
                        programa_governo,
                        paoe,
                        natureza_despesa,
                        cat_econ,
                        grupo,
                        modalidade,
                        elemento,
                        nome_elemento_despesa,
                        fonte,
                        nome_fonte_recurso,
                        iduso,
                        historico_liq,
                        nome_credor_principal,
                        cpf_cnpj_credor_principal,
                        credor,
                        nome_credor,
                        cpf_cnpj_credor,
                        data_nob,
                        data_cadastro_nob,
                        data_hora_cadastro_liq
                    FROM nob
                    WHERE ativo = 1
                    """
                )
            )
            .mappings()
            .all()
        )
        last_upload = NobUpload.query.order_by(NobUpload.uploaded_at.desc()).first()
        data_arquivo = _as_iso(getattr(last_upload, "data_arquivo", None)) if last_upload else None
        uploaded_at = _as_iso(getattr(last_upload, "uploaded_at", None)) if last_upload else None
        user_email = last_upload.user_email if last_upload else None
        data = []
        for r in rows:
            data.append(
                {
                    "exercicio": r.get("exercicio"),
                    "numero_nob": r.get("numero_nob"),
                    "numero_nob_estorno": r.get("numero_nob_estorno"),
                    "numero_liq": r.get("numero_liq"),
                    "numero_emp": r.get("numero_emp"),
                    "empenho_atual": r.get("empenho_atual"),
                    "empenho_rp": r.get("empenho_rp"),
                    "numero_ped": r.get("numero_ped"),
                    "valor_nob": _to_float(r.get("valor_nob")),
                    "devolucao_gcv": _to_float(r.get("devolucao_gcv")),
                    "valor_nob_gcv": _to_float(r.get("valor_nob_gcv")),
                    "uo": r.get("uo"),
                    "ug": r.get("ug"),
                    "dotacao_orcamentaria": r.get("dotacao_orcamentaria"),
                    "funcao": r.get("funcao"),
                    "subfuncao": r.get("subfuncao"),
                    "programa_governo": r.get("programa_governo"),
                    "paoe": r.get("paoe"),
                    "natureza_despesa": r.get("natureza_despesa"),
                    "cat_econ": r.get("cat_econ"),
                    "grupo": r.get("grupo"),
                    "modalidade": r.get("modalidade"),
                    "elemento": r.get("elemento"),
                    "nome_elemento_despesa": r.get("nome_elemento_despesa"),
                    "fonte": r.get("fonte"),
                    "nome_fonte_recurso": r.get("nome_fonte_recurso"),
                    "iduso": r.get("iduso"),
                    "historico_liq": r.get("historico_liq"),
                    "nome_credor_principal": r.get("nome_credor_principal"),
                    "cpf_cnpj_credor_principal": r.get("cpf_cnpj_credor_principal"),
                    "credor": r.get("credor"),
                    "nome_credor": r.get("nome_credor"),
                    "cpf_cnpj_credor": r.get("cpf_cnpj_credor"),
                    "data_nob": _format_date(r.get("data_nob")),
                    "data_cadastro_nob": _format_date(r.get("data_cadastro_nob")),
                    "data_hora_cadastro_liq": _format_datetime(r.get("data_hora_cadastro_liq")),
                }
            )
        return jsonify(
            {
                "ok": True,
                "data": data,
                "data_arquivo": data_arquivo,
                "uploaded_at": uploaded_at,
                "user_email": user_email,
            }
        )
    except Exception as exc:
        return jsonify({"error": f"Falha ao buscar dados do NOB: {exc}"}), 500


@home_bp.route("/api/relatorios/nob/download", methods=["GET"])
@login_required
@require_feature("relatorios/nob")
def api_relatorio_nob_download():
    def _to_float(val):
        try:
            if val in (None, ""):
                return 0.0
            if isinstance(val, str):
                cleaned = val.replace(".", "").replace(",", ".")
                return float(cleaned)
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    def _format_date(val):
        if not val:
            return None
        if hasattr(val, "strftime"):
            return val.strftime("%d/%m/%Y")
        return str(val)

    def _format_datetime(val):
        if not val:
            return None
        if hasattr(val, "strftime"):
            return val.strftime("%d/%m/%Y %H:%M:%S")
        return str(val)

    try:
        rows = (
            db.session.execute(
                text(
                    """
                    SELECT
                        exercicio,
                        numero_nob,
                        numero_nob_estorno,
                        numero_liq,
                        numero_emp,
                        empenho_atual,
                        empenho_rp,
                        numero_ped,
                        valor_nob,
                        devolucao_gcv,
                        valor_nob_gcv,
                        uo,
                        ug,
                        dotacao_orcamentaria,
                        funcao,
                        subfuncao,
                        programa_governo,
                        paoe,
                        natureza_despesa,
                        cat_econ,
                        grupo,
                        modalidade,
                        elemento,
                        nome_elemento_despesa,
                        fonte,
                        nome_fonte_recurso,
                        iduso,
                        historico_liq,
                        nome_credor_principal,
                        cpf_cnpj_credor_principal,
                        credor,
                        nome_credor,
                        cpf_cnpj_credor,
                        data_nob,
                        data_cadastro_nob,
                        data_hora_cadastro_liq
                    FROM nob
                    WHERE ativo = 1
                    """
                )
            )
            .mappings()
            .all()
        )
        if not rows:
            return jsonify({"error": "Nenhum dado para exportar."}), 404

        df = pd.DataFrame(rows)
        for col in ("valor_nob", "devolucao_gcv", "valor_nob_gcv"):
            if col in df.columns:
                df[col] = df[col].apply(_to_float)
        for col in ("data_nob", "data_cadastro_nob"):
            if col in df.columns:
                df[col] = df[col].apply(_format_date)
        if "data_hora_cadastro_liq" in df.columns:
            df["data_hora_cadastro_liq"] = df["data_hora_cadastro_liq"].apply(_format_datetime)

        rename_map = {
            "exercicio": "Exercicio",
            "numero_nob": "Nº NOB",
            "numero_nob_estorno": "Nº NOB Estorno/Estornado",
            "numero_liq": "Nº LIQ",
            "numero_emp": "Nº EMP",
            "empenho_atual": "Empenho Atual",
            "empenho_rp": "Empenho RP",
            "numero_ped": "Nº PED",
            "valor_nob": "Valor NOB",
            "devolucao_gcv": "Devolucao GCV",
            "valor_nob_gcv": "Valor NOB - GCV",
            "uo": "UO",
            "ug": "UG",
            "dotacao_orcamentaria": "Dotacao Orcamentaria",
            "funcao": "Funcao",
            "subfuncao": "Subfuncao",
            "programa_governo": "Programa de Governo",
            "paoe": "PAOE",
            "natureza_despesa": "Natureza de Despesa",
            "cat_econ": "Cat.Econ",
            "grupo": "Grupo",
            "modalidade": "Modalidade",
            "elemento": "Elemento",
            "nome_elemento_despesa": "Nome do Elemento da Despesa",
            "fonte": "Fonte",
            "nome_fonte_recurso": "Nome da Fonte de Recurso",
            "iduso": "Iduso",
            "historico_liq": "Historico LIQ",
            "nome_credor_principal": "Nome do Credor Principal",
            "cpf_cnpj_credor_principal": "CPF/CNPJ do Credor Principal",
            "credor": "Credor",
            "nome_credor": "Nome do Credor",
            "cpf_cnpj_credor": "CPF/CNPJ do Credor",
            "data_nob": "Data NOB",
            "data_cadastro_nob": "Data Cadastro NOB",
            "data_hora_cadastro_liq": "Data/Hora de Cadastro da LIQ",
        }
        df.rename(columns=rename_map, inplace=True)

        col_order = [
            "Exercicio",
            "Nº NOB",
            "Nº NOB Estorno/Estornado",
            "Nº LIQ",
            "Nº EMP",
            "Empenho Atual",
            "Empenho RP",
            "Nº PED",
            "Valor NOB",
            "Devolucao GCV",
            "Valor NOB - GCV",
            "UO",
            "UG",
            "Dotacao Orcamentaria",
            "Funcao",
            "Subfuncao",
            "Programa de Governo",
            "PAOE",
            "Natureza de Despesa",
            "Cat.Econ",
            "Grupo",
            "Modalidade",
            "Elemento",
            "Nome do Elemento da Despesa",
            "Fonte",
            "Nome da Fonte de Recurso",
            "Iduso",
            "Historico LIQ",
            "Nome do Credor Principal",
            "CPF/CNPJ do Credor Principal",
            "Credor",
            "Nome do Credor",
            "CPF/CNPJ do Credor",
            "Data NOB",
            "Data Cadastro NOB",
            "Data/Hora de Cadastro da LIQ",
        ]
        col_order = [c for c in col_order if c in df.columns]
        if col_order:
            df = df[col_order]

        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="NOB", header=False, startrow=1)
            workbook = writer.book
            worksheet = writer.sheets["NOB"]
            cell_fmt = workbook.add_format({"font_name": "Helvetica", "font_size": 8})
            header_fmt = workbook.add_format({"font_name": "Helvetica", "font_size": 8})
            worksheet.set_default_row(12, cell_fmt)
            if len(df.columns) > 0:
                worksheet.set_column(0, len(df.columns) - 1, None, cell_fmt)
                worksheet.write_row(0, 0, df.columns, header_fmt)
                worksheet.set_row(0, None, header_fmt)
        output.seek(0)
        filename = f"nob_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as exc:
        return jsonify({"error": f"Falha ao exportar: {exc}"}), 500


@home_bp.route("/api/relatorios/emp/download", methods=["GET"])
@login_required
@require_feature("relatorios/emp")
def api_relatorio_emp_download():
    def _to_float(val):
        try:
            if val in (None, ""):
                return 0.0
            if isinstance(val, str):
                cleaned = val.replace(".", "").replace(",", ".")
                return float(cleaned)
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    def _format_date(val):
        if not val:
            return None
        if hasattr(val, "strftime"):
            return val.strftime("%d/%m/%Y")
        return str(val)

    try:
        rows = (
            db.session.execute(
                text(
                    """
                    SELECT
                        chave,
                        chave_planejamento,
                        regiao,
                        subfuncao_ug,
                        adj,
                        macropolitica,
                        pilar,
                        eixo,
                        politica_decreto,
                        exercicio,
                        numero_emp,
                        numero_ped,
                        valor_emp,
                        devolucao_gcv,
                        valor_emp_devolucao_gcv,
                        uo,
                        nome_unidade_orcamentaria,
                        ug,
                        nome_unidade_gestora,
                        dotacao_orcamentaria,
                        funcao,
                        subfuncao,
                        programa_governo,
                        paoe,
                        natureza_despesa,
                        cat_econ,
                        grupo,
                        modalidade,
                        elemento,
                        fonte,
                        iduso,
                        historico,
                        tipo_despesa,
                        credor,
                        nome_credor,
                        cpf_cnpj_credor,
                        categoria_credor,
                        tipo_empenho,
                        situacao,
                        data_emissao,
                        data_criacao,
                        numero_contrato,
                        numero_convenio
                    FROM emp
                    WHERE ativo = 1
                    """
                )
            )
            .mappings()
            .all()
        )
        if not rows:
            return jsonify({"error": "Nenhum dado para exportar."}), 404

        df = pd.DataFrame(rows)

        def _chave_display(row):
            try:
                ex = int(str(row.get("exercicio") or 0)[:4])
            except Exception:
                ex = 0
            chave = row.get("chave") or ""
            chave_plan = row.get("chave_planejamento") or ""
            if ex >= 2026 and chave:
                return chave
            return chave_plan or chave

        df["Chave / Chave de Planejamento"] = df.apply(_chave_display, axis=1)
        df.drop(columns=["chave", "chave_planejamento"], inplace=True, errors="ignore")
        for col in ("valor_emp", "devolucao_gcv", "valor_emp_devolucao_gcv"):
            if col in df.columns:
                df[col] = df[col].apply(_to_float)
        for col in ("data_emissao", "data_criacao"):
            if col in df.columns:
                df[col] = df[col].apply(_format_date)

        rename_map = {
            "regiao": "Regiao",
            "subfuncao_ug": "Subfuncao + UG",
            "adj": "ADJ",
            "macropolitica": "Macropolitica",
            "pilar": "Pilar",
            "eixo": "Eixo",
            "politica_decreto": "Politica_Decreto",
            "exercicio": "Exercicio",
            "numero_emp": "Nº EMP",
            "numero_ped": "Nº PED",
            "valor_emp": "Valor EMP",
            "devolucao_gcv": "Devolucao GCV",
            "valor_emp_devolucao_gcv": "Valor EMP-Devolucao GCV",
            "uo": "UO",
            "nome_unidade_orcamentaria": "Nome da Unidade Orcamentaria",
            "ug": "UG",
            "nome_unidade_gestora": "Nome da Unidade Gestora",
            "dotacao_orcamentaria": "Dotacao Orcamentaria",
            "funcao": "Funcao",
            "subfuncao": "Subfuncao",
            "programa_governo": "Programa de Governo",
            "paoe": "PAOE",
            "natureza_despesa": "Natureza de Despesa",
            "cat_econ": "Cat.Econ",
            "grupo": "Grupo",
            "modalidade": "Modalidade",
            "elemento": "Elemento",
            "fonte": "Fonte",
            "iduso": "Iduso",
            "historico": "Historico",
            "tipo_despesa": "Tipo de Despesa",
            "credor": "Credor",
            "nome_credor": "Nome do Credor",
            "cpf_cnpj_credor": "CPF/CNPJ do Credor",
            "categoria_credor": "Categoria do Credor",
            "tipo_empenho": "Tipo Empenho",
            "situacao": "Situacao",
            "data_emissao": "Data emissao",
            "data_criacao": "Data criacao",
            "numero_contrato": "Nº Contrato",
            "numero_convenio": "Nº Convênio",
        }
        df.rename(columns=rename_map, inplace=True)

        col_order = [
            "Chave / Chave de Planejamento",
            "Regiao",
            "Subfuncao + UG",
            "ADJ",
            "Macropolitica",
            "Pilar",
            "Eixo",
            "Politica_Decreto",
            "Exercicio",
            "Nº EMP",
            "Nº PED",
            "Valor EMP",
            "Devolucao GCV",
            "Valor EMP-Devolucao GCV",
            "UO",
            "Nome da Unidade Orcamentaria",
            "UG",
            "Nome da Unidade Gestora",
            "Dotacao Orcamentaria",
            "Funcao",
            "Subfuncao",
            "Programa de Governo",
            "PAOE",
            "Natureza de Despesa",
            "Cat.Econ",
            "Grupo",
            "Modalidade",
            "Elemento",
            "Fonte",
            "Iduso",
            "Historico",
            "Tipo de Despesa",
            "Credor",
            "Nome do Credor",
            "CPF/CNPJ do Credor",
            "Categoria do Credor",
            "Tipo Empenho",
            "Situacao",
            "Data emissao",
            "Data criacao",
            "Nº Contrato",
            "Nº Convênio",
        ]
        col_order = [c for c in col_order if c in df.columns]
        if col_order:
            df = df[col_order]

        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="EMP", header=False, startrow=1)
            workbook = writer.book
            worksheet = writer.sheets["EMP"]
            cell_fmt = workbook.add_format({"font_name": "Helvetica", "font_size": 8})
            header_fmt = workbook.add_format({"font_name": "Helvetica", "font_size": 8})
            worksheet.set_default_row(12, cell_fmt)
            if len(df.columns) > 0:
                worksheet.set_column(0, len(df.columns) - 1, None, cell_fmt)
                worksheet.write_row(0, 0, df.columns, header_fmt)
                worksheet.set_row(0, None, header_fmt)
        output.seek(0)
        filename = f"emp_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as exc:
        return jsonify({"error": f"Falha ao exportar: {exc}"}), 500


@home_bp.route("/api/relatorios/est-emp/download", methods=["GET"])
@login_required
@require_feature("relatorios/est-emp")
def api_relatorio_est_emp_download():
    def _to_float(val):
        try:
            if val in (None, ""):
                return 0.0
            if isinstance(val, str):
                cleaned = val.replace(".", "").replace(",", ".")
                return float(cleaned)
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    def _format_date(val):
        if not val:
            return None
        if hasattr(val, "strftime"):
            return val.strftime("%d/%m/%Y")
        return str(val)

    try:
        rows = (
            db.session.execute(
                text(
                    """
                    SELECT
                        exercicio,
                        numero_est,
                        numero_emp,
                        empenho_atual,
                        empenho_rp,
                        numero_ped,
                        valor_emp,
                        valor_est_emp_sem_aqs,
                        valor_est_emp_com_aqs,
                        valor_emp_liquido,
                        uo,
                        nome_unidade_orcamentaria,
                        ug,
                        nome_unidade_gestora,
                        dotacao_orcamentaria,
                        historico,
                        credor,
                        nome_credor,
                        cpf_cnpj_credor,
                        data_criacao,
                        data_emissao,
                        situacao,
                        rp
                    FROM est_emp
                    WHERE ativo = 1
                    """
                )
            )
            .mappings()
            .all()
        )
        if not rows:
            return jsonify({"error": "Nenhum dado para exportar."}), 404

        df = pd.DataFrame(rows)
        for col in (
            "valor_emp",
            "valor_est_emp_sem_aqs",
            "valor_est_emp_com_aqs",
            "valor_emp_liquido",
        ):
            if col in df.columns:
                df[col] = df[col].apply(_to_float)
        for col in ("data_criacao", "data_emissao"):
            if col in df.columns:
                df[col] = df[col].apply(_format_date)

        rename_map = {
            "exercicio": "Exercicio",
            "numero_est": "Nº EST",
            "numero_emp": "Nº EMP",
            "empenho_atual": "Empenho Atual",
            "empenho_rp": "Empenho RP",
            "numero_ped": "Nº PED",
            "valor_emp": "Valor EMP",
            "valor_est_emp_sem_aqs": "Valor Est EMP (A LIQ/Em LIQ sem AQS)",
            "valor_est_emp_com_aqs": "Valor Est EMP (Em LIQ com AQS)",
            "valor_emp_liquido": "Valor EMP - (A LIQ/Em LIQ sem AQS) - (Em LIQ com AQS)",
            "uo": "UO",
            "nome_unidade_orcamentaria": "Nome da Unidade Orcamentaria",
            "ug": "UG",
            "nome_unidade_gestora": "Nome da Unidade Gestora",
            "dotacao_orcamentaria": "Dotacao Orcamentaria",
            "historico": "Historico",
            "credor": "Credor",
            "nome_credor": "Nome do Credor",
            "cpf_cnpj_credor": "CPF/CNPJ do Credor",
            "data_criacao": "Data Criacao",
            "data_emissao": "Data Emissao",
            "situacao": "Situacao",
            "rp": "RP",
        }
        df.rename(columns=rename_map, inplace=True)

        col_order = [
            "Exercicio",
            "Nº EST",
            "Nº EMP",
            "Empenho Atual",
            "Empenho RP",
            "Nº PED",
            "Valor EMP",
            "Valor Est EMP (A LIQ/Em LIQ sem AQS)",
            "Valor Est EMP (Em LIQ com AQS)",
            "Valor EMP - (A LIQ/Em LIQ sem AQS) - (Em LIQ com AQS)",
            "UO",
            "Nome da Unidade Orcamentaria",
            "UG",
            "Nome da Unidade Gestora",
            "Dotacao Orcamentaria",
            "Historico",
            "Credor",
            "Nome do Credor",
            "CPF/CNPJ do Credor",
            "Data Criacao",
            "Data Emissao",
            "Situacao",
            "RP",
        ]
        col_order = [c for c in col_order if c in df.columns]
        if col_order:
            df = df[col_order]

        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="EST_EMP", header=False, startrow=1)
            workbook = writer.book
            worksheet = writer.sheets["EST_EMP"]
            cell_fmt = workbook.add_format({"font_name": "Helvetica", "font_size": 8})
            header_fmt = workbook.add_format({"font_name": "Helvetica", "font_size": 8})
            worksheet.set_default_row(12, cell_fmt)
            if len(df.columns) > 0:
                worksheet.set_column(0, len(df.columns) - 1, None, cell_fmt)
                worksheet.write_row(0, 0, df.columns, header_fmt)
                worksheet.set_row(0, None, header_fmt)
        output.seek(0)
        filename = f"est_emp_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as exc:
        return jsonify({"error": f"Falha ao exportar: {exc}"}), 500


@home_bp.route("/api/relatorios/plan20-seduc/download", methods=["GET"])
@login_required
@require_feature("relatorios/plan20-seduc")
def api_relatorio_plan20_download():
    def _to_float(val):
        try:
            if val in (None, ""):
                return 0.0
            if isinstance(val, str):
                cleaned = val.replace(".", "").replace(",", ".")
                return float(cleaned)
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    try:
        rows = (
            db.session.execute(
                text(
                    """
                    SELECT
                        exercicio,
                        chave_planejamento,
                        regiao,
                        subfuncao_ug,
                        adj,
                        macropolitica,
                        pilar,
                        eixo,
                        politica_decreto,
                        publico_transversal_chave,
                        programa,
                        funcao,
                        unidade_orcamentaria,
                        acao_paoe,
                        subfuncao,
                        objetivo_especifico,
                        esfera,
                        responsavel_acao,
                        produto_acao,
                        unid_medida_produto,
                        regiao_produto,
                        meta_produto,
                        saldo_meta_produto,
                        publico_transversal,
                        subacao_entrega,
                        responsavel,
                        prazo,
                        unid_gestora,
                        unidade_setorial_planejamento,
                        produto_subacao,
                        unidade_medida,
                        regiao_subacao,
                        codigo,
                        municipios_entrega,
                        meta_subacao,
                        detalhamento_produto,
                        etapa,
                        responsavel_etapa,
                        prazo_etapa,
                        regiao_etapa,
                        natureza,
                        cat_econ,
                        grupo,
                        modalidade,
                        elemento,
                        subelemento,
                        fonte,
                        idu,
                        descricao_item_despesa,
                        unid_medida_item,
                        quantidade,
                        valor_unitario,
                        valor_total
                    FROM plan20_seduc
                    WHERE ativo = 1
                    """
                )
            )
            .mappings()
            .all()
        )

        headers = [
            ("Exercício", "exercicio"),
            ("Chave de Planejamento", "chave_planejamento"),
            ("Região", "regiao"),
            ("Subfunção + UG", "subfuncao_ug"),
            ("ADJ", "adj"),
            ("Macropolitica", "macropolitica"),
            ("Pilar", "pilar"),
            ("Eixo", "eixo"),
            ("Politica_Decreto", "politica_decreto"),
            ("Público Transversal (chave)", "publico_transversal_chave"),
            ("Programa", "programa"),
            ("Função", "funcao"),
            ("Unidade Orçamentária", "unidade_orcamentaria"),
            ("Ação (P/A/OE)", "acao_paoe"),
            ("Subfunção", "subfuncao"),
            ("Objetivo Específico", "objetivo_especifico"),
            ("Esfera", "esfera"),
            ("Responsável pela Ação", "responsavel_acao"),
            ("Produto(s) da Ação", "produto_acao"),
            ("Unidade de Medida do Produto", "unid_medida_produto"),
            ("Região do Produto", "regiao_produto"),
            ("Meta do Produto", "meta_produto"),
            ("Saldo Meta do Produto", "saldo_meta_produto"),
            ("Público Transversal", "publico_transversal"),
            ("Subação/entrega", "subacao_entrega"),
            ("Responsável", "responsavel"),
            ("Prazo", "prazo"),
            ("Unid. Gestora", "unid_gestora"),
            ("Unidade Setorial de Planejamento", "unidade_setorial_planejamento"),
            ("Produto da Subação", "produto_subacao"),
            ("Unidade de Medida", "unidade_medida"),
            ("Região da Subação", "regiao_subacao"),
            ("Código", "codigo"),
            ("Município(s) da entrega", "municipios_entrega"),
            ("Meta da Subação", "meta_subacao"),
            ("Detalhamento do produto", "detalhamento_produto"),
            ("Etapa", "etapa"),
            ("Responsável da Etapa", "responsavel_etapa"),
            ("Prazo da Etapa", "prazo_etapa"),
            ("Região da Etapa", "regiao_etapa"),
            ("Natureza", "natureza"),
            ("Cat.Econ", "cat_econ"),
            ("Grupo", "grupo"),
            ("Modalidade", "modalidade"),
            ("Elemento", "elemento"),
            ("Subelemento", "subelemento"),
            ("Fonte", "fonte"),
            ("IDU", "idu"),
            ("Descrição do Item de Despesa", "descricao_item_despesa"),
            ("Unid. Medida", "unid_medida_item"),
            ("Quantidade", "quantidade"),
            ("Valor Unitário", "valor_unitario"),
            ("Valor Total", "valor_total"),
        ]

        data = []
        for r in rows:
            row_dict = {}
            for label, key in headers:
                val = r.get(key)
                if key in {"quantidade", "valor_unitario", "valor_total"}:
                    val = _to_float(val)
                row_dict[label] = val
            data.append(row_dict)

        df = None
        try:
            import pandas as pd
            from io import BytesIO
            from flask import send_file
            from openpyxl import load_workbook
            from openpyxl.styles import Font

            df = pd.DataFrame(data, columns=[h[0] for h in headers])
            output = BytesIO()
            df.to_excel(output, index=False)
            output.seek(0)

            wb = load_workbook(output)
            ws = wb.active
            font = Font(name="Helvetica", size=8)
            idx_map = {label: i + 1 for i, (label, _) in enumerate(headers)}
            numeric_cols = {
                idx_map.get("Quantidade"),
                idx_map.get("Valor Unitário"),
                idx_map.get("Valor Total"),
            }
            numeric_cols = {c for c in numeric_cols if c}
            number_format = "#,##0.00"
            for row in ws.iter_rows():
                for cell in row:
                    cell.font = font
                    if cell.col_idx in numeric_cols and isinstance(cell.value, (int, float)):
                        cell.number_format = number_format
                    if cell.col_idx == idx_map.get("Exercício") and isinstance(cell.value, (int, float, str)):
                        try:
                            cell.value = int(str(cell.value).split(".")[0])
                        except Exception:
                            pass
                        cell.number_format = "0"

            styled = BytesIO()
            wb.save(styled)
            styled.seek(0)

            filename = f"plan20_seduc_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
            return send_file(
                styled,
                as_attachment=True,
                download_name=filename,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as exc:
            return jsonify({"error": f"Falha ao preparar planilha: {exc}"}), 500
    except Exception as exc:
        return jsonify({"error": f"Falha ao exportar: {exc}"}), 500


@home_bp.route("/api/usuarios", methods=["POST"])
@login_required
@require_feature("usuarios/cadastrar")
def api_criar_usuario():
    data = request.get_json() or {}
    email = (data.get("email") or "").lower().strip()
    nome = (data.get("nome") or "").strip()
    perfil = (data.get("perfil") or "").strip()
    senha = (data.get("senha") or "").strip()
    ativo = bool(data.get("ativo", True))

    if not email or not nome or not perfil or not senha:
        return jsonify({"error": "Campos obrigatorios ausentes."}), 400

    caller_nivel = getattr(g, "user_nivel", None)
    # Apenas nivel 1 pode criar usuario com perfil nivel 1
    if caller_nivel != 1 and _is_nivel1(perfil):
        return jsonify({"error": "Apenas admin pode criar perfil admin."}), 403

    existing = Usuario.query.filter_by(email=email).first()
    if existing:
        return jsonify({"error": "Usuario ja existe."}), 400

    usuario = Usuario(email=email, nome=nome, perfil=perfil, ativo=ativo)
    usuario.set_password(senha)
    db.session.add(usuario)
    db.session.commit()

    return jsonify({"ok": True, "message": "Usuario criado."}), 201


@home_bp.route("/api/usuarios/<email>", methods=["GET", "PUT", "DELETE", "POST"])
@login_required
def api_usuario(email):
    usuario = Usuario.query.filter_by(email=email).first()
    if not usuario:
        return jsonify({"error": "Usuario nao encontrado."}), 404

    caller_nivel = getattr(g, "user_nivel", None)
    target_is_nivel1 = _is_nivel1(usuario.perfil)
    if caller_nivel != 1 and target_is_nivel1:
        return jsonify({"error": "Apenas admin pode alterar usuario admin."}), 403

    if request.method == "GET":
        if not (has_permission("usuarios/editar") or has_permission("usuarios/senha") or getattr(g, "user_nivel", None) == 1):
            return jsonify({"error": "Sem permissao."}), 403
        return jsonify(
            {
                "email": usuario.email,
                "nome": usuario.nome,
                "perfil": usuario.perfil,
                "ativo": usuario.ativo,
            }
        )

    if request.method == "DELETE":
        if not (has_permission("usuarios/editar") or getattr(g, "user_nivel", None) == 1):
            return jsonify({"error": "Sem permissao."}), 403
        usuario.ativo = False
        db.session.commit()
        return jsonify({"ok": True, "message": "Usuario desativado."})

    if request.method not in ("PUT", "POST"):
        return jsonify({"error": "Metodo nao permitido."}), 405

    if not (has_permission("usuarios/editar") or getattr(g, "user_nivel", None) == 1):
        return jsonify({"error": "Sem permissao."}), 403

    data = request.get_json() or {}
    usuario.nome = (data.get("nome") or usuario.nome).strip()
    novo_perfil = (data.get("perfil") or usuario.perfil).strip()
    if caller_nivel != 1 and _is_nivel1(novo_perfil):
        return jsonify({"error": "Apenas admin pode definir perfil admin."}), 403
    usuario.perfil = novo_perfil
    usuario.ativo = bool(data.get("ativo", usuario.ativo))
    nova_senha = (data.get("senha") or "").strip()
    if nova_senha:
        usuario.set_password(nova_senha)
    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        return jsonify({"error": f"Falha ao atualizar: {exc}"}), 500
    return jsonify({"ok": True, "message": "Usuario atualizado."})


@home_bp.route("/api/usuarios/<email>/senha", methods=["POST"])
@login_required
def api_usuario_senha(email):
    usuario = Usuario.query.filter_by(email=email).first()
    if not usuario:
        return jsonify({"error": "Usuario nao encontrado."}), 404
    caller_nivel = getattr(g, "user_nivel", None)
    target_is_nivel1 = _is_nivel1(usuario.perfil)
    if caller_nivel != 1 and target_is_nivel1:
        return jsonify({"error": "Apenas admin pode alterar usuario admin."}), 403
    if not (has_permission("usuarios/senha") or getattr(g, "user_nivel", None) == 1):
        return jsonify({"error": "Sem permissao."}), 403

    data = request.get_json() or {}
    senha_atual = (data.get("senha_atual") or "").strip()
    senha_nova = (data.get("senha_nova") or "").strip()
    senha_confirmar = (data.get("senha_confirmar") or "").strip()

    if not senha_atual or not senha_nova or not senha_confirmar:
        return jsonify({"error": "Preencha todos os campos de senha."}), 400
    if senha_nova != senha_confirmar:
        return jsonify({"error": "Confirmacao diferente da nova senha."}), 400
    if not usuario.check_password(senha_atual):
        return jsonify({"error": "Senha atual incorreta."}), 400

    usuario.set_password(senha_nova)
    db.session.commit()
    return jsonify({"ok": True, "message": "Senha atualizada."})


@home_bp.route("/api/perfis", methods=["GET", "POST"])
@login_required
@role_required("admin")
def api_perfis():
    if request.method == "GET":
        perfis = Perfil.query.order_by(Perfil.nivel, Perfil.nome).all()
        return jsonify(
            [
                {"id": p.id, "nome": p.nome, "nivel": p.nivel, "ativo": p.ativo}
                for p in perfis
            ]
        )
    data = request.get_json() or {}
    nome = (data.get("nome") or "").strip()
    nivel = data.get("nivel")
    ativo = bool(data.get("ativo", True))
    if not nome or nivel is None:
        return jsonify({"error": "Campos obrigatorios ausentes."}), 400
    existing = Perfil.query.filter_by(nome=nome).first()
    if existing:
        if existing.ativo:
            return jsonify({"error": "Perfil ja existe."}), 400
        # Reaproveita perfil inativo existente
        existing.nivel = int(nivel)
        existing.ativo = ativo
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            return jsonify({"error": "Perfil ja existe."}), 400
        return jsonify({"ok": True, "message": "Perfil ativado.", "id": existing.id}), 200
    try:
        nivel_int = int(nivel)
    except (TypeError, ValueError):
        return jsonify({"error": "Nivel invalido."}), 400
    if nivel_int < 1 or nivel_int > 4:
        return jsonify({"error": "Nivel deve estar entre 1 e 4."}), 400
    perfil = Perfil(id=_next_pk(Perfil), nome=nome, nivel=nivel_int, ativo=ativo)
    db.session.add(perfil)
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        return jsonify({"error": "Perfil ja existe."}), 400
    return jsonify({"ok": True, "message": "Perfil criado.", "id": perfil.id}), 201


@home_bp.route("/api/perfis/<int:perfil_id>", methods=["PUT", "DELETE"])
@login_required
@role_required("admin")
def api_perfil(perfil_id):
    perfil = db.session.get(Perfil, perfil_id)
    if not perfil:
        return jsonify({"error": "Perfil nao encontrado."}), 404

    if request.method == "DELETE":
        try:
            PerfilPermissao.query.filter_by(perfil_id=perfil_id).delete()
            db.session.delete(perfil)
            db.session.commit()
            return jsonify({"ok": True, "message": "Perfil excluido."})
        except IntegrityError:
            db.session.rollback()
            return jsonify({"error": "Não foi possível excluir este perfil."}), 400

    data = request.get_json() or {}
    nome = (data.get("nome") or perfil.nome).strip()
    nivel = data.get("nivel", perfil.nivel)
    ativo = bool(data.get("ativo", perfil.ativo))
    existing = Perfil.query.filter(Perfil.nome == nome, Perfil.id != perfil_id).first()
    if existing:
        if existing.ativo:
            return jsonify({"error": "Perfil ja existe."}), 400
        # remover perfil inativo duplicado para liberar nome
        db.session.delete(existing)
    try:
        nivel_int = int(nivel)
    except (TypeError, ValueError):
        return jsonify({"error": "Nivel invalido."}), 400
    if nivel_int < 1 or nivel_int > 4:
        return jsonify({"error": "Nivel deve estar entre 1 e 4."}), 400

    perfil.nome = nome
    perfil.nivel = nivel_int
    perfil.ativo = ativo
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        return jsonify({"error": "Perfil ja existe."}), 400
    return jsonify({"ok": True, "message": "Perfil atualizado."})
    if chave_field == "chave_planejamento":
        ped_rows = [r for r in ped_rows if r]
        ped_rows = (
            PedRegistro.query.with_entities(PedRegistro.valor_ped)
            .filter(
                PedRegistro.ativo == True,  # noqa: E712
                PedRegistro.exercicio == exercicio,
                PedRegistro.programa_governo == programa_key,
                PedRegistro.paoe == acao_paoe_key,
                PedRegistro.fonte == fonte,
                PedRegistro.iduso == iduso,
                PedRegistro.uo == uo_norm,
                PedRegistro.subfuncao_ug.like(f"%.{ug_norm}"),
                PedRegistro.regiao == regiao,
                PedRegistro.chave_planejamento == chave_planejamento,
            )
            .all()
        )
    else:
        ped_rows = (
            PedRegistro.query.with_entities(PedRegistro.valor_ped)
            .filter(
                PedRegistro.ativo == True,  # noqa: E712
                PedRegistro.exercicio == exercicio,
                PedRegistro.programa_governo == programa_key,
                PedRegistro.paoe == acao_paoe_key,
                PedRegistro.fonte == fonte,
                PedRegistro.iduso == iduso,
                PedRegistro.uo == uo_norm,
                PedRegistro.subfuncao_ug.like(f"%.{ug_norm}"),
                PedRegistro.regiao == regiao,
                PedRegistro.chave == chave_planejamento,
            )
            .all()
        )
    valor_ped = sum((_dec_or_zero(r.valor_ped) for r in ped_rows), Decimal("0"))
    ped_count = len(ped_rows)

