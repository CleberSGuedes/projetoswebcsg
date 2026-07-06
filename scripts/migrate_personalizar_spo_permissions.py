"""Migra a permissão legada de personalização para a tela de temas."""

from datetime import datetime
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import app
from models import db, NivelPermissao, PerfilPermissao, UsuarioPermissao


OLD_FEATURE = "atualizar/personalizar-spo"
NEW_FEATURE = "atualizar/personalizar-spo/temas"


def migrate_model(model, identity_fields):
    migrated = 0
    legacy_rows = model.query.filter_by(feature=OLD_FEATURE, ativo=True).all()
    for legacy in legacy_rows:
        identity = {field: getattr(legacy, field) for field in identity_fields}
        target = model.query.filter_by(feature=NEW_FEATURE, **identity).first()
        if target:
            target.ativo = True
            if hasattr(target, "permitido") and hasattr(legacy, "permitido"):
                target.permitido = legacy.permitido
        else:
            values = {**identity, "feature": NEW_FEATURE, "ativo": True}
            if hasattr(legacy, "permitido"):
                values["permitido"] = legacy.permitido
            if model is PerfilPermissao:
                values["created_at"] = datetime.utcnow()
            db.session.add(model(**values))
        legacy.ativo = False
        migrated += 1
    return migrated


with app.app_context():
    total = 0
    total += migrate_model(PerfilPermissao, ("perfil_id",))
    total += migrate_model(NivelPermissao, ("nivel",))
    total += migrate_model(UsuarioPermissao, ("usuario_id",))
    db.session.commit()
    print(f"Migração concluída: {total} permissão(ões) legada(s) processada(s).")
