"""Tempo de inatividade da sessao (configuravel pelo Admin).

O valor fica na tabela `configuracoes_sistema` (chave `sessao_timeout_minutos`).
Sem linha, ou em caso de falha do banco, vale o padrao de 120 minutos (2 horas).
O valor e lido com cache curto para nao pesar em toda requisicao.
"""

import threading
import time
from datetime import timedelta

CHAVE_TIMEOUT = "sessao_timeout_minutos"
PADRAO_MINUTOS = 120
MIN_MINUTOS = 15
MAX_MINUTOS = 720  # 12 horas
CACHE_TTL_S = 30

_lock = threading.Lock()
_cache = {"expires_at": 0.0, "minutos": PADRAO_MINUTOS}


def _clamp(minutos) -> int:
    try:
        valor = int(minutos)
    except (TypeError, ValueError):
        return PADRAO_MINUTOS
    return max(MIN_MINUTOS, min(MAX_MINUTOS, valor))


def invalidar_cache() -> None:
    with _lock:
        _cache["expires_at"] = 0.0


def get_timeout_minutos() -> int:
    agora = time.monotonic()
    with _lock:
        if _cache["expires_at"] > agora:
            return int(_cache["minutos"])
        ultimo = int(_cache["minutos"])
    minutos = ultimo
    try:
        from models import ConfiguracaoSistema, db

        row = db.session.get(ConfiguracaoSistema, CHAVE_TIMEOUT)
        minutos = _clamp(row.valor) if row else PADRAO_MINUTOS
    except Exception:
        # Banco indisponivel (ou tabela ainda nao criada): mantem o ultimo valor conhecido.
        try:
            from models import db

            db.session.rollback()
        except Exception:
            pass
    with _lock:
        _cache["minutos"] = minutos
        _cache["expires_at"] = agora + CACHE_TTL_S
    return minutos


def get_session_timeout() -> timedelta:
    return timedelta(minutes=get_timeout_minutos())


def set_timeout_minutos(minutos, usuario_email: str | None = None) -> int:
    """Valida e grava o novo tempo. Levanta ValueError se estiver fora dos limites."""
    try:
        valor = int(minutos)
    except (TypeError, ValueError):
        raise ValueError("Informe o tempo em minutos (numero inteiro).")
    if valor < MIN_MINUTOS or valor > MAX_MINUTOS:
        raise ValueError(f"O tempo deve ficar entre {MIN_MINUTOS} e {MAX_MINUTOS} minutos.")
    from models import ConfiguracaoSistema, db

    row = db.session.get(ConfiguracaoSistema, CHAVE_TIMEOUT)
    if row is None:
        row = ConfiguracaoSistema(chave=CHAVE_TIMEOUT, valor=str(valor))
        db.session.add(row)
    else:
        row.valor = str(valor)
    row.atualizado_por = (usuario_email or "")[:255] or None
    from datetime import datetime

    row.atualizado_em = datetime.utcnow()
    db.session.commit()
    invalidar_cache()
    return valor


def _formatar_local(valor_utc) -> str | None:
    """A data e gravada em UTC (como o resto do banco); exibe no horario de Mato Grosso."""
    if not valor_utc:
        return None
    import pytz

    local = pytz.utc.localize(valor_utc).astimezone(pytz.timezone("America/Cuiaba"))
    return local.strftime("%d/%m/%Y %H:%M")


def get_info() -> dict:
    """Valor atual + quem/quando alterou (para a tela de configuracao)."""
    info = {
        "minutos": get_timeout_minutos(),
        "padrao": PADRAO_MINUTOS,
        "min": MIN_MINUTOS,
        "max": MAX_MINUTOS,
        "atualizado_em": None,
        "atualizado_por": None,
    }
    try:
        from models import ConfiguracaoSistema, db

        row = db.session.get(ConfiguracaoSistema, CHAVE_TIMEOUT)
        if row:
            info["atualizado_em"] = _formatar_local(row.atualizado_em)
            info["atualizado_por"] = row.atualizado_por
    except Exception:
        try:
            from models import db

            db.session.rollback()
        except Exception:
            pass
    return info
