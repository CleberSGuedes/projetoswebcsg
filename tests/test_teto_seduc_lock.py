"""Reproduz a condicao de corrida do Teto-SEDUC (duas gravacoes concorrentes na
tabela `momp` para o mesmo exercicio) usando o mesmo mecanismo de trava
(GET_LOCK/RELEASE_LOCK por exercicio) adotado em
rotas/home_routes.py::_start_teto_seduc_thread, e confirma que ela impede a
duplicacao. Usa um exercicio descartavel (9997) no banco remoto compartilhado
e remove os dados de teste ao final, independente do resultado.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from app import create_app
from models import db, Momp

TEST_EXERCICIO = "9997"
TEST_FONTE = "TESTE-LOCK"
CHAVE = dict(
    exercicio=TEST_EXERCICIO,
    fonte=TEST_FONTE,
    grupo_despesa="TESTE",
    teto_despesa_momp="TESTE",
    subteto_despesa_momp="TESTE",
)


def _persistir_como_teto_seduc(session, hold_seconds: float = 0.0) -> None:
    """Mesma sequencia de _persistir_plan23: adquire lock por exercicio,
    procura registros ativos com a mesma chave, desativa e insere um novo."""
    lock_name = f"teto_seduc:{TEST_EXERCICIO}"
    obtido = session.execute(
        text("SELECT GET_LOCK(:name, :timeout)"), {"name": lock_name, "timeout": 10}
    ).scalar()
    assert obtido, "nao conseguiu obter o lock dentro do timeout"
    try:
        antigos = session.query(Momp).filter_by(**CHAVE).all()
        old_ids = [m.id for m in antigos]

        if hold_seconds:
            time.sleep(hold_seconds)  # simula o tempo de processamento real

        novo = Momp(**CHAVE, teto_anual=999, ativo=True)
        session.add(novo)
        session.flush()

        if old_ids:
            session.query(Momp).filter(Momp.id.in_(old_ids)).update(
                {Momp.ativo: False, Momp.excluido_em: datetime.utcnow()},
                synchronize_session=False,
            )
        session.commit()
    finally:
        session.execute(text("SELECT RELEASE_LOCK(:name)"), {"name": lock_name})


def test_lock_impede_duplicidade_sob_concorrencia():
    app = create_app()
    with app.app_context():
        Session = sessionmaker(bind=db.engine)
        sessao_a = Session()
        sessao_b = Session()
        erros: list[Exception] = []

        def rodar_a() -> None:
            try:
                _persistir_como_teto_seduc(sessao_a, hold_seconds=1.0)
            except Exception as exc:  # pragma: no cover - reportado via assert abaixo
                erros.append(exc)

        def rodar_b() -> None:
            time.sleep(0.2)  # garante que A ja adquiriu o lock primeiro
            try:
                _persistir_como_teto_seduc(sessao_b, hold_seconds=0.0)
            except Exception as exc:  # pragma: no cover
                erros.append(exc)

        try:
            thread_a = threading.Thread(target=rodar_a)
            thread_b = threading.Thread(target=rodar_b)
            thread_a.start()
            thread_b.start()
            thread_a.join(timeout=15)
            thread_b.join(timeout=15)

            assert not erros, f"erros durante execucao concorrente: {erros}"

            verificacao = Session()
            ativos = (
                verificacao.query(Momp)
                .filter_by(exercicio=TEST_EXERCICIO, fonte=TEST_FONTE, ativo=True)
                .all()
            )
            verificacao.close()
            assert len(ativos) == 1, (
                "esperado exatamente 1 registro ativo apos as duas gravacoes "
                f"concorrentes, encontrado {len(ativos)} - a trava nao esta "
                "evitando a duplicidade"
            )
        finally:
            limpeza = Session()
            limpeza.query(Momp).filter_by(
                exercicio=TEST_EXERCICIO, fonte=TEST_FONTE
            ).delete()
            limpeza.commit()
            limpeza.close()
            sessao_a.close()
            sessao_b.close()
