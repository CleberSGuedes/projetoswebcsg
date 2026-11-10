"""Plan20 com as UOs novas (docs/plano_implementacao_novas_uos.md, Fase 2).

Planilhas sintéticas em diretório temporário; nada é gravado no banco.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest

from services import uo as uo_mod
from services.plan20_runner import run_plan20, verificar_uo_plan20
from services.uo import UORecusada
from tests.test_plan20_runner import _blank, _bloco_acao, _bloco_subacao, _row


def _gera(root: Path, uo_texto: str, exercicio: int = 2027) -> Path:
    cod = uo_texto.split(" - ")[0]
    rows = [
        _row("PLAN 20 - Relatório do PTA"),
        _blank(),
        _row(f"*Exercício igual a {exercicio}"),
        _row(f"Código da Unidade Orcamentária igual a {cod}"),
        _blank(), _blank(),
    ]
    rows += _bloco_acao("007", "7007", 300.0, "Item novo", uo=uo_texto)
    rows.append(_row("PLANO DE AÇÃO POR PRODUTO: Produto novo"))
    rows += [_blank(), _blank()]
    rows += _bloco_subacao(1, "Subação sem chave de planejamento", 300.0, "Item novo")
    entrada = root / f"plan20_{cod}.xlsx"
    pd.DataFrame(rows).to_excel(entrada, sheet_name="FIPLAN", index=False, header=False)
    return run_plan20(entrada, root / "saida")


def test_uo_nova_nao_liberada_e_recusada_com_motivo():
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        saida = _gera(Path(tmp), "26201 - UNEMAT")
        assert len(pd.read_excel(saida, sheet_name="Plan20_SEDUC")) == 0
        with pytest.raises(UORecusada, match="ainda não está liberada"):
            verificar_uo_plan20(saida)


def test_uo_nao_cadastrada_e_recusada():
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        saida = _gera(Path(tmp), "99999 - OUTRA")
        with pytest.raises(UORecusada, match="não cadastrada"):
            verificar_uo_plan20(saida)


def test_liberando_a_uo_nova_ela_e_gravavel(monkeypatch):
    monkeypatch.setattr(uo_mod, "UOS_PLAN20_LIBERADAS", set(uo_mod.UOS_PLAN20))
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        saida = _gera(Path(tmp), "26201 - UNEMAT")
        p20 = pd.read_excel(saida, sheet_name="Plan20_SEDUC")
        assert len(p20) == 1
        assert p20.iloc[0]["Unidade Orçamentária"].startswith("26201")
        assert verificar_uo_plan20(saida) == ""


def test_secretaria_continua_sem_aviso():
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        saida = _gera(Path(tmp), "14101 - SECRETARIA DE ESTADO DE EDUCAÇÃO")
        assert len(pd.read_excel(saida, sheet_name="Plan20_SEDUC")) == 1
        assert verificar_uo_plan20(saida) == ""
