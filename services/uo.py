"""Unidades Orçamentárias (UO) da secretaria - regra única usada pelo Plan20
e pelo Teto-SEDUC (docs/claude.md, seções 14, 17.2 e 20)."""

from __future__ import annotations

import re
from typing import Any

# UOs aceitas nos relatórios FIPLAN da secretaria. 14101 = Secretaria de
# Estado de Educação; 14601 = Fundo Estadual de Apoio à Melhoria das
# Condições de Oferta da Educação Infantil, Ensino Fundamental e Ensino
# Médio no MT (FMTE) - vinculado à SEDUC, mas é uma UO própria (a partir de
# 2027 também no Teto-SEDUC). Uma UO nova da secretaria precisa ser
# adicionada aqui.
UOS_ACEITAS = {"14101", "14601"}

# Rotulo curto para exibicao (filtros/dashboard) - mesmas siglas que o
# FIPLAN usa na coluna "U.O" do Plan 134.
UO_NOMES = {
    "14101": "14101 - SEDUC",
    "14601": "14601 - FMTE",
}


def uo_label(codigo: Any) -> str:
    codigo = uo_key(codigo)
    return UO_NOMES.get(codigo, codigo)


def uo_key(valor: Any) -> str:
    """Extrai só os dígitos do código da UO (antes do " - "), ignorando
    pontuação. A empresa que gera o relatório mudou de "14.101 - ..." pra
    "14101 - ..." no layout 2027 - comparar por igualdade de texto exato
    quebrava silenciosamente com essa mudança (docs/claude.md, seção 14).
    """
    s = str(valor or "").strip()
    codigo = s.split(" - ", 1)[0] if " - " in s else s
    return re.sub(r"\D", "", codigo)
