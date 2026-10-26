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

# Plano de novas UOs (docs/plano_implementacao_novas_uos.md).
# UOs da secretaria: as unicas em que as regras de Chave de Planejamento
# valem (decisao D2). Outra UO fica "NAO IDENTIFICADO".
UOS_SEDUC = set(UOS_ACEITAS)
# UOs possiveis de entrar nos modulos de execucao e no Plan20: 26101
# (SECITECI), 26201 (UNEMAT) e 26202 (FAPEMAT) + as da secretaria. O
# Teto-SEDUC continua com UOS_ACEITAS (decisao D3: duas listas).
UOS_EXECUCAO = UOS_SEDUC | {"26101", "26201", "26202"}
UOS_PLAN20 = set(UOS_EXECUCAO)
# UOs LIBERADAS no Plan20. Enquanto a confirmacao oficial das novas UOs nao
# sair, so a secretaria. Para liberar: `set(UOS_PLAN20)` (Fase 3 do plano).
# TESTE EM HOMOLOGACAO: 26101 liberada (reverter antes de ir para a main).
UOS_PLAN20_LIBERADAS = set(UOS_SEDUC) | {"26101"}
# UOs LIBERADAS para upload nos modulos de execucao. Enquanto a confirmacao
# oficial das novas UOs nao sair, so a secretaria. Para liberar, trocar por
# `set(UOS_EXECUCAO)` (Fase 3 do plano) - nao ha outro lugar a alterar.
# TESTE EM HOMOLOGACAO: 26101 liberada (reverter antes de ir para a main).
UOS_EXECUCAO_LIBERADAS = set(UOS_SEDUC) | {"26101"}

# Rotulo curto para exibicao (filtros/dashboard) - mesmas siglas que o
# FIPLAN usa na coluna "U.O" do Plan 134.
UO_NOMES = {
    "14101": "14101 - SEDUC",
    "14601": "14601 - FMTE",
    "26101": "26101 - SECITECI",
    "26201": "26201 - UNEMAT",
    "26202": "26202 - FAPEMAT",
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


def uo_codigo(valor: Any) -> str:
    """Código da UO com 5 dígitos. Cargas antigas do EST EMP guardaram a UO
    sem o zero à esquerda ("1101" em vez de "01101"); a conta é a mesma."""
    digitos = uo_key(valor)
    return digitos.zfill(5) if digitos else ""


def uo_usa_chave(valor: Any) -> bool:
    """Se as regras de Chave de Planejamento valem para a UO (decisão D2).
    Único ponto de decisão: quando outra UO passar a ter chave, trocar o
    interior desta função (hoje lista fixa; depois, cadastro de UO)."""
    return uo_codigo(valor) in UOS_SEDUC


def exercicio_4d(valor: Any) -> str:
    """"26", "2026", 2026 e "2026.0" -> "2026". Vazio/inválido -> ""."""
    m = re.match(r"\s*(\d+)", str(valor or ""))
    if not m:
        return ""
    n = int(m.group(1))
    if n < 100:
        n += 2000
    return str(n) if 2000 <= n <= 2100 else ""


class UORecusada(ValueError):
    """Arquivo recusado pela UO (ausente, não liberada ou mais de uma)."""


def validar_uo_execucao(uos: Any, liberadas: set[str] | None = None) -> str:
    """Valida a UO lida do CONTEÚDO do relatório (nunca do nome do arquivo)
    e devolve o código único. Recusa: UO ausente, mais de uma UO, UO não
    cadastrada e UO cadastrada mas ainda não liberada."""
    liberadas = UOS_EXECUCAO_LIBERADAS if liberadas is None else liberadas
    encontradas = sorted({c for c in (uo_codigo(u) for u in uos) if c})
    aceitas = ", ".join(sorted(liberadas))
    if not encontradas:
        raise UORecusada("Não foi possível identificar a UO no relatório. Nada foi gravado.")
    if len(encontradas) > 1:
        raise UORecusada(
            "O arquivo tem mais de uma UO (" + ", ".join(encontradas) + "). "
            "Envie um relatório por UO. Nada foi gravado."
        )
    uo = encontradas[0]
    if uo in liberadas:
        return uo
    if uo in UOS_EXECUCAO:
        raise UORecusada(f"A UO {uo} ainda não está liberada neste módulo (liberadas: {aceitas}). Nada foi gravado.")
    raise UORecusada(f"UO {uo} não cadastrada (liberadas: {aceitas}). Nada foi gravado.")
