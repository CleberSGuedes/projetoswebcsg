"""Cadastro DRE/Escola das Notas SEE: importação e identificação da escola de cada DANFE.

Regras (o código de lotação é a única chave confiável; nomes se repetem entre municípios/DREs):
- Nota com código: valida o código no cadastro e confere a DRE. Divergência vira alerta,
  sem corrigir automaticamente.
- Nota sem código: só preenche quando, no mesmo município + DRE, existe exatamente uma
  escola com o mesmo nome (ignorando tipo EE/EECM/EEI, acentos e abreviações). Nome apenas
  parecido gera alerta com sugestão, nunca preenchimento.
"""
from __future__ import annotations

import difflib
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

# Entregas feitas na própria DRE usam estes códigos nas notas (não constam no cadastro de escolas).
DRE_CODIGOS = {
    "1": "DRE ALTA FLORESTA",
    "2": "DRE BARRA DO GARCAS",
    "3": "DRE CACERES",
    "4": "DRE CONFRESA",
    "5": "DRE METROPOLITANA",
    "6": "DRE DIAMANTINO",
    "7": "DRE JUINA",
    "8": "DRE MATUPA",
    "9": "DRE PONTES E LACERDA",
    "10": "DRE PRIMAVERA DO LESTE",
    "12": "DRE RONDONOPOLIS",
    "13": "DRE SINOP",
    "14": "DRE TANGARA DA SERRA",
}

_TIPO_PREFIXO = r"^(?:ESCOLA ESTADUAL INDIGENA|ESCOLA ESTADUAL|EEEMTI|EECM|EEI|EIEE|CEJA|EE)\s+"
_ABREVIACOES = [
    (r"\bPROFESSORA?\b|\bPROFA?\b|\bPROFO\b", "PROF"),
    (r"\bDOUTORA?\b|\bDRA?\b", "DR"),
    (r"\bCOMENDADOR\b|\bCOM\b", "COM"),
    (r"\bSENADOR\b|\bSEN\b", "SEN"),
    (r"\bDEPUTADO\b|\bDEP\b", "DEP"),
    (r"\bCORONEL\b|\bCEL\b", "CEL"),
    (r"\bPADRE\b|\bPE\b", "PE"),
    (r"\bMONSENHOR\b|\bMONS\b", "MONS"),
    (r"\bGOVERNADOR\b|\bGOV\b", "GOV"),
    (r"\bSANTO\b|\bSTO\b|\bSAO\b", "S"),
    (r"\b(?:DE|DA|DO|DAS|DOS|E)\b", " "),
]
SUGESTAO_MIN_SIMILARIDADE = 0.75
SUGESTAO_MIN_VANTAGEM = 0.15


def normalizar(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).upper()
    text = text.replace("º", "O").replace("ª", "A").replace("´", "").replace("'", "")
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def chave_dre(value: str | None) -> str:
    """DRE normalizada sem o prefixo 'DRE' (algumas notas trazem só o nome)."""
    return re.sub(r"^DRE\s+", "", normalizar(value))


def nucleo_nome(value: str | None) -> str:
    """Nome da escola sem o tipo (EE, EECM...) e com abreviações padronizadas."""
    text = re.sub(_TIPO_PREFIXO, "", normalizar(value))
    for pattern, replacement in _ABREVIACOES:
        text = re.sub(pattern, replacement, text)
    return re.sub(r"\s+", " ", text).strip()


@dataclass
class Escola:
    codigo: str
    nome: str
    municipio: str | None
    dre: str
    tipo: str = "escola"
    ativo: bool = True


@dataclass
class EscolaIndex:
    por_codigo: dict[str, Escola] = field(default_factory=dict)
    por_local: dict[tuple[str, str], list[Escola]] = field(default_factory=lambda: defaultdict(list))
    dres_por_municipio: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    dres: set[str] = field(default_factory=set)

    @classmethod
    def build(cls, escolas: list[Escola]) -> "EscolaIndex":
        index = cls()
        for escola in escolas:
            # Códigos inativos continuam válidos para notas antigas; a busca por nome usa só as ativas.
            index.por_codigo[escola.codigo] = escola
            index.dres.add(chave_dre(escola.dre))
            if escola.tipo == "escola" and escola.ativo:
                municipio = normalizar(escola.municipio)
                index.por_local[(municipio, chave_dre(escola.dre))].append(escola)
                index.dres_por_municipio[municipio].add(escola.dre)
        return index

    def __bool__(self) -> bool:
        return bool(self.por_codigo)


def _warning(codigo: str, mensagem: str) -> dict:
    return {"codigo": codigo, "mensagem": mensagem}


def identificar_escola(metadata: dict, index: EscolaIndex) -> dict:
    """Confere/completa DRE e escola da nota usando o cadastro.

    Retorna codigo, nome (oficial quando identificada), dre, municipio, origem do código,
    alertas e destaque ('sugestao' | 'divergente' | 'nao_encontrada' | None) para a planilha.
    """
    codigo = (metadata.get("codigo_escola") or "").strip() or None
    dre_nota = metadata.get("dre")
    result = {
        "codigo": codigo,
        "nome": metadata.get("nome_escola"),
        "dre": dre_nota,
        "municipio": metadata.get("municipio"),
        "origem_codigo": "nota" if codigo else None,
        "warnings": [],
        "destaque": None,
        "conferencia": None,
    }
    if not index:
        return result

    def alert(code: str, message: str, destaque: str) -> dict:
        result["warnings"].append(_warning(code, message))
        result["destaque"] = result["destaque"] or destaque
        result["conferencia"] = result["conferencia"] or message
        return result

    if dre_nota and chave_dre(dre_nota) not in index.dres:
        alert("DRE_FORA_DO_CADASTRO", f"DRE '{dre_nota}' não consta no cadastro de DREs.", "divergente")

    if codigo:
        escola = index.por_codigo.get(codigo)
        if not escola:
            return alert("ESCOLA_CODIGO_NAO_CADASTRADO", f"Código {codigo} não consta no cadastro de escolas.", "nao_encontrada")
        if dre_nota and chave_dre(escola.dre) != chave_dre(dre_nota):
            return alert(
                "ESCOLA_DRE_DIVERGENTE",
                f"Código {codigo} pertence a {escola.dre} no cadastro, mas a nota informa {dre_nota}.",
                "divergente",
            )
        result.update(nome=escola.nome, dre=escola.dre, municipio=escola.municipio or result["municipio"])
        if not escola.ativo:
            result["warnings"].append(_warning("ESCOLA_INATIVA", f"Código {codigo} ({escola.nome}) está inativo no cadastro atual."))
        return result

    nome_nota = metadata.get("nome_escola")
    municipio = normalizar(metadata.get("municipio"))
    if not nome_nota or not municipio or not dre_nota:
        return result
    candidatas = index.por_local.get((municipio, chave_dre(dre_nota)), [])
    if not candidatas:
        outras = sorted(index.dres_por_municipio.get(municipio, set()))
        if outras:
            return alert(
                "ESCOLA_MUNICIPIO_DRE_INCONSISTENTE",
                f"O município {metadata.get('municipio')} pertence a {', '.join(outras)} no cadastro, mas a nota informa {dre_nota}.",
                "divergente",
            )
        return alert(
            "ESCOLA_NAO_ENCONTRADA",
            f"Nenhuma escola cadastrada em {metadata.get('municipio')} / {dre_nota}.",
            "nao_encontrada",
        )
    nucleo = nucleo_nome(nome_nota)
    iguais = [escola for escola in candidatas if nucleo_nome(escola.nome) == nucleo]
    if len(iguais) == 1:
        escola = iguais[0]
        result.update(codigo=escola.codigo, nome=escola.nome, dre=escola.dre, municipio=escola.municipio, origem_codigo="cadastro (nome + município)")
        return result
    ranking = sorted(
        ((difflib.SequenceMatcher(None, nucleo, nucleo_nome(escola.nome)).ratio(), escola) for escola in candidatas),
        key=lambda item: item[0],
        reverse=True,
    )
    melhor, segunda = ranking[0], (ranking[1][0] if len(ranking) > 1 else 0)
    if melhor[0] >= SUGESTAO_MIN_SIMILARIDADE and melhor[0] - segunda >= SUGESTAO_MIN_VANTAGEM:
        sugerida = melhor[1]
        return alert(
            "ESCOLA_SUGESTAO",
            f"Escola '{nome_nota}' não encontrada com o mesmo nome. Sugestão: {sugerida.codigo} - {sugerida.nome} ({sugerida.municipio}). Confira e informe o código.",
            "sugestao",
        )
    return alert(
        "ESCOLA_NAO_ENCONTRADA",
        f"Escola '{nome_nota}' não encontrada no cadastro de {metadata.get('municipio')} / {dre_nota}.",
        "nao_encontrada",
    )


def ler_planilha_escolas(path: Path, sheet: str = "2026") -> list[Escola]:
    """Lê a planilha de matrículas (colunas DRE, Municipio, LotacaoID, Lotacao)."""
    raw = pd.read_excel(path, sheet_name=sheet, dtype=str, header=None)
    header_row = next(
        (idx for idx in range(min(len(raw), 30)) if "LotacaoID" in [str(value).strip() for value in raw.iloc[idx]]),
        None,
    )
    if header_row is None:
        raise ValueError(f"Coluna 'LotacaoID' não encontrada na aba {sheet}.")
    df = pd.read_excel(path, sheet_name=sheet, dtype=str, header=header_row)
    df.columns = [str(column).strip() for column in df.columns]
    missing = {"DRE", "Municipio", "LotacaoID", "Lotacao"} - set(df.columns)
    if missing:
        raise ValueError(f"Colunas ausentes na aba {sheet}: {', '.join(sorted(missing))}.")
    escolas: dict[str, Escola] = {}
    conflitos = []
    for row in df[["DRE", "Municipio", "LotacaoID", "Lotacao"]].dropna(subset=["LotacaoID"]).itertuples(index=False):
        codigo = str(row.LotacaoID).strip()
        if not codigo.isdigit():
            continue
        escola = Escola(codigo=codigo, nome=str(row.Lotacao).strip(), municipio=str(row.Municipio).strip(), dre=str(row.DRE).strip())
        atual = escolas.get(codigo)
        if atual and (atual.nome, atual.municipio, atual.dre) != (escola.nome, escola.municipio, escola.dre):
            conflitos.append(codigo)
        escolas.setdefault(codigo, escola)
    if conflitos:
        raise ValueError(f"Códigos com dados divergentes na planilha: {', '.join(sorted(set(conflitos))[:20])}.")
    for codigo, dre in DRE_CODIGOS.items():
        escolas.setdefault(codigo, Escola(codigo=codigo, nome=dre, municipio=None, dre=dre, tipo="dre"))
    return list(escolas.values())
