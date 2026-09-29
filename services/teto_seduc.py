from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd

from services.uo import UOS_ACEITAS, uo_key


def _as_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value)


def _norm(value) -> str:
    return re.sub(r"\s+", " ", _as_text(value)).strip()


def _strip_accents(value) -> str:
    return "".join(
        char
        for char in unicodedata.normalize("NFKD", _as_text(value))
        if not unicodedata.combining(char)
    )


def _norm_key(value) -> str:
    return _strip_accents(_norm(value)).upper()


def _find_column(columns, target: str):
    target_key = re.sub(r"\s+", "", _norm_key(target))
    for column in columns:
        if re.sub(r"\s+", "", _norm_key(column)) == target_key:
            return column
    for column in columns:
        if target_key in re.sub(r"\s+", "", _norm_key(column)):
            return column
    return None


def _remap(series: pd.Series, mapping: dict[str, str]) -> pd.Series:
    normalized = {_norm_key(key): value for key, value in mapping.items()}
    return series.fillna("").map(lambda value: normalized.get(_norm_key(value), value))


def _only_8_digits(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    match = re.search(r"(?<!\d)(\d{8})(?!\d)", text)
    if match:
        return match.group(1)
    try:
        number = str(int(float(text)))
        return number if len(number) == 8 else None
    except (TypeError, ValueError):
        return None


def _br_to_number(value):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text.replace(".", "").replace(",", "."))
    except ValueError:
        return None


FONTE_MAP = {
    "15000000": "15000000 - Recursos não vinculados de Impostos",
    "15001001": "15001001 - Recursos destinados à Manutenção e Desenvolvimento do Ensino",
    "15010000": "15010000 - Outros Recursos não Vinculados",
    "15010100": "15010100 - Outros Recursos não vinculados destinados ao Tesouro",
    "15400000": "15400000 - Transferência de recursos do FUNDEB desenvolvimento do Ensino",
    "15401070": "15401070 - Transferência de recursos do FUNDEB Remuneração Educação Básica",
    "15460000": "15460000 - Transferências do FUNDEB - Complementação da União - ETI",
    "15500000": "15500000 - Recursos da Contribuição ao Salário Educação",
    "15510000": "15510000 - Transferências de Recursos do FNDE referente ao Programa Dinheiro Direto na Escola (PDDE)",
    "15520000": "15520000 - Transferências de Recursos do FNDE referente ao Programa Nacional de Alimentação Escolar (PNAE)",
    "15530000": "15530000 - Transferências de Recursos do FNDE referente ao P. N. de Apoio ao Transporte Escolar (PNATE)",
    "15690000": "15690000 - Outras Transferências de Recursos do FNDE",
    "15700000": "15700000 - Transferências do Governo Federal ref. a Convênios e outros Repasses vinculados à Educação",
    "15740000": "15740000 - Recursos de Operações de Crédito Educação",
}

GRUPO_PLAN23_MAP = {
    "3 - OUTRAS DESPESAS CORRENTES": "3 - Outras Despesas Correntes",
    "4 - INVESTIMENTOS": "4 - Investimentos",
    "1 - PESSOAL E ENCARGOS SOCIAIS": "1 - Pessoal e Encargos Sociais",
}

SUBTETO_PLAN23_MAP = {
    "a. Despesas Obrigatórias": "A - Despesas Obrigatórias",
    "b. Essenciais à Manutenção da Unidade": "B - Essenciais à Manutenção da Unidade",
    "c. Despesas Prioridades Estratégicas": "C - Prioridades Estratégicas LDO",
    "d. Despesas Essenciais Finalísticas": "D - Essenciais Finalísticas",
    "e. Projetos de Investimentos": "E - Projetos de Investimentos",
    "f. Demais ações e projetos": "F - Demais Ações e Projetos Finalísticos",
}


# Linhas de filtro/identificacao impressas pelo FIPLAN no topo (e no rodape)
# do Plan 23 e do Plan 134 - comparadas ja normalizadas por _norm_key (sem
# acento, maiusculas, espacos colapsados).
_RE_EXERCICIO = re.compile(r"EXERCICIO\s+IGUAL\s+A\s+(\d{4})")
_RE_UO_FILTRO = re.compile(r"CODIGO\s+DA\s+UNIDADE\s+ORCAMENTARIA\s+IGUAL\s+A\s+([\d.]+)")
_RE_UO_PLAN23 = re.compile(r"^UO\s*:\s*([\d.]+)\s*-")
_RE_UO_PLAN134 = re.compile(r"^([\d.]+)\s*-\s*\S")
_RE_TOTAL_UO_PLAN134 = re.compile(r"^SUBTOTAL\s+UO\s+[\d.]+\s*:\s*([\d.]+,\d{2})")
_RE_TOTAL_GERAL_PLAN134 = re.compile(r"^TOTAL\s+GERAL\s+([\d.]+,\d{2})")
_RE_TOTAL_UO_PLAN23 = re.compile(r"^TOTAL\s+DA\s+UO\s*:?$")


def ler_cabecalho_fiplan(input_path: Path) -> dict:
    """Le do proprio relatorio (Plan 23 ou Plan 134) o exercicio, a UO e o
    total impresso pelo FIPLAN, para o upload nao depender so do que o
    usuario digitou na tela (docs/claude.md, secao 20).

    - uo_filtro: UO da linha de filtro "Codigo da Unidade Orcamentaria igual a";
    - uos_conteudo: UOs encontradas no corpo ("UO : 14101 - ..." no Plan 23,
      coluna "U.O" de cada linha no Plan 134) - conferencia do filtro;
    - total_relatorio: "Total da UO:" (Plan 23) ou "SUBTOTAL UO"/"TOTAL
      GERAL" (Plan 134), para conferir com o que foi gravado.
    """
    raw = pd.read_excel(input_path, sheet_name=0, header=None, dtype=str)
    exercicio = None
    uo_filtro = None
    uos_conteudo: set[str] = set()
    total_uo = None
    total_geral = None
    col_uo_plan134 = None

    for _, row in raw.iterrows():
        cells = [_norm(value) for value in row.tolist()]
        keys = [_norm_key(value) for value in cells]
        for index, key in enumerate(keys):
            if not key:
                continue
            if exercicio is None:
                match = _RE_EXERCICIO.search(key)
                if match:
                    exercicio = match.group(1)
            if uo_filtro is None:
                match = _RE_UO_FILTRO.search(key)
                if match:
                    uo_filtro = uo_key(match.group(1))
            match = _RE_UO_PLAN23.match(key)
            if match:
                uos_conteudo.add(uo_key(match.group(1)))
            match = _RE_TOTAL_UO_PLAN134.match(key)
            if match:
                total_uo = _br_to_number(match.group(1))
            match = _RE_TOTAL_GERAL_PLAN134.match(key)
            if match:
                total_geral = _br_to_number(match.group(1))
            if _RE_TOTAL_UO_PLAN23.match(key):
                valores = [c for c in cells[index + 1 :] if c]
                if valores:
                    total_uo = _br_to_number(valores[0])
            if key == "U.O" and col_uo_plan134 is None:
                col_uo_plan134 = index
        if col_uo_plan134 is not None and col_uo_plan134 < len(keys):
            match = _RE_UO_PLAN134.match(keys[col_uo_plan134])
            if match:
                uos_conteudo.add(uo_key(match.group(1)))

    uos_conteudo.discard("")
    return {
        "exercicio": exercicio,
        "uo_filtro": uo_filtro or None,
        "uos_conteudo": uos_conteudo,
        "total_relatorio": total_uo if total_uo is not None else total_geral,
    }


def validar_cabecalho_fiplan(cabecalho: dict, exercicio_informado: str) -> str:
    """Confere UO e exercicio lidos do arquivo e devolve o codigo da UO.
    Levanta ValueError (mensagem mostrada ao usuario) quando o arquivo nao
    pode ser gravado com seguranca."""
    uos = set(cabecalho.get("uos_conteudo") or set())
    if cabecalho.get("uo_filtro"):
        uos.add(cabecalho["uo_filtro"])
    if not uos:
        raise ValueError(
            "Não foi possível identificar a Unidade Orçamentária no arquivo. "
            "Gere o relatório no FIPLAN filtrando pelo código da UO."
        )
    if len(uos) > 1:
        raise ValueError(
            "O arquivo contém mais de uma Unidade Orçamentária "
            f"({', '.join(sorted(uos))}). Gere um relatório para cada UO."
        )
    uo = next(iter(uos))
    if uo not in UOS_ACEITAS:
        raise ValueError(
            f"A Unidade Orçamentária {uo} não está cadastrada no Teto - SEDUC "
            f"(aceitas: {', '.join(sorted(UOS_ACEITAS))})."
        )
    exercicio_arquivo = cabecalho.get("exercicio")
    if exercicio_arquivo and exercicio_arquivo != str(exercicio_informado).strip():
        raise ValueError(
            f"O exercício informado ({exercicio_informado}) é diferente do "
            f"exercício do arquivo ({exercicio_arquivo})."
        )
    return uo


def detectar_tipo_relatorio(input_path: Path) -> str | None:
    sheets = pd.read_excel(input_path, sheet_name=None, header=None, dtype=str, nrows=100)
    for raw in sheets.values():
        for _, row in raw.iterrows():
            values = [_norm(value) for value in row.tolist()]
            row_text = " ".join(value for value in values if value)
            row_key = _norm_key(row_text)
            if "PLAN 134" in row_key:
                return "politicateto"
            if "PLAN 23" in row_key:
                return "momp"

            header_keys = {_norm_key(value) for value in values if value}
            plan134_required = {_norm_key(column) for column in PLAN134_HEADERS.values()}
            if plan134_required.issubset(header_keys):
                return "politicateto"
            if {
                _norm_key("FONTE"),
                _norm_key("GRUPO DE DESPESA / QUADRO ORÇAMENTÁRIO"),
                _norm_key("TETO ANUAL"),
                _norm_key("SALDO ANUAL"),
            }.issubset(header_keys):
                return "momp"
    return None


def processar_plan23(input_path: Path, exercicio: str) -> pd.DataFrame:
    raw = pd.read_excel(input_path, sheet_name=0, header=None, dtype=str)
    header_row = None
    for index in range(len(raw)):
        if any(_norm_key(value) == "FONTE" for value in raw.iloc[index]):
            header_row = index
            break
    if header_row is None:
        raise ValueError("Não foi encontrada a linha de cabeçalho com FONTE.")

    data = raw.iloc[header_row + 1 :].copy()
    data.columns = raw.iloc[header_row].tolist()

    col_fonte = _find_column(data.columns, "FONTE")
    col_grupo = _find_column(data.columns, "GRUPO DE DESPESA / QUADRO ORÇAMENTÁRIO")
    col_teto = _find_column(data.columns, "TETO ANUAL")
    col_saldo = _find_column(data.columns, "SALDO ANUAL")
    if not all([col_fonte, col_grupo, col_teto, col_saldo]):
        raise ValueError(
            "Cabeçalhos necessários não encontrados: FONTE, "
            "GRUPO DE DESPESA / QUADRO ORÇAMENTÁRIO, TETO ANUAL e SALDO ANUAL."
        )

    grupo_pattern = re.compile(r"^\s*\d\s*-\s+.+", re.IGNORECASE)
    quadro_pattern = re.compile(r"^\s*[a-fA-F]\.\s+.+")
    total_pattern = re.compile(r"^\s*Total\s+da\s+Fonte\s*:?\s*$", re.IGNORECASE)
    fonte_atual = None
    grupo_atual = None
    registros = []

    for _, row in data.iterrows():
        fonte_cell = row.get(col_fonte, "")
        grupo_cell = _norm(row.get(col_grupo, ""))
        teto_cell = _norm(row.get(col_teto, ""))
        saldo_cell = _norm(row.get(col_saldo, ""))

        codigo_fonte = _only_8_digits(fonte_cell)
        if codigo_fonte:
            fonte_atual = codigo_fonte
            grupo_atual = grupo_cell if grupo_pattern.match(grupo_cell) else None
            continue
        if not fonte_atual:
            continue
        if total_pattern.match(grupo_cell):
            fonte_atual = None
            grupo_atual = None
            continue
        if quadro_pattern.match(grupo_cell) and grupo_atual:
            registros.append(
                {
                    "fonte": fonte_atual,
                    "grupo_despesa": grupo_atual,
                    "subteto_despesa_momp": grupo_cell,
                    "teto_anual": teto_cell,
                    "_saldo_anual": saldo_cell,
                }
            )

    resultado = pd.DataFrame(registros)
    if resultado.empty:
        raise ValueError("Nenhum registro de teto foi encontrado no Plan 23.")

    resultado["_teto_num"] = resultado["teto_anual"].apply(_br_to_number)
    resultado = resultado[resultado["_teto_num"].fillna(0) != 0].copy()
    resultado["fonte"] = resultado["fonte"].map(lambda value: FONTE_MAP.get(value, value))
    resultado["grupo_despesa"] = _remap(resultado["grupo_despesa"], GRUPO_PLAN23_MAP)
    resultado["subteto_despesa_momp"] = _remap(
        resultado["subteto_despesa_momp"], SUBTETO_PLAN23_MAP
    )
    resultado.insert(0, "exercicio", exercicio)
    resultado.insert(
        resultado.columns.get_loc("grupo_despesa") + 1,
        "teto_despesa_momp",
        "4 - A Classificar",
    )
    resultado["teto_anual"] = resultado["_teto_num"]
    return resultado[
        [
            "exercicio",
            "fonte",
            "grupo_despesa",
            "teto_despesa_momp",
            "subteto_despesa_momp",
            "teto_anual",
        ]
    ]


PLAN134_HEADERS = {
    "acao": "Ação (PAOE)",
    "produto": "Produto da Ação",
    "subacao": "Subação/entrega",
    "fonte": "Fonte",
    "grupo": "Grupo",
    "subteto": "Tipificação da Despesa",
    "valor": "Valor PTA",
}

GRUPO_PLAN134_MAP = {
    "4-INVESTIMENTOS": "4 - Investimentos",
    "3-OUTRAS DESPESAS CORRENTES": "3 - Outras Despesas Correntes",
    "1-PESSOAL E ENCARGOS SOCIAIS": "1 - Pessoal e Encargos Sociais",
}

SUBTETO_PLAN134_MAP = {
    "Demais Ações e Projetos": "F - Demais Ações e Projetos Finalísticos",
    "Despesas Essenciais Finalísticas": "D - Essenciais Finalísticas",
    "Despesas Obrigatórias": "A - Despesas Obrigatórias",
    "Despesas Prioridades Estratégicas": "C - Prioridades Estratégicas LDO",
    "Essenciais à Manutenção da Unidade": "B - Essenciais à Manutenção da Unidade",
}

_RE_ACAO_CODIGO = re.compile(r"^(\d+)\s*-\s*(.*)$")


def normalizar_acao(value) -> str:
    """Padroniza o nome da Acao (PAOE) do Plan 134: o FIPLAN imprime
    "2009 -Manutencao ..." (sem espaco depois do traco) e as vezes com ponto
    final solto. Regra em vez de lista fixa, para cobrir acoes novas
    (ex.: 4537, 4538, 4541, 4545) sem precisar cadastrar uma a uma
    (docs/claude.md, secao 20). Texto sem codigo numerico fica como esta."""
    text = _norm(value)
    match = _RE_ACAO_CODIGO.match(text)
    if not match:
        return text
    descricao = match.group(2).strip().rstrip(".").strip()
    return f"{match.group(1)} - {descricao}"


def _split_subacao(value) -> tuple[str, str]:
    if pd.isna(value):
        return "", ""
    text = str(value)
    first = text.find("*")
    last = text.rfind("*")
    if first == -1:
        return "", text.strip()
    if last > first:
        return text[first : last + 1].strip(), text[last + 1 :].strip()
    return text[first:].strip(), text[:first].strip()


def _parse_chave(value) -> tuple[str, ...]:
    parts = [part.strip() for part in str(value or "").split("*") if part.strip()]
    return tuple((parts + [""] * 8)[:8])


def processar_plan134(input_path: Path) -> pd.DataFrame:
    sheets = pd.read_excel(input_path, sheet_name=None, header=None, dtype=str)
    if not sheets:
        raise ValueError("O arquivo não possui planilhas.")

    required = list(PLAN134_HEADERS.values())
    data = None
    for raw in sheets.values():
        for index in range(min(len(raw), 100)):
            header_values = [_norm(value) for value in raw.iloc[index].tolist()]
            header_keys = {_norm_key(value) for value in header_values if value}
            if all(_norm_key(column) in header_keys for column in required):
                data = raw.iloc[index + 1 :].copy()
                data.columns = header_values
                data = data.reset_index(drop=True)
                break
        if data is not None:
            break

    if data is None:
        raise ValueError(
            "Não foi localizada uma linha de cabeçalho válida no Plan 134. "
            f"Colunas esperadas: {required}"
        )

    missing = [column for column in required if column not in data.columns]
    if missing:
        raise ValueError(f"Colunas ausentes no Plan 134: {missing}")

    for column in required[:-1]:
        data[column] = data[column].fillna("").astype(str).str.strip()
    data[PLAN134_HEADERS["valor"]] = data[PLAN134_HEADERS["valor"]].apply(_br_to_number)

    group_columns = [
        PLAN134_HEADERS["acao"],
        PLAN134_HEADERS["produto"],
        PLAN134_HEADERS["subacao"],
        PLAN134_HEADERS["fonte"],
        PLAN134_HEADERS["grupo"],
        PLAN134_HEADERS["subteto"],
    ]
    data = (
        data.groupby(group_columns, dropna=False, as_index=False)[PLAN134_HEADERS["valor"]]
        .sum(min_count=1)
    )

    chaves = data[PLAN134_HEADERS["subacao"]].map(_split_subacao)
    data["chave_planejamento"] = chaves.map(lambda item: item[0])
    data = (
        data.groupby(
            [
                "chave_planejamento",
                PLAN134_HEADERS["acao"],
                PLAN134_HEADERS["produto"],
                PLAN134_HEADERS["fonte"],
                PLAN134_HEADERS["grupo"],
                PLAN134_HEADERS["subteto"],
            ],
            dropna=False,
            as_index=False,
        )[PLAN134_HEADERS["valor"]]
        .sum(min_count=1)
    )

    partes = data["chave_planejamento"].map(_parse_chave)
    data["regiao"] = partes.map(lambda item: item[0])
    data["subfuncao_ug"] = partes.map(lambda item: item[1])
    data["adj"] = partes.map(lambda item: item[2])
    data["macropolitica"] = partes.map(lambda item: item[3])
    data["pilar"] = partes.map(lambda item: item[4])
    data["eixo"] = partes.map(lambda item: item[5])
    data["politica_decreto"] = partes.map(lambda item: item[6])
    data["publico_transversal"] = partes.map(lambda item: item[7])

    data = data.rename(
        columns={
            PLAN134_HEADERS["acao"]: "acao_paoe",
            PLAN134_HEADERS["fonte"]: "fonte",
            PLAN134_HEADERS["grupo"]: "grupo_despesa",
            PLAN134_HEADERS["subteto"]: "subteto_despesa_momp",
            PLAN134_HEADERS["valor"]: "teto_politica_decreto",
        }
    )
    data["grupo_despesa"] = _remap(data["grupo_despesa"], GRUPO_PLAN134_MAP)
    data["subteto_despesa_momp"] = _remap(
        data["subteto_despesa_momp"], SUBTETO_PLAN134_MAP
    )
    data["acao_paoe"] = data["acao_paoe"].map(normalizar_acao)

    columns = [
        "regiao",
        "subfuncao_ug",
        "adj",
        "macropolitica",
        "pilar",
        "eixo",
        "politica_decreto",
        "publico_transversal",
        "chave_planejamento",
        "acao_paoe",
        "fonte",
        "grupo_despesa",
        "subteto_despesa_momp",
        "teto_politica_decreto",
    ]
    resultado = data.reindex(columns=columns).copy()
    resultado = resultado.replace({"nan": pd.NA, "NaN": pd.NA, "None": pd.NA})
    resultado = resultado.replace(r"^\s*$", pd.NA, regex=True)
    resultado = resultado.dropna(
        subset=[
            "regiao",
            "subfuncao_ug",
            "adj",
            "macropolitica",
            "pilar",
            "eixo",
            "politica_decreto",
            "publico_transversal",
            "chave_planejamento",
        ],
        how="all",
    )
    # Subacao sem item de despesa: o FIPLAN imprime a linha com fonte "-" e
    # Valor PTA 0/vazio. Nao carrega informacao orcamentaria - antes virava
    # linha inutil em politicateto ou era contada como "sem correspondencia
    # no MOMP", mascarando descartes de verdade (docs/claude.md, secao 20).
    # Descarta so pelo valor zerado: uma linha COM valor e sem fonte segue
    # adiante e aparece no aviso de "nao gravados", nunca some calada.
    valor = pd.to_numeric(resultado["teto_politica_decreto"], errors="coerce").fillna(0)
    resultado = resultado[valor != 0].copy()
    if resultado.empty:
        raise ValueError("Nenhum registro válido foi encontrado no Plan 134.")
    return resultado


def fonte_key(value) -> str:
    text = str(value or "").strip()
    match = re.match(r"^\s*(\d{5,})", text)
    return match.group(1) if match else text


def grupo_key(value) -> str:
    text = str(value or "").strip()
    match = re.match(r"^\s*(\d+)", text)
    return match.group(1) if match else text


def subteto_key(value) -> str:
    text = str(value or "").strip()
    match = re.match(r"^\s*([A-Za-z])", text)
    return match.group(1).upper() if match else text
