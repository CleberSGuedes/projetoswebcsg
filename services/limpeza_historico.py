"""Limpeza de Histórico das cargas de execução (FIP 613, PED, EMP, NOB e EST EMP).

Cada upload diário desses módulos desativa a carga anterior (ativo = 0) e
grava uma nova carga completa - a tabela acumula uma cópia inteira por dia.
A aplicação só lê registros ativos, então as cargas desativadas só ocupam
espaço (docs/claude.md, seção 23).

Regra (decisão do usuário): o usuário escolhe o exercício e o mês/ano de
referência. Para cada tabela marcada:
- mantém a ÚLTIMA carga daquele exercício carregada no mês/ano de referência;
- remove as demais cargas do mesmo exercício carregadas até o fim desse mês;
- cargas de outros exercícios e cargas posteriores ao mês não são tocadas;
- uma carga com qualquer registro ativo nunca é removida (protegida);
- sem carga do exercício no mês de referência, nada é removido da tabela.

A unidade é sempre a carga inteira (upload_id), nunca linhas soltas. O
exercício da carga vem dos próprios registros - a carga final de um
exercício pode chegar no ano seguinte (ex.: FIP 613 de 2025 carregado em
14/01/2026), por isso o mês/ano de referência é independente do exercício.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from sqlalchemy import bindparam, text

# Ordem de exibição na tela. "exercicio" = coluna do exercício em cada tabela
# (o formato varia: int, "2026" ou "26" - ver normalizar_exercicio).
TABELAS: dict[str, dict[str, str]] = {
    "fip613": {"label": "FIP 613", "exercicio": "ano"},
    "ped": {"label": "PED", "exercicio": "exercicio"},
    "emp": {"label": "EMP", "exercicio": "exercicio"},
    "nob": {"label": "NOB", "exercicio": "exercicio"},
    "est_emp": {"label": "EST EMP", "exercicio": "exercicio"},
}

# Faixa de ids apagada por comando. EMP/NOB não têm índice por upload_id,
# então a remoção percorre a chave primária em faixas (rápido e sem travar
# a tabela por muito tempo).
TAMANHO_LOTE = 20_000
NOME_TRAVA = "limpeza_historico"

# Processamento em etapas curtas, conduzidas pela tela (docs/claude.md,
# seção 23.9). No servidor online (LiteSpeed) uma thread em segundo plano
# é reciclada/congelada com o processo; por isso cada requisição faz no
# máximo PRAZO_PASSO segundos de trabalho, grava onde parou e responde.
PRAZO_PASSO = 10.0
# Sem etapa por mais de LIMITE_PAUSA segundos: a tela foi fechada ou a
# conexão caiu. A execução fica "pausada" e pode ser continuada.
LIMITE_PAUSA = 300
# Análise por faixas de id: começa com FAIXA_INICIAL ids e ajusta a faixa
# para cada GROUP BY levar cerca de ALVO_FAIXA segundos.
FAIXA_INICIAL = 200_000
FAIXA_MIN, FAIXA_MAX = 20_000, 5_000_000
ALVO_FAIXA = 3.0


def pausada(status: str, atualizado_em: datetime | None, agora: datetime) -> bool:
    """True se uma execução "em andamento" está sem etapas (tela fechada)."""
    if status != "em andamento":
        return False
    if atualizado_em is None:
        return True
    return (agora - atualizado_em).total_seconds() > LIMITE_PAUSA


@dataclass
class Carga:
    upload_id: int
    data_arquivo: datetime | None
    exercicio: int | None  # None = carga com mais de um exercício (nunca removida)
    linhas: int
    ativos: int
    id_min: int
    id_max: int


@dataclass
class PlanoTabela:
    tabela: str
    mantida: Carga | None = None
    remover: list[Carga] = field(default_factory=list)
    protegidas: list[Carga] = field(default_factory=list)
    linhas_total: int = 0
    tamanho_mb: float = 0.0

    @property
    def linhas_remover(self) -> int:
        return sum(c.linhas for c in self.remover)

    @property
    def mb_estimado(self) -> float:
        if not self.linhas_total:
            return 0.0
        return round(self.tamanho_mb * self.linhas_remover / self.linhas_total, 1)

    def resumo(self) -> dict:
        def _carga(c: Carga) -> dict:
            return {
                "upload_id": c.upload_id,
                "data_arquivo": c.data_arquivo.isoformat() if c.data_arquivo else None,
                "linhas": c.linhas,
            }

        return {
            "tabela": self.tabela,
            "label": TABELAS[self.tabela]["label"],
            "mantida": _carga(self.mantida) if self.mantida else None,
            "cargas_remover": len(self.remover),
            "linhas_remover": self.linhas_remover,
            "protegidas": [_carga(c) for c in self.protegidas],
            "linhas_total": self.linhas_total,
            "tamanho_mb": round(self.tamanho_mb, 1),
            "mb_estimado": self.mb_estimado,
        }


def normalizar_exercicio(valor) -> int | None:
    """2026, "2026" e "26" (EST EMP) -> 2026."""
    if valor is None:
        return None
    digitos = "".join(ch for ch in str(valor) if ch.isdigit())
    if not digitos:
        return None
    numero = int(digitos)
    return numero + 2000 if numero < 100 else numero


def validar_parametros(exercicio, ref_mes, ref_ano, tabelas) -> tuple[int, int, int, list[str]]:
    try:
        exercicio = int(exercicio)
        ref_mes = int(ref_mes)
        ref_ano = int(ref_ano)
    except (TypeError, ValueError):
        raise ValueError("Informe o exercício e o mês/ano da carga a manter.")
    if not 2000 <= exercicio <= 2100 or not 2000 <= ref_ano <= 2100:
        raise ValueError("Exercício ou ano inválido.")
    if not 1 <= ref_mes <= 12:
        raise ValueError("Mês inválido.")
    if ref_ano < exercicio:
        raise ValueError("O mês/ano da carga a manter não pode ser anterior ao exercício.")
    tabelas = [t for t in TABELAS if t in set(tabelas or [])]
    if not tabelas:
        raise ValueError("Selecione ao menos uma tabela.")
    return exercicio, ref_mes, ref_ano, tabelas


def _inicio_mes_seguinte(ref_ano: int, ref_mes: int) -> datetime:
    return datetime(ref_ano + 1, 1, 1) if ref_mes == 12 else datetime(ref_ano, ref_mes + 1, 1)


def planejar_tabela(
    tabela: str, cargas: list[Carga], exercicio: int, ref_mes: int, ref_ano: int
) -> PlanoTabela:
    """Decide o que fica e o que sai (função pura - sem banco)."""
    plano = PlanoTabela(tabela=tabela, linhas_total=sum(c.linhas for c in cargas))
    inicio_ref = datetime(ref_ano, ref_mes, 1)
    fim_ref = _inicio_mes_seguinte(ref_ano, ref_mes)

    do_exercicio = [c for c in cargas if c.exercicio == exercicio and c.data_arquivo is not None]
    no_mes = [c for c in do_exercicio if inicio_ref <= c.data_arquivo < fim_ref]
    if not no_mes:
        return plano  # sem carga de referência: nada é removido
    plano.mantida = max(no_mes, key=lambda c: (c.data_arquivo, c.upload_id))

    for c in do_exercicio:
        if c.upload_id == plano.mantida.upload_id or c.data_arquivo >= fim_ref:
            continue
        (plano.protegidas if c.ativos > 0 else plano.remover).append(c)
    plano.remover.sort(key=lambda c: (c.data_arquivo, c.upload_id))
    return plano


def carregar_cargas(conn, tabela: str) -> list[Carga]:
    """Uma passada pela tabela inteira (EMP/NOB levam ~1 min)."""
    col_ex = TABELAS[tabela]["exercicio"]
    rows = conn.execute(
        text(
            f"SELECT upload_id, MAX(data_arquivo), MIN({col_ex}), MAX({col_ex}), COUNT(*), "
            f"SUM(CAST(ativo AS UNSIGNED)), MIN(id), MAX(id) FROM {tabela} GROUP BY upload_id"
        )
    ).fetchall()
    cargas = []
    for upload_id, data_arquivo, ex_min, ex_max, linhas, ativos, id_min, id_max in rows:
        ex_a, ex_b = normalizar_exercicio(ex_min), normalizar_exercicio(ex_max)
        cargas.append(
            Carga(
                upload_id=int(upload_id) if upload_id is not None else 0,
                data_arquivo=data_arquivo,
                exercicio=ex_a if ex_a == ex_b else None,
                linhas=int(linhas or 0),
                ativos=int(ativos or 0),
                id_min=int(id_min),
                id_max=int(id_max),
            )
        )
    return cargas


def atualizar_estatisticas(conn, tabelas: list[str]) -> None:
    """ANALYZE TABLE recalcula as estatísticas (não mexe nos dados, ~0,1 s).

    Sem isso o information_schema devolve números velhos - em 14/10/2026 o
    EMP aparecia com 6,4 GB e tinha 19,1 GB; o PED com 4,5 MB e tinha 11 MB.
    """
    for tabela in tabelas:
        if tabela in TABELAS:
            conn.execute(text(f"ANALYZE TABLE {tabela}")).fetchall()
    conn.commit()


def espaco_mb(conn, tabelas: list[str], analisar: bool = True) -> dict[str, dict[str, float]]:
    """Por tabela: "usado" (páginas com dados), "livre" e "disco" (usado + livre).

    Depois de um DELETE o MariaDB passa a contar como DATA_LENGTH só as
    páginas em uso; o espaço das linhas apagadas continua no arquivo, como
    DATA_FREE, até o OPTIMIZE TABLE. Em 14/10/2026 o NOB aparecia com 1,9 GB
    logo após a limpeza, mas ocupava ~15,4 GB em disco.
    """
    if analisar:
        atualizar_estatisticas(conn, tabelas)
    rows = conn.execute(
        text(
            "SELECT TABLE_NAME, (DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, DATA_FREE / 1024 / 1024 "
            "FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME IN :tabelas"
        ).bindparams(bindparam("tabelas", expanding=True)),
        {"tabelas": list(tabelas)},
    ).fetchall()
    resultado = {}
    for nome, usado, livre in rows:
        usado, livre = float(usado or 0), float(livre or 0)
        resultado[nome] = {"usado": usado, "livre": livre, "disco": usado + livre}
    return resultado


def tamanhos_mb(conn, tabelas: list[str], analisar: bool = True) -> dict[str, float]:
    """Tamanho usado (sem o espaço livre) - base da estimativa da limpeza."""
    return {nome: e["usado"] for nome, e in espaco_mb(conn, tabelas, analisar).items()}


def simular(
    conn,
    exercicio: int,
    ref_mes: int,
    ref_ano: int,
    tabelas: list[str],
    progresso: Callable[[str, int], None] | None = None,
) -> list[PlanoTabela]:
    tamanhos = tamanhos_mb(conn, tabelas)
    planos = []
    for i, tabela in enumerate(tabelas):
        if progresso:
            progresso(
                f"Analisando {TABELAS[tabela]['label']} ({i + 1}/{len(tabelas)})...",
                int(5 + 90 * i / len(tabelas)),
            )
        plano = planejar_tabela(tabela, carregar_cargas(conn, tabela), exercicio, ref_mes, ref_ano)
        plano.tamanho_mb = tamanhos.get(tabela, 0.0)
        planos.append(plano)
    return planos


def remover_cargas(
    conn,
    plano: PlanoTabela,
    progresso: Callable[[int], None] | None = None,
) -> int:
    """Apaga as cargas de `plano.remover` em lotes por faixa de id.

    `ativo = 0` e `upload_id IN (...)` vão em todo DELETE - mesmo que a
    carga tenha sido reativada entre a análise e a execução, nada ativo sai.
    Commit a cada lote: uma falha no meio deixa só cargas inteiras ou
    parcialmente removidas, todas já desativadas (nenhum dado em uso).
    """
    if not plano.remover:
        return 0
    ids = [c.upload_id for c in plano.remover]
    inicio = min(c.id_min for c in plano.remover)
    fim = max(c.id_max for c in plano.remover)
    total = 0
    atual = inicio
    while atual <= fim:
        atual, removidas = remover_lotes(conn, plano.tabela, ids, atual, fim, prazo=None, max_lotes=1)
        total += removidas
        if progresso:
            progresso(int(100 * min(atual - inicio, fim - inicio + 1) / (fim - inicio + 1)))
    return total


def remover_lotes(
    conn,
    tabela: str,
    ids: list[int],
    cursor: int,
    fim: int,
    prazo: float | None,
    max_lotes: int | None = None,
) -> tuple[int, int]:
    """Apaga lotes de TAMANHO_LOTE ids a partir de `cursor` até `fim`.

    Para quando passar do `prazo` (time.monotonic()) ou após `max_lotes`.
    Devolve (novo cursor, linhas removidas). Mesmas garantias de
    remover_cargas: `ativo = 0 AND upload_id IN (...)` e commit por lote -
    retomar do cursor nunca apaga nada ativo nem repete trabalho.
    """
    stmt = text(
        f"DELETE FROM {tabela} WHERE id >= :a AND id < :b "
        "AND ativo = 0 AND upload_id IN :ids"
    ).bindparams(bindparam("ids", expanding=True))
    removidas = lotes = 0
    while cursor <= fim:
        proximo = cursor + TAMANHO_LOTE
        removidas += conn.execute(stmt, {"a": cursor, "b": proximo, "ids": ids}).rowcount or 0
        conn.commit()
        cursor = proximo
        lotes += 1
        if (prazo is not None and time.monotonic() >= prazo) or (max_lotes and lotes >= max_lotes):
            break
    return cursor, removidas


def otimizar_tabela(conn, tabela: str) -> str:
    """OPTIMIZE TABLE devolve o espaço das linhas apagadas ao disco.

    No InnoDB a mensagem "Table does not support optimize, doing recreate +
    analyze instead" é normal; erro vem com Msg_type = 'error'.
    """
    rows = conn.execute(text(f"OPTIMIZE TABLE {tabela}")).fetchall()
    conn.commit()
    erros = [str(r[3]) for r in rows if str(r[2]).lower() == "error"]
    if erros:
        raise RuntimeError("; ".join(erros))
    return "ok"


def obter_trava(conn) -> bool:
    return bool(conn.execute(text("SELECT GET_LOCK(:n, 0)"), {"n": NOME_TRAVA}).scalar())


def liberar_trava(conn) -> None:
    conn.execute(text("SELECT RELEASE_LOCK(:n)"), {"n": NOME_TRAVA})


# ---------------------------------------------------------------- etapas
#
# O estado de uma execução é um dict serializável em JSON, guardado na
# coluna `estado` do limpeza_historico_log entre uma etapa e outra - qualquer
# processo da aplicação continua de onde o anterior parou.


def analisar_faixa(conn, tabela: str, de: int, ate: int) -> dict[int, list]:
    """Totais por carga das linhas com id em [de, ate).

    Formato: {upload_id: [data_arquivo ISO, exercícios, linhas, ativos,
    id_min, id_max]}. "exercícios" = valores normalizados encontrados
    (-1 = valor que não é exercício); a carga só tem exercício se, somando
    todas as faixas, houver exatamente um.
    """
    col_ex = TABELAS[tabela]["exercicio"]
    rows = conn.execute(
        text(
            f"SELECT upload_id, MAX(data_arquivo), MIN({col_ex}), MAX({col_ex}), COUNT(*), "
            f"SUM(CAST(ativo AS UNSIGNED)), MIN(id), MAX(id) FROM {tabela} "
            "WHERE id >= :a AND id < :b GROUP BY upload_id"
        ),
        {"a": de, "b": ate},
    ).fetchall()
    parcial = {}
    for upload_id, data_arquivo, ex_min, ex_max, linhas, ativos, id_min, id_max in rows:
        exercicios = set()
        for valor in (ex_min, ex_max):
            if valor is not None:  # MIN/MAX ignoram NULL, como em carregar_cargas
                normalizado = normalizar_exercicio(valor)
                exercicios.add(normalizado if normalizado is not None else -1)
        parcial[int(upload_id) if upload_id is not None else 0] = [
            data_arquivo.isoformat() if data_arquivo else None,
            sorted(exercicios),
            int(linhas or 0),
            int(ativos or 0),
            int(id_min),
            int(id_max),
        ]
    return parcial


def mesclar_cargas(acumulado: dict[str, list], parcial: dict[int, list]) -> dict[str, list]:
    """Soma os totais de uma faixa aos já acumulados (chaves em texto - JSON)."""
    for upload_id, (data, exercicios, linhas, ativos, id_min, id_max) in parcial.items():
        atual = acumulado.get(str(upload_id))
        if atual is None:
            acumulado[str(upload_id)] = [data, list(exercicios), linhas, ativos, id_min, id_max]
            continue
        datas = [d for d in (atual[0], data) if d]
        atual[0] = max(datas, key=datetime.fromisoformat) if datas else None
        atual[1] = sorted(set(atual[1]) | set(exercicios))
        atual[2] += linhas
        atual[3] += ativos
        atual[4] = min(atual[4], id_min)
        atual[5] = max(atual[5], id_max)
    return acumulado


def cargas_de_agregados(acumulado: dict[str, list]) -> list[Carga]:
    cargas = []
    for upload_id, (data, exercicios, linhas, ativos, id_min, id_max) in acumulado.items():
        unico = len(exercicios) == 1 and exercicios[0] != -1
        cargas.append(
            Carga(
                upload_id=int(upload_id),
                data_arquivo=datetime.fromisoformat(data) if data else None,
                exercicio=exercicios[0] if unico else None,
                linhas=linhas,
                ativos=ativos,
                id_min=id_min,
                id_max=id_max,
            )
        )
    return cargas


def faixa_ids(conn, tabela: str) -> tuple[int, int] | None:
    mn, mx = conn.execute(text(f"SELECT MIN(id), MAX(id) FROM {tabela}")).fetchone()
    return None if mn is None else (int(mn), int(mx))


def novo_estado(acao: str, tabelas: list[str], exercicio=None, ref_mes=None, ref_ano=None) -> dict:
    """acao: "simulacao" | "limpeza" | "otimizacao"."""
    return {
        "acao": acao,
        "tabelas": list(tabelas),
        "params": [exercicio, ref_mes, ref_ano],
        "fase": "inicio",
        "i": 0,
        "resultado": [],
        "linhas": 0,
    }


def executar_passo(conn, estado: dict, prazo: float) -> None:
    """Avança `estado` até o `prazo` (time.monotonic()) ou até o fim.

    A otimização faz uma tabela por etapa: o OPTIMIZE TABLE é um comando
    único e não pode ser dividido nem interrompido no meio.
    """
    # Sempre ao menos um pedaço de trabalho por etapa: mesmo com o servidor
    # lento (prazo estourado), cada chamada avança.
    while estado["fase"] != "fim":
        if _avancar(conn, estado, prazo) or time.monotonic() >= prazo:
            break


def _proxima_tabela(estado: dict) -> None:
    estado["i"] += 1
    estado.pop("an", None)
    estado.pop("rem", None)
    if estado["i"] >= len(estado["tabelas"]):
        estado["fase"] = "fim"
    else:
        estado["fase"] = "otimizacao" if estado["acao"] == "otimizacao" else "analise"


def _avancar(conn, estado: dict, prazo: float) -> bool:
    """Um pedaço de trabalho. True = encerrar a etapa agora."""
    tabelas = estado["tabelas"]
    fase = estado["fase"]

    if fase == "inicio":
        espacos = espaco_mb(conn, tabelas)
        if estado["acao"] == "otimizacao":
            estado["antes"] = {t: e["disco"] for t, e in espacos.items()}
            estado["fase"] = "otimizacao"
        else:
            estado["tamanhos"] = {t: e["usado"] for t, e in espacos.items()}
            estado["fase"] = "analise"
        return False

    tabela = tabelas[estado["i"]]

    if fase == "otimizacao":
        otimizar_tabela(conn, tabela)
        depois = espaco_mb(conn, [tabela]).get(tabela, {}).get("disco", 0.0)
        estado["resultado"].append(
            {
                "tabela": tabela,
                "label": TABELAS[tabela]["label"],
                "antes_mb": round(estado["antes"].get(tabela, 0.0), 1),
                "depois_mb": round(depois, 1),
            }
        )
        _proxima_tabela(estado)
        return True

    if fase == "analise":
        an = estado.get("an")
        if an is None:
            faixa = faixa_ids(conn, tabela)
            inicio, fim = faixa if faixa else (0, -1)
            an = estado["an"] = {"inicio": inicio, "cursor": inicio, "fim": fim, "faixa": FAIXA_INICIAL, "agg": {}}
        while an["cursor"] <= an["fim"]:
            t0 = time.monotonic()
            ate = an["cursor"] + an["faixa"]
            mesclar_cargas(an["agg"], analisar_faixa(conn, tabela, an["cursor"], ate))
            an["cursor"] = ate
            gasto = max(time.monotonic() - t0, 0.05)
            nova = an["faixa"] * ALVO_FAIXA / gasto
            an["faixa"] = int(min(max(nova, FAIXA_MIN), FAIXA_MAX, an["faixa"] * 4))
            if time.monotonic() >= prazo:
                break
        if an["cursor"] <= an["fim"]:
            return False
        exercicio, ref_mes, ref_ano = estado["params"]
        plano = planejar_tabela(tabela, cargas_de_agregados(an["agg"]), exercicio, ref_mes, ref_ano)
        plano.tamanho_mb = estado["tamanhos"].get(tabela, 0.0)
        resumo = plano.resumo()
        estado.pop("an", None)
        if estado["acao"] == "limpeza" and plano.remover:
            inicio = min(c.id_min for c in plano.remover)
            estado["rem"] = {
                "ids": [c.upload_id for c in plano.remover],
                "inicio": inicio,
                "cursor": inicio,
                "fim": max(c.id_max for c in plano.remover),
                "removidas": 0,
                "resumo": resumo,
            }
            estado["fase"] = "remocao"
            return False
        if estado["acao"] == "limpeza":
            resumo["linhas_removidas"] = 0
        estado["resultado"].append(resumo)
        _proxima_tabela(estado)
        return False

    if fase == "remocao":
        rem = estado["rem"]
        rem["cursor"], removidas = remover_lotes(conn, tabela, rem["ids"], rem["cursor"], rem["fim"], prazo)
        rem["removidas"] += removidas
        estado["linhas"] += removidas
        if rem["cursor"] > rem["fim"]:
            resumo = rem["resumo"]
            resumo["linhas_removidas"] = rem["removidas"]
            estado["resultado"].append(resumo)
            _proxima_tabela(estado)
        return False

    raise ValueError(f"Fase desconhecida: {fase}")


def _fracao(inicio: int, cursor: int, fim: int) -> float:
    if fim < inicio:
        return 1.0
    return min(max((cursor - inicio) / (fim - inicio + 1), 0.0), 1.0)


def progresso_estado(estado: dict) -> tuple[str, int]:
    """(mensagem do que está sendo feito agora, % de 0 a 100)."""
    tabelas = estado["tabelas"]
    fase, i, total = estado["fase"], estado["i"], len(tabelas)
    if fase == "fim":
        return "Concluído.", 100
    if fase == "inicio":
        return "Medindo as tabelas...", 0
    label = f"{TABELAS[tabelas[i]]['label']} ({i + 1}/{total})"
    if fase == "otimizacao":
        return (
            f"Otimizando {label}... As tabelas grandes podem levar vários minutos.",
            int(100 * i / total),
        )
    limpeza = estado["acao"] == "limpeza"
    if fase == "analise":
        an = estado.get("an")
        dentro = _fracao(an["inicio"], an["cursor"], an["fim"]) if an else 0.0
        parte = dentro * (0.3 if limpeza else 1.0)
        mensagem = f"Analisando {label}... {int(dentro * 100)}%"
    else:  # remocao
        rem = estado["rem"]
        dentro = _fracao(rem["inicio"], rem["cursor"], rem["fim"])
        parte = 0.3 + 0.7 * dentro
        mensagem = f"Removendo cargas antigas de {label}... {int(dentro * 100)}%"
    return mensagem, min(int(100 * (i + parte) / total), 99)
