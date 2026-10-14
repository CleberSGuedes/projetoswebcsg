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
    stmt = text(
        f"DELETE FROM {plano.tabela} WHERE id >= :a AND id < :b "
        "AND ativo = 0 AND upload_id IN :ids"
    ).bindparams(bindparam("ids", expanding=True))
    total = 0
    atual = inicio
    while atual <= fim:
        proximo = atual + TAMANHO_LOTE
        total += conn.execute(stmt, {"a": atual, "b": proximo, "ids": ids}).rowcount or 0
        conn.commit()
        atual = proximo
        if progresso:
            progresso(int(100 * min(atual - inicio, fim - inicio + 1) / (fim - inicio + 1)))
    return total


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
