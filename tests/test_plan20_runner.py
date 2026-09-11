"""Cobre o ajuste do parser do Plan20 (services/plan20_runner.py) pro
layout novo do relatorio (2027), que inseriu um bloco de capa por
Programa (Eixo/Objetivo Estratégico/Público Alvo/Tipo/UO Responsável) e
uma tabela "ODS | Código Meta | Metas" por Ação, além de mudar a
formatação do código da UO ("14.101" -> "14101").

Bugs reais encontrados e corrigidos nesta sessão, cobertos aqui:
- filtro de UO por igualdade exata de texto (quebrava com a mudança de
  formatação - docs/claude.md, seção 14);
- bloco de capa do 2º Programa em diante contaminando a última
  Subação/Produto do Programa anterior (docs/claude.md, seção 15);
- dados da capa (Eixo/Objetivo Estratégico/...) colapsando pra um só
  valor repetido em todos os Programas do arquivo, em vez de um valor
  por Programa;
- "PLANO DE AÇÃO POR PRODUTO: <produto>" mudou de "rótulo numa célula +
  valor em col_5" (2026) pra "rótulo e valor na mesma célula" (2027) -
  o valor real nunca era lido no layout novo, e toda Subação caía no
  Produto errado (o primeiro da lista) em vez do Produto que o próprio
  relatório vincula a ela (docs/claude.md, seção 16).
- filtro de UO aceitava só "14101" - a SEDUC também usa a UO "14601"
  (FMTE, um fundo vinculado) pra outro Plan20; sem estar na lista, esse
  arquivo processava certinho mas saía com Plan20_SEDUC vazio (docs/
  claude.md, seção 17).

Não toca no banco - roda run_plan20() contra planilhas sintéticas
descartáveis, geradas em diretório temporário.
"""
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from services.plan20_runner import run_plan20


def _blank(n=8):
    return [None] * n


def _row(col1="", col2="", col3="", col4="", col5="", col6="", col7="", col8=""):
    return [col1 or None, col2 or None, col3 or None, col4 or None, col5 or None, col6 or None, col7 or None, col8 or None]


def _bloco_acao(programa_num, acao_num, valor, item_desc, fonte="15001001", uo="14101 - SECRETARIA DE ESTADO DE EDUCAÇÃO"):
    """Bloco C+D+E/F/G/H/I completo (igual ao layout ja existente desde
    2026) pra uma Acao com uma Subacao/Etapa/Item so."""
    return [
        _row("Programa:", "", "", f"{programa_num} - Programa {programa_num}"),
        _row("Função:", "", "", "12 - EDUCAÇÃO"),
        _row("Unidade Orçamentária:", "", "", uo),
        _row("Ação (P/A/OE):", "", "", f"{acao_num} - Ação {acao_num}", "", "", "", f"{valor:.2f}".replace(".", ",")),
        _row("Subfunção:", "", "", "361 - ENSINO"),
        _row("Objetivo Específico:", "", "", "Objetivo específico de teste"),
        _row("Esfera:", "", "", "FISCAL"),
        _row("Responsável pela Ação:", "", "", "Responsável Teste"),
        _blank(),
        _row("Produto(s) da Ação:", "", "", "Descrição (Unidade de Medida)", "", "Região", "Quantidade", "Saldo"),
        _row("", "", "", "", "", "9900"),
        _blank(), _blank(), _blank(),
    ]


def _bloco_ods(entradas):
    """entradas: lista de (nome_ods_ou_vazio, codigo_meta, texto_meta).
    Vazia = so o cabecalho, sem nenhuma linha de dado (acao padronizada)."""
    rows = [_row("ODS", "", "", "Código Meta", "Metas")]
    for nome, codigo, meta in entradas:
        rows.append(_row(nome, "", "", codigo, meta))
    rows.append(_blank())
    rows.append(_blank())
    return rows


def _bloco_subacao(idx, texto, valor, item_desc, fonte="15001001"):
    v = f"{valor:.2f}".replace(".", ",")
    return [
        _row(f"Subação/entrega: {idx} - {texto}"),
        _row("Responsável: Responsável Teste", "", "", "", "Prazo 01/01/2027 até 31/12/2027", "", v),
        _row("Unid. Gestora: 0001 - Sede", "", "", "Unidade Setorial de Planejamento: 001 - Geral", "Produto da Subação: 0109 - Contratação realizada", "", "Unidade de Medida: 13 - Percentual"),
        _row("Região / Município", "Região", "", "Código", "Município(s) da entrega", "", "Quantidade"),
        _row("", "9900", "", "5100000", "ESTADO", "", "100,00"),
        _row(f"Detalhamento do produto:Detalhamento {idx}"),
        _blank(), _blank(),
        _row("Etapa:", "", "", f"1 - Etapa {idx}", "", "", v),
        _row("Responsável:", "", "Responsável Teste", "", "Prazo: 01/01/2027  até  31/12/2027"),
        _blank(), _blank(),
        _row("Região de Planejamento:", "", "", "9900 - ESTADO", "Produto:", "", "Unidade:", "Qtde: 0,00"),
        _row("Natureza", "Fonte", "IDU", "Descrição do Item de Despesa", "Unid. Medida", "Quantidade", "Valor Unitário", "Valor Total"),
        _row("3.3.90.30.001", fonte, "OD", item_desc, "Real (R$)", "1,00", v, v),
        _blank(), _blank(),
    ]


def _bloco_capa(eixo, objetivo_estrategico, programa_num, publico_alvo, tipo, ods_nome_programa=None):
    rows = [
        _row("Eixo:", "", "", eixo),
        _row("Objetivo Estratégico:", "", "", objetivo_estrategico),
        _row("Programa:", "", "", f"{programa_num} - Programa {programa_num}"),
        _row("Público Alvo:", "", "", publico_alvo or ""),
        _row("Tipo:", "", "", tipo),
        _row("UO Responsável:", "", "", "14101 - SECRETARIA DE ESTADO DE EDUCAÇÃO"),
        _blank(), _blank(),
    ]
    if ods_nome_programa:
        rows += [
            _row("Objetivo de Desenvolvimento Sustentável"),
            _blank(),
            _row(ods_nome_programa),
            _blank(), _blank(),
        ]
    return rows


def _monta_arquivo_2027(path: Path, uo_com_ponto: bool) -> None:
    uo = "14.101 - SECRETARIA DE ESTADO DE EDUCAÇÃO" if uo_com_ponto else "14101 - SECRETARIA DE ESTADO DE EDUCAÇÃO"
    rows: list[list] = [
        _row("PLAN 20 - Relatório do PTA"),
        _blank(),
        _row("*Exercício igual a 2027"),
        _row("Código da Unidade Orcamentária igual a 14101"),
        _row("Emitir Relatório (1-até Subação / 2-até Etapa / 3-até Memória de Cálculo)  igual a 3"),
        _blank(), _blank(),
    ]

    # Programa 1 (administrativo/padronizado): ODS vazio, Público Alvo vazio.
    rows += _bloco_capa("08 - Programas e ações padronizados", "Objetivo estratégico administrativo", "001", "", "Gestão de Políticas Públicas")
    rows += _bloco_acao("001", "1001", 1000.0, "Item administrativo", uo=uo)
    rows += _bloco_ods([])
    rows.append(_row("PLANO DE AÇÃO POR PRODUTO: Produto exclusivo para ação padronizada"))
    rows += [_blank(), _blank()]
    rows += _bloco_subacao(1, "* R1 * chave1 * Subação administrativa", 1000.0, "Item administrativo")

    # Programa 2 (finalístico): ODS preenchido, Público Alvo preenchido -
    # é aqui que a capa do Programa 2 tentaria contaminar a última
    # Subação/Etapa do Programa 1 se o reset não fosse feito direito.
    rows += _bloco_capa("01 - Social", "Objetivo estratégico de educação", "002", "Sociedade", "Finalístico", ods_nome_programa="Educação de qualidade")
    rows += _bloco_acao("002", "2002", 2000.0, "Item finalístico", uo=uo)
    rows += _bloco_ods([("Educação de qualidade", "4.1", "Meta um de teste"), ("", "4.2", "Meta dois de teste")])
    rows.append(_row("PLANO DE AÇÃO POR PRODUTO: Produto Real Dois"))
    rows += [_blank(), _blank()]
    rows += _bloco_subacao(1, "* R2 * chave2 * Subação finalística", 2000.0, "Item finalístico")

    pd.DataFrame(rows).to_excel(path, sheet_name="FIPLAN", index=False, header=False)


def test_plan20_layout_2027_uo_sem_ponto_e_dois_programas_sem_contaminacao():
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        root = Path(tmp)
        input_path = root / "plan20_2027.xlsx"
        _monta_arquivo_2027(input_path, uo_com_ponto=False)

        out_path = run_plan20(input_path, root / "saida")
        p20 = pd.read_excel(out_path, sheet_name="Plan20_SEDUC")

        # O filtro de UO (sem ponto, igual ao layout 2027 real) não pode
        # zerar a saída - era exatamente o bug original.
        assert len(p20) == 2, f"esperado 2 linhas (1 por Programa), veio {len(p20)}"

        prog1 = p20[p20["Programa"].str.startswith("001")].iloc[0]
        prog2 = p20[p20["Programa"].str.startswith("002")].iloc[0]

        # Cada Programa mantém sua própria capa - não pode colapsar pra
        # um valor só repetido nos dois (bug do "ab" como chave).
        assert prog1["Eixo do Programa"] == "08 - Programas e ações padronizados"
        assert prog2["Eixo do Programa"] == "01 - Social"
        assert prog1["Objetivo Estratégico"] == "Objetivo estratégico administrativo"
        assert prog2["Objetivo Estratégico"] == "Objetivo estratégico de educação"
        assert prog1["Tipo"] == "Gestão de Políticas Públicas"
        assert prog2["Tipo"] == "Finalístico"
        assert prog2["Público Alvo"] == "Sociedade"
        # Público Alvo vazio na planilha não pode virar o texto do rótulo
        # ("Público Alvo:") - tem que cair no default "-".
        assert prog1["Público Alvo"] == "-"

        # ODS: vazio (ação padronizada) vs preenchido (ação finalística),
        # sem vazar de um Programa pro outro.
        assert prog1["ODS"] == "Ação padronizada - sem ODS vinculado"
        assert prog2["ODS"] == "Educação de qualidade"
        assert prog2["Código Meta (ODS)"] == "4.1 * 4.2"
        assert prog2["Metas (ODS)"] == "Meta um de teste * Meta dois de teste"

        # A última Subação/Etapa/Item do Programa 1 não pode ter sido
        # contaminada pela capa do Programa 2 (o bug de contaminação
        # entre Programas, encontrado ao inspecionar o arquivo real).
        assert "Eixo" not in str(prog1["Subação/entrega"])
        assert "Objetivo Estratégico" not in str(prog1["Detalhamento do produto"])
        assert prog1["Subação/entrega"].startswith("1 -")

        # Campos do bloco C continuam corretos com o casamento por rótulo
        # (não mais por posição).
        assert prog1["Função"] == "12 - EDUCAÇÃO"
        assert prog1["Unidade Orçamentária"] == "14101 - SECRETARIA DE ESTADO DE EDUCAÇÃO"
        assert prog1["Subfunção"] == "361 - ENSINO"
        assert prog1["Esfera"] == "FISCAL"
        assert prog1["Responsável pela Ação"] == "Responsável Teste"


def test_plan20_layout_2027_uo_com_ponto_continua_funcionando():
    # Formato antigo da UO ("14.101") não pode parar de funcionar so
    # porque o filtro ficou tolerante ao formato novo.
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        root = Path(tmp)
        input_path = root / "plan20_2027_ponto.xlsx"
        _monta_arquivo_2027(input_path, uo_com_ponto=True)
        out_path = run_plan20(input_path, root / "saida")
        p20 = pd.read_excel(out_path, sheet_name="Plan20_SEDUC")
        assert len(p20) == 2


def test_plan20_layout_2026_sem_capa_continua_identico():
    # Regressão: arquivo sem nenhum bloco de capa (layout antigo) não
    # pode ter o comportamento alterado - Eixo/ODS ficam no default, o
    # resto processa igual.
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        root = Path(tmp)
        input_path = root / "plan20_2026.xlsx"
        rows: list[list] = [
            _row("PLAN 20 - Relatório do PTA"),
            _blank(),
            _row("*Exercício igual a 2026"),
            _row("Código da Unidade Orcamentária igual a 14101"),
            _blank(), _blank(),
        ]
        rows += _bloco_acao("001", "1001", 500.0, "Item único")
        rows.append(_row("PLANO DE AÇÃO POR PRODUTO"))
        rows += [_blank(), _blank()]
        rows += _bloco_subacao(1, "* R1 * chave1 * Subação única", 500.0, "Item único")
        pd.DataFrame(rows).to_excel(input_path, sheet_name="FIPLAN", index=False, header=False)

        out_path = run_plan20(input_path, root / "saida")
        p20 = pd.read_excel(out_path, sheet_name="Plan20_SEDUC")
        assert len(p20) == 1
        row = p20.iloc[0]
        assert row["Eixo do Programa"] == "-"
        assert row["Objetivo Estratégico"] == "-"
        assert row["Público Alvo"] == "-"
        assert row["ODS"] == "Ação padronizada - sem ODS vinculado"
        assert row["Programa"] == "001 - Programa 001"
        assert row["Função"] == "12 - EDUCAÇÃO"


def _bloco_produto_tabela(produtos):
    """produtos: lista de (nome, meta, saldo). Emite o cabeçalho da
    tabela "Produto(s) da Ação" + uma linha de detalhe por produto
    (região fixa 9900)."""
    rows = [_row("Produto(s) da Ação:", "", "", "Descrição (Unidade de Medida)", "", "Região", "Quantidade", "Saldo")]
    for nome, meta, saldo in produtos:
        rows.append(_row("", "", "", f"{nome}(Percentual)", "", "9900", meta, saldo))
    rows += [_blank(), _blank(), _blank()]
    return rows


def test_plan20_layout_2027_produto_por_plano_inline_casa_subacao_certa():
    # Bug real encontrado conferindo o arquivo de saída manualmente: no
    # layout 2027, "PLANO DE AÇÃO POR PRODUTO: <produto>" vem com rótulo
    # e valor na MESMA célula (col_1) - no layout 2026 o valor vinha numa
    # coluna separada (col_5). Sem o fix, o valor real nunca era lido, e
    # toda Subação caía no primeiro Produto da lista (com Meta/Saldo
    # daquele Produto errado) em vez do Produto que o próprio relatório
    # vincula a ela via essa linha.
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        root = Path(tmp)
        input_path = root / "plan20_produto_inline.xlsx"
        rows: list[list] = [
            _row("PLAN 20 - Relatório do PTA"),
            _blank(),
            _row("*Exercício igual a 2027"),
            _row("Código da Unidade Orcamentária igual a 14101"),
            _blank(), _blank(),
        ]
        rows += [
            _row("Programa:", "", "", "003 - Programa 003"),
            _row("Função:", "", "", "12 - EDUCAÇÃO"),
            _row("Unidade Orçamentária:", "", "", "14101 - SECRETARIA DE ESTADO DE EDUCAÇÃO"),
            _row("Ação (P/A/OE):", "", "", "3003 - Ação Três", "", "", "", "3000,00"),
            _row("Subfunção:", "", "", "361 - ENSINO"),
            _row("Objetivo Específico:", "", "", "Objetivo específico de teste"),
            _row("Esfera:", "", "", "FISCAL"),
            _row("Responsável pela Ação:", "", "", "Responsável Teste"),
            _blank(),
        ]
        rows += _bloco_produto_tabela([("Produto Um", "10,00", "2,00"), ("Produto Dois", "20,00", "5,00")])

        # Grupo do Produto Dois, com sua própria Subação/Etapa/Item.
        rows.append(_row("PLANO DE AÇÃO POR PRODUTO: Produto Dois"))
        rows += [_blank(), _blank()]
        rows += _bloco_subacao(1, "SubDois - vinculada ao Produto Dois", 100.0, "Item da SubDois")

        # Grupo do Produto Um, com sua própria Subação/Etapa/Item.
        rows.append(_row("PLANO DE AÇÃO POR PRODUTO: Produto Um"))
        rows += [_blank(), _blank()]
        rows += _bloco_subacao(1, "SubUm - vinculada ao Produto Um", 200.0, "Item da SubUm")

        pd.DataFrame(rows).to_excel(input_path, sheet_name="FIPLAN", index=False, header=False)

        out_path = run_plan20(input_path, root / "saida")
        p20 = pd.read_excel(out_path, sheet_name="Plan20_SEDUC")

        linha_dois = p20[p20["Subação/entrega"].str.contains("SubDois", na=False)].iloc[0]
        linha_um = p20[p20["Subação/entrega"].str.contains("SubUm", na=False)].iloc[0]

        # Cada Subação tem que casar com o Produto que o próprio
        # relatório vincula a ela - não as duas caindo no mesmo (o
        # primeiro da lista, que era o bug).
        assert linha_dois["Produto(s) da Ação"] == "Produto Dois"
        assert float(str(linha_dois["Meta do Produto"]).replace(",", ".")) == 20.0
        assert float(str(linha_dois["Saldo Meta do Produto"]).replace(",", ".")) == 5.0

        assert linha_um["Produto(s) da Ação"] == "Produto Um"
        assert float(str(linha_um["Meta do Produto"]).replace(",", ".")) == 10.0
        assert float(str(linha_um["Saldo Meta do Produto"]).replace(",", ".")) == 2.0


def test_plan20_produto_sem_subacao_fica_de_fora_da_saida():
    # Por pedido do usuário, conferindo o arquivo real: Produtos sem
    # nenhuma Subação vinculada não entram mais na saída (confirmado que
    # sempre têm Valor Total = 0 - não carregam informação orçamentária,
    # só poluíam a planilha com uma linha de metadado).
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        root = Path(tmp)
        input_path = root / "plan20_produto_sem_subacao.xlsx"
        rows: list[list] = [
            _row("PLAN 20 - Relatório do PTA"),
            _blank(),
            _row("*Exercício igual a 2027"),
            _row("Código da Unidade Orcamentária igual a 14101"),
            _blank(), _blank(),
        ]
        rows += [
            _row("Programa:", "", "", "004 - Programa 004"),
            _row("Função:", "", "", "12 - EDUCAÇÃO"),
            _row("Unidade Orçamentária:", "", "", "14101 - SECRETARIA DE ESTADO DE EDUCAÇÃO"),
            _row("Ação (P/A/OE):", "", "", "4004 - Ação Quatro", "", "", "", "1000,00"),
            _row("Subfunção:", "", "", "361 - ENSINO"),
            _row("Objetivo Específico:", "", "", "Objetivo específico de teste"),
            _row("Esfera:", "", "", "FISCAL"),
            _row("Responsável pela Ação:", "", "", "Responsável Teste"),
            _blank(),
        ]
        # "Produto Órfão" nunca vai ter uma Subação/PLANO DE AÇÃO POR
        # PRODUTO apontando pra ele - só "Produto Vinculado" tem.
        rows += _bloco_produto_tabela([("Produto Órfão", "5,00", "0.0"), ("Produto Vinculado", "10,00", "2,00")])
        rows.append(_row("PLANO DE AÇÃO POR PRODUTO: Produto Vinculado"))
        rows += [_blank(), _blank()]
        rows += _bloco_subacao(1, "SubVinculada - a única com Subação", 1000.0, "Item vinculado")

        pd.DataFrame(rows).to_excel(input_path, sheet_name="FIPLAN", index=False, header=False)

        out_path = run_plan20(input_path, root / "saida")
        p20 = pd.read_excel(out_path, sheet_name="Plan20_SEDUC")

        assert len(p20) == 1, f"esperado so a linha do Produto Vinculado, veio {len(p20)}"
        assert p20.iloc[0]["Produto(s) da Ação"] == "Produto Vinculado"
        assert (p20["Produto(s) da Ação"] == "Produto Órfão").sum() == 0


def test_plan20_uo_14601_tambem_e_aceita():
    # A SEDUC tem uma segunda UO própria (14601 - FMTE), além da 14101.
    # UOS_ACEITAS precisa incluir as duas; qualquer UO fora da lista
    # continua de fora do Plan20_SEDUC.
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        root = Path(tmp)
        input_path = root / "plan20_uo14601.xlsx"
        rows: list[list] = [
            _row("PLAN 20 - Relatório do PTA"),
            _blank(),
            _row("*Exercício igual a 2027"),
            _row("Código da Unidade Orcamentária igual a 14601"),
            _blank(), _blank(),
        ]
        rows += _bloco_acao(
            "005", "5005", 700.0, "Item FMTE",
            uo="14601 - FUNDO EST DE APOIO À MELHORIA DAS CONDIÇ. DE OFERTA DA EDUC INFANT., ENS. FUNDAM. E ENS. MÉDIO NO MT",
        )
        rows.append(_row("PLANO DE AÇÃO POR PRODUTO: Produto FMTE"))
        rows += [_blank(), _blank()]
        rows += _bloco_subacao(1, "* R1 * chave1 * Subação FMTE", 700.0, "Item FMTE")
        pd.DataFrame(rows).to_excel(input_path, sheet_name="FIPLAN", index=False, header=False)

        out_path = run_plan20(input_path, root / "saida")
        p20 = pd.read_excel(out_path, sheet_name="Plan20_SEDUC")
        assert len(p20) == 1
        assert p20.iloc[0]["Unidade Orçamentária"].startswith("14601")


def test_plan20_uo_fora_da_lista_continua_de_fora():
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        root = Path(tmp)
        input_path = root / "plan20_uo_outra.xlsx"
        rows: list[list] = [
            _row("PLAN 20 - Relatório do PTA"),
            _blank(),
            _row("*Exercício igual a 2027"),
            _row("Código da Unidade Orcamentária igual a 99999"),
            _blank(), _blank(),
        ]
        rows += _bloco_acao("006", "6006", 100.0, "Item de outra UO", uo="99999 - OUTRA SECRETARIA QUALQUER")
        rows.append(_row("PLANO DE AÇÃO POR PRODUTO: Produto de outra UO"))
        rows += [_blank(), _blank()]
        rows += _bloco_subacao(1, "* R1 * chave1 * Subação de outra UO", 100.0, "Item de outra UO")
        pd.DataFrame(rows).to_excel(input_path, sheet_name="FIPLAN", index=False, header=False)

        out_path = run_plan20(input_path, root / "saida")
        p20 = pd.read_excel(out_path, sheet_name="Plan20_SEDUC")
        assert len(p20) == 0
