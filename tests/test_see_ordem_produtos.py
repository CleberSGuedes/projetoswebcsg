"""Ordem dos produtos do catalogo Notas SEE: alunos antes de professores; regular, EJA, especial,
artes; e por serie. Vale para o formato antigo (1º reg_al) e o novo (3º ANO PR, 6º ANO ARTE PR)."""
from types import SimpleNamespace

from rotas.notas_see_routes import _see_produto_ordem


def _ordenar(nomes):
    produtos = [SimpleNamespace(nome=n, codigo=str(i)) for i, n in enumerate(nomes)]
    return [p.nome for p in sorted(produtos, key=_see_produto_ordem)]


def test_formato_novo_ano_al_pr():
    nomes = ["12º ANO PR", "6º ANO ARTE PR", "3º ANO PR", "10º ANO AL", "1º ANO AL", "9º ANO ARTE PR", "2º ANO AL", "5º ANO PR"]
    assert _ordenar(nomes) == [
        "1º ANO AL", "2º ANO AL", "10º ANO AL",
        "3º ANO PR", "5º ANO PR", "12º ANO PR", "6º ANO ARTE PR", "9º ANO ARTE PR",
    ]


def test_formato_antigo_continua_igual():
    nomes = ["6º art_pr", "10º _esp_al", "10º_eja_al", "2º reg_pr", "11º reg_al", "1º reg_al"]
    assert _ordenar(nomes) == ["1º reg_al", "11º reg_al", "10º_eja_al", "10º _esp_al", "2º reg_pr", "6º art_pr"]


def test_nome_fora_do_padrao_vai_para_o_fim():
    assert _ordenar(["Qualquer coisa", "1º ANO AL"]) == ["1º ANO AL", "Qualquer coisa"]
