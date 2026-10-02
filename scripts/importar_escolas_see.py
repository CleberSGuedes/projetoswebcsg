"""Importa o cadastro DRE/Escola das Notas SEE a partir da planilha de matrículas.

Uso:
    python scripts/importar_escolas_see.py "C:\\workspace\\Planilhas\\RELATÓRIO...xlsx" [--aba 2026]

Atualiza as escolas pelo código de lotação; escolas que não estão mais na planilha
ficam inativas (não são apagadas). Os códigos de entrega na própria DRE (1-14) são mantidos.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app  # noqa: E402
from models import db, SeeEscola  # noqa: E402
from services.see_escolas import ler_planilha_escolas  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("planilha", type=Path)
    parser.add_argument("--aba", default="2026")
    args = parser.parse_args()

    escolas = ler_planilha_escolas(args.planilha, args.aba)
    fonte = f"{args.planilha.name} [{args.aba}]"
    app = create_app()
    with app.app_context():
        atuais = {row.codigo: row for row in SeeEscola.query.all()}
        criadas = atualizadas = 0
        for escola in escolas:
            row = atuais.get(escola.codigo)
            if row is None:
                row = SeeEscola(codigo=escola.codigo)
                db.session.add(row)
                criadas += 1
            elif (row.nome, row.municipio, row.dre, row.tipo, row.ativo) != (escola.nome, escola.municipio, escola.dre, escola.tipo, True):
                atualizadas += 1
            row.nome = escola.nome
            row.municipio = escola.municipio
            row.dre = escola.dre
            row.tipo = escola.tipo
            row.ativo = True
            row.fonte = fonte
        codigos = {escola.codigo for escola in escolas}
        inativadas = 0
        for codigo, row in atuais.items():
            if codigo not in codigos and row.ativo:
                row.ativo = False
                inativadas += 1
        db.session.commit()
        total = SeeEscola.query.filter_by(ativo=True).count()
    print(f"Cadastro importado de {fonte}: {criadas} nova(s), {atualizadas} atualizada(s), {inativadas} inativada(s); {total} ativas.")


if __name__ == "__main__":
    main()
