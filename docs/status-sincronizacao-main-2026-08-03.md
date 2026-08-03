# Status de sincronizacao seletiva com main - 2026-08-03

Este documento registra a analise feita na branch local `dev/cleber` em
`C:\workspace\projetoswebcsg_dev_cleber`, comparando a branch atual com
`origin/main`.

## Objetivo

Verificar se `origin/main` tinha funcionalidades novas que ainda nao existiam
na branch atual e trazer somente essas funcionalidades, preservando o visual
implementado nesta branch. A decisao foi nao executar merge direto do `main`,
porque isso substituiria grande parte dos arquivos visuais locais.

## Estado inicial observado

- Branch local: `dev/cleber`.
- Remoto principal: `origin`.
- Apos `git fetch origin`, `origin/main` apontava para `66da64f`
  (`RELEASE_2_2026_07_27.1`).
- A branch local estava atras de `origin/dev/cleber` em commits que, na pratica,
  traziam apenas o arquivo `LICENSE`.
- O diff `HEAD..origin/main` indicava muitas remocoes e alteracoes em arquivos
  visuais, incluindo `static/css/style.css`, `static/js/main.js`,
  `templates/base.html`, templates parciais e ativos do PTA 2027.

## Analise do main

Commits novos relevantes em `origin/main` desde o ponto comum:

- `d093193` / `aa1dbdc` / `fe3010f`: alteracoes de deploy/homologacao que foram
  revertidas no proprio `main`. Nao havia funcionalidade util a trazer.
- `0fbc174` / `60ddf72`: adicao do arquivo `LICENSE`.
- `66da64f`: release funcional com:
  - modelos, rotas, permissao, template e JavaScript para `Componentes da Revista`;
  - replicacao de `Componentes da Revista` e seus vinculos entre exercicios;
  - item externo `Programar PTA/LOA`;
  - atualizacao textual de release em rodape do layout antigo.

## O que ja existia nesta branch

A funcionalidade principal do release `66da64f` ja estava presente em
`dev/cleber`:

- modelos `ComponenteRev`, `ComponenteRevMacropolitica`, `ComponenteRevEixo`,
  `ComponenteRevProdutoAcao` e `ComponenteRevPoliticaDecreto`;
- exportacao desses modelos em `models/__init__.py`;
- permissao `atualizar/estrutura-planejamento/componentes-rev`;
- rotas e APIs de cadastro, edicao, desativacao e vinculos;
- suporte no template `templates/partials/atualizar_estrutura_planejamento.html`;
- suporte no JavaScript de `static/js/main.js`;
- suporte na replicacao de exercicio.

Por isso, nao foi necessario trazer esses blocos novamente.

## O que foi trazido

Foram aplicadas apenas mudancas funcionais pequenas e isoladas:

- `LICENSE`: arquivo novo trazido de `origin/main`.
- `services/features.py`: adicionado o item
  `cadastrar/planejamento/programar-pta-loa` como item de menu principal,
  logo abaixo de `Administrar SPO`.
- `services/features.py`: configurado o item externo com icone
  `box-arrow-up-right` e URL `https://pta2025.projetoswebcsg.life/`.
- `templates/_menu.html`: o menu dinamico passou a aceitar `node.url` para
  renderizar link externo usando a mesma classe visual `menu-item`.
- `static/js/main.js`: o listener do menu passou a ignorar links com
  `data-external-url`, permitindo abertura normal em nova aba sem tentar
  carregar `/partial/...`.

## O que foi preservado

Nao foi feito merge direto com `origin/main`.

Nao foram sobrescritos os arquivos de contrato visual e layout que sustentam a
versao visual desta branch, especialmente:

- `static/css/style.css`;
- `templates/base.html`;
- os blocos visuais do PTA 2027;
- os documentos e templates de governanca/contrato visual;
- a estrutura visual dinamica de menu ja existente nesta branch.

## Status atual do working tree

Arquivos modificados pela sincronizacao seletiva:

- `services/features.py`;
- `static/js/main.js`;
- `templates/_menu.html`;
- `LICENSE` novo.

Documento adicionado:

- `docs/status-sincronizacao-main-2026-08-03.md`.

Status observado antes deste documento:

```text
## dev/cleber...origin/dev/cleber [behind 4]
 M services/features.py
 M static/js/main.js
 M templates/_menu.html
?? LICENSE
```

Com este documento, o status esperado passa a incluir tambem:

```text
?? docs/status-sincronizacao-main-2026-08-03.md
```

## Validacoes executadas

Comandos executados com sucesso:

```powershell
py -3.11 -m py_compile services\features.py
node --check static\js\main.js
git -c safe.directory=C:/workspace/projetoswebcsg_dev_cleber diff --check
py -3.11 -m py_compile app.py rotas\home_routes.py services\features.py
```

Observacao: `git diff --check` retornou apenas avisos de conversao de LF para
CRLF em arquivos tocados pelo Git no Windows. Nao foram encontrados erros de
espaco em branco.

## Pendencias

- O commit e o push serão feitos após a documentação final da Receita Anexo 10.
- A sincronização com `main` foi seletiva: trouxe funcionalidades sem substituir
  o visual desenvolvido nesta branch.
- O `LICENSE` novo foi mantido como alteração trazida seletivamente de
  `origin/main`.
