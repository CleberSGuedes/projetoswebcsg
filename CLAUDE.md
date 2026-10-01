# CLAUDE.md — SPO (Sistema de Planejamento e Orçamento) · SEDUC-MT

## Visão geral do projeto

- **Backend:** Flask 3.1 / Werkzeug 3.1 (`app.py`, `create_app()`), rotas principais em `rotas/home_routes.py`.
- **Banco:** MariaDB 10.11 (dev) via SQLAlchemy; modelos em `models/user.py` (exportados em `models/__init__.py`).
- **Frontend:** templates Jinja em `templates/partials/*.html`, todo o JS em `static/js/main.js` (arquivo único) e CSS em `static/css/style.css`.
- **Processamentos longos:** executados fora da requisição por `worker.py --kind <tipo> --upload-id <id>` (subprocesso; recarrega os módulos a cada execução).
- **Ambiente dev:** `.\run_dev_cleber.ps1` (PowerShell) → http://127.0.0.1:5010, com `--debug`.
  Se a política bloquear scripts: `powershell -ExecutionPolicy Bypass -File .\run_dev_cleber.ps1`.

## Convenções importantes

- **Sem framework de migração.** `db.create_all()` cria tabelas novas no start, mas **não altera tabelas existentes**. Alterações de colunas/índices vão em scripts SQL na pasta `db/` e devem ser executadas manualmente em cada ambiente **antes** de subir o código.
- **Cache de estáticos:** ao alterar `main.js`/`style.css`, trocar o sufixo `?v=` em `templates/base.html`.
- **Uploads multipart:** o Werkzeug 3.1 limita uma requisição a **1000 partes** (`MAX_FORM_PARTS`) → erro 413. Envios com muitos arquivos devem ser feitos em lotes.

---

## Notas SEE (Área UENs › SAGE) — alterações de 2026-10-01

Funcionalidade que lê DANFEs em PDF, extrai DRE, escola e quantidades dos produtos de um catálogo e gera uma planilha consolidada.

Arquivos principais: `services/see_notes.py` (extração e Excel), `services/see_escolas.py` (cadastro DRE/Escola), `rotas/home_routes.py` (rotas `/api/notas-see/*` e `_run_see_processamento`), `templates/partials/notas_see.html`, função `initNotasSee` em `static/js/main.js`.

### 1. Correção: upload travado com muitos PDFs (erro 413)

- **Problema:** com 1.283 PDFs em uma única requisição, o servidor respondia 413 (limite de 1000 partes) e a tela ficava parada em "Enviando PDFs...".
- **Correção:** o envio passou a ser feito em três etapas, com lotes de até 50 arquivos ou 20 MB:
  - `POST /api/notas-see/processamentos`: cria o processamento (status `enviando`) e copia as notas anteriores quando o modo é "acrescentar";
  - `POST /api/notas-see/processamentos/<id>/arquivos`: recebe um lote; os duplicados são detectados por SHA-256 também entre lotes;
  - `POST /api/notas-see/processamentos/<id>/iniciar`: dispara o worker.
- Envio interrompido ou cancelado: o processamento vira `descartado` e os PDFs enviados são apagados. A rota `/processamentos/ultimo` ignora `enviando` e `descartado`.
- A tela mostra o progresso do envio ("Enviando PDFs: X de N").
- Subpastas: a opção "Selecionar pasta" envia os arquivos de **todas as subpastas**. Arquivos que não são PDF e PDFs duplicados são ignorados e listados como tais.

### 2. Correção: DRE, escola e quantidades extraídas erradas

Auditoria do 3º bimestre/2026: um leitor independente do texto dos 641 PDFs foi comparado com o que estava gravado no banco. Há dois layouts de DANFE:

| Fornecedor | Nº DANFE | Onde ficam os dados |
|---|---|---|
| Somos Sistemas de Ensino | ~2482xxx (série 2) | "ESCOLA: … DRE: … COD: …" nas Informações Complementares |
| Athos Manuseio e Armazenagem | ~439xx/440xx (série 1) | Escola = destinatário (nome, CNPJ, município); DRE em linha própria ("DRE ALTA FLORESTA"); **sem código da escola** |

Problemas encontrados e corrigidos em `services/see_notes.py`:

- **Quantidade = valor total da linha** (ex.: 210 un × R$ 28,00 gravado como 5.880). As palavras eram agrupadas em faixas fixas de 3pt e a quantidade, 0,75pt abaixo do código, caía na faixa vizinha. Agora a linha é montada pela distância ao próprio código do produto (`_row_around`, tolerância de 1,5pt).
- **Produto perdido em descrição longa** (ex.: 930512): havia um limite de 18 palavras entre o código e a unidade. Agora a unidade é procurada em toda a linha.
- **Conferência de quantidade:** quantidade × valor unitário deve bater com o total da linha; caso contrário, gera o alerta `QUANTIDADE_INCONSISTENTE`.
- **Layout Athos:** a DRE vem da linha própria e a escola do destinatário (`_destinatario_escola`). O município é lido do bloco do destinatário (`_municipio`).
- "ESCOLA: DRE X … COD: 1" **não é erro**: é entrega feita na própria DRE.
- Validação: depois da correção, o extrator bate 100% com a auditoria nos 641 PDFs do 3º bimestre e repete exatamente o resultado do 2º bimestre (sem regressão).

### 3. Cadastro DRE/Escola (tabela `see_escolas`)

- Fonte: aba **"2026"** de `C:\workspace\Planilhas\RELATÓRIO_2023_A_2026 MATRÍCULAS_2026.05.12==2026.08.24.xlsx`, colunas `DRE`, `Municipio`, `LotacaoID` (código da escola) e `Lotacao` (nome da escola). O cabeçalho está na linha 5.
- Importação e atualização anual: `python scripts/importar_escolas_see.py "<planilha.xlsx>" --aba 2026`. Atualiza pelo código de lotação, inclui escolas novas e **inativa** (não apaga) as que saíram da planilha. Carga em dev: 643 registros (630 escolas + 13 códigos de DRE).
- Códigos de entrega na própria DRE (1–10, 12, 13, 14) ficam em `DRE_CODIGOS` (`services/see_escolas.py`) e são gravados com `tipo='dre'`.
- **Regras de identificação** (`identificar_escola`). O código de lotação é a única chave confiável; há nomes repetidos entre municípios/DREs.
  - Nota **com código**: valida o código e confere a DRE. Divergência vira alerta, **sem correção automática**.
  - Nota **sem código**: preenche o código só se houver **exatamente uma** escola com o mesmo nome no mesmo município + DRE, ignorando o tipo (EE/EECM/EEI/ESCOLA ESTADUAL), acentos e abreviações (PROF/PROFESSORA, DR…).
  - Nome apenas parecido (título omitido, grafia): alerta `ESCOLA_SUGESTAO` com a escola sugerida, **nunca** preenchimento.
  - Outros alertas: `DRE_FORA_DO_CADASTRO`, `ESCOLA_CODIGO_NAO_CADASTRADO`, `ESCOLA_DRE_DIVERGENTE`, `ESCOLA_MUNICIPIO_DRE_INCONSISTENTE`, `ESCOLA_NAO_ENCONTRADA`, `ESCOLA_INATIVA` (o código inativo continua válido para notas antigas; a busca por nome usa só escolas ativas).
- Resultado no 3º bimestre: 551 códigos conferidos na nota, 79 preenchidos pelo cadastro, 9 sugestões e 2 não encontradas ou inconsistentes (11 notas com alerta, antes eram 90). No 2º bimestre: nenhum alerta.

### 4. Planilha Excel gerada

- **Planilha Consolidada:** usa o nome **oficial** do cadastro; nova coluna **"Conferência"**; linhas pintadas (amarelo = sugestão do cadastro; laranja = não encontrada ou divergente); **legenda** ao lado da tabela (o cabeçalho continua na linha 1).
- **Dados_Extraídos / Auditoria_Arquivos:** novas colunas "Escola (nota)" (nome como veio no PDF) e "Origem do código" (`nota`, `cadastro (nome + município)` ou vazio).
- Os dados do cadastro ficam em `see_processamento_arquivos.dados_extraidos_json["cadastro"]`.

### 5. Tela

- Card "Selecionar DANFEs": passo **"1. Catálogo a ser utilizado"** antes da seleção dos PDFs (sincronizado com o card de catálogo); os botões de pasta/arquivos ficam desabilitados até a escolha de um catálogo com produtos.
- Ao concluir: o card de envio é **limpo** e abre um **modal** de conclusão (resumo e botão Baixar Excel). A pergunta "Deseja acrescentar…" só aparece quando há novos PDFs selecionados para um catálogo já processado.
- Card **"DANFEs Processados"** no topo da página: filtros **Exercício** e Catálogo; mostra só a **versão atual** (último processamento concluído) de cada **catálogo ativo**; permite baixar o Excel novamente (`GET /api/notas-see/processamentos/historico?exercicio=&catalogo_id=`).

### 6. Exercício no catálogo

- Nova coluna `see_catalogos.exercicio` (obrigatória). O nome passa a ser único **por exercício** (`uq_see_catalogo_exercicio_nome`), e não mais no sistema todo.
- Migração: `db/see_catalogo_exercicio.sql` (preenche os catálogos existentes com o ano escrito no nome ou o ano de criação). **Já executada em dev.**
- Formulário do catálogo com o campo Exercício (padrão: ano atual); seletores exibem "exercício · nome".

### Implantação em homologação/produção (checklist)

> **Situação (2026-10-01):** as alterações estão só na branch `dev/cleber`. A subida para homologação/produção foi **adiada** e será feita futuramente; não implantar sem pedido explícito.

1. Executar `db/see_catalogo_exercicio.sql` **antes** de subir o código.
2. Subir o código. A tabela `see_escolas` é criada pelo `db.create_all()` no start.
3. Rodar `scripts/importar_escolas_see.py` com a planilha de escolas.
4. Reprocessar os catálogos desejados ("Não, iniciar uma nova lista") para gerar planilhas com o cadastro, as cores e a legenda.

### Pendências e observações

- Cada "acrescentar" cria um processamento novo com **cópia completa** das notas anteriores (o 2º bimestre tem 19 versões, ~86% dos itens duplicados). A tela mostra só a versão atual; limpar versões antigas ainda não foi feito.
- Os nomes dos catálogos existentes ainda contêm o ano ("2º Bimistres 2026"); renomear pela tela.
- **Não criar tela de cadastro DRE/Escola.** Decisão: aguardar a futura funcionalidade de **cadastro de matrículas de alunos** (desenvolvida em outro projeto), que já trará DRE, município e escolas. O Notas SEE deverá passar a usar essa fonte, para não haver cadastros duplicados em lugares diferentes. Até lá, o cadastro é mantido pelo `scripts/importar_escolas_see.py`.
- Fornecedor com layout novo de DANFE: processar uma amostra e conferir 5–10 notas contra o PDF antes de confiar no resultado. Produtos e quantidades tendem a funcionar (quadro padrão da NF-e); DRE e escola dependem do layout.
- Notas a conferir no 3º bimestre: 44001 ("EE SAGRADO CORACAO DE JESUS", Denise: não consta no cadastro) e 44021 ("EE POXOREO": destinatário em Guarantã do Norte, mas a nota informa DRE Primavera do Leste).
