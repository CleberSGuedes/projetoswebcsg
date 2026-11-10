# Receita Anexo 10 - relatorio de conferencia

Documento para organizar a proxima etapa da funcionalidade `Receita Anexo 10`.

Esta etapa nao deve fazer analise, consolidacao ou cruzamento com outras bases.
O objetivo e criar um relatorio para conferencia dos registros que ja foram
extraidos dos arquivos `Anexo 10` e gravados no banco de dados.

## Nome e local da funcionalidade

Local no menu:

`Relatorios > Execucao > Receita Anexo 10`

Nome visivel:

`Receita Anexo 10`

Permissao sugerida:

`relatorios/receita-anexo10`

Icone sugerido:

`file-earmark-spreadsheet` ou `file-spreadsheet`

## Objetivo

Permitir que o usuario confira, na propria aplicacao, se a extracao dos arquivos
do `Anexo 10` foi gravada corretamente no banco.

O relatorio deve responder perguntas como:

- quais fontes foram importadas;
- quais competencias foram importadas;
- quantas linhas foram extraidas;
- qual arquivo gerou cada linha;
- quais valores foram gravados por codigo de receita;
- se ha registros ativos ou historicos;
- qual `Cod.UO` e `UO` foram gravados para cada registro;
- quem fez o upload;
- quando o arquivo foi enviado;
- qual data/hora de download foi informada no upload.

## Escopo desta etapa

Incluido:

- criar item em `Relatorios > Execucao`;
- criar permissao propria para o relatorio;
- criar tela parcial com tabela de conferencia;
- consultar somente dados ja existentes nas tabelas:
  - `receita_anexo10_registros`;
  - `receita_anexo10_arquivos`;
  - `receita_anexo10_uploads`;
- exibir filtros por coluna;
- exibir totais da visao filtrada;
- permitir baixar Excel com os dados filtraveis ou, no minimo, com os dados
  ativos do banco;
- indicar metadados da ultima carga.

Fora desta etapa:

- criar tabela consolidada de receita;
- fazer analise comparativa;
- criar dashboard;
- cruzar receita com despesa, dotacao, PED, EMP ou NOB;
- criar regras de validacao contabil;
- alterar o processo de upload/extracao.

## Fonte dos dados

Tabela principal:

`receita_anexo10_registros`

Relacionamentos:

- `receita_anexo10_registros.upload_id` -> `receita_anexo10_uploads.id`;
- `receita_anexo10_registros.arquivo_id` -> `receita_anexo10_arquivos.id`.

Regra inicial:

- por padrao, listar apenas `receita_anexo10_registros.ativo = 1`;
- se necessario, adicionar filtro para consultar historico (`ativo = 0`) em uma
  evolucao posterior.

## Campos a exibir na tabela

Campos de conferencia do registro:

- `competencia`;
- `exercicio`;
- `mes`;
- `fonte_recurso`;
- `cod_uo`;
- `uo`;
- `tipo_carga`;
- `mes_fechado`;
- `situacao_movimentacao`;
- `codigo_receita`;
- `descricao_receita`;
- `orcado_atualizado`;
- `arrecadada`;
- `diferenca_para_mais`;
- `diferenca_para_menos`;
- `linha_origem`;
- `pagina_origem`;
- `ativo`;
- `created_at`.

Campos do arquivo de origem:

- `original_filename`;
- `formato_detectado`;
- `hash_sha256`;
- `status`;
- `mensagem`;
- `total_linhas_importadas`.

Campos do upload/lote:

- `upload_id`;
- `user_email`;
- `data_arquivo`;
- `uploaded_at`;
- `modo_upload`;
- `total_arquivos`;
- `total_registros`;
- `total_alertas`;
- `total_erros`.

## Filtros esperados

Filtros principais:

- competencia;
- exercicio;
- mes;
- fonte de recurso;
- codigo da receita;
- descricao da receita;
- Cod.UO;
- UO;
- arquivo original;
- formato detectado;
- usuario;
- status do arquivo.
- tipo da carga;
- mes fechado.

Filtros opcionais:

- intervalo de `uploaded_at`;
- intervalo de `data_arquivo`;
- somente registros ativos;
- upload/lote especifico.

## Totais da tela

Na parte superior do relatorio, mostrar totais considerando os filtros aplicados:

- quantidade de registros;
- quantidade de arquivos distintos;
- quantidade de fontes distintas;
- competencias encontradas;
- total de `orcado_atualizado`;
- total de `arrecadada`;
- total de `diferenca_para_mais`;
- total de `diferenca_para_menos`.

Observacao: esses totais sao apenas somatorios de conferencia da tabela exibida.
Nao devem ser tratados ainda como analise oficial de receita.

## Endpoints propostos

Parcial HTML:

- `GET /partial/relatorios/receita-anexo10`

API JSON:

- `GET /api/relatorios/receita-anexo10`

Download Excel:

- `GET /api/relatorios/receita-anexo10/download`

## Arquivos a criar ou alterar

Alterar:

- `services/features.py`
  - adicionar `relatorios/receita-anexo10` em `Relatorios > Execucao`;
  - adicionar icone em `MENU_META`.
- `rotas/home_routes.py`
  - criar rota parcial;
  - criar API JSON;
  - criar API de download Excel.
- `static/js/main.js`
  - criar inicializador do relatorio;
  - carregar dados da API;
  - renderizar tabela, filtros, paginacao e totais;
  - abrir download Excel.

Criar:

- `templates/partials/relatorios_receita_anexo10.html`

## Padrao visual e tecnico

Seguir o mesmo padrao dos relatorios existentes:

- `templates/partials/relatorios_emp.html`;
- `templates/partials/relatorios_est_emp.html`;
- `templates/partials/relatorios_nob.html`;
- APIs `/api/relatorios/emp`, `/api/relatorios/est-emp` e
  `/api/relatorios/nob`;
- funcoes JS de filtros, paginacao, totais e download ja usadas em relatórios.

## Consulta SQL base sugerida

```sql
SELECT
  r.id,
  r.upload_id,
  r.arquivo_id,
  r.competencia,
  r.exercicio,
  r.mes,
  r.fonte_recurso,
  r.cod_uo,
  r.uo,
  r.codigo_receita,
  r.descricao_receita,
  r.orcado_atualizado,
  r.arrecadada,
  r.diferenca_para_mais,
  r.diferenca_para_menos,
  r.linha_origem,
  r.pagina_origem,
  r.ativo,
  r.created_at,
  a.original_filename,
  a.formato_detectado,
  a.hash_sha256,
  a.status AS arquivo_status,
  a.mensagem AS arquivo_mensagem,
  a.total_linhas_importadas,
  u.user_email,
  u.data_arquivo,
  u.uploaded_at,
  u.modo_upload,
  u.total_arquivos,
  u.total_registros,
  u.total_alertas,
  u.total_erros
FROM receita_anexo10_registros r
LEFT JOIN receita_anexo10_arquivos a ON a.id = r.arquivo_id
LEFT JOIN receita_anexo10_uploads u ON u.id = r.upload_id
WHERE r.ativo = 1
ORDER BY
  r.exercicio,
  r.mes,
  r.fonte_recurso,
  r.cod_uo,
  r.codigo_receita,
  r.id;
```

## Criterios de aceite

Considerar esta etapa concluida quando:

- o menu `Relatorios > Execucao > Receita Anexo 10` aparecer no painel de
  permissoes;
- usuarios com permissao conseguirem abrir o relatorio;
- a tela listar os registros ativos gravados em `receita_anexo10_registros`;
- cada linha mostrar o arquivo de origem;
- filtros por fonte, competencia, Cod.UO, UO, codigo e arquivo funcionarem;
- totais forem recalculados conforme filtros aplicados;
- download Excel gerar arquivo com os mesmos campos principais;
- quando nao houver dados, a tela mostrar estado vazio sem erro;
- a funcionalidade nao alterar, reprocessar ou apagar registros.

## Observacoes para a etapa seguinte

Depois que este relatorio de conferencia estiver validado, a etapa futura
`Analise de Receita` pode usar esses dados como entrada para:

- consolidacao por fonte/competencia;
- comparativos entre competencias;
- totalizadores por codigo de receita;
- cruzamentos com planejamento ou execucao;
- dashboards e indicadores.

## Status da implementacao em 2026-08-03

Implementado:

- item/permissao `relatorios/receita-anexo10` em `Relatorios > Execucao`;
- template `templates/partials/relatorios_receita_anexo10.html`;
- rota parcial `GET /partial/relatorios/receita-anexo10`;
- API JSON `GET /api/relatorios/receita-anexo10`;
- download Excel `GET /api/relatorios/receita-anexo10/download`;
- inicializador JS `initRelatorioReceitaAnexo10()` em `static/js/main.js`;
- tabela com filtros por coluna, paginacao e estado vazio;
- totais recalculados na tela conforme filtros aplicados;
- exportacao Excel com os registros ativos do banco e metadados de arquivo/lote.

Validacoes executadas:

- renderizacao da parcial em contexto autenticado simulado;
- API JSON retornando `ok = true`;
- download Excel retornando arquivo `.xlsx`;
- `py_compile` de `services/features.py` e `rotas/home_routes.py`;
- `node --check static/js/main.js`.

Atualizacao apos regras de carga aberta/fechada:

- a tabela passou a exibir `Cod.UO`, `UO`, `Tipo Carga` e `Mes Fechado`;
- os filtros seguem o mesmo padrao multi-selecao dos demais relatorios;
- os totais permanecem calculados sobre os filtros aplicados;
- a API do relatorio foi validada contra a carga final no banco remoto:
  - 2.226 linhas de conferencia retornadas;
  - 2.212 registros de receita ativos;
  - 14 linhas de arquivo valido com `Não houve movimentação no período.`;
  - primeira linha validada com `cod_uo = 9900`, `uo = ESTADO`,
    `tipo_carga = fechada` e `mes_fechado = Sim`;
- a exportacao Excel inclui os campos de carga e UO normalizada.

## Ajustes finais em 2026-08-03

- Os filtros da tabela foram alinhados ao padrão dos demais relatórios.
- O bloco `Totais (filtros aplicados)` foi organizado em duas colunas.
- As linhas sem movimentação aparecem no relatório como conferência, com
  `Situação = Não houve movimentação no período.`
- O download Excel recebeu cabeçalhos com acentuação e campos normalizados de
  UO.
- O nome da UO não deve carregar mês/ano extraído do cabeçalho do arquivo.
- Após a limpeza final para teste manual, o relatório deve iniciar sem dados até
  que um novo upload seja feito pelo usuário.
