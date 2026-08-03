# Receita Anexo 10 - tabelas necessarias no banco

Documento criado apos analise do banco remoto MySQL usado pelo projeto.

## Banco remoto analisado

Consulta executada no banco remoto configurado no projeto, via SQLAlchemy e
`information_schema`.

Resultado geral:

- total de tabelas existentes: 90;
- nao existem tabelas especificas para `Receita Anexo 10`, `receita_anexo10`
  ou similar;
- ja existem tabelas genericas de processamento em segundo plano:
  - `processamento_jobs`;
  - `processamento_eventos`.

Essas duas tabelas genericas devem ser reutilizadas para status, progresso,
cancelamento, tentativas, eventos, alertas e erros.

## Padrao existente observado

Tabelas de upload existentes seguem este padrao:

- `emp_uploads`;
- `est_emp_uploads`;
- `fip613_uploads`;
- `ped_uploads`;
- `nob_uploads`;
- `plan20_uploads`.

Campos comuns:

- `id`;
- `user_email`;
- `original_filename`;
- `stored_filename`;
- `output_filename`;
- `data_arquivo`;
- `uploaded_at`.

Tabelas de dados importados seguem este padrao:

- `emp`;
- `est_emp`;
- `fip613`;
- `ped`;
- `nob`.

Campos comuns importantes:

- `id`;
- `upload_id`;
- campos extraidos do relatorio;
- `raw_payload`;
- `data_arquivo`;
- `user_email`;
- `ativo`;
- `created_at`.

## Decisao de modelagem para Receita Anexo 10

A nova funcionalidade precisa processar:

- um unico arquivo;
- varios arquivos enviados juntos;
- futuramente, todos os arquivos de uma pasta/lote.

Por isso, apenas uma tabela simples de upload nao e suficiente. Precisamos de
um registro pai para o lote e um registro por arquivo fisico processado.

Tabelas novas propostas:

1. `receita_anexo10_uploads`
2. `receita_anexo10_arquivos`
3. `receita_anexo10_registros`

Tabelas reutilizadas:

4. `processamento_jobs`
5. `processamento_eventos`

A tabela consolidada/unificada de receita fica fora desta primeira etapa.

## Tabela 1: receita_anexo10_uploads

Finalidade:

Controlar a solicitacao de upload/processamento. Um registro pode representar um
upload de arquivo unico ou um lote com varios arquivos.

Uso:

- `processamento_jobs.upload_id` deve apontar para `receita_anexo10_uploads.id`
  quando `processamento_jobs.tipo = 'receita_anexo10'`;
- a tela de status deve consultar o ultimo upload/lote;
- reprocessamento pode usar o ultimo lote;
- cancelamento atua sobre o job do lote.

Campos propostos:

| Campo | Tipo sugerido | Obrigatorio | Observacao |
|---|---:|---:|---|
| `id` | `BIGINT AUTO_INCREMENT` | sim | chave primaria |
| `user_email` | `VARCHAR(255)` | sim | usuario que enviou |
| `modo_upload` | `VARCHAR(30)` | sim | `arquivo_unico`, `multiplos_arquivos`, `zip`, `pasta` |
| `original_filename` | `VARCHAR(255)` | nao | nome original quando for arquivo unico ou ZIP |
| `stored_filename` | `VARCHAR(255)` | nao | nome armazenado quando for arquivo unico ou ZIP |
| `output_filename` | `VARCHAR(255)` | nao | opcional, caso seja gerado resumo/planilha |
| `data_arquivo` | `DATETIME` | nao | data/hora do download informada pelo usuario |
| `uploaded_at` | `DATETIME` | sim | default `CURRENT_TIMESTAMP` |
| `total_arquivos` | `INT` | sim | default `0` |
| `arquivos_processados` | `INT` | sim | default `0` |
| `arquivos_sucesso` | `INT` | sim | default `0` |
| `arquivos_alerta` | `INT` | sim | default `0` |
| `arquivos_erro` | `INT` | sim | default `0` |
| `total_registros` | `BIGINT` | sim | default `0` |
| `total_alertas` | `INT` | sim | default `0` |
| `total_erros` | `INT` | sim | default `0` |
| `detalhes_alertas` | `LONGTEXT` | nao | JSON de alertas do lote |
| `erro_tecnico` | `LONGTEXT` | nao | erro geral do lote |

Indices sugeridos:

- `PRIMARY KEY (id)`;
- `INDEX idx_receita_uploads_uploaded_at (uploaded_at)`;
- `INDEX idx_receita_uploads_data_arquivo (data_arquivo)`;
- `INDEX idx_receita_uploads_user_email (user_email)`.

DDL sugerido:

```sql
CREATE TABLE receita_anexo10_uploads (
  id BIGINT NOT NULL AUTO_INCREMENT,
  user_email VARCHAR(255) NOT NULL,
  modo_upload VARCHAR(30) NOT NULL DEFAULT 'arquivo_unico',
  original_filename VARCHAR(255) NULL,
  stored_filename VARCHAR(255) NULL,
  output_filename VARCHAR(255) NULL,
  data_arquivo DATETIME NULL,
  uploaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  total_arquivos INT NOT NULL DEFAULT 0,
  arquivos_processados INT NOT NULL DEFAULT 0,
  arquivos_sucesso INT NOT NULL DEFAULT 0,
  arquivos_alerta INT NOT NULL DEFAULT 0,
  arquivos_erro INT NOT NULL DEFAULT 0,
  total_registros BIGINT NOT NULL DEFAULT 0,
  total_alertas INT NOT NULL DEFAULT 0,
  total_erros INT NOT NULL DEFAULT 0,
  detalhes_alertas LONGTEXT NULL,
  erro_tecnico LONGTEXT NULL,
  PRIMARY KEY (id),
  KEY idx_receita_uploads_uploaded_at (uploaded_at),
  KEY idx_receita_uploads_data_arquivo (data_arquivo),
  KEY idx_receita_uploads_user_email (user_email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## Tabela 2: receita_anexo10_arquivos

Finalidade:

Registrar cada arquivo fisico recebido dentro do upload/lote.

Uso:

- em upload unico, tera 1 registro;
- em lote/pasta, tera 1 registro por arquivo;
- erros devem ser registrados por arquivo, sem derrubar necessariamente o lote
  inteiro;
- a deteccao de formato real fica registrada aqui.

Campos propostos:

| Campo | Tipo sugerido | Obrigatorio | Observacao |
|---|---:|---:|---|
| `id` | `BIGINT AUTO_INCREMENT` | sim | chave primaria |
| `upload_id` | `BIGINT` | sim | referencia ao lote em `receita_anexo10_uploads` |
| `user_email` | `VARCHAR(255)` | sim | redundante para auditoria |
| `original_filename` | `VARCHAR(255)` | sim | nome recebido |
| `stored_filename` | `VARCHAR(255)` | sim | nome salvo em `upload/receita_anexo10` |
| `extensao_original` | `VARCHAR(20)` | nao | extensao informada pelo arquivo |
| `formato_detectado` | `VARCHAR(40)` | sim | `pdf`, `xls_biff`, `html_excel`, `xml_excel`, `xlsx`, `csv_br`, `desconhecido` |
| `mime_detectado` | `VARCHAR(120)` | nao | opcional |
| `hash_sha256` | `CHAR(64)` | sim | rastreabilidade e duplicidade |
| `tamanho_bytes` | `BIGINT` | sim | tamanho do arquivo |
| `relatorio_detectado` | `VARCHAR(120)` | nao | esperado: `Anexo 10` |
| `fonte_recurso` | `VARCHAR(50)` | nao | exemplo `1.500.1001` |
| `exercicio` | `SMALLINT` | nao | exemplo `2026` |
| `mes` | `TINYINT` | nao | `1` a `12` |
| `competencia` | `VARCHAR(7)` | nao | exemplo `2026.01` |
| `orgao_codigo` | `VARCHAR(30)` | nao | exemplo `14101`, quando houver |
| `orgao_nome` | `VARCHAR(255)` | nao | exemplo `SECRETARIA DE ESTADO DE EDUCACAO` |
| `escopo_relatorio` | `VARCHAR(255)` | nao | exemplo `CONSOLIDADO DO ESTADO` |
| `status` | `VARCHAR(40)` | sim | `aguardando`, `processado`, `alerta`, `erro`, `ignorado` |
| `mensagem` | `VARCHAR(1000)` | nao | resumo por arquivo |
| `total_linhas_detectadas` | `BIGINT` | sim | default `0` |
| `total_linhas_importadas` | `BIGINT` | sim | default `0` |
| `total_alertas` | `INT` | sim | default `0` |
| `total_erros` | `INT` | sim | default `0` |
| `alertas_json` | `LONGTEXT` | nao | alertas por arquivo |
| `erro_tecnico` | `LONGTEXT` | nao | erro tecnico por arquivo |
| `data_arquivo` | `DATETIME` | nao | herdada do upload ou detectada |
| `created_at` | `DATETIME` | sim | default `CURRENT_TIMESTAMP` |

Indices sugeridos:

- `PRIMARY KEY (id)`;
- `INDEX idx_receita_arquivos_upload_id (upload_id)`;
- `INDEX idx_receita_arquivos_hash (hash_sha256)`;
- `INDEX idx_receita_arquivos_competencia_fonte (competencia, fonte_recurso)`;
- `INDEX idx_receita_arquivos_status (status)`.

DDL sugerido:

```sql
CREATE TABLE receita_anexo10_arquivos (
  id BIGINT NOT NULL AUTO_INCREMENT,
  upload_id BIGINT NOT NULL,
  user_email VARCHAR(255) NOT NULL,
  original_filename VARCHAR(255) NOT NULL,
  stored_filename VARCHAR(255) NOT NULL,
  extensao_original VARCHAR(20) NULL,
  formato_detectado VARCHAR(40) NOT NULL,
  mime_detectado VARCHAR(120) NULL,
  hash_sha256 CHAR(64) NOT NULL,
  tamanho_bytes BIGINT NOT NULL DEFAULT 0,
  relatorio_detectado VARCHAR(120) NULL,
  fonte_recurso VARCHAR(50) NULL,
  exercicio SMALLINT NULL,
  mes TINYINT NULL,
  competencia VARCHAR(7) NULL,
  orgao_codigo VARCHAR(30) NULL,
  orgao_nome VARCHAR(255) NULL,
  escopo_relatorio VARCHAR(255) NULL,
  status VARCHAR(40) NOT NULL DEFAULT 'aguardando',
  mensagem VARCHAR(1000) NULL,
  total_linhas_detectadas BIGINT NOT NULL DEFAULT 0,
  total_linhas_importadas BIGINT NOT NULL DEFAULT 0,
  total_alertas INT NOT NULL DEFAULT 0,
  total_erros INT NOT NULL DEFAULT 0,
  alertas_json LONGTEXT NULL,
  erro_tecnico LONGTEXT NULL,
  data_arquivo DATETIME NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_receita_arquivos_upload_id (upload_id),
  KEY idx_receita_arquivos_hash (hash_sha256),
  KEY idx_receita_arquivos_competencia_fonte (competencia, fonte_recurso),
  KEY idx_receita_arquivos_status (status),
  CONSTRAINT fk_receita_arquivos_upload
    FOREIGN KEY (upload_id) REFERENCES receita_anexo10_uploads (id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

Observacao: as tabelas antigas de dados importados no projeto nem sempre usam
foreign key em `upload_id`. Para esta nova funcionalidade, recomenda-se usar FK
entre lote e arquivos porque o relacionamento e direto e ajuda na limpeza de
processamentos incompletos.

## Tabela 3: receita_anexo10_registros

Finalidade:

Guardar as linhas extraidas do `Anexo 10`.

Uso:

- cada linha do relatorio vira um registro;
- cada registro deve apontar para o lote (`upload_id`) e para o arquivo fisico
  (`arquivo_id`);
- esta tabela sera a base para a futura tabela consolidada/unificada.

Campos propostos:

| Campo | Tipo sugerido | Obrigatorio | Observacao |
|---|---:|---:|---|
| `id` | `BIGINT AUTO_INCREMENT` | sim | chave primaria |
| `upload_id` | `BIGINT` | sim | lote |
| `arquivo_id` | `BIGINT` | sim | arquivo origem |
| `fonte_recurso` | `VARCHAR(50)` | nao | fonte do relatorio |
| `exercicio` | `SMALLINT` | nao | exercicio |
| `mes` | `TINYINT` | nao | mes |
| `competencia` | `VARCHAR(7)` | nao | `YYYY.MM` |
| `orgao_codigo` | `VARCHAR(30)` | nao | quando houver |
| `orgao_nome` | `VARCHAR(255)` | nao | quando houver |
| `escopo_relatorio` | `VARCHAR(255)` | nao | consolidado/orgao |
| `codigo_receita` | `VARCHAR(50)` | sim | codigo da receita |
| `descricao_receita` | `VARCHAR(1000)` | sim | descricao |
| `orcado_atualizado` | `DECIMAL(20,2)` | nao | valor monetario |
| `arrecadada` | `DECIMAL(20,2)` | nao | valor monetario |
| `diferenca_para_mais` | `DECIMAL(20,2)` | nao | valor monetario |
| `diferenca_para_menos` | `DECIMAL(20,2)` | nao | valor monetario, pode ser negativo |
| `linha_origem` | `INT` | nao | linha original ou sequencia extraida |
| `pagina_origem` | `INT` | nao | pagina do PDF |
| `raw_payload` | `LONGTEXT` | nao | JSON/texto original da linha |
| `data_arquivo` | `DATETIME` | nao | data/hora do download |
| `user_email` | `VARCHAR(255)` | nao | usuario |
| `ativo` | `TINYINT(1)` | sim | default `1`; usar `0` durante carga provisoria |
| `created_at` | `DATETIME` | sim | default `CURRENT_TIMESTAMP` |

Indices sugeridos:

- `PRIMARY KEY (id)`;
- `INDEX idx_receita_registros_upload_id (upload_id)`;
- `INDEX idx_receita_registros_arquivo_id (arquivo_id)`;
- `INDEX idx_receita_registros_competencia_fonte (competencia, fonte_recurso)`;
- `INDEX idx_receita_registros_codigo (codigo_receita)`;
- `INDEX idx_receita_registros_ativo (ativo)`.

DDL sugerido:

```sql
CREATE TABLE receita_anexo10_registros (
  id BIGINT NOT NULL AUTO_INCREMENT,
  upload_id BIGINT NOT NULL,
  arquivo_id BIGINT NOT NULL,
  fonte_recurso VARCHAR(50) NULL,
  exercicio SMALLINT NULL,
  mes TINYINT NULL,
  competencia VARCHAR(7) NULL,
  orgao_codigo VARCHAR(30) NULL,
  orgao_nome VARCHAR(255) NULL,
  escopo_relatorio VARCHAR(255) NULL,
  codigo_receita VARCHAR(50) NOT NULL,
  descricao_receita VARCHAR(1000) NOT NULL,
  orcado_atualizado DECIMAL(20,2) NULL,
  arrecadada DECIMAL(20,2) NULL,
  diferenca_para_mais DECIMAL(20,2) NULL,
  diferenca_para_menos DECIMAL(20,2) NULL,
  linha_origem INT NULL,
  pagina_origem INT NULL,
  raw_payload LONGTEXT NULL,
  data_arquivo DATETIME NULL,
  user_email VARCHAR(255) NULL,
  ativo TINYINT(1) NOT NULL DEFAULT 1,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_receita_registros_upload_id (upload_id),
  KEY idx_receita_registros_arquivo_id (arquivo_id),
  KEY idx_receita_registros_competencia_fonte (competencia, fonte_recurso),
  KEY idx_receita_registros_codigo (codigo_receita),
  KEY idx_receita_registros_ativo (ativo),
  CONSTRAINT fk_receita_registros_upload
    FOREIGN KEY (upload_id) REFERENCES receita_anexo10_uploads (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_receita_registros_arquivo
    FOREIGN KEY (arquivo_id) REFERENCES receita_anexo10_arquivos (id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## Tabelas que nao precisamos criar

Nao criar tabelas novas para status/eventos nesta primeira etapa, porque ja
existem:

- `processamento_jobs`;
- `processamento_eventos`.

Uso planejado:

- `processamento_jobs.tipo = 'receita_anexo10'`;
- `processamento_jobs.upload_id = receita_anexo10_uploads.id`;
- eventos de progresso e alertas gravados em `processamento_eventos`.

Tambem nao criar ainda a tabela consolidada/unificada de receita. Ela pertence
ao segundo momento mencionado no planejamento funcional.

## Ajustes necessarios no codigo apos criar as tabelas

Modelos SQLAlchemy:

- `ReceitaAnexo10Upload`;
- `ReceitaAnexo10Arquivo`;
- `ReceitaAnexo10Registro`.

Arquivos a alterar:

- `models/user.py`;
- `models/__init__.py`;
- `services/features.py`;
- `rotas/home_routes.py`;
- `worker.py`;
- novo runner `services/receita_anexo10_runner.py`;
- novo template `templates/partials/atualizar_receita_anexo10.html`;
- `static/js/main.js`.

## Observacao sobre nomes

O nome `receita_anexo10_*` foi escolhido para deixar claro que a primeira etapa
trata especificamente do relatorio `Anexo 10`. Se depois a area de Receita
absorver outros relatorios, criar novas tabelas por relatorio ou uma camada
consolidada generica em uma segunda fase.

## Status atual em 2026-08-03

As tres tabelas foram criadas no banco remoto usado pelo projeto:

- `receita_anexo10_uploads`;
- `receita_anexo10_arquivos`;
- `receita_anexo10_registros`.

Tambem foram criados os modelos SQLAlchemy correspondentes em `models/user.py` e
exportados em `models/__init__.py`.

Validacao de estrutura executada apos a criacao inicial:

- `receita_anexo10_uploads`: 18 colunas;
- `receita_anexo10_arquivos`: 28 colunas;
- `receita_anexo10_registros`: 23 colunas.

Validacao de estrutura executada apos a aplicacao das regras de mes
aberto/fechado:

- `receita_anexo10_uploads`: 24 colunas;
- `receita_anexo10_arquivos`: 34 colunas;
- `receita_anexo10_registros`: 29 colunas.

Durante os testes autorizados, foi executada limpeza somente nas tabelas
`receita_anexo10_*` e nos jobs/eventos com `tipo = 'receita_anexo10'`.

## Campos adicionais implementados para mes aberto/fechado

A regra de upload por `Mes fechado = Sim/Nao` exige controle explicito do tipo
de carga. Esses campos ja foram aplicados no banco remoto.

Alteracoes sugeridas em `receita_anexo10_uploads`:

| Campo | Tipo sugerido | Observacao |
|---|---:|---|
| `mes_fechado` | `TINYINT(1)` | valor informado pelo usuario |
| `tipo_carga` | `VARCHAR(20)` | `aberta` ou `fechada` |
| `status_validacao` | `VARCHAR(30)` | `pendente`, `validado`, `bloqueado` |
| `mensagem_validacao` | `LONGTEXT` | detalhes de bloqueio/validacao |
| `substitui_upload_ids` | `LONGTEXT` | JSON com uploads abertos substituidos |

Alteracoes sugeridas em `receita_anexo10_arquivos`:

| Campo | Tipo sugerido | Observacao |
|---|---:|---|
| `mes_fechado` | `TINYINT(1)` | herdado do upload |
| `tipo_carga` | `VARCHAR(20)` | `aberta` ou `fechada` |
| `cod_uo` | `VARCHAR(30)` | codigo normalizado da UO, exemplo `9900` ou `14101` |
| `uo` | `VARCHAR(255)` | nome normalizado da UO, exemplo `ESTADO` |
| `chave_competencia_fonte_uo` | `VARCHAR(120)` | exemplo `2026.07|1.540.1070|9900` |
| `substituido_por_upload_id` | `BIGINT` | upload que substituiu a carga aberta |

Alteracoes sugeridas em `receita_anexo10_registros`:

| Campo | Tipo sugerido | Observacao |
|---|---:|---|
| `mes_fechado` | `TINYINT(1)` | herdado do upload |
| `tipo_carga` | `VARCHAR(20)` | `aberta` ou `fechada` |
| `cod_uo` | `VARCHAR(30)` | codigo normalizado da UO, exemplo `9900` ou `14101` |
| `uo` | `VARCHAR(255)` | nome normalizado da UO, exemplo `ESTADO` |
| `chave_competencia_fonte_uo` | `VARCHAR(120)` | exemplo `2026.07|1.540.1070|9900` |
| `substituido_por_upload_id` | `BIGINT` | upload que substituiu estes registros |

Indices sugeridos:

```sql
CREATE INDEX idx_receita_registros_comp_fonte_tipo_ativo
  ON receita_anexo10_registros (competencia, fonte_recurso, cod_uo, tipo_carga, ativo);

CREATE INDEX idx_receita_arquivos_comp_fonte_tipo_status
  ON receita_anexo10_arquivos (competencia, fonte_recurso, cod_uo, tipo_carga, status);

CREATE INDEX idx_receita_uploads_tipo_uploaded
  ON receita_anexo10_uploads (tipo_carga, uploaded_at);
```

Regra de negocio esperada:

- carga `aberta`: pode ser substituida por novo upload aberto da mesma
  competencia/fonte/Cod.UO;
- carga `fechada`: bloqueia novo upload comum da mesma competencia/fonte/Cod.UO;
- uma carga fechada pode substituir uma carga aberta existente da mesma
  competencia/fonte/Cod.UO;
- registros substituidos permanecem no banco com `ativo = 0` para auditoria.

Normalizacao obrigatoria de UO:

- quando o conteudo do arquivo indicar `CONSOLIDADO DO ESTADO`, gravar
  `cod_uo = '9900'` e `uo = 'ESTADO'`;
- quando o conteudo indicar orgao especifico, exemplo
  `14101 - SECRETARIA DE ESTADO DE EDUCACAO`, gravar `cod_uo = '14101'` e
  `uo = 'SECRETARIA DE ESTADO DE EDUCACAO'`;
- a competencia deve ser detectada pelo conteudo do arquivo, nao pelo nome do
  arquivo.

## Validação integrada executada

Data da validação: 2026-08-03.

Antes da limpeza final para teste manual, foi executada uma carga fechada com os
arquivos da pasta de amostra `ANEXO 10`:

- `receita_anexo10_registros`: 2.212 registros ativos;
- `receita_anexo10_arquivos`: 101 arquivos no lote;
- status dos arquivos: 83 `processado`, 14 `alerta` e 4 `erro`;
- `receita_anexo10_uploads`: 1 lote;
- `tipo_carga`: `fechada`;
- `mes_fechado`: `Sim`.

Arquivos com `alerta` correspondem a Anexo 10 válido sem movimentação no
período, portanto ficam registrados como arquivo válido sem linhas importadas.

Arquivos com `erro` nessa validação:

- 2 arquivos `FIP 729`, rejeitados porque não são `Anexo 10`;
- `Fiplan (1).xls`, rejeitado por ausência de fonte de recurso no conteúdo;
- `Fiplan.xls`, rejeitado por duplicar a mesma chave
  `competencia + fonte_recurso + Cod.UO` dentro do lote.

## Estado atual para testes manuais

Depois da validação integrada, foi executada nova limpeza restrita à Receita
Anexo 10 para o usuário realizar os testes manuais de upload:

- `receita_anexo10_uploads`: 0 registros;
- `receita_anexo10_arquivos`: 0 registros;
- `receita_anexo10_registros`: 0 registros;
- `processamento_jobs` com `tipo = 'receita_anexo10'`: 0 registros;
- `processamento_eventos` vinculados aos jobs de Receita Anexo 10: removidos.

Nenhuma tabela fora da funcionalidade Receita Anexo 10 foi truncada.
