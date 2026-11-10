# Receita Anexo 10 - requisitos e plano de implementacao

Documento inicial para organizar a nova funcionalidade `Receita Anexo 10`.
Este documento trata apenas da primeira etapa: upload, leitura dos arquivos do
relatorio `Anexo 10` e persistencia no banco. A etapa de consolidacao/uniao das
informacoes em outra tabela fica para um segundo momento.

Decisao de nomenclatura: esta primeira etapa fica como `Receita Anexo 10`,
porque executa extracao e carga do relatorio. O nome `Analise de Receita` fica
reservado para a segunda etapa, quando os dados ja estiverem consolidados no
banco e houver consulta/analise propriamente dita.

## Objetivo da funcionalidade

Criar uma funcionalidade em:

`Administrar SPO > Atualizar Execucao Orcamentaria > Receita Anexo 10`

O usuario deve conseguir enviar arquivos do relatorio `Anexo 10 - Comparativo da
Receita Orcada com a Arrecadada`, baixados por fonte de recurso em sistema
externo legado, acompanhar o processamento em segundo plano e gravar os dados
extraidos no banco de dados.

O comportamento operacional deve seguir o mesmo padrao usado em `EMP`:

- tela de upload em modulo de atualizacao;
- informacao de data/hora do download;
- processamento em background;
- consulta de status;
- reprocessamento;
- cancelamento;
- registro de upload;
- registros extraidos vinculados ao upload;
- preservacao de carga anterior quando um processamento novo falhar, sempre que
  possivel.

## Escopo da primeira etapa

Incluido nesta etapa:

- criar item de permissao e menu em `Administrar SPO > Atualizar Execucao
  Orcamentaria`;
- criar tela parcial de upload para `Receita Anexo 10`;
- aceitar varios formatos de entrada gerados pelo sistema externo;
- detectar o tipo real do arquivo por assinatura/conteudo, nao apenas pela
  extensao;
- validar se o arquivo parece ser `Anexo 10`;
- extrair cabecalho e linhas do relatorio;
- gravar metadados do upload;
- gravar linhas extraidas em tabela bruta/normalizada de receita;
- registrar status, progresso, alertas e erros do processamento;
- permitir reprocessar o ultimo upload;
- permitir cancelar processamento em andamento.

Fora desta etapa:

- tabela consolidada que une todas as informacoes;
- dashboards, graficos e analises finais;
- cruzamento com outras bases;
- regras contabeis de interpretacao alem da leitura fiel do `Anexo 10`;
- deduplicacao definitiva entre diferentes fontes/competencias, exceto controles
  basicos para evitar duplicidade exata por upload.

## Amostras analisadas

Pasta analisada:

`C:\workspace\Documentacao\FIP-613+\ANEXO 10`

Observacao: o caminho original no Windows usa a pasta `Documentacao` com
acentuacao. No documento mantemos ASCII para evitar problemas de encoding.

Resumo encontrado:

- total de arquivos: 101;
- PDFs: 99;
- arquivos `.xls`: 2;
- arquivos identificados como `Anexo 10` pelo nome: 97;
- fontes identificadas nos nomes dos PDFs do Anexo 10: 16;
- competencias identificadas: `2026.01` a `2026.07`;
- arquivos fora do padrao `Anexo 10` na mesma pasta:
  - `FIP 729 F 1.500.1001_2026.06_2026+07.21.pdf`;
  - `FIP 729 F 1.500.1001_2026.07_2026+07.21.pdf`;
  - `Fiplan.xls`;
  - `Fiplan (1).xls`.

Fontes observadas:

- `1.500.1001`;
- `1.501.0000`;
- `1.540.0000`;
- `1.540.1070`;
- `1.540.1071`;
- `1.540.1072`;
- `1.546.0000`;
- `1.550.0000`;
- `1.551.0000`;
- `1.552.0000`;
- `1.553.0000`;
- `1.569.0000`;
- `1.570.0000`;
- `1.570.3110`;
- `1.570.3120`;
- `1.574.0000`.

## Formatos que a aplicacao deve aceitar

O sistema externo usa extensoes inconsistentes. Portanto, a aplicacao deve
detectar o formato real.

Formatos previstos:

- `.pdf`: PDF real do FIPLAN.
- `.xls` BIFF antigo: planilha Excel binaria real, assinatura OLE/CFB
  `D0 CF 11 E0 A1 B1 1A E1`.
- `.xls` HTML: arquivo com extensao `.xls`, mas conteudo inicia com `<html ...>`
  e estrutura de tabela HTML gerada pelo Excel/FIPLAN.
- `.xls` XML/SpreadsheetML: arquivo com extensao `.xls`, mas conteudo XML.
- `.xlsx` real: arquivo ZIP com estrutura Office Open XML.
- `.xlsx` falso: extensao `.xlsx`, mas conteudo real HTML, XML, CSV ou outro.
- `.csv`: CSV com padrao brasileiro, separador `;`, decimal `,` e milhares `.`.

Regra importante: extensao deve ser usada apenas como pista. A decisao final
deve usar assinatura/conteudo.

## Estrutura observada do Anexo 10

Titulo:

`Anexo 10 - Comparativo da Receita Orcada com a Arrecadada`

Cabecalhos observados no PDF:

- `ESTADO DE MATO GROSSO`;
- `SECRETARIA DE ESTADO DE FAZENDA`;
- `FIPLAN - SISTEMA INTEGRADO DE PLANEJAMENTO, CONTABILIDADE E FINANCAS`;
- `Fonte de Recurso: 1.500.1001`;
- periodo/competencia, por exemplo `Janeiro/2026`;
- em alguns arquivos HTML: orgao/unidade `14101 - SECRETARIA DE ESTADO DE
  EDUCACAO`;
- em PDFs analisados: aparece tambem `CONSOLIDADO DO ESTADO`.

Colunas da tabela:

- `CODIGO`;
- `DESCRICAO`;
- `ORCADO ATUALIZADO`;
- `ARRECADADA`;
- `DIFERENCA PARA MAIS`;
- `DIFERENCA PARA MENOS`.

Exemplo de linha extraida do PDF:

```text
1.0.0.0.00.0.0.00 | Receitas Correntes | 1.857.190.289,00 | 155.319.502,52 | 0,00 | -1.701.870.786,48
```

Exemplo de linha extraida do `.xls` HTML:

```text
1.3.2.1.01.1.1.36 | Receita de Aplicacao Financeira de Recursos do Fundeb-Principal | 28.835.298,00 | 13.134.487,42 | 0,00 | -15.700.810,58
```

Observacao do relatorio:

```text
A partir do exercicio de 2021 este anexo nao demonstra valores referente cotas e repasses financeiros.
```

## Campos que precisamos persistir

Tabela de uploads, sugestao: `receita_anexo10_uploads`.

Campos sugeridos:

- `id`;
- `user_email`;
- `original_filename`;
- `stored_filename`;
- `output_filename`, se houver arquivo gerado;
- `data_arquivo`, data/hora informada pelo usuario;
- `uploaded_at`;
- `formato_detectado`: `pdf`, `xls_biff`, `html_excel`, `xml_excel`, `xlsx`,
  `csv_br`, `desconhecido`;
- `hash_arquivo`, para rastreabilidade e deteccao de duplicidade;
- `tamanho_bytes`;
- `fonte_recurso_detectada`;
- `exercicio_detectado`;
- `mes_detectado`;
- `competencia_detectada`;
- `relatorio_detectado`;
- `total_linhas_detectadas`;
- `total_linhas_importadas`;
- `alertas_json`, opcional;
- `erro_tecnico`, opcional.

Tabela de linhas, sugestao: `receita_anexo10_registros`.

Campos sugeridos:

- `id`;
- `upload_id`;
- `fonte_recurso`;
- `exercicio`;
- `mes`;
- `competencia`;
- `orgao_codigo`;
- `orgao_nome`;
- `escopo_relatorio`, exemplo `CONSOLIDADO DO ESTADO` ou orgao;
- `codigo_receita`;
- `descricao_receita`;
- `orcado_atualizado`;
- `arrecadada`;
- `diferenca_para_mais`;
- `diferenca_para_menos`;
- `linha_origem`;
- `pagina_origem`, para PDF;
- `raw_payload`, JSON/texto com a linha original e dados auxiliares;
- `data_arquivo`;
- `user_email`;
- `ativo`;
- `created_at`.

Tipos monetarios devem ser `Numeric(18, 2)` ou superior. Entrada brasileira deve
ser normalizada:

- `1.857.190.289,00` -> `1857190289.00`;
- `0,00` -> `0.00`;
- `-1.701.870.786,48` -> `-1701870786.48`.

## Pipeline tecnico proposto

1. Receber arquivo pela tela `Receita Anexo 10`.
2. Salvar em `upload/receita_anexo10`.
3. Calcular hash SHA-256 e tamanho.
4. Detectar formato real:
   - `%PDF` -> PDF;
   - `PK\x03\x04` -> XLSX/ZIP real;
   - `D0 CF 11 E0 A1 B1 1A E1` -> XLS BIFF/OLE;
   - `<html`, `<!doctype html` -> HTML Excel;
   - `<?xml`, `<Workbook` -> XML Excel;
   - texto com `;` e muitas quebras de linha -> CSV brasileiro.
5. Criar registro em `receita_anexo10_uploads`.
6. Criar job em `processamento_jobs` com tipo `receita_anexo10`.
7. Worker inicia processamento em background.
8. Parser extrai cabecalho:
   - relatorio;
   - fonte de recurso;
   - mes/exercicio;
   - orgao/escopo, quando houver.
9. Parser extrai linhas da tabela.
10. Validar colunas obrigatorias.
11. Converter valores monetarios.
12. Gravar registros com `ativo=0` durante processamento, seguindo o padrao de
    cargas gerenciadas.
13. Ao finalizar com sucesso:
    - desativar carga anterior equivalente, se esta for a regra definida;
    - ativar registros novos;
    - atualizar upload e job.
14. Em falha:
    - remover carga provisoria;
    - preservar carga anterior;
    - registrar erro e alertas.

## Padrao de background a reutilizar

Referencia principal:

- `templates/partials/atualizar_emp.html`;
- rotas `/api/emp/status`, `/api/emp/upload`, `/api/emp/reprocess`,
  `/api/emp/cancel`;
- `worker.py`;
- `services/processamento_jobs.py`;
- modelos `EmpUpload` e `ProcessamentoJob`.

Para esta nova funcionalidade, a recomendacao e usar o padrao gerenciado mais
recente de `ProcessamentoJob`, como `PED`, `FIP613` e `EST EMP`, em vez do
status historico simples do `EMP`.

Itens a criar/adaptar:

- `ReceitaAnexo10Upload` em `models/user.py`;
- `ReceitaAnexo10Registro` em `models/user.py`;
- exportar modelos em `models/__init__.py`;
- adicionar permissao `atualizar/receita-anexo10` em `services/features.py`;
- adicionar icone no `MENU_META`;
- rota parcial `/partial/atualizar/receita-anexo10`;
- template `templates/partials/atualizar_receita_anexo10.html`;
- endpoints:
  - `GET /api/receita-anexo10/status`;
  - `POST /api/receita-anexo10/upload`;
  - `POST /api/receita-anexo10/reprocess`;
  - `POST /api/receita-anexo10/cancel`;
- runner Python, sugestao `services/receita_anexo10_runner.py`;
- incluir `receita_anexo10` nas configuracoes de `_run_managed_job` em
  `worker.py`;
- adicionar inicializacao JS em `static/js/main.js`, seguindo o padrao visual dos
  uploads de execucao.

## Dependencias e riscos tecnicos

Dependencias atuais observadas:

- `pdfplumber` esta no `requirements.txt` e existe na `.venv` do projeto;
- `pandas` e `openpyxl` existem;
- `lxml`, `bs4` e `xlrd` nao estavam instalados na `.venv` testada.

Implicacoes:

- PDF pode ser lido com `pdfplumber`;
- XLSX real pode ser lido com `openpyxl`/`pandas`;
- CSV brasileiro pode ser lido com `pandas.read_csv(sep=';', decimal=',',
  thousands='.')` ou parser proprio;
- `.xls` HTML pode ser lido com parser proprio baseado em `html.parser`, ou
  instalar `lxml`/`beautifulsoup4` para usar `pandas.read_html`;
- `.xls` BIFF antigo exigira `xlrd` ou outra estrategia. Se for requisito real,
  adicionar `xlrd` ao `requirements.txt` deve ser considerado.

Riscos:

- PDFs podem quebrar linhas longas de descricao em duas linhas; parser precisa
  juntar continuacoes.
- PDFs podem ter mais de uma pagina e repetir cabecalho.
- Arquivos por fonte podem ter nomes com timestamp adicional.
- A pasta de amostra contem arquivos que nao sao `Anexo 10`; upload precisa
  rejeitar ou marcar como erro de validacao.
- Extensoes falsas podem fazer validacao por sufixo falhar; a validacao deve ser
  por assinatura.
- Valores negativos aparecem em `PARA MENOS` e devem preservar sinal.
- O mesmo arquivo pode ser reenviado; hash deve ajudar na rastreabilidade.

## Validacoes minimas da primeira entrega

Para considerar a primeira etapa concluida:

- upload aceita `.pdf`, `.xls`, `.xlsx`, `.csv`;
- arquivos falsos sao detectados corretamente;
- PDF real do `Anexo 10` da pasta de amostra e processado;
- `.xls` HTML `Fiplan.xls` e `Fiplan (1).xls` sao processados ou, se ficarem
  fora da primeira entrega, sao rejeitados com mensagem clara;
- arquivos `FIP 729` sao rejeitados por nao serem `Anexo 10`;
- status mostra progresso, total de linhas, alertas e erro quando houver;
- cancelamento interrompe antes de confirmar registros;
- reprocessamento usa o ultimo upload;
- registros importados ficam vinculados ao upload;
- carga anterior permanece disponivel em caso de falha.

## Politica de carga por mes aberto e mes fechado

Nova regra funcional a ser implementada antes da carga oficial.

Ao realizar upload do `Anexo 10`, a tela deve perguntar:

`Mes fechado?`

Opcoes:

- `Sim`;
- `Nao`.

Essa escolha define como a aplicacao valida a competencia e como atualiza os
registros ativos no banco.

### Conceitos

Competencia:

- par `exercicio` + `mes` detectado no arquivo;
- exemplo: `2026.07`.
- deve ser detectada pelo conteudo/cabecalho do arquivo, nunca pelo nome do
  arquivo. O nome pode estar errado, fora de ordem ou digitado manualmente pelo
  usuario.

Fonte:

- `fonte_recurso` detectada no arquivo;
- exemplo: `1.540.1070`.

UO:

- unidade orcamentaria detectada no conteudo do arquivo;
- a regra de carga deve usar `Cod.UO` + `UO`;
- quando o relatorio vier como `CONSOLIDADO DO ESTADO`, a aplicacao deve
  interpretar como:
  - `Cod.UO = 9900`;
  - `UO = ESTADO`;
- quando o relatorio trouxer orgao especifico, exemplo
  `14101 - SECRETARIA DE ESTADO DE EDUCACAO`, a aplicacao deve interpretar como:
  - `Cod.UO = 14101`;
  - `UO = SECRETARIA DE ESTADO DE EDUCACAO`.

Carga aberta:

- carga temporaria do mes atual;
- pode ser substituida por novo upload do mesmo mes atual;
- representa a receita ainda em acompanhamento, antes do fechamento oficial.

Carga fechada:

- carga permanente de uma competencia encerrada;
- nao deve aceitar sobrescrita normal;
- deve bloquear novo upload comum para a mesma competencia/fonte/UO;
- pode ser corrigida por usuario com perfil administrador, mas a correcao nunca
  deve deletar dados: a aplicacao desativa a carga anterior e grava a nova.

### Regra quando `Mes fechado = Sim`

Uso esperado:

- upload de relatorios de meses anteriores ao mes atual.

Regra fechada:

- se a data atual e `2026-08`, entao `Mes fechado = Sim` aceita ate
  `2026.07`;
- nao aceita `2026.08` nem meses futuros.

Validacao:

- a aplicacao deve detectar todas as competencias, fontes e UOs pelo conteudo dos
  arquivos antes de gravar registros definitivos;
- para cada chave `competencia + fonte_recurso + Cod.UO`, verificar se ja
  existe carga fechada ativa no banco;
- se existir carga fechada ativa, barrar o processamento e informar ao usuario
  quais competencias/fontes/UOs ja existem;
- se existir carga aberta ativa para a mesma chave
  `competencia + fonte_recurso + Cod.UO`, a carga fechada deve substituir a
  aberta:
  - desativar registros abertos anteriores;
  - gravar/ativar registros novos como fechados;
  - marcar a carga anterior como substituida;
  - manter historico para auditoria.

Mensagem de bloqueio sugerida:

```text
Nao foi possivel processar alguns arquivos. As seguintes competencias/fontes/UOs
ja possuem carga fechada no banco:
- 2026.07 / 1.500.1001 / 9900 - ESTADO
- 2026.07 / 1.540.1070 / 14101 - SECRETARIA DE ESTADO DE EDUCACAO
```

### Regra quando `Mes fechado = Nao`

Uso esperado:

- upload do mes corrente ainda aberto;
- o usuario pode reenviar o mesmo mes varias vezes durante o acompanhamento.

Regra:

- aceitar somente arquivos da competencia do mes/ano atual;
- se a data atual e `2026-08`, aceitar somente `2026.08`;
- barrar meses anteriores;
- barrar meses futuros.

Atualizacao:

- para cada chave `competencia + fonte_recurso + Cod.UO` enviada:
  - desativar a carga aberta anterior daquela chave, se existir;
  - manter cargas fechadas intactas;
  - gravar os novos registros como carga aberta;
  - deixar apenas a ultima carga aberta ativa para aquela chave.

Mensagem de bloqueio sugerida para competencia errada:

```text
Mes fechado = Nao aceita somente a competencia atual 2026.08.
Arquivos enviados possuem competencias diferentes:
- 2026.07 / 1.500.1001
```

### Regra de substituicao aberta para fechada

Quando um mes que estava sendo trabalhado como aberto for enviado depois como
fechado:

- se nao existir carga fechada para a mesma competencia/fonte/UO:
  - desativar registros abertos ativos dessa chave;
  - gravar registros novos como fechados;
  - marcar a carga aberta anterior como substituida;
  - a carga fechada passa a ser a fonte oficial do relatorio;
- se ja existir carga fechada:
  - barrar o upload;
  - informar que a competencia/fonte/UO ja foi fechada.

### Correcao administrativa de carga fechada

Usuario com perfil administrador podera corrigir uma carga fechada.

Regra:

- a aplicacao deve permitir novo upload fechado para a mesma
  `competencia + fonte_recurso + Cod.UO` somente para perfil administrador;
- a carga fechada anterior deve ser desativada;
- os novos registros devem ser gravados como fechados e ativos;
- nenhum registro deve ser deletado;
- a carga substituida deve manter referencia para o novo upload em
  `substituido_por_upload_id` ou campo equivalente.

### Regra para lote com varios arquivos

Um upload pode conter varios arquivos e varias fontes.

Validacao recomendada:

- detectar formato e cabecalho de todos os arquivos primeiro;
- montar uma lista de chaves `competencia + fonte_recurso + Cod.UO`;
- processar somente os arquivos validos;
- arquivos invalidos, bloqueados ou fora da regra de mes aberto/fechado nao
  devem ser processados;
- ao final, a aplicacao deve mostrar mensagem ao usuario com os arquivos nao
  processados e o motivo de cada rejeicao.

### Impacto no banco de dados

As tabelas atuais ainda nao possuem todos os campos para controlar essa politica.
Precisaremos adicionar campos de estado da carga.

Campos sugeridos em `receita_anexo10_uploads`:

- `mes_fechado`: booleano informado pelo usuario;
- `tipo_carga`: `aberta` ou `fechada`;
- `status_validacao`: `pendente`, `validado`, `bloqueado`;
- `mensagem_validacao`: texto com motivo de bloqueio;
- `substitui_upload_ids`: JSON/texto com uploads abertos substituidos.

Campos sugeridos em `receita_anexo10_arquivos`:

- `mes_fechado`: booleano herdado do upload;
- `tipo_carga`: `aberta` ou `fechada`;
- `cod_uo`: codigo da UO normalizado, exemplo `9900` ou `14101`;
- `uo`: nome da UO normalizado, exemplo `ESTADO` ou
  `SECRETARIA DE ESTADO DE EDUCACAO`;
- `chave_competencia_fonte_uo`: texto auxiliar `YYYY.MM|fonte|cod_uo`;
- `substituido_por_upload_id`: id do upload que substituiu este arquivo/carga,
  se aplicavel.

Campos sugeridos em `receita_anexo10_registros`:

- `mes_fechado`: booleano;
- `tipo_carga`: `aberta` ou `fechada`;
- `cod_uo`: codigo da UO normalizado, exemplo `9900` ou `14101`;
- `uo`: nome da UO normalizado, exemplo `ESTADO` ou
  `SECRETARIA DE ESTADO DE EDUCACAO`;
- `chave_competencia_fonte_uo`: texto auxiliar `YYYY.MM|fonte|cod_uo`;
- `substituido_por_upload_id`: id do upload que substituiu o registro, se
  aplicavel.

Indices recomendados:

- `receita_anexo10_registros(competencia, fonte_recurso, cod_uo, tipo_carga, ativo)`;
- `receita_anexo10_arquivos(competencia, fonte_recurso, cod_uo, tipo_carga, status)`;
- `receita_anexo10_uploads(tipo_carga, uploaded_at)`.

### Regra de unicidade logica

Nao recomendamos uma constraint unica simples no banco neste momento, porque a
carga pode ter varios registros por competencia/fonte. A unicidade deve ser
controlada por regra de negocio:

- no maximo uma carga aberta ativa por `competencia + fonte_recurso + Cod.UO`;
- no maximo uma carga fechada ativa por `competencia + fonte_recurso + Cod.UO`;
- uma carga fechada ativa bloqueia qualquer novo upload comum da mesma chave.

### Ajuste esperado no relatorio de conferencia

O relatorio `Relatorios > Execucao > Receita Anexo 10` deve exibir tambem:

- `Tipo da carga`: aberta ou fechada;
- `Mes fechado`: sim ou nao;
- `Cod.UO`;
- `UO`;
- filtro por `Tipo da carga`;
- filtro por `Mes fechado`;
- filtro por `Cod.UO` e `UO`;
- totais separados, se necessario, por carga aberta/fechada.

### Decisoes fechadas antes da implementacao

1. `Mes fechado = Sim` aceita somente meses anteriores ao mes atual.
2. Em lote misto, processar somente arquivos validos e informar os arquivos nao
   processados com seus motivos.
3. Carga fechada pode ser corrigida por perfil administrador, sempre desativando
   a carga anterior e gravando nova carga, sem deletar registros.
4. A regra de substituicao/bloqueio sera por
   `competencia + fonte_recurso + Cod.UO`.
5. A competencia deve ser detectada pelo conteudo do arquivo, nao pelo nome.
6. `CONSOLIDADO DO ESTADO` deve ser normalizado como `9900 - ESTADO`.

## Decisoes aplicadas na implementacao

- O upload aceita um arquivo ou varios arquivos no mesmo lote.
- A data/hora do download permanece obrigatoria.
- O processamento e por arquivo: arquivos validos seguem, arquivos invalidos
  ficam registrados com motivo.
- `FIP 729` deve ser rejeitado nesta funcionalidade, porque nao e `Anexo 10`.
- Registros ativos seguem a regra de uma carga ativa por
  `competencia + fonte_recurso + Cod.UO`.
- Arquivos validos sem movimentacao ficam como `alerta` e zero registros
  importados, porque representam uma carga valida do Anexo 10.
- Arquivos com a frase `Não houve movimentação no período.` aparecem no
  relatorio de conferencia como linha propria, com essa situacao indicada, mesmo
  sem gerar registro de receita.
- Duplicidade da mesma chave dentro do mesmo upload e rejeitada com mensagem
  clara, para nao haver substituicao silenciosa dentro do lote.

## Proposta de ordem de trabalho

1. Criar modelos e migracao/tabelas.
2. Criar detector de formato e testes com os arquivos da pasta de amostra.
3. Criar parser PDF com `pdfplumber`.
4. Criar parser para `.xls` HTML e CSV brasileiro.
5. Criar endpoints de upload/status/reprocess/cancel.
6. Integrar worker `receita_anexo10`.
7. Criar tela parcial e JS de acompanhamento.
8. Validar com amostra completa da pasta `ANEXO 10`.
9. Documentar layout detectado e erros conhecidos.

## Status da implementacao em 2026-08-03

Implementado nesta etapa:

- modelos SQLAlchemy `ReceitaAnexo10Upload`, `ReceitaAnexo10Arquivo` e
  `ReceitaAnexo10Registro`;
- tabelas remotas criadas: `receita_anexo10_uploads`,
  `receita_anexo10_arquivos` e `receita_anexo10_registros`;
- permissao/menu `atualizar/receita-anexo10` dentro de
  `Administrar SPO > Atualizar Execucao Orcamentaria`;
- tela parcial `templates/partials/atualizar_receita_anexo10.html`;
- endpoints:
  - `GET /api/receita-anexo10/status`;
  - `POST /api/receita-anexo10/upload`;
  - `POST /api/receita-anexo10/reprocess`;
  - `POST /api/receita-anexo10/cancel`;
- runner `services/receita_anexo10_runner.py`;
- integracao do tipo `receita_anexo10` no `worker.py`;
- integracao JS em `static/js/main.js`.
- pergunta obrigatoria `Mes fechado?` na tela de upload;
- validacao de competencia pelo conteudo do arquivo;
- normalizacao de `CONSOLIDADO DO ESTADO` como `9900 - ESTADO`;
- controle de carga `aberta` e `fechada`;
- substituicao por desativacao, sem delete de registros de receita;
- correcao de carga fechada somente para perfil admin;
- relatorio de conferencia com `Cod.UO`, `UO`, `Tipo Carga` e `Mes Fechado`.

Formatos ja cobertos pelo detector/parser:

- PDF real;
- `.xls` com conteudo HTML;
- `.xls` com conteudo XML/SpreadsheetML;
- `.xlsx` real;
- `.csv` brasileiro com `;`;
- `.xls` BIFF antigo, desde que a dependencia `xlrd` esteja instalada.

Validacoes executadas:

- PDF `Anexo 10 - F 1.500.1001__2026.01.pdf`: detectado como PDF e extraidas
  87 linhas;
- arquivo `Fiplan.xls`: detectado como HTML Excel e extraidas 24 linhas;
- arquivo `FIP 729 F 1.500.1001_2026.06_2026+07.21.pdf`: rejeitado por nao
  ser `Anexo 10`;
- processamento via `worker.py --kind receita_anexo10` finalizado com sucesso em
  lote de teste.
- varredura completa sem gravacao no banco da pasta `ANEXO 10`:
  - 101 arquivos lidos;
  - 99 reconhecidos como `Anexo 10`;
  - 2 rejeitados corretamente como `FIP 729`;
  - 0 erros tecnicos;
  - 2.334 linhas extraidas dos arquivos `Anexo 10`;
  - formatos reais encontrados: 99 PDFs e 2 HTML Excel.

Validacao integrada final:

- foi executado `TRUNCATE` somente nas tabelas `receita_anexo10_*`;
- foram removidos somente jobs/eventos com `tipo = 'receita_anexo10'`;
- foram lidos os 101 arquivos da pasta `ANEXO 10`;
- carga final fechada gravada no banco:
  - 2.212 registros ativos;
  - 14 linhas adicionais de conferencia no relatorio para meses/fontes sem
    movimentacao;
  - 83 arquivos processados com registros;
  - 14 arquivos validos com alerta por ausencia de movimentacao;
  - 4 arquivos rejeitados com motivo;
  - fontes ativas: `1.500.1001`, `1.501.0000`, `1.540.0000`,
    `1.540.1070`, `1.546.0000`, `1.550.0000`, `1.551.0000`,
    `1.552.0000`, `1.553.0000`, `1.569.0000`, `1.570.0000`,
    `1.570.3110`, `1.570.3120`, `1.574.0000`;
  - `Cod.UO` ativos: `14101` e `9900`.

Arquivos rejeitados na carga final:

- `FIP 729 F 1.500.1001_2026.06_2026+07.21.pdf`: relatorio nao identificado
  como Anexo 10;
- `FIP 729 F 1.500.1001_2026.07_2026+07.21.pdf`: relatorio nao identificado
  como Anexo 10;
- `Fiplan (1).xls`: metadado obrigatorio ausente, fonte de recurso;
- `Fiplan.xls`: chave `competencia + fonte + Cod.UO` duplicada no mesmo upload.

## Status atual para testes manuais

Data: 2026-08-03.

Depois da validação integrada, o ambiente remoto de desenvolvimento foi limpo
para permitir os testes manuais do usuário. A limpeza ficou restrita à
funcionalidade Receita Anexo 10:

- `receita_anexo10_uploads`: 0 registros;
- `receita_anexo10_arquivos`: 0 registros;
- `receita_anexo10_registros`: 0 registros;
- `processamento_jobs` com `tipo = 'receita_anexo10'`: 0 registros;
- `processamento_eventos` vinculados aos jobs de Receita Anexo 10: removidos.

Com isso, o cartão `Última atualização` não deve mais exibir processamento
anterior após recarregar a tela. O próximo passo operacional é realizar o upload
manual dos arquivos atuais do Anexo 10.
