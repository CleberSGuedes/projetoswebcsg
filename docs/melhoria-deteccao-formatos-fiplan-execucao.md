# Melhoria: detecção de formatos FIPLAN por conteúdo

Data: 2026-08-04.

Este documento organiza a melhoria solicitada para as funcionalidades:

- `FIP 613`;
- `PED`;
- `EMP`;
- `EST EMP`;
- `NOB`.

Objetivo: fazer essas rotinas aceitarem relatórios exportados pelo FIPLAN antigo
em formatos com extensão inconsistente, usando detecção pelo conteúdo do arquivo,
como já foi feito na funcionalidade `Receita Anexo 10`.

Nenhuma alteração de código foi feita nesta etapa.

## Problema atual

Hoje essas funcionalidades tratam o upload como se o arquivo fosse sempre um
`.xlsx` real.

Pontos observados no projeto:

- os endpoints de upload validam a extensão com `filename.endswith(".xlsx")`;
- as mensagens da interface ainda orientam o usuário a selecionar apenas
  `.xlsx`;
- os arquivos são salvos internamente sempre com sufixo `.xlsx`;
- a validação de layout em `_detect_upload_layouts()` usa `pd.read_excel()`
  direto no arquivo;
- `FIP 613`, `PED` e `EST EMP` usam `pandas.ExcelFile` ou `pd.read_excel`;
- `EMP` e `NOB` passam por processamento em Node, também assumindo arquivo de
  planilha;
- a busca de arquivos antigos em `tmp` usa padrão `*.xlsx`.

Com isso, arquivos válidos do FIPLAN podem ser rejeitados antes do
processamento ou falhar durante a leitura, mesmo quando o conteúdo possui o
relatório correto.

## Formatos que devem ser aceitos

A melhoria deve aceitar pelo conteúdo, não pela extensão:

| Caso | Exemplo | Tratamento esperado |
|---|---|---|
| `.xlsx` real | arquivo ZIP Office Open XML | ler normalmente |
| `.xlsx` falso | extensão `.xlsx`, mas conteúdo HTML, XML ou CSV | detectar conteúdo real e ler |
| `.xls` BIFF real | Excel antigo binário | ler com engine compatível |
| `.xls` falso HTML | extensão `.xls`, conteúdo HTML com tabela | extrair tabelas HTML |
| `.xls` falso XML | extensão `.xls`, conteúdo XML/SpreadsheetML | extrair planilha XML |
| `.csv` brasileiro | separado por `;`, decimal `,` | ler como CSV BR |

Nesta melhoria, o escopo citado pelo usuário não inclui PDF para essas cinco
funcionalidades. PDF continua fora do escopo, salvo decisão posterior.

## Estratégia recomendada

Criar um leitor compartilhado, por exemplo:

`services/fiplan_file_reader.py`

Esse leitor deve concentrar:

1. detecção do formato real;
2. leitura para uma estrutura tabular;
3. normalização opcional para `.xlsx` real;
4. validação do tipo de relatório pelo conteúdo.

Assim evitamos duplicar a mesma lógica em `FIP 613`, `PED`, `EMP`, `EST EMP` e
`NOB`.

## Fluxo proposto

### 1. Upload recebe o arquivo bruto

O endpoint deve aceitar:

```text
.xls, .xlsx, .csv
```

Mas a extensão será apenas informativa. A validação real deve vir depois da
leitura dos primeiros bytes/conteúdo.

### 2. Detectar formato real

Usar assinatura/conteúdo:

```text
%PDF                  -> PDF, rejeitar neste escopo
D0 CF 11 E0 ...       -> XLS BIFF
PK 03 04              -> XLSX real
<html / <!doctype     -> HTML Excel
<?xml / <Workbook     -> XML Excel/SpreadsheetML
texto com ; e quebras -> CSV brasileiro
```

Também registrar quando o formato for desconhecido.

### 3. Converter para tabela

O leitor compartilhado deve retornar um objeto com:

```text
formato_detectado
extensao_original
abas
dataframes
texto_amostra
alertas
```

Para preservar os processadores existentes, a primeira implementação pode
gerar um `.xlsx` normalizado e entregar esse arquivo aos runners atuais.

Essa abordagem é mais segura porque:

- mantém as regras atuais de tratamento de `FIP 613`, `PED`, `EMP`, `EST EMP` e
  `NOB`;
- evita reescrever os processadores de uma vez;
- permite que `EMP` e `NOB`, que passam pelo Node, continuem recebendo `.xlsx`
  real.

### 4. Validar o relatório pelo conteúdo

Depois da conversão/leitura, a aplicação deve identificar se o arquivo é mesmo
do tipo esperado.

Exemplos:

- upload em `FIP 613` só deve aceitar relatório com marcadores de FIP 613;
- upload em `PED` só deve aceitar layout PED;
- upload em `EMP` só deve aceitar layout EMP;
- upload em `EST EMP` só deve aceitar layout EST EMP;
- upload em `NOB` só deve aceitar layout NOB.

Essa regra já existe parcialmente em `_detect_upload_layouts()`, mas ela precisa
ser alimentada por uma leitura universal, não apenas por `pd.read_excel()`.

## Impacto por funcionalidade

### FIP 613

Estado atual:

- endpoint aceita apenas `.xlsx`;
- runner usa `pd.read_excel`;
- procura cabeçalho dentro da planilha;
- grava em `fip613`.

Melhoria:

- aceitar `.xls`, `.xlsx` e `.csv`;
- detectar conteúdo real;
- converter o arquivo para DataFrame ou `.xlsx` normalizado;
- reaproveitar a regra atual de localização de cabeçalho;
- manter gravação na mesma tabela.

Risco principal:

- FIP 613 pode ter linhas de título antes do cabeçalho; o detector precisa
  preservar essas linhas para a busca atual continuar funcionando.

### PED

Estado atual:

- endpoint aceita apenas `.xlsx`;
- runner possui lógica robusta para encontrar cabeçalho e preparar aba PED;
- grava em `ped`;
- atualiza relacionamento com dotação e identifica chaves de planejamento.

Melhoria:

- aceitar os formatos FIPLAN antigos;
- entregar ao runner uma planilha normalizada com a aba e linhas preservadas;
- manter a identificação de cabeçalho existente;
- manter alertas de dotação e planejamento.

Risco principal:

- o PED tem mais regras de negócio depois da leitura. A normalização não pode
  alterar nomes de colunas nem quebrar a posição da linha de cabeçalho.

### EMP

Estado atual:

- endpoint aceita apenas `.xlsx`;
- processamento ocorre em background;
- worker chama runner Node;
- grava em `emp`;
- atualiza recorde/status diário de EMP.

Melhoria:

- aceitar arquivos brutos do FIPLAN;
- converter para `.xlsx` real antes de chamar o Node;
- manter o contrato atual do Node runner;
- registrar formato original e formato normalizado nos detalhes do job.

Risco principal:

- se o Node runner depender de características específicas do Excel original,
  a conversão precisa preservar cabeçalhos, tipos de valores e datas.

### EST EMP

Estado atual:

- endpoint aceita apenas `.xlsx`;
- runner usa `pd.ExcelFile` e `pd.read_excel`;
- procura linha de cabeçalho;
- grava em `est_emp`.

Melhoria:

- aceitar `.xls`, `.xlsx` e `.csv`;
- converter para `.xlsx` real ou DataFrame;
- reaproveitar a rotina atual de cabeçalho e tratamento numérico.

Risco principal:

- os campos de valor de EST EMP têm nomes longos e variações. A detecção deve
  continuar usando normalização de cabeçalho, não igualdade literal frágil.

### NOB

Estado atual:

- endpoint aceita apenas `.xlsx`;
- processamento ocorre em background;
- worker chama runner Node;
- grava em `nob`.

Melhoria:

- aceitar arquivos brutos do FIPLAN;
- converter para `.xlsx` real antes de chamar o Node;
- manter a detecção de layout NOB existente;
- aceitar tanto layout NOB clássico quanto variações GCV/NOB já previstas na
  validação atual.

Risco principal:

- datas e números em CSV brasileiro precisam ser preservados corretamente para
  evitar troca de dia/mês ou perda de casas decimais.

## Alterações esperadas na interface

As telas de upload devem trocar mensagens como:

```text
Selecione um arquivo .xlsx.
Envie um arquivo .xlsx.
```

Por algo equivalente a:

```text
Selecione um arquivo FIPLAN nos formatos .xls, .xlsx ou .csv.
```

Também atualizar o atributo `accept` dos inputs para:

```text
.xls,.xlsx,.csv
```

## Alterações esperadas no backend

### Novo serviço compartilhado

Criar serviço sugerido:

```text
services/fiplan_file_reader.py
```

Responsabilidades:

- `detect_format(path)`;
- `read_fiplan_table(path)`;
- `normalize_to_xlsx(path, output_dir)`;
- `detect_report_kind(path_or_dataframe)`;
- `validate_expected_report(path, expected_kind)`.

### Ajustar validação de upload

Substituir validação por extensão:

```python
if not arquivo.filename.lower().endswith(".xlsx"):
    return jsonify({"error": "Envie um arquivo .xlsx."}), 400
```

Por validação por conteúdo:

```text
1. salvar arquivo bruto;
2. detectar formato real;
3. rejeitar somente se formato não for suportado;
4. normalizar para tabela;
5. validar layout esperado pelo conteúdo;
6. iniciar processamento.
```

### Ajustar armazenamento

Recomendação inicial:

- preservar o nome original em `original_filename`;
- salvar o arquivo bruto com a extensão original;
- gerar arquivo normalizado `.xlsx` quando necessário;
- gravar em `stored_filename` o arquivo que o worker deve usar;
- opcionalmente registrar o nome bruto e o formato detectado em campos novos ou
  no detalhe do job.

Se quisermos auditoria completa, criar campos nas tabelas de upload:

```text
extensao_original
formato_detectado
stored_original_filename
stored_normalized_filename
hash_sha256
```

Essa alteração de banco é desejável, mas não obrigatória para uma primeira
entrega se os metadados forem registrados no job/status.

## Regras de validação por relatório

A validação deve continuar impedindo troca de relatório entre módulos.

Exemplos:

- arquivo de `PED` enviado em `EMP` deve ser bloqueado;
- arquivo de `NOB` enviado em `EST EMP` deve ser bloqueado;
- arquivo desconhecido deve ser bloqueado com mensagem clara;
- arquivo com formato suportado, mas layout sem cabeçalho esperado, deve ser
  bloqueado como relatório inválido para a funcionalidade.

Mensagem sugerida:

```text
O arquivo foi lido como HTML Excel, mas o conteúdo identificado foi PED. Envie um relatório EMP.
```

Ou:

```text
Formato suportado, porém não foi possível identificar o layout FIP 613 no conteúdo.
```

## Testes necessários

Para cada uma das cinco funcionalidades:

1. upload de `.xlsx` real atual;
2. upload de `.xls` HTML;
3. upload de `.xls` BIFF antigo;
4. upload de `.xlsx` falso com conteúdo HTML/XML/CSV;
5. upload de `.csv` brasileiro com `;`;
6. rejeição de arquivo de outro relatório;
7. rejeição de arquivo desconhecido;
8. processamento em background;
9. reprocessamento do último upload;
10. cancelamento;
11. conferência dos registros no relatório correspondente;
12. download Excel do relatório após a carga.

## Ordem sugerida de implementação

1. Criar o leitor compartilhado `services/fiplan_file_reader.py`.
2. Portar para ele a detecção usada em `Receita Anexo 10`.
3. Criar conversor para `.xlsx` normalizado.
4. Ajustar `_detect_upload_layouts()` para receber DataFrame/tabelas lidas pelo
   leitor compartilhado.
5. Alterar os endpoints de upload para aceitar `.xls`, `.xlsx` e `.csv`.
6. Ajustar nomes internos para não forçar `.xlsx` antes da detecção.
7. Para `FIP 613`, `PED` e `EST EMP`, fazer os runners lerem o arquivo
   normalizado ou DataFrame retornado pelo leitor.
8. Para `EMP` e `NOB`, entregar ao Node runner sempre um `.xlsx` real
   normalizado.
9. Atualizar mensagens da interface e atributos `accept`.
10. Testar um relatório válido e um relatório trocado em cada funcionalidade.

## Decisão recomendada

Implementar primeiro como camada de compatibilidade, não como reescrita dos
runners.

Ou seja:

```text
arquivo bruto do usuário
-> detecção por conteúdo
-> validação do tipo de relatório
-> normalização para .xlsx real quando necessário
-> runner atual
-> banco atual
```

Isso entrega a melhoria de formatos com menor risco e preserva o comportamento
já validado das cinco funcionalidades.

## Decisão validada antes da implementação

Foi validado manter os motores atuais, inclusive o processamento em Node de
`EMP` e `NOB`, porque essas duas rotinas já funcionam corretamente.

A padronização será feita na entrada:

```text
upload
-> salvar arquivo bruto
-> detectar formato real em Python
-> validar o relatório pelo conteúdo
-> gerar .xlsx real normalizado quando necessário
-> chamar o runner atual
```

Com isso:

- todos os módulos passam a aceitar os mesmos formatos de entrada;
- `EMP` e `NOB` continuam usando Node sem mudança de regra de negócio;
- `FIP 613`, `PED` e `EST EMP` continuam usando os runners Python atuais;
- o usuário não precisa saber se o arquivo original era `.xls` falso, HTML,
  XML, BIFF antigo, `.xlsx` real ou CSV brasileiro;
- internamente, o worker recebe um arquivo compatível com o processamento que
  já existe.

## Pontos para validação antes de codificar

1. Confirmar se PDF fica fora do escopo para `FIP 613`, `PED`, `EMP`,
   `EST EMP` e `NOB`.
2. Confirmar se devemos adicionar campos de auditoria nas tabelas de upload ou
   registrar somente nos jobs/status.
3. Confirmar se o usuário poderá enviar apenas um arquivo por vez nessas cinco
   funcionalidades, mantendo o fluxo atual.
4. Confirmar se a normalização para `.xlsx` real pode ser considerada arquivo
   técnico interno, sem aparecer para o usuário.

## Implementação iniciada em 2026-08-04

Primeira etapa implementada:

- criado o serviço compartilhado `services/fiplan_file_reader.py`;
- adicionada detecção por conteúdo para:
  - `.xlsx` real;
  - `.xls` BIFF antigo;
  - `.xls`/`.xlsx` falso com HTML;
  - `.xls`/`.xlsx` falso com XML/SpreadsheetML;
  - `.csv` brasileiro;
- adicionada normalização interna para `.xlsx` real quando o arquivo original
  não for um `.xlsx` real;
- ajustada a validação de layout para ler prévia do conteúdo por meio do leitor
  compartilhado;
- ajustados os uploads de `FIP 613`, `PED`, `EMP`, `EST EMP` e `NOB` para
  aceitar `.xls`, `.xlsx` e `.csv`;
- preservado o processamento atual:
  - `FIP 613`, `PED` e `EST EMP` continuam nos runners Python;
  - `EMP` e `NOB` continuam no runner Node;
- atualizadas as mensagens e o `accept` dos inputs nas cinco telas;
- reprocessamento passa a localizar também arquivos com extensões variadas em
  `tmp`.

Decisões aplicadas:

- PDF permanece fora do escopo dessas cinco funcionalidades nesta etapa.
- Não foram criadas novas colunas no banco. O formato detectado fica registrado
  no detalhe do job quando o fluxo usa `ProcessamentoJob`.
- O fluxo continua aceitando um arquivo por upload nessas cinco funcionalidades.
- O `.xlsx` normalizado é arquivo técnico interno, usado para preservar os
  runners existentes.

## Ajuste apos teste de EMP BIFF antigo

Durante o teste com EMP em `.xls` BIFF antigo, foi identificado que a conversao
completa para `.xlsx` dentro da rota de upload poderia deixar a requisicao
pesada e resultar no erro do navegador `NetworkError when attempting to fetch
resource`.

Regra tecnica ajustada:

- a rota de upload salva o arquivo bruto enviado pelo usuario;
- a rota detecta o formato real pelo conteudo;
- a rota valida somente uma amostra inicial do layout;
- a conversao completa para `.xlsx` real acontece no worker em segundo plano;
- EMP e NOB continuam chamando o runner Node com `.xlsx` real;
- FIP 613, PED e EST EMP continuam chamando os runners Python com `.xlsx` real
  quando o arquivo original nao for um `.xlsx` valido.

Resultado esperado: a interface responde rapidamente ao usuario e o
processamento pesado permanece em background.
