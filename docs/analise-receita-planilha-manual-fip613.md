# Análise de Receita - leitura da planilha manual FIP 613

Data da análise: 2026-08-03.

Arquivo analisado:

`C:\workspace\Documentação\FIP-613+\FIP 613 14101_2026.06.16_10.06H=+++.xlsx`

Este documento registra o entendimento inicial da planilha manual usada hoje
para análise de receita. A intenção é transformar essas regras, fórmulas e
visões em uma funcionalidade futura chamada `Análise de Receita`, usando os
dados já extraídos pela funcionalidade `Receita Anexo 10`.

Observação importante: `FIP 613` aparece neste documento apenas porque é parte
do nome do arquivo manual enviado para análise. Isso não significa relação com a
funcionalidade `FIP 613` já existente na aplicação. A funcionalidade existente
`FIP 613` permanece independente; este documento trata da frente de receita.

## Etapas da frente de receita

A frente de receita fica organizada em três etapas:

1. `Receita Anexo 10`: upload, leitura, validação e gravação dos relatórios
   Anexo 10 no banco de dados.
2. `Análise de Receita`: etapa atual a ser desenhada/implementada, baseada nos
   dados já gravados pela `Receita Anexo 10`, reproduzindo as análises manuais
   da planilha de referência.
3. `Painéis/Dashboards de Receita`: etapa futura para gráficos, painéis visuais,
   comparativos executivos e indicadores consolidados.

## Estrutura do arquivo

A pasta de trabalho possui 3 abas:

| Aba | Linhas | Colunas | Fórmulas | Tema identificado |
|---|---:|---:|---:|---|
| `A10 F MDE` | 417 | 127 | 588 | Receita MDE |
| `A10 F FUNDEB` | 448 | 213 | 1.950 | Receita FUNDEB |
| `A10 F SAL EDUC` | 570 | 112 | 662 | Receita Salário Educação |

As abas trabalham com séries mensais por exercício. Cada bloco anual possui 12
linhas, uma por mês.

As primeiras colunas concentram a base de análise. As colunas muito distantes
parecem existir principalmente por causa de objetos de gráfico, ancoragens e
áreas visuais da planilha, não por tabelas adicionais relevantes.

## Padrão geral da análise

As três abas seguem a mesma lógica:

1. Informar o orçamento anual da fonte.
2. Dividir o orçamento anual por 12 para criar uma referência mensal linear.
3. Ler a arrecadação acumulada mensal do Anexo 10.
4. Calcular a arrecadação mensal pela diferença entre acumulados.
5. Comparar a arrecadação mensal realizada contra o orçamento mensal linear.
6. Medir a evolução acumulada contra o orçamento anual.
7. Medir o percentual acumulado já realizado sobre o orçamento anual.
8. Registrar as séries necessárias para, em etapa futura, exibir gráficos
   comparando planejado x realizado e evolução mensal.

## Colunas padrão

### MDE e FUNDEB

Nas abas `A10 F MDE` e `A10 F FUNDEB`, a tabela principal usa:

| Coluna | Campo | Significado |
|---|---|---|
| `A` | marcador | aparece com valores como `z` e `a` em alguns blocos |
| `B` | Exercício | ano/mês inicial ou ano |
| `C` | Mês | número do mês |
| `D` | Fonte | fonte de recurso |
| `E` | Orçado | orçamento anual |
| `F` | Orçado/Mês | orçamento anual dividido por 12 |
| `G` | Arrec/Mensal | arrecadação realizada no mês |
| `H` | % Arrec Mensal | arrecadação mensal sobre orçamento anual |
| `I` | % Var Arrec Mensal | variação percentual contra orçamento mensal |
| `J` | Var Arrec Mensal | diferença em valor contra orçamento mensal |
| `K` | Arrec/Acum | arrecadação acumulada |
| `L` | Evolução | diferença acumulada contra orçamento anual |
| `M` | % Total | percentual acumulado realizado |

Na aba `A10 F FUNDEB`, existem também:

| Coluna | Campo | Significado |
|---|---|---|
| `N` | 70% | 70% da arrecadação acumulada |
| `O` | 90% | 90% da arrecadação acumulada |
| `P` | excesso projetado | diferença acumulada contra orçamento em linhas de fechamento |

### Salário Educação

Na aba `A10 F SAL EDUC`, existe uma coluna extra antes do orçamento:

| Coluna | Campo | Significado |
|---|---|---|
| `E` | Curva S? | curva planejada acumulada em alguns anos |
| `F` | Orçado | orçamento anual |
| `G` | Orçado/Mês | orçamento anual dividido por 12 |
| `H` | Arrec/Mensal | arrecadação realizada no mês |
| `I` | % Arrec Mensal | arrecadação mensal sobre orçamento anual |
| `J` | % Var Arrec Mensal | variação percentual contra orçamento mensal |
| `K` | Var Arrec Mensal | diferença em valor contra orçamento mensal |
| `L` | Arrec/Acum | arrecadação acumulada |
| `M` | Evolução | diferença acumulada contra orçamento anual |
| `N` | % Total | percentual acumulado realizado |

## Fórmulas identificadas

### Orçado por mês

MDE/FUNDEB:

```excel
F2 = E2 / 12
```

Salário Educação:

```excel
G2 = F2 / 12
```

Regra para a aplicação:

```text
orcado_mes = orcado_anual / 12
```

### Arrecadação mensal

No primeiro mês do bloco anual:

```excel
G2 = K2
```

Nos meses seguintes:

```excel
G3 = K3 - K2
```

Na aba Salário Educação, por causa do deslocamento de colunas:

```excel
H2 = L2
H3 = L3 - L2
```

Regra para a aplicação:

```text
se mes = 1:
    arrecadacao_mensal = arrecadacao_acumulada
senão:
    arrecadacao_mensal = acumulado_mes_atual - acumulado_mes_anterior
```

Observação importante: se o acumulado do mês atual estiver vazio, a aplicação
não deve gerar uma arrecadação mensal negativa artificial. A planilha manual
gera esse efeito em meses futuros porque calcula `vazio - acumulado_anterior`.
Na aplicação, mês sem acumulado deve ficar como `sem dado` ou `não apurado`.

### Percentual de arrecadação mensal

MDE/FUNDEB:

```excel
H2 = G2 / E2
```

Salário Educação:

```excel
I2 = H2 / F2
```

Regra para a aplicação:

```text
percentual_arrecadacao_mensal = arrecadacao_mensal / orcado_anual
```

Quando o orçamento anual for zero, usar regra segura equivalente a:

```excel
=IFERROR(arrecadacao_mensal / orcado_anual, 0)
```

### Variação percentual contra o orçamento mensal

MDE/FUNDEB:

```excel
I2 = (G2 - F2) / F2
```

Salário Educação:

```excel
J2 = (H2 - G2) / G2
```

Regra para a aplicação:

```text
variacao_percentual_mensal = (arrecadacao_mensal - orcado_mes) / orcado_mes
```

Quando o orçamento mensal for zero, usar regra segura equivalente a:

```excel
=IFERROR((arrecadacao_mensal - orcado_mes) / orcado_mes, 0)
```

### Variação em valor contra o orçamento mensal

MDE/FUNDEB:

```excel
J2 = G2 - F2
```

Salário Educação:

```excel
K2 = H2 - G2
```

Regra para a aplicação:

```text
variacao_valor_mensal = arrecadacao_mensal - orcado_mes
```

### Evolução acumulada

MDE/FUNDEB:

```excel
L2 = K2 - E2
```

Salário Educação:

```excel
M2 = L2 - F2
```

Regra para a aplicação:

```text
evolucao_acumulada = arrecadacao_acumulada - orcado_anual
```

Há exceção observada na linha final de 2023 da aba MDE, onde a fórmula usa o
orçamento da linha anterior:

```excel
L49 = K49 - E48
M49 = K49 / E48
```

Esse ponto precisa ser validado antes da implementação. Pode ser uma correção
manual, uma regra específica de orçamento inicial ou apenas uma inconsistência
da planilha.

### Percentual total acumulado

MDE/FUNDEB:

```excel
M2 = K2 / E2
```

Salário Educação:

```excel
N2 = L2 / F2
```

Regra para a aplicação:

```text
percentual_total = arrecadacao_acumulada / orcado_anual
```

Quando o orçamento anual for zero, usar regra segura equivalente a:

```excel
=IFERROR(arrecadacao_acumulada / orcado_anual, 0)
```

### FUNDEB - 70% e 90%

Na aba FUNDEB:

```excel
N2 = K2 * 70%
O2 = K2 * 90%
```

Regra para a aplicação:

```text
fundeb_70 = arrecadacao_acumulada * 0.70
fundeb_90 = arrecadacao_acumulada * 0.90
```

Esses campos parecem ser indicadores específicos do FUNDEB e não aparecem nas
abas MDE e Salário Educação.

## Séries históricas identificadas

### MDE

| Período | Fonte | Observação |
|---|---|---|
| 2020 a 2022 | `120` | fonte antiga |
| 2023 a 2026 | `1.500.1001` | fonte nova |

Na aba MDE, o bloco de 2026 vai até dezembro, mas só há acumulado real até
junho. Os meses futuros ficam sem acumulado.

### FUNDEB

| Período | Fonte | Observação |
|---|---|---|
| 2020 a 2022 | `122` | fonte antiga |
| 2023 a 2026 | `1.540.1070+` | agrupamento manual |

O agrupamento `1.540.1070+` é calculado por soma de fontes detalhadas abaixo.

Exemplo de 2026:

```excel
E74 = E170 + E182
F74 = F170 + F182
K74 = K170 + K182
```

Para os meses seguintes:

```excel
K75 = K171 + K183
K76 = K172 + K184
...
K85 = K181 + K193
```

Fontes detalhadas observadas:

| Ano | Fontes detalhadas |
|---|---|
| 2023 | `1.540.1070`, `1.540.0000` |
| 2024 | `1.540.1070`, `1.540.0000` |
| 2025 | `1.540.1070`, `1.540.0000`, `1.546.0000` |
| 2026 | `1.540.1070`, `1.540.0000`, `1.546.0000` |

Observação: a fonte `1.546.0000` aparece no detalhamento de 2025/2026, mas não
foi somada no agrupamento `1.540.1070+` nas fórmulas analisadas. Antes de
implementar, precisamos confirmar se ela deve ficar fora do total ou se a
planilha manual ainda não foi ajustada para essa fonte.

### Salário Educação

| Período | Fonte | Observação |
|---|---|---|
| 2020 a 2022 | `110` | fonte antiga |
| 2023 a 2026 | `1.550.0000` | fonte nova |

Na aba Salário Educação, há coluna `CURVA S?`, mas o cálculo observado é uma
curva acumulada linear baseada no orçamento mensal:

```excel
E14 = G14
E15 = E14 + G14
E16 = E15 + G15
```

Esse campo precisa ser validado para decidir se será uma análise exibida na
aplicação ou apenas apoio visual da planilha.

## Valores de referência extraídos para 2026

Os valores abaixo ajudam a conferir a futura implementação.

### MDE 2026 - fonte `1.500.1001`

| Mês | Orçado anual | Orçado/mês | Arrecadado mensal | Arrecadado acumulado | % total |
|---:|---:|---:|---:|---:|---:|
| 1 | 1.857.190.289,00 | 154.765.857,42 | 155.307.733,64 | 155.307.733,64 | 8,36% |
| 2 | 1.857.190.289,00 | 154.765.857,42 | 167.453.566,12 | 322.761.299,76 | 17,38% |
| 3 | 1.857.190.289,00 | 154.765.857,42 | 163.947.396,41 | 486.708.696,17 | 26,21% |
| 4 | 1.857.190.289,00 | 154.765.857,42 | 171.512.716,13 | 658.221.412,30 | 35,44% |
| 5 | 1.857.190.289,00 | 154.765.857,42 | 165.183.735,48 | 823.405.147,78 | 44,34% |
| 6 | 1.857.190.289,00 | 154.765.857,42 | 95.753.534,08 | 919.158.681,86 | 49,49% |

### FUNDEB 2026 - agrupamento `1.540.1070+`

| Mês | Orçado anual | Orçado/mês | Arrecadado mensal | Arrecadado acumulado | % total |
|---:|---:|---:|---:|---:|---:|
| 1 | 3.214.913.190,00 | 267.909.432,50 | 279.079.519,38 | 279.079.519,38 | 8,68% |
| 2 | 3.214.913.190,00 | 267.909.432,50 | 258.090.486,52 | 537.170.005,90 | 16,71% |
| 3 | 3.214.913.190,00 | 267.909.432,50 | 308.982.403,11 | 846.152.409,01 | 26,32% |
| 4 | 3.214.913.190,00 | 267.909.432,50 | 286.165.369,09 | 1.132.317.778,10 | 35,22% |
| 5 | 3.214.913.190,00 | 267.909.432,50 | 288.942.412,19 | 1.421.260.190,29 | 44,21% |
| 6 | 3.214.913.190,00 | 267.909.432,50 | 69.121.825,92 | 1.490.382.016,21 | 46,36% |

### Salário Educação 2026 - fonte `1.550.0000`

| Mês | Orçado anual | Orçado/mês | Arrecadado mensal | Arrecadado acumulado | % total |
|---:|---:|---:|---:|---:|---:|
| 1 | 187.272.454,00 | 15.606.037,83 | 27.162.888,70 | 27.162.888,70 | 14,50% |
| 2 | 187.272.454,00 | 15.606.037,83 | 16.218.854,37 | 43.381.743,07 | 23,17% |
| 3 | 187.272.454,00 | 15.606.037,83 | 16.879.509,28 | 60.261.252,35 | 32,18% |
| 4 | 187.272.454,00 | 15.606.037,83 | 16.701.549,73 | 76.962.802,08 | 41,10% |
| 5 | 187.272.454,00 | 15.606.037,83 | 16.191.105,77 | 93.153.907,85 | 49,74% |

Na planilha analisada, junho de 2026 para Salário Educação está sem acumulado,
mas a fórmula manual gera valor mensal negativo em junho porque subtrai o
acumulado de maio de uma célula vazia. A aplicação deve tratar esse caso como
ausência de dado.

## Insumos para terceira etapa: gráficos identificados

A pasta contém gráficos associados às análises:

- `A10 F MDE`: 1 gráfico de barras. O título visível no objeto indica análise
  relacionada a `LOA MDE + superávit`.
- `A10 F SAL EDUC`: 3 gráficos, incluindo evolução mensal e comparação
  planejado x realizado.
- `A10 F FUNDEB`: a planilha possui títulos de gráficos em células, mas nenhum
  objeto de gráfico foi detectado pelo leitor utilizado. Pode ser imagem, objeto
  não suportado pelo `openpyxl`, ou gráfico removido.

Para a terceira etapa (`Painéis/Dashboards de Receita`), o ideal é não depender
dos objetos do Excel, mas reconstruir os gráficos a partir das séries
calculadas na `Análise de Receita`.

## O que a aplicação precisa reproduzir

### Visões mínimas

1. Análise MDE.
2. Análise FUNDEB.
3. Análise Salário Educação.

Cada visão deve permitir escolher:

- exercício;
- mês de referência;
- UO;
- fonte ou grupo de fonte;
- tipo de carga, aberta ou fechada;
- mês fechado, sim ou não.

### Indicadores por mês

Para cada mês com dado real:

- orçamento anual;
- orçamento mensal linear;
- arrecadação mensal;
- arrecadação acumulada;
- variação mensal em valor;
- variação mensal em percentual;
- evolução acumulada em valor;
- percentual total realizado.

Para FUNDEB, incluir também:

- 70% do acumulado;
- 90% do acumulado.

### Gráficos sugeridos para a terceira etapa

1. Planejado x realizado mensal:
   - série planejada: `orcado_mes`;
   - série realizada: `arrecadacao_mensal`.
2. Evolução acumulada:
   - série orçada acumulada linear;
   - série arrecadada acumulada.
3. Percentual de execução:
   - `percentual_total` por mês.
4. Variação mensal:
   - `variacao_valor_mensal` ou `variacao_percentual_mensal`.

## Dependência dos dados do Anexo 10

A futura análise deve consumir os dados gravados pela funcionalidade
`Receita Anexo 10`, principalmente:

- `competencia`;
- `exercicio`;
- `mes`;
- `fonte_recurso`;
- `cod_uo`;
- `uo`;
- `codigo_receita`;
- `descricao_receita`;
- `orcado_atualizado`;
- `arrecadada`;
- `situacao_movimentacao`;
- `tipo_carga`;
- `mes_fechado`;
- `ativo`.

Para a análise, a aplicação deve considerar somente registros ativos.

## Pontos que precisam de validação antes da implementação

1. Confirmar se a fonte `1.546.0000` deve ou não entrar no agrupamento FUNDEB
   `1.540.1070+`.
2. Confirmar se a regra do MDE em 2023, usando `E48` na linha final em vez de
   `E49`, é regra de negócio ou inconsistência manual.
3. Confirmar se a coluna `CURVA S?` do Salário Educação deve virar indicador da
   aplicação.
4. Confirmar a tabela de equivalência entre fontes antigas e fontes novas:
   - MDE: `120` para `1.500.1001`;
   - FUNDEB: `122` para agrupamento `1.540.1070+`;
   - Salário Educação: `110` para `1.550.0000`.
5. Confirmar se a análise deve usar sempre `orcado_atualizado` ou se em algum
   ponto deve comparar com orçamento inicial.
6. Confirmar como a aplicação deve apresentar meses futuros sem dado:
   recomendação atual: não calcular arrecadação negativa, exibir como `sem dado`.

## Entendimento final

A planilha manual de referência, apesar de ter `FIP 613` no nome do arquivo,
não representa a funcionalidade `FIP 613` da aplicação. Ela transforma a
receita do Anexo 10 em uma análise temporal por fonte/grupo, comparando
orçamento anual contra arrecadação mensal e acumulada.

A implementação da `Análise de Receita` deve ser uma camada calculada em cima
das tabelas da `Receita Anexo 10`, sem novo upload nesta etapa. O usuário deve
poder conferir os mesmos números hoje obtidos no Excel, com filtros e
totalizadores gerados diretamente pela aplicação.

Os gráficos, visões executivas e sugestões visuais identificados na planilha
ficam registrados como insumo da terceira etapa: `Painéis/Dashboards de
Receita`.
