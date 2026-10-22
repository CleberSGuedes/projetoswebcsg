# Plano de implementação: novas UOs 26101, 26201 e 26202

> **Situação: PLANO APROVADO EM CONCEITO, AGUARDANDO CONFIRMAÇÃO OFICIAL.** Nenhuma linha de código foi alterada. Nada deste plano começa sem a confirmação oficial e a autorização explícita do usuário (ver `claude.md`, seção 22).
> Base: [testes_uos_26101_26201_26202.md](testes_uos_26101_26201_26202.md). Decisões tomadas com o usuário em 2026-10-22.

## Decisões

| # | Tema | Decisão |
|---|---|---|
| D1 | Entrega | **Faseada.** Fase 1 vai a produção só com a SEDUC (comportamento igual ao de hoje); as novas UOs só são liberadas na Fase 3. |
| D2 | Chave de Planejamento | **Todas as regras de chave** (8026, `chave_planejamento_regra`, `chave_arrumar`, `forcar_chave`) valem **só para UOs da SEDUC** (14101/14601). Outras UOs ficam "NÃO IDENTIFICADO". |
| D2a | Evolução das chaves | **Em duas etapas.** Agora: lista fixa `UOS_SEDUC`, mas consultada por uma função única `uo_usa_chave(uo)` (nenhum módulo compara UO direto). Quando a primeira UO nova tiver chave: trocar o interior dessa função por um cadastro de UO (tabela com indicador "usa chave"); regras próprias de outra UO exigirão coluna `uo` nas tabelas de regra. |
| D3 | Listas de UO | **Duas listas.** Plan20 e módulos de execução aceitam as 5 UOs; o Teto-SEDUC continua só com 14101/14601. |
| D4 | Dotações (PED/EMP) | O recálculo fica **limitado à UO (e exercício) do arquivo**. |
| D5 | Painéis/relatórios | Na Fase 1, **fixar SEDUC (14101/14601)** em todos; depois, filtro de UO por tela. |
| D6 | Validação do upload | **Recusar** arquivo com mais de uma UO, cabeçalho ≠ linhas, ou UO não cadastrada, com mensagem clara. A UO vem sempre do conteúdo, nunca do nome do arquivo (seção 8.1 dos testes). |
| D7 | FIP 613 `.xls` (HTML) | Fica fora: o usuário continua convertendo para `.xlsx`. Melhoria futura. |
| D8 | Nome "Plan20 - SEDUC" | Muda **só o texto da tela/menu**; a tabela continua `plan20_seduc`. |

Aprovado antes: no PED, criar sempre as colunas da Dotação e da Chave de Planejamento ("-" sem chave).

## Fase 0: preparação (sem mudar comportamento)

1. Confirmar no banco de cada ambiente que `fip613`, `ped`, `emp`, `nob`, `est_emp` e `dotacao` têm a coluna `uo` preenchida (models já têm), e checar valores (formato "14101" × "14.101"). Se houver `NULL` ou formato misto, preparar `UPDATE ... SET uo = '14101' WHERE uo IS NULL` (pedir autorização antes de rodar).
2. Conferir índice em `(uo, exercicio, ativo)` nas 5 tabelas. `emp` e `nob` são grandes, então qualquer `ALTER`/índice novo entra no script de schema e é feito fora do horário das cargas (~05h).
3. Backup do banco antes da Fase 3.
4. Criar helper único em `services/uo.py`: `UOS_EXECUCAO` (5 UOs), `UOS_PLAN20` (5 UOs), mantendo `UOS_ACEITAS` (14101/14601) para o Teto-SEDUC, e `UOS_SEDUC` para as regras de chave e os painéis, exposta pela função `uo_usa_chave(uo)` (D2a). Incluir `UO_NOMES` das novas UOs.
5. Criar validador comum de cabeçalho/UO para os módulos de execução, no padrão de `validar_cabecalho_fiplan()` (`services/teto_seduc.py`): lê a UO do conteúdo, recusa UO ausente, não cadastrada, múltipla ou divergente. Retorna a UO única do arquivo.

### Resultado do diagnóstico de leitura da Fase 0 (2026-10-22, produção, só `SELECT`/`SHOW`)

- **Coluna `uo` existe e está preenchida** em `fip613`, `ped`, `emp`, `nob`, `est_emp` e `dotacao`: nenhuma linha com `uo` nula ou vazia. Linhas ativas, todas `14101`, exercício 2026: fip613 1.661, ped 57, emp 56.917, nob 70.444, est_emp 980. **Não é preciso `UPDATE` de preenchimento nem `ALTER TABLE`.**
- **Formato do exercício varia por tabela:** `fip613.ano` é inteiro (`2026`); `ped`/`emp`/`nob` guardam texto `'2026'`; `est_emp` guarda `'26'`. A desativação por exercício precisa normalizar (`normalizar_exercicio`, já usado na Limpeza de Histórico) para não deixar a carga anterior ativa.
- **Formato da UO varia:** nas cargas antigas de `est_emp` aparece sem zero à esquerda (`1101`, `3101`), e em `ped`/`est_emp` há muitas UOs no histórico (cargas de antes). A comparação deve usar `uo_key()` (só dígitos) e, para o histórico, comparar sem zeros à esquerda ou só tocar as linhas ativas.
- **`dotacao.uo` é texto com nome:** `'14.101 - SECRETARIA DE ESTADO DE EDUCAÇÃO'` (com ponto), diferente das demais. Limitar o recálculo por UO (D4) precisa comparar por código (`REPLACE(SUBSTRING_INDEX(uo,' - ',1),'.','')`, como no Plan20).
- **Índices:** as 5 tabelas de execução só têm `PRIMARY`. Hoje o `UPDATE ... WHERE ativo = 1` já varre a tabela inteira, então a nova condição não piora; as tabelas estão pequenas após a Limpeza de Histórico (≤ 70 mil linhas). Índice `(uo, exercicio, ativo)` é opcional, a avaliar.
- **Runners não filtram UO:** só `fip613_runner.py:78` cita 14101 (corte "Total UO 14101", item 7 da seção 9 dos testes). PED e EST EMP gravam o que vier no arquivo.
- **Tabelas de regra de chave:** `chave_planejamento_regra` não tem coluna de UO. `chave_arrumar` e `forcar_chave` **não são tabelas** (são arquivos JSON em `static/js/`). Isso confirma que a restrição por UO (D2) tem de ser feita no código, na função `uo_usa_chave(uo)`.
- **Tabelas `*_uploads` não têm coluna de UO nem de exercício.** O histórico de quais cargas foram de qual UO só existe nas linhas. A Limpeza de Histórico já trabalha assim (lê UO/exercício dos registros).

## Fase 1: desativação por UO + painéis fixos na SEDUC (vai a produção; sem novas UOs ainda)

Este é o ajuste mais importante. Ordem sugerida, do menor risco ao maior:

1. **EST EMP** (`services/est_emp_runner.py:477`): `UPDATE est_emp SET ativo = 0 WHERE ativo = 1 AND uo = :uo AND exercicio = :ex`.
2. **FIP 613** (`services/fip613_runner.py:192`): idem com `ano`; trocar o corte fixo "Total UO 14101" por regra genérica; manter UG como texto (`0011`).
3. **PED** (`services/ped_runner.py:1154`): idem; criar sempre as colunas da Dotação e da Chave; limitar `_update_dotacao_from_ped()` à UO/exercício do arquivo (D4).
4. **NOB** (`node_runners/nob_runner.js:662-664`): idem. Os dois ramos (mssql/mysql) do arquivo têm de ser alterados juntos.
5. **EMP** (`node_runners/emp_runner.js:1758-1760`): idem; limitar `atualizarDotacaoComEmp()` à UO/exercício.
6. **Painéis e relatórios:** as consultas diretas em `rotas/home_routes.py` (ped nas linhas ~16137/16324; emp ~17090/17930; est_emp ~17458/18519; nob ~17598/17736) e as leituras por ORM passam a filtrar `uo IN (SEDUC)`. Fazer um levantamento completo (inclusive `rotas/limpeza_historico_routes.py`, API `/api/v1/dados/...` e `node_runners/`) antes de editar.
7. **Validador de UO** (Fase 0, item 5) ligado em todos os uploads. Nesta fase aceita só UOs da SEDUC; qualquer outra é recusada com mensagem clara. Isso também resolve o "processado com sucesso" sem gravar do Plan20.
8. Mensagens de erro de validação com acentuação corrompida (`rotas/home_routes.py`): corrigir junto, por ser baixo risco.

**Critério de saída:** com o mesmo arquivo da SEDUC de hoje, o resultado no banco (linhas ativas, somas, dotações) é idêntico ao da versão atual.

## Fase 2: Plan20 e chaves de planejamento

1. **Plan20:** `UOS_PLAN20` com as 5 UOs; texto da tela/menu sem "SEDUC" (D8); recusa clara de UO não aceita. Desativação do `plan20_seduc` já é por UO/exercício (`rotas/home_routes.py:16899`).
2. **Chaves (D2):** restringir a `UOS_SEDUC` em `services/ped_runner.py` (`identificar_chave_planejamento()` e `_build_fallback_chave_8026`) e em `node_runners/emp_runner.js` (`buildFallbackChave8026` e as demais regras). As duas implementações precisam ficar iguais.
3. Para o Plan20, definir o que fazer com a Chave de Planejamento vazia nas novas UOs (100% das linhas): manter vazia e exibir "-", sem inventar chave.

## Fase 3: liberar as novas UOs

1. Backup; abrir `UOS_EXECUCAO` para 26101, 26201 e 26202.
2. Carga de teste em ambiente de homologação, módulo a módulo, com os mesmos arquivos dos testes de 13 e 14/10.
3. Em seguida em produção, nesta ordem: EST EMP → FIP 613 → PED → NOB → EMP. Após cada carga, conferir que a SEDUC não mudou e que as somas da nova UO batem com as dos testes (seções 4 a 7 do documento de testes).
4. Filtro de UO nas telas (D5, segunda parte), com padrão SEDUC até decisão em contrário.

## Fase 4: sincronização e documentação

- Fluxo `main` → `homologacao` → `dev/cleber` e `dev/jean`, com PR de destino fixo (lições da seção 21.8 do `claude.md`) e `git merge-tree` antes.
- `claude.md`: atualizar seção 22 e este plano.
- Limpeza de Histórico (seção 23): revisar se cada carga continua com UO e exercício únicos. Como a desativação passa a ser por UO, a regra "nunca remove carga com registro ativo" continua protegendo, mas a simulação deve mostrar a UO de cada carga.

## Testes

- Unitários sem banco para o validador de UO e a regra de chave por UO.
- Testes de banco em **tabela temporária ou exercício descartável** (padrão de `tests/test_teto_seduc_uo.py`): carga da UO A não desativa a UO B, e o reenvio da mesma UO desativa só a versão anterior dela.
- Regressão: arquivo real da SEDUC antes e depois (Fase 1) com saída idêntica.
- Os testes não podem chamar `run_fip613()`, `run_ped()`, `run_est_emp()` nem os runners Node contra o banco real, pois gravam.

## Riscos e pontos de atenção

- **Cargas automáticas diárias da SEDUC** (~03h): a Fase 1 precisa estar estável antes de qualquer UO nova, senão a carga seguinte mexe nos dados dela.
- **Registros antigos com `uo` nula ou em outro formato** não seriam desativados pela nova regra e ficariam ativos duplicados. A Fase 0 trata isso antes.
- **EMP e NOB são tabelas grandes:** o `UPDATE ... WHERE uo = ... AND exercicio = ...` precisa de índice, senão fica lento.
- **Código duplicado Python/Node** (regras de chave e desativação): alterar sempre os dois lados.
- **Dashboards de dotação** dependem de PED/EMP; validar números da SEDUC depois da Fase 1.
