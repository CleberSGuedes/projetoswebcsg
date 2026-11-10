# Plano: igualar main, homologação, dev/cleber e dev/jean

Análise de 10/11/2026 (somente leitura; nada foi alterado nas branches). Referência: `origin/main` 09f5495, `origin/homologacao` 412ab0e, `origin/dev/cleber` 403355e, `origin/dev/jean` a820a32.

## 1. Situação encontrada

As branches **não são três sistemas diferentes**: a homologação é a base do layout novo e recebeu a main em 02/10, 16/10 e 22/10; a dev/cleber e a dev/jean são cópias da homologação de 16/10 com algo a mais. A main ficou com o layout antigo e seguiu criando funcionalidades.

| Branch | Ponto de separação | Só nela (commits) | Falta nela (da main) |
|---|---|---|---|
| main | — | 8: novas UOs (reescritas no layout antigo), PLAN 51, API de dados + Google Planilhas, correção do painel inicial lento + índices, Notas SEE (blueprint + DRE/Escola) | — |
| homologacao | 22/10 | 44: layout novo, tema/personalização, governança, PTA 2027, menu gerado, permissão por usuário, jobs com cancelar/reprocessar, deploy cPanel, novas UOs | os 8 da main |
| dev/cleber | 15/10 | 48: tudo da homologação (até 16/10) + **Receita Anexo 10**, leitor de formatos FIPLAN, correções das Notas SEE | os 8 da main + 12 da homologação (novas UOs de 22/10 a 03/11) |
| dev/jean | 15/10 | 50: homologação (até 16/10) + identificação/diagnóstico de ambiente DEV, scripts `.cmd/.ps1` | idem cleber |

Simulação de merge (`git merge-tree`, sem tocar em nada): **homologação + dev/cleber** conflita só em `worker.py`; **homologação + dev/jean** só em `templates/base.html`. Ou seja, as três já estão quase iguais entre si; o trabalho real é **main × homologação** (15 arquivos em conflito, 38 blocos) e **main × cleber** (mesmos arquivos + `est_emp_runner`, `fip613_runner`).

## 2. O que cada lado tem que o outro não tem

**Só na homologação/cleber (vem para a main)**
- Layout novo: casca `spo-shell`, topo, cores de destaque (`--accent*`), modo claro/escuro/automático, densidade, contraste, estilo de cartões, barras de rolagem, responsivo (split do Chrome, mobile), `login-theme.js`, `docs/contrato-visual-spo.md`. CSS: 7.360 linhas contra 3.199 da main; JS: 25.968 contra 23.813.
- Menu gerado do catálogo (`_menu.html`, `MENU_META`) e a hierarquia/nomes novos: **Administrar SPO** (no lugar de Atualizar) com Personalizar SPO, Governança e Resultados, PTA - Programação, PTA-Gerencial NGER e Atualizar Execução Orçamentária; **Emitir/Ajustar Dotação** (no lugar de Cadastrar); grupos Planejamento/Execução em Relatórios e Painéis; Usuários e **Painel - Permissões** no fim.
- Telas: Personalizar SPO (temas e contrato visual), 7 telas de Governança e Resultados, PTA 2027 (app estático `static/pta2027_v63r`), cabeçalho padronizado dos relatórios.
- Permissão por usuário (`usuario_permissoes`, `/api/permissoes/usuario`) e painel de permissões novo.
- Jobs com **cancelar/reprocessar** para PED, EST EMP e FIP 613 (`services/processamento_jobs.py`, tabelas `processamento_jobs/eventos`).
- Selo/rótulo de ambiente por variável (`APP_ENV_LABEL`; vazio = sem selo).
- `scripts/atualizar_schema_sync_main.py` e `scripts/migrate_personalizar_spo_permissions.py`.
- **Só na cleber:** Receita Anexo 10 (atualizar, relatório, 3 tabelas) e `fiplan_file_reader.py` (detecção padronizada dos formatos FIPLAN).

**Só na main (fica na main; o plano preserva)**
- PLAN 51 (atualizar, relatório, `plan51_*`), API de dados + script Google Planilhas em lotes + manual (`docs/api`), botão excluir da chave de API, `tests/apps_script`.
- Painel inicial rápido (`_uos_das_cargas`) e índices `upload_id`/`ativo` (`db/indices_upload_id_ativo.sql`).
- Notas SEE em blueprint próprio (`notas_see_routes.py`), catálogo por exercício, código do produto só com dígitos, ordem dos produtos, PDFs apagados ao concluir, worker `--kind see`, cadastro DRE/Escola (`see_escolas`, 643 registros). A versão da homologação está dentro do `home_routes.py`, sem DRE/Escola; a da cleber tem DRE/Escola, também no `home_routes.py`. **Vale a da main.**
- Ajustes de UO da main já reescritos (mesmo conteúdo da homologação; o conflito é só de layout).
- `.cpanel.yml` simples (`cp -R`).

**Não vão para a main (exclusivos de ambiente de desenvolvimento)**
- `scripts/deploy_cpanel.sh`: usa `rsync --delete` sem excluir `upload/`, `outputs/` e `node_modules`; na produção apagaria arquivos gerados. Mantém-se o `.cpanel.yml` da main (lição da seção 21 do claude.md).
- dev/jean: `*.cmd`, `*.ps1`, diagnóstico git na home e o selo "Integração DEV Jean" (padrão de ambiente com texto fixo).
- `CLAUDE.md` da cleber (a main usa `docs/claude.md`).

## 3. Banco de dados (produção = compartilhado com o local)
- Já existem, vazias: `usuario_permissoes`, `processamento_jobs`, `processamento_eventos`, `receita_anexo10_{uploads,registros,arquivos}`; `see_escolas` tem 643 linhas. **Não há tabela nova a criar.**
- Rodar antes, em modo leitura, `scripts/atualizar_schema_sync_main.py` (só lista o que falta) e conferir colunas dos modelos novos contra o banco.
- Nos bancos próprios de homologação, cleber e jean (depois da migração): aplicar `db/plan51_schema.sql`, `db/notas_see_schema.sql` e `db/indices_upload_id_ativo.sql`.

## 4. Plano de migração (integração em branch temporária, depois PR; nada direto na main)

**Fase 0 — Preparação**
- Tags de backup (`backup/main-2026-11-10`, e das três branches).
- Decidir o destino da branch local `feat/painel-permissoes` (seção 7, item 1).
- Aviso de manutenção aos usuários (muda menu e visual para todos).

**Fase 1 — Integração** em `integracao/layout-novo` a partir da main
1. `merge origin/homologacao`. Regras de conflito: layout, menu, CSS, JS de menu/tema/painel e cabeçalhos = **homologação**; PLAN 51, API, Notas SEE (`see_notes`, `notas_see.html`, `worker.py --kind see`), dashboard rápido e índices = **main**.
2. `merge origin/dev/cleber`: Receita Anexo 10, `fiplan_file_reader`, `est_emp_runner`/`fip613_runner`/`ped_runner` (7 blocos em `ped_runner`).
3. dev/jean: não mesclar (ver seção 7, item 2).

**Fase 2 — Reconciliação do que o Git junta sozinho** (maior risco; a seção 21 do claude.md já registrou perdas silenciosas assim)
- `features.py`: acrescentar `atualizar/plan51`, `relatorios/plan51` e manter `area-uens/sage/notas-see` na hierarquia nova (Atualizar Execução Orçamentária; Relatórios > Execução; Área UENs > SAGE), com `MENU_META` (ícone/título). Verificar que nenhum item da main ficou só no menu antigo desativado (`{% if false %}`).
- Atalho do **admin (nível 1) passa em tudo** mantido em `has_permission`, `require_feature` e no JS (`fetchCurrentPermissions`), como já decidido; filtro `ativo = 1` mantido.
- Tratamento de falha de conexão ao carregar permissões (seção 13) preservado.
- `worker.py`: manter `--kind see` e acrescentar o que vier da homologação/cleber.
- Telas só da main (Notas SEE, PLAN 51 atualizar/relatório, API de Acessos, dashboard) ajustadas ao layout novo: cabeçalho `report-header`, variáveis de cor e modo escuro; hoje usam o CSS antigo.
- Numeração do `docs/claude.md`: as seções 21 a 29 de cada lado colidem; renumerar e consolidar.

**Fase 3 — Verificação**
- Suíte completa (main 162 + testes de UO da homologação + os da cleber).
- Comparação do HTML do menu gerado com o esperado (ferramenta já usada em 09/11), para cada perfil de teste (admin, NGER, SAGE, consultor) e com exceção por usuário.
- Telas em Chrome headless, claro e escuro, menu expandido e recolhido: Início, relatórios, Notas SEE, PLAN 51, Painel de permissões, Personalizar SPO.
- Carga real: upload de PED/EMP/NOB/FIP 613 e Notas SEE com 27 DANFEs (como em 09/11), mais cancelar/reprocessar de um job.

**Fase 4 — Produção**
- PR `integracao/layout-novo` → main; `?v=` de CSS/JS e RELEASE trocados por você; Update from Remote + Deploy HEAD + Restart.
- Conferir menu por perfil, permissões (403 em acesso direto) e uma atualização de cada relatório. Reverter = voltar à tag de backup.

**Fase 5 — Igualar as demais** (documentado: merges entre branches fixas via branch temporária e PR com destino fixo)
- main → homologacao, main → dev/cleber, main → dev/jean, um PR por branch, simulando com `git merge-tree` antes e checando perdas silenciosas nos dois sentidos.
- Em cada ambiente: SQLs do item 3 e `pip install -r requirements.txt` (`pdfplumber`).
- Resultado: as quatro branches com o mesmo código; só variam `.env`, rótulo de ambiente e banco.

## 5. Riscos
1. Merge automático que parece limpo e perde item de menu ou regra (mitigado pela comparação do menu e pelos testes).
2. Visual da main mudar para todos os usuários de uma vez (mitigado pelo aviso e pela tag de reversão).
3. Permissões: nomes e posições de menu mudam, mas **os ids gravados não mudam** (confirmado no catálogo da homologação: só entram ids novos); perfis já configurados continuam valendo.
4. Deploy da homologação (`deploy_cpanel.sh`) chegando à produção por engano.
5. Tela de Governança/PTA 2027 e Receita Anexo 10 passam a existir em produção: sem ninguém marcado no painel, só o admin vê.

## 6. Esforço estimado
Fase 1 e 2: 1 a 2 dias de trabalho com validação; Fase 3: meio dia; Fase 4: 1 hora com você acompanhando; Fase 5: 1 dia (três PRs).

## 7. Decisões pendentes
1. A branch local `feat/painel-permissoes` (permissão por usuário e menu dinâmico, já testada, **não commitada**) fica **redundante**: a homologação traz o mesmo recurso, só que completo. Sugestão: não commitar; guardar como referência e apagar ao fim.
2. dev/jean: tratar como ambiente de desenvolvimento (recebe a main no fim, sem entrar na main) ou também igualar?
3. Telas de Governança e Resultados / PTA 2027 / Receita Anexo 10 em produção: aceitar que fiquem visíveis só para admin até você liberar?
4. Janela da troca de layout em produção (data e aviso aos usuários).
