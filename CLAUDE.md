# CLAUDE.md — SPO (Sistema de Planejamento e Orçamento) · SEDUC-MT

> **Regras fixas desta branch (Cleber):**
> - Responda, raciocine e escreva comentários, commits, PRs e documentação sempre em português do Brasil.
> - Nunca faça commit, push, PR ou merge sem autorização explícita do Cleber; a autorização vale só para aquele conjunto de mudanças.
> - Trabalhe sempre numa branch `feat/<tema>` ou `fix/<tema>` criada a partir da `dev/cleber`, nunca direto nela (fluxo nas seções 30 e 40 do `docs/claude.md`).
> - Antes de pedir autorização para o commit, confira o `RELEASE_2_AAAA_MM_DD.N` no rodapé de `templates/base.html`: com data diferente de hoje, troque a data e use `.1`; com a data de hoje e N já usado em commit, some 1.
> - Nas mensagens de commit, não inclua a linha `Co-Authored-By` nem outras linhas de atribuição.

O documento principal do projeto é o `docs/claude.md`. Antes de qualquer tarefa, leia as seções 30 e 40 e as seções ligadas ao tema.

> **Este arquivo é próprio da `dev/cleber`** e nunca vai para a `homologacao` nem para a `main`.
> Na promoção por cherry-pick (§40.1), não levar este arquivo, o `.cpanel.yml` nem o `scripts/deploy_cpanel.sh`.

## Ambiente da dev/cleber

- Pasta: `C:\workspace\projetoswebcsg_dev_cleber`.
- Subir o servidor: `.\run_dev_cleber.ps1` → http://127.0.0.1:5010 (com `--debug`).
  Se a política bloquear scripts: `powershell -ExecutionPolicy Bypass -File .\run_dev_cleber.ps1`.
- Banco próprio: `proj5954_spo_csg_amb_cleber`. O `.env` local aponta para ele; testes, scripts e uploads locais gravam nele.

## Convenções (resumo; detalhes nas seções indicadas)

- Sem framework de migração: `db.create_all()` só cria tabelas novas; mudança de schema vai num script idempotente em `db/`, rodado à mão em cada ambiente **antes** do código (§40.4).
- `main.js`/`style.css`: código novo em bloco próprio no fim do arquivo e troca do `?v=` em `templates/base.html` com sufixo `?v=AAAAMMDD.cleber-<tema>.N` (§40.2).
- `rotas/home_routes.py`: tocar o mínimo; código novo em `rotas/<tema>_routes.py` e `services/<tema>.py` (§40.2).
- Arquivos que não se toca sem combinar: §40.3. Limite de 15 conexões e uso do `tests/conftest.py` nos testes: §40.4.

## Referência rápida

- Notas SEE: §28 · Jobs, worker e fila de processamentos pesados: §38 e §39 · Integração com a main: §32.
