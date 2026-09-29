# Sistema SPO — Sistema de Planejamento e Orçamento

> Documento gerado a partir de uma análise completa do repositório em 2026-08-31, e atualizado nas sessões seguintes (última atualização: 2026-09-29) à medida que os riscos identificados foram corrigidos e novas investigações aconteceram.
> Objetivo: servir de contexto rápido para quem (humano ou IA) for trabalhar neste projeto.

---

## 1. Visão geral

- **Nome do projeto:** projetoswebcsg (apelido: **Sistema SPO** — Sistema de Planejamento e Orçamento).
- **Domínio de negócio:** gestão orçamentária e de planejamento governamental — módulos como Dotação, Empenho (EMP), Estorno de Empenho (EST EMP), Notas de Obrigação (NOB), Pedidos (PED), FIP 613, Plan20/Plan21 SEDUC, Estrutura de Planejamento (programas, ações, produtos, chaves de planejamento), Teto Orçamentário (MOMP), etc. Fortes indícios de uso pela SEDUC (Secretaria de Educação) de algum estado, dado o vocabulário (PTA/LOA, UO/UG, PAOE, dotação, empenho, liquidação).
- **Tipo de aplicação:** aplicação web monolítica server-rendered, com APIs JSON internas consumidas via fetch/AJAX pelo próprio front-end, e uma API pública versionada (`/api/v1/...`) com autenticação por client credentials.
- **Estágio:** em desenvolvimento ativo. Existe uma instância "online" apontando para o **mesmo banco de dados remoto** usado localmente — ou seja, **não há isolamento entre ambiente de desenvolvimento e produção/homologação a nível de dados**. Isso é um ponto crítico de atenção (ver seção 9).
- **Licença:** GNU GPLv3 (arquivo `LICENSE` na raiz). Há branches de trabalho relacionadas a isso (`agent/licenca-gplv3-dev-cleber`, `feature/licenca-gplv3-homolog`).

---

## 2. Stack tecnológica

### Backend
- **Python 3.11** (venv local em `.venv`, `pyvenv.cfg` aponta para Python 3.11.9).
- **Flask 3.1.2** como framework web (`app.py`, `config.py`).
- **Flask-SQLAlchemy 3.1.1** / **SQLAlchemy 2.0.44** como ORM.
- **Flask-Mail 0.10.0** para envio de e-mail (recuperação de senha).
- **PyMySQL 1.1.1** (driver MySQL) — único engine de banco suportado hoje. O suporte a SQL Server (`pyodbc`) foi removido em 2026-08-31 por ser código morto (ver seção 9, itens 3/4) — se você leu uma versão antiga deste documento ou um histórico de código mencionando MSSQL/`DB_ENGINE`, isso já não reflete o estado atual.
- **pandas / numpy / openpyxl / XlsxWriter / rapidfuzz** — processamento de planilhas Excel (upload/tratamento/relatórios), fuzzy matching de chaves de planejamento.
- **python-dotenv** para carregar `.env`.
- **pytest 8.3.4** (adicionado em 2026-08-31, ver seção 9 item 8) — testes em `tests/`, executados com `pytest` a partir da raiz do projeto (`pytest.ini` configura `pythonpath = .`). Ainda sem CI (GitHub Actions) — decisão consciente, os testes usam o mesmo banco MySQL remoto compartilhado, sem banco de teste dedicado.

### Processamento em segundo plano (Node.js)
- Pasta `node_runners/` — processos Node chamados via `subprocess` pelo Python (`worker.py`) para processar uploads pesados (EMP e NOB).
- Dependências: `exceljs` (leitura/escrita de Excel), `mysql2` e `mssql` (o Node também fala com os dois engines de banco, espelhando `config.py`).
- `node_runners/run.js` é o entrypoint chamado como `node run.js --kind emp|nob --file ... --upload-id ... --user-email ...`.

### Frontend
- Server-side rendering com **Jinja2** (`templates/`), sem framework SPA (React/Vue) — há um `base.html`, páginas completas (`login.html`, `home.html`) e uma pasta grande de **partials** (`templates/partials/*.html`, ~40 arquivos) carregados dinamicamente via rotas `/partial/...` (padrão "AJAX partial swap", tipo HTMX/jQuery manual).
- `static/js/main.js` (JS único, não há bundler/webpack/vite configurado).
- `static/js/*.json` — dados de referência para regras de "chave de planejamento" (`chaves_planejamento.json`, `chave_arrumar.json`, `forcar_chave.json`), usados também pelo script utilitário `gerar_migracao_chaves.py` para gerar SQL de migração de dados.
- `static/css/style.css` — CSS único, sem pré-processador.

> Observação de escala: `templates/home.html` tem ~741 KB / muitas linhas e `rotas/home_routes.py` tem **19.214 linhas** — arquivos monolíticos muito grandes que concentram praticamente todas as rotas HTML/API do sistema. Isso é um ponto de atenção para manutenibilidade (ver seção 9).

---

## 3. Estrutura de pastas

```
projetoswebcsg/
├── app.py                  # Factory da aplicação Flask, middlewares, sessão, logging
├── config.py                # Configuração (lê .env, monta SQLALCHEMY_DATABASE_URI, Mail)
├── worker.py                 # Worker chamado como processo separado p/ uploads pesados (EMP/NOB)
├── gerar_migracao_chaves.py  # Script utilitário standalone p/ gerar SQL de chaves de planejamento
├── requirements.txt
├── .env                      # Credenciais reais (git-ignored) — ver seção 6
├── .cpanel.yml                # Deploy automático via cPanel Git Version Control
├── models/
│   ├── db.py                 # instância única do SQLAlchemy (db = SQLAlchemy())
│   ├── __init__.py           # reexporta todos os models (~65 classes)
│   └── user.py                # TODAS as models ficam neste único arquivo (~1300 linhas)
├── rotas/
│   ├── __init__.py            # registra os blueprints
│   ├── auth_routes.py         # login/logout/esqueci senha/reset senha
│   └── home_routes.py         # TODAS as demais rotas HTML+API (19k+ linhas)
├── services/                  # regras de negócio / parsers de planilhas por módulo
│   ├── auth.py                 # decorators login_required / role_required
│   ├── features.py             # árvore de features/menus do sistema (controle de permissão por feature)
│   ├── job_status.py           # status de jobs assíncronos em arquivos JSON (outputs/status/*.json)
│   ├── fip613_runner.py, ped_runner.py, plan20_runner.py, est_emp_runner.py, emp_record.py, teto_seduc.py
│   ├── uo.py                   # UOs aceitas (14101/14601) + uo_key/uo_label — regra única do Plan20 e do Teto-SEDUC (seção 20)
├── node_runners/               # workers Node.js p/ EMP e NOB (processamento pesado de planilhas)
├── db/                          # scripts .sql de schema (CREATE TABLE) por módulo — não são migrations versionadas (tipo Alembic)
├── tests/                       # suíte pytest (adicionada em 2026-08-31, ver seção 9 item 8)
├── pytest.ini                   # pythonpath = . — permite rodar `pytest` de qualquer diretório
├── templates/, static/          # front-end server-side (Jinja2)
├── upload/, outputs/             # armazenamento de arquivos enviados/gerados (NÃO versionado, ver seção 9)
├── logs/                         # log rotativo da aplicação (app.log)
├── .venv/                        # ambiente virtual Python local
└── __pycache__/
```

---

## 4. Banco de dados

### 4.1 Motor e conexão
- ORM: **SQLAlchemy** via Flask-SQLAlchemy, instância única em `models/db.py`, todas as tabelas mapeadas em `models/user.py` (nome do arquivo é enganoso — não contém só o model `Usuario`, contém **todo o schema do sistema**, ~65 classes/tabelas).
- `config.py` suporta **um único engine**: MySQL (via PyMySQL), configurado pelas variáveis `DB_*_CSG` (com fallback para `DB_USER`/`DB_PASSWORD`/etc. genéricos). Até 2026-08-31 havia um segundo caminho (SQL Server/MSSQL via pyodbc, selecionado por `DB_ENGINE=mssql` + variáveis `DB_*_HMG`) que nunca era efetivamente usado — foi removido por ser código morto (histórico completo na seção 9, itens 3/4). A variável `DB_ENGINE` deixou de ser lida pelo Python; se ainda existir no `.env` de algum ambiente, é inofensiva.
  - `node_runners/db.js` (lado Node, usado só por EMP/NOB) **ainda mantém** essa lógica de dois engines duplicada — decisão consciente de não mexer lá agora, fica a cargo do trabalho de separação de ambientes da v2.
- Host do banco remoto: `186.209.113.112:3306`, schema `proj5954_spo-csg` (nome típico de hospedagem cPanel: `proj5954_...`).
- Pool de conexões configurado via env vars (`DB_POOL_SIZE`, `DB_POOL_RECYCLE`, `DB_POOL_TIMEOUT`, `DB_MAX_OVERFLOW`, `DB_CONNECT_TIMEOUT`) com `pool_pre_ping=True`.
- `db.create_all()` é chamado no `create_app()` (app.py) — cria tabelas que não existem automaticamente ao subir a aplicação, **sem uso de um sistema formal de migrations** (não há Alembic/Flask-Migrate). Alterações de schema são feitas via os arquivos soltos em `db/*.sql` (aplicados manualmente, aparentemente) e/ou via `gerar_migracao_chaves.py`.

### 4.2 Modelo de dados (visão geral)
Categorias principais de tabelas (ver `models/user.py` para o detalhe completo):
- **Autenticação/autorização:** `usuarios`, `perfil`, `perfil_permissoes`, `nivel_permissoes`, `active_sessions`, `logs_login`.
- **API pública (client credentials):** `api_clients`, `api_client_scopes`, `api_refresh_tokens`, `api_access_logs`, `api_keys`.
- **Módulos de upload/tratamento de planilhas** (cada um com padrão `*_uploads` + tabela de registros tratados): `fip613_uploads`/`fip613`, `ped_uploads`/`ped`, `emp_uploads`/`emp` (+ `emp_status_diario`), `est_emp_uploads`/`est_emp`, `nob_uploads`/`nob`, `plan20_uploads`.
- **Estrutura de planejamento (taxonomia/domínio):** `adj`, `regiao`, `municipio`, `funcao`, `subfuncao`, `ug`, `macropolitica`, `pilar`, `eixo`, `politica_decr`, `publico_transversal`, `metas_pee`, `indicadores_pee`, `programa_planejamento`, `acao_planejamento`, `produto_acao_planejamento`, `componentes_rev` — mais um grande número de tabelas de associação N:N (`*_adj`, `*_pilar`, `*_eixo`, etc.).
- **"Chave de planejamento" (motor de regras de normalização):** `chave_planejamento_regra`, `modelo_chave`, `modelo_chave_componente`, `chave_catalogo`, `chave_catalogo_valor`, `chave_contexto`, `chave_catalogo_historico` — sistema próprio para montar/normalizar chaves compostas (ex.: região*subfunção-ug*adj*macropolítica*pilar*eixo*política) usadas para casar dados entre os módulos.
- **Fluxos de solicitação/aprovação:** `dotacao`, `cadastrar_subacao`, `cadastrar_etapa`, `alterar_meta`/`alterar_meta_item` — todos com padrão `status_aprovacao`, `aprovado_por`, `data_aprovacao`, `motivo_rejeicao`, soft delete via `excluido_em`.
- **Teto orçamentário:** `momp`, `politicateto` (dados de teto anual/saldo por política-decreto).
- **Planejamento consolidado:** `plan21_nger` (visão consolidada usada por vários relatórios/painéis).

### 4.3 Padrões observados no schema
- **Soft delete** amplamente usado via coluna `excluido_em` (datetime nulo = ativo) combinado com flag booleana `ativo`.
- Timestamps em português: `criado_em`/`created_at`, `alterado_em`/`updated_at`, `atualizado_em` — nomenclatura **inconsistente** entre tabelas (mistura de inglês e português para os mesmos conceitos).
- ~~Alguns PKs usavam `autoincrement=False` com geração manual de próximo ID em Python~~ — **resolvido em 2026-08-31** (commit `caf1701`, detalhes na seção 9 item 5): `logs_login`, `active_sessions` e `perfil` já tinham `AUTO_INCREMENT` nativo no MySQL; removida a geração manual (`_next_pk`/`_next_pk_active_session`), os `INSERT`s hoje deixam o banco gerar o `id`.
- **Esse mesmo padrão de risco (casar/gerar dado sem lock, assumindo que a concorrência "não vai acontecer") se repetiu no módulo Teto-SEDUC** (tabela `momp`) e causou um incidente real de duplicação de dados — investigado e corrigido em 2026-08-31/09-01, ver seção 12.
- Os arquivos SQL soltos em `db/` (`emp_schema.sql`, `est_emp_schema.sql`, `nob_schema.sql`, `ped_schema.sql`, `meta_fisica_indexes.sql`) são `CREATE TABLE IF NOT EXISTS` — parecem ser a origem "manual" do schema para módulos específicos, complementando o `db.create_all()` automático do SQLAlchemy.

---

## 5. Autenticação, sessão e autorização

- Login via `/login` (`rotas/auth_routes.py`), com suporte a requisição normal (form) e requisição "fetch" (JSON), senha validada com `werkzeug.security.check_password_hash`.
- **Sessão única por usuário:** a tabela `active_sessions` guarda um `session_token` por e-mail. Login de um novo dispositivo/aba detecta sessão ativa recente (últimas 2h) e pede confirmação (`force_login`) antes de derrubar a sessão anterior — mecanismo de "single active session".
- Timeout de sessão fixo: `SESSION_TIMEOUT = timedelta(hours=2)`, verificado a cada request em `app.py` (`before_request` / `load_current_user`), com toda uma camada de cache (TTLs configuráveis via env: `ACTIVE_SESSIONS_COUNT_TTL_S`, `PROFILE_CACHE_TTL_S`, `ACTIVE_SESSION_CHECK_TTL_S`) para reduzir carga no banco a cada requisição — lógica bastante elaborada e com muitos caminhos de fallback (session cache local quando o banco falha).
- Autorização por **perfil** (`perfil` → `nivel`) e por **feature** (`services/features.py` define a árvore de menus/telas do sistema; `perfil_permissoes`/`nivel_permissoes` controlam quem vê o quê). `services/auth.py` expõe decorators `login_required` e `role_required("admin")` (nível 1 = admin).
- **Recuperação de senha:** `/forgot-password` + `/reset-password/<token>`, token assinado com `itsdangerous.URLSafeTimedSerializer` usando `SECRET_KEY`, expira em 1h, envia e-mail via Flask-Mail/SMTP Gmail.
- **API pública** (`/api/v1/dados/...`, `/api/auth/token`, `/api/api-clients/...`): sistema próprio de client credentials (`api_clients`, `api_client_scopes`, `api_refresh_tokens`, `api_keys`, `api_access_logs`) — parece ser uma camada de integração para consumo externo dos dados tratados (EMP, NOB, PED, FIP613, EST-EMP), independente da sessão de usuário web.
- Não há proteção CSRF explícita (nenhuma referência a `flask-wtf`/CSRF no código) nem CORS configurado — como o sistema é majoritariamente server-rendered com submissão via mesma origem, o risco é mitigado, mas as rotas JSON sob `/api/` merecem revisão se algum dia forem expostas cross-origin.

---

## 6. Configuração de ambiente (`.env`)

O arquivo `.env` está presente na raiz e é **corretamente ignorado pelo Git** (`.gitignore` lista `.env`). Ele contém credenciais reais em texto puro — este documento **intencionalmente não reproduz os valores**, apenas os nomes das variáveis, para não vazar segredos para o histórico do repositório (o `docs/` NÃO está no `.gitignore`, logo qualquer coisa aqui É versionada).

Variáveis atualmente definidas:
| Variável | Finalidade |
|---|---|
| `DB_USER_CSG`, `DB_PASSWORD_CSG`, `DB_HOST_CSG`, `DB_PORT_CSG`, `DB_NAME_CSG` | Credenciais do MySQL remoto (produção/único banco atual) |
| `DB_QUERY_STRING` | String de query da URI SQLAlchemy (`charset=utf8mb4`) |
| `DB_ENGINE` | Seleciona engine (`mysql`/`mssql`) — ver observação na seção 4.1 sobre inconsistência atual |
| `SECRET_KEY` | Chave usada para assinar sessão Flask e tokens de reset de senha |
| `SMTP_SERVER`, `SMTP_PORT`, `MAIL_USE_TLS`, `MAIL_USE_SSL` | Configuração SMTP (Gmail) |
| `EMAIL_ADDRESS`, `EMAIL_PASSWORD` | Credencial da conta de e-mail (senha de app) usada para enviar e-mails transacionais |
| `MAIL_DEFAULT_SENDER` | Remetente padrão dos e-mails |

Variáveis suportadas pelo código mas **não presentes** no `.env` atual (usam default do `config.py`/`app.py` quando ausentes):
- `DB_USER_HMG`, `DB_PASSWORD_HMG`, `DB_HOST_HMG`, `DB_PORT_HMG`, `DB_NAME_HMG`, `DB_DRIVER`, `DB_ENCRYPT` — para o branch MSSQL (hoje inativo na prática).
- `SESSION_COOKIE_SECURE` (default `false` — cookie de sessão **não** marcado `Secure` a menos que essa var seja setada `true`; atenção se o site já roda em HTTPS em produção).
- `DB_POOL_RECYCLE`, `DB_POOL_TIMEOUT`, `DB_POOL_SIZE`, `DB_MAX_OVERFLOW`, `DB_CONNECT_TIMEOUT` — tuning de pool (usam defaults razoáveis).
- `ACTIVE_SESSIONS_COUNT_TTL_S`, `PROFILE_CACHE_TTL_S`, `ACTIVE_SESSION_CHECK_TTL_S`, `REQUEST_SLOW_MS` — tuning de cache/observabilidade no `app.py`.
- `AUTH_DEBUG_PRINTS` — liga logs extras de debug de autenticação no console (`app.py`, `auth_routes.py`).
- `NODE_EXE`, `NODE_MAX_OLD_SPACE_MB`, `NODE_OPTIONS` — usados por `worker.py` ao invocar os runners Node.

---

## 7. Processamento assíncrono / uploads pesados

- Uploads de planilhas (EMP, NOB, PED, FIP613, EST-EMP, PLAN20) são recebidos via rotas `/api/<modulo>/upload`, armazenados em `upload/<modulo>/tmp/`.
- Para **EMP e NOB**, o processamento pesado é delegado a um processo **Node.js separado** (`worker.py` chama `node node_runners/run.js` via `subprocess.run`), com `--max-old-space-size` configurável para lidar com arquivos grandes. O resultado processado é gravado em `outputs/<modulo>/`.
- Status de cada job fica em arquivos JSON (`outputs/status/<kind>_<upload_id>.json`), lidos/escritos por `services/job_status.py` — não usa fila/broker (Celery/RQ/etc.), é um mecanismo simples baseado em arquivo + polling do front-end, com suporte a cancelamento via arquivo `.cancel`.
- Demais módulos (FIP613, PED, PLAN20, EST-EMP, Teto SEDUC) são processados diretamente em Python/pandas dentro do próprio processo Flask (`services/*_runner.py`, `services/teto_seduc.py`), sem separação em worker — podem bloquear o worker WSGI durante o processamento se os arquivos forem grandes.
- **Volume de dados local:** as pastas `upload/` (≈985 MB) e `outputs/` (≈1.3 GB) já acumulam uma quantidade grande de arquivos `.xlsx` temporários e históricos (milhares de arquivos, visível em `upload/*/tmp/` e `outputs/*/tmp/`). Não há rotina de limpeza/retenção automática aparente — vale considerar um processo de expurgo, já que o disco tende a crescer indefinidamente. Essas pastas são git-ignoradas, então isso é um problema de operação local/servidor, não do repositório.

---

## 8. Deploy e ambientes

- **Hospedagem:** cPanel (indícios fortes: prefixo `proj5954_` no schema do banco e no e-mail de contexto, `.cpanel.yml` na raiz).
- **`.cpanel.yml`** (mecanismo nativo do cPanel Git Version Control):
  ```yaml
  deployment:
    tasks:
      - export DEPLOYPATH=/home/proj5954/projetoswebcsg_app/
      - /bin/cp -R * $DEPLOYPATH
  ```
  Isso significa: a cada push/deploy pelo painel cPanel, **todo o conteúdo do repositório é copiado** para `/home/proj5954/projetoswebcsg_app/` — um deploy simples de "cópia de arquivos", sem build step, sem instalação automática de dependências (`pip install`/`npm install` não estão no `.cpanel.yml`) e sem restart explícito do serviço de aplicação. Isso indica que a instalação de dependências Python/Node no servidor é feita manualmente ou por outro mecanismo fora deste repositório (ex.: interface "Setup Python App" do cPanel, que gerencia o `virtualenv` e o restart do Passenger separadamente).
- **`application = app`** em `app.py` — é o ponto de entrada WSGI padrão esperado pelo Passenger do cPanel (também compatível com IIS/wfastcgi, conforme comentário no código, embora não haja `web.config`/`wfastcgi` no repositório — provavelmente vestígio de uma tentativa de deploy alternativa).
- **Repositório remoto:** `https://github.com/CleberSGuedes/projetoswebcsg.git`, branch atual local `main` (limpo, sem alterações pendentes).
- **Branches identificadas** (locais e remotas): `main` (produção), `homologacao`/`remotes/origin/homologacao` (ambiente de homologação/staging), além de branches de feature (`dev/cleber`, `dev/jean`, `agent/licenca-gplv3-dev-cleber`, `feature/licenca-gplv3-homolog`, `jeancaf1-patch-1`). O histórico de commits recente confirma o fluxo (`"Configura deploy de homologacao"`, `"estabiliza deploy homologacao"`, `"selo homologação"`).
- **Ponto crítico confirmado pelo usuário:** a versão "online" (servidor de homologação/produção) e o ambiente de desenvolvimento local apontam para o **mesmo banco MySQL remoto** (`186.209.113.112`). Isso significa que:
  - Qualquer `db.create_all()`, script de migração manual (`db/*.sql`, `gerar_migracao_chaves.py`) ou teste local que grave dados **afeta diretamente o ambiente que está no ar**.
  - Não há, hoje, uma separação de schema/banco por ambiente (ex.: um `spo-dev` local vs. `spo-prod` remoto) — o único mecanismo de diferenciação seria o branch `homologacao` do próprio código-fonte, não do dado.
  - Isso deve ser tratado com cautela extra ao propor qualquer alteração de schema, migração de dados ou operação em massa.

---

## 9. Pontos de atenção / achados da análise (sem alterações feitas)

Estes são **apenas observações** — nada foi modificado no código ou no banco.

1. **Banco único compartilhado entre dev e produção/homologação** (confirmado pelo usuário) — maior risco operacional do projeto no estágio atual. Qualquer ação destrutiva local (DROP, DELETE em massa, `db.create_all()` mudando algo inesperado) afeta o ambiente real.
2. **Segredos em texto puro no `.env`** (senha de banco, `SECRET_KEY`, senha de app de e-mail) — padrão aceitável desde que o arquivo continue fora do controle de versão (confirmado no `.gitignore`) e com acesso restrito no servidor. Nunca deve ser copiado para `docs/` ou qualquer arquivo versionado.
3. **`DB_ENGINE=mssql` no `.env` não reflete o comportamento real** (o sistema roda em MySQL na prática, por ausência das variáveis `_HMG`) — fonte de confusão para quem configurar um novo ambiente ou tentar "ativar" o SQL Server sem entender a regra de fallback em `config.py`/`node_runners/db.js`.
   ✅ **Resolvido em 2026-08-31** (commit `3da226c`): suporte a SQL Server (`_HMG`/`DB_ENGINE`) removido de `config.py` por ser código morto (vestígio da arquitetura IIS/SQL Server anterior). O banco agora é sempre o MySQL configurado por `DB_*_CSG`, sem branch condicional.
4. **Lógica de resolução de engine duplicada** em Python (`config.py`) e JavaScript (`node_runners/db.js`) — mesma regra de negócio mantida em dois lugares, risco de dessincronização futura.
   ✅ **Resolvido do lado Python em 2026-08-31** (commit `3da226c`): `config.py` não tem mais lógica de engine nenhuma. `node_runners/db.js` continua com a lógica duplicada — decisão consciente, fica a cargo do trabalho de separação de ambientes da v2.
5. **Geração manual de PK (`MAX(id)+1`)** em `active_sessions` e `logs_login` (código legado para SQL Server 2008 sem IDENTITY) rodando hoje sobre MySQL, que já suporta `AUTO_INCREMENT` nativamente — potencial condição de corrida sob concorrência real, já que não há lock explícito nem transação serializável em torno do cálculo.
   ✅ **Resolvido em 2026-08-31** (commit `caf1701`): confirmado via `SHOW CREATE TABLE` que `logs_login`, `active_sessions` e `perfil` já tinham `AUTO_INCREMENT` nativo no banco (nenhuma DDL foi necessária). Removida a geração manual de ID (`_next_pk`/`_next_pk_active_session`, triplicada em `app.py`/`rotas/auth_routes.py`/`rotas/home_routes.py`) — os `INSERT`s agora deixam o MySQL gerar o `id`. Validado com inserção real de teste (linha gravada e removida em seguida).
6. **Sem migrations formais** (Alembic/Flask-Migrate) — schema evolui via `db.create_all()` automático (só cria tabelas novas, não altera colunas existentes) + scripts `.sql` soltos em `db/` aplicados manualmente + script standalone `gerar_migracao_chaves.py`. Não há histórico versionado e reproduzível de alterações de schema.
   ⏸️ **Adiado conscientemente** — ver seção 10 (Riscos adiados conscientemente) para o raciocínio completo.
7. **Arquivos monolíticos muito grandes:** `rotas/home_routes.py` (19.214 linhas) concentra praticamente todas as rotas HTML/API/partial do sistema; `templates/home.html` tem ~741 KB; `models/user.py` concentra ~65 classes de todas as entidades do sistema. Isso dificulta navegação, revisão de código e aumenta o risco de conflitos de merge entre desenvolvedores trabalhando em módulos diferentes.
   ⏸️ **Adiado conscientemente** — ver seção 10.
8. **Sem suíte de testes automatizada.** `requirements.txt` não inclui `pytest` (ou similar); o único artefato de teste (`scripts/test_teto_seduc_processors.py`) é um script manual com `assert`s, executado via `python scripts/...py`, sem integração a CI (não há pasta `.github/workflows` nem outro CI configurado no repositório).
   🟡 **Parcialmente resolvido em 2026-08-31** (commit `df4c784`): `pytest` adicionado (`pytest.ini` com `pythonpath = .`), script antigo migrado para `tests/test_teto_seduc_processors.py`, mais dois testes novos (`tests/test_config.py`, `tests/test_active_session_autoincrement.py`) travando contra regressão dos itens 3/4/5/12. Cobertura ainda é pequena (5 testes para ~183 rotas). CI real (GitHub Actions) fica **fora de escopo por decisão consciente** — os testes se conectam ao mesmo banco MySQL remoto compartilhado (não há banco de teste dedicado), então automatizar isso num pipeline exigiria colocar as credenciais de produção como secret de CI. Revisitar quando houver um banco de teste separado (v2).
9. **Sem proteção CSRF explícita** nas rotas que alteram estado via formulário/POST, e sem CORS configurado — coerente com uma aplicação same-origin, mas vale revisão se a API `/api/v1/...` ou `/api/` vier a ser consumida por outros domínios.
   🟡 **Reavaliado e parcialmente resolvido em 2026-08-31** (commit `df4c784`): investigação mostrou que `SESSION_COOKIE_SAMESITE=Lax` já mitiga a maior parte do risco de CSRF (bloqueia envio do cookie em `POST`/`PUT`/`DELETE` disparados por outro site); a única rota de mutação de estado que aceitava `GET` era `/logout`, corrigida para `methods=["POST"]`. CORS **não é risco** — a ausência de configuração é o comportamento seguro por padrão (navegador já bloqueia leitura cross-origin sem cabeçalho explícito). Retrofit completo de CSRF (`flask-wtf`, token em todo POST) continua fora de escopo — `static/js/main.js` sozinho tem 99 chamadas `fetch()`, fora o que existe embutido nos templates parciais; é um projeto à parte.
10. **`SESSION_COOKIE_SECURE` tem default `false`** — só fica `True` se a variável de ambiente homônima for setada explicitamente; se o servidor de produção já serve por HTTPS, vale confirmar se essa variável está de fato configurada lá (não está no `.env` local).
    ℹ️ **Confirmado em 2026-08-31**: produção/homologação é sempre HTTPS. Nenhuma mudança de código é necessária (já é opt-in via env var); ação recomendada é operacional — setar `SESSION_COOKIE_SECURE=true` no `.env` do(s) servidor(es), fora deste repositório. Não alterar o `.env` local (dev roda em HTTP puro).
11. **`app.run(..., debug=True)`** está hardcoded no bloco `if __name__ == "__main__":` do `app.py`. Isso só é relevante para quem rodar `python app.py` diretamente (em produção via Passenger/WSGI esse bloco não executa, pois o import usa `application`/`app` diretamente) — mas é um risco caso alguém suba o servidor de outra forma.
12. **Sem limite explícito de tamanho de upload** (`MAX_CONTENT_LENGTH` não configurado no Flask) — uploads de planilhas grandes dependem apenas dos limites default do stack (Flask/Werkzeug/servidor WSGI/cPanel), sem um teto de negócio definido no código.
    ✅ **Resolvido em 2026-08-31** (commit `df4c784`): `MAX_CONTENT_LENGTH = 150 MB` por padrão em `config.py`, configurável via `MAX_CONTENT_LENGTH_MB` sem mexer em código. Valor escolhido com margem confortável acima do maior upload real encontrado em disco na época (~35,8 MB, um NOB). Excesso já é tratado pelo `handle_exception` genérico existente em `app.py` (responde 413 como JSON em rotas `/api/...`).
13. **Volume de armazenamento local crescente sem rotina de limpeza aparente:** `upload/` (~985 MB) e `outputs/` (~1.3 GB) acumulam milhares de arquivos `.xlsx` temporários e de histórico (subpastas `tmp/`) sem expurgo automático visível no código.
    ℹ️ **Esclarecido em 2026-08-31**: existe sim um script de limpeza (`node_runners/cleanup_xlsx.js`, mantém os 2 arquivos mais recentes em cada `tmp/` e 1 nas demais pastas), mas nada no repositório o invoca (não está em nenhum `package.json`, `.cpanel.yml` ou código Python). Confirmado pelo usuário que esse script já está agendado via cron diretamente no cPanel do servidor (fora do repositório) — ou seja, não é código morto na prática, só não é visível/rastreável a partir do Git.
14. **Nomenclatura inconsistente** entre português/inglês para colunas de auditoria (`criado_em` vs `created_at`, `alterado_em` vs `updated_at`) espalhada pelas ~65 tabelas — não é um bug, mas exige atenção redobrada ao escrever queries/joins novos.
    ⏸️ **Adiado conscientemente (para sempre, nesse código)** — ver seção 10.
15. Comentário em `app.py` menciona compatibilidade com **IIS/wfastcgi**, mas não há `web.config` nem dependência `wfastcgi` no projeto — parece ser vestígio de uma tentativa de deploy alternativa (Windows/IIS) que não está mais em uso; o deploy real observado é via cPanel (`.cpanel.yml`).
    ✅ **Resolvido em 2026-08-31**: comentário corrigido para `# WSGI entrypoint para o Passenger do cPanel (Setup Python App)`.

---

## 10. Riscos adiados conscientemente (revisar no futuro)

Os três itens abaixo (6, 7 e 14 da lista acima) foram deliberadamente **não corrigidos** nesta rodada de ajustes. Em todos os três, a conclusão foi a mesma: corrigir agora é mais arriscado do que conviver com o problema, dado o estágio atual do projeto (sem suíte de testes ampla, banco de dados compartilhado com produção, e uma reescrita — v2 — já em andamento). Registrando aqui o raciocínio para não precisar refazer essa análise no futuro.

**Sem migrations formais (Alembic/Flask-Migrate)**
- Risco de **não** mexer: médio prazo — schema continua evoluindo sem histórico nem rollback, dependendo de alguém lembrar de documentar/aplicar mudanças manualmente.
- Risco de mexer **agora**: alto. Já confirmamos que o schema real diverge dos models em pelo menos um caso (o `AUTO_INCREMENT` do item 5, que o banco já tinha e o model não refletia) — ou seja, é provável que existam outras pequenas divergências acumuladas. Um `autogenerate` do Alembic comparando models × banco real poderia gerar `ALTER`s indesejados contra o banco compartilhado com produção. Também muda o processo de deploy (`.cpanel.yml` precisaria passar a rodar a migração).
- Quando revisitar: quando a v2 tiver um banco de ambiente próprio onde seja possível validar migrações com segurança antes de aplicar em produção.

**Arquivos monolíticos** (`rotas/home_routes.py` 19.214 linhas, `models/user.py` ~65 classes, `templates/home.html` ~741 KB)
- Risco de **não** mexer: baixo/médio — só manutenibilidade (mais difícil revisar código, mais chance de conflito de merge entre devs trabalhando em módulos diferentes).
- Risco de mexer **agora**: alto. Dividir um arquivo desse tamanho em blueprints/módulos menores é um refactor mecânico de alto raio de ação (~183 rotas no mesmo arquivo) — e a ferramenta que tornaria isso seguro de verificar (cobertura ampla de testes automatizados) ainda não existe: hoje são 5 testes cobrindo uma fração mínima do sistema (ver item 8). Um erro de import/registro de blueprint durante o split poderia quebrar alguma funcionalidade silenciosamente em produção.
- Quando revisitar: quando a cobertura de testes automatizados crescer o suficiente para validar um refactor desse porte com segurança — ou deixar para a arquitetura da v2, que parte do zero.

**Nomenclatura inconsistente PT/EN** (`criado_em` vs `created_at`, `alterado_em` vs `updated_at`)
- Risco de **não** mexer: mínimo — puramente cosmético/legibilidade, não afeta funcionamento.
- Risco de mexer **agora**: desproporcional ao ganho. Exigiria `ALTER TABLE ... RENAME COLUMN` em várias das ~65 tabelas do banco compartilhado com produção, mais caçar e atualizar toda referência a esses nomes em `models/user.py`, rotas, services, templates, `static/js/main.js` e possivelmente `node_runners/` (que acessa algumas tabelas via SQL direto) — tudo isso só para ganhar consistência de nome, sem nenhum benefício funcional.
- Recomendação: **não retrofitar isso no código/schema existente em nenhum momento futuro deste projeto** (o custo/risco nunca vai valer a pena aqui). Vale adotar como convenção só para tabelas/colunas **novas** daqui pra frente, o que não tem custo nem risco.

---

## 11. Convenções úteis para quem for mexer no código

- O ponto de entrada da aplicação Flask é sempre `app.py::create_app()` — tanto o `worker.py` quanto `tests/test_teto_seduc_processors.py` reutilizam essa factory (`from app import create_app`) para ter acesso a `db`/config dentro de um `app.app_context()`.
- Toda rota HTML "parcial" (carregada via AJAX no `home.html`) segue o padrão de URL `/partial/<módulo>/<ação>`, servida por `rotas/home_routes.py`, e a contraparte de dados fica em `/api/<módulo>/...`. Ao adicionar uma nova tela, normalmente é necessário: (1) uma entrada em `services/features.py` (menu + controle de permissão), (2) uma rota `/partial/...` retornando um template em `templates/partials/`, (3) uma ou mais rotas `/api/...` para os dados.
- Regras de acesso por papel usam `login_required`/`role_required` de `services/auth.py`, combinadas com o nível do perfil (`g.user_nivel`) carregado no `before_request` de `app.py`.
- Alterações de schema devem considerar que o banco é **compartilhado com o ambiente online** (seção 8) — qualquer novo `CREATE TABLE`/`ALTER TABLE` deveria, idealmente, ser adicionado tanto ao model SQLAlchemy quanto (se seguir o padrão dos módulos de upload existentes) a um script em `db/*.sql`, documentando a intenção antes de rodar contra o banco remoto.

---

## 12. Incidente investigado e corrigido: duplicação de dados no Teto-SEDUC (2026-08-31 a 2026-09-01)

Registro de uma investigação real, de ponta a ponta, motivada por um problema relatado pelo usuário (dashboard mostrando valor errado). Mantido aqui como referência de "como isso foi diagnosticado e corrigido", útil se um problema parecido aparecer em outro módulo.

### 12.1 O módulo, por baixo dos panos
- **Teto-SEDUC** (`atualizar/teto-seduc`) processa dois relatórios do FIPLAN: **Plan 23** (`processar_plan23`/`_persistir_plan23` em `services/teto_seduc.py`/`rotas/home_routes.py`, alimenta a tabela `momp`) e **Plan 134** (`processar_plan134`/`_persistir_plan134`, alimenta `politicateto`, vinculada a `momp` por `momp_id`).
- O painel **"Teto Orçamentário"** (`paineis-dashboards/teto-orcamentario`) lê tudo via `/api/paineis-dashboards/teto-orcamentario`, **sem filtro de exercício no servidor** — a rota devolve todo `momp`/`politicateto` ativo, e a filtragem é 100% client-side em `static/js/main.js` (função `initTetoOrcamentarioDashboard`).

### 12.2 Sintoma
Dashboard mostrando **R$ 13.070.486.258,00** para o exercício 2027 — exatamente o dobro do valor real (R$ 6.535.243.129,00), calculado de forma independente processando o arquivo FIPLAN original (`Plan23_14101_2027.xlsx`) pelo próprio parser do sistema.

### 12.3 Causa raiz 1 — condição de corrida em `_persistir_plan23`/`_persistir_plan134`
- Confirmado por consulta direta ao banco (só leitura): **82 linhas ativas** em `momp` para 2027 (deveria ser 41) — 41 pares idênticos, nenhum nunca desativado (`alterado_em` nulo em todas).
- Mecanismo: o código segue o padrão "`SELECT` acha antigos → `INSERT` do novo → `UPDATE` desativa os antigos", **sem nenhuma trava**, sob isolamento `REPEATABLE READ` (confirmado no servidor via `SHOW VARIABLES`). Se dois uploads do mesmo exercício se sobrepõem no tempo — não precisa ser no mesmo instante, só a primeira consulta do segundo acontecer antes do `commit()` final do primeiro — a transação do segundo fica cega para os dados do primeiro durante toda a sua duração, mesmo depois do commit dele.
- **Reproduzido de forma controlada** (duas sessões SQLAlchemy independentes, exercício fictício descartável, sem tocar dado real) para confirmar o mecanismo exato antes de corrigir.
- **Decisão de arquitetura (deixada a critério da IA pelo usuário):** manter o padrão atual — `_persistir_plan23`/`_persistir_plan134` rodam dentro de uma `threading.Thread` iniciada em `_start_teto_seduc_thread()`, a única funcionalidade do sistema que grava no banco a partir de thread em background dentro do próprio processo Flask (diferente de EMP/NOB, que rodam como processo Node separado via `worker.py`). **Não foi migrado** para o padrão de processo separado: a causa é uma corrida de **transações MySQL**, não de threads Python — dois processos concorrentes teriam exatamente o mesmo problema, cada um com sua própria conexão/transação. Migrar de arquitetura não resolveria nada aqui.
- **Fix:** lock nomeado do MySQL (`GET_LOCK`/`RELEASE_LOCK`) por exercício, serializando qualquer upload de Plan 23/Plan 134 para o mesmo exercício. Commit `20e1a89`. Validado com um teste que usa duas threads reais concorrentes (`tests/test_teto_seduc_lock.py`).

### 12.4 Causa raiz 2 — casamento de registros por texto exato, não por código estável
- Depois do fix acima, um reenvio do Plan 23 ainda deixou o total R$ 26.826.480,00 acima do correto. Causa: 4 registros órfãos (fonte gravada como `"15460000"`, texto cru sem descrição) sobraram da duplicação original — gravados **antes** de `FONTE_MAP` ganhar a entrada dessa fonte (ver 12.5). O reenvio gerou a mesma fonte já com nome completo, e como o casamento antigo×novo é por **texto exato** de `fonte`/`grupo_despesa`/`subteto_despesa_momp`, os registros antigos nunca foram reconhecidos como "a mesma fonte".
- Risco mais amplo identificado: qualquer correção futura de texto em `FONTE_MAP`, `GRUPO_PLAN23_MAP` ou `SUBTETO_PLAN23_MAP` reproduziria o mesmo problema — órfãos silenciosos, sem erro, só o total do dashboard errado até alguém notar.
- **Fix:** novos `grupo_key()`/`subteto_key()` em `services/teto_seduc.py` (ao lado do `fonte_key()` que já existia e já era usado como fallback parcial em `_persistir_plan134`), extraindo o código estável (dígito/letra) antes do "` - `". `_persistir_plan23` passou a pré-carregar os registros ativos do exercício (1 consulta em vez de uma por linha, ganho de brinde) e casar pela chave estável; `_persistir_plan134` ganhou a mesma normalização no seu fallback. Commit `56f4b74`. Teste dedicado reproduzindo o cenário exato de texto antigo × novo (`tests/test_teto_seduc_key_normalization.py`).
- Dado real de 2027 corrigido com um reenvio do arquivo já com o fix em produção — confirmado pelo usuário e validado no banco: 41 registros ativos, R$ 6.535.243.129,00.

### 12.5 Achado lateral: fonte 15460000 sem descrição
Durante a análise do arquivo `Plan23_14101_2027.xlsx` (fora do repositório, em `C:\workspace\Planilhas\`), identificada a fonte `15460000` sem entrada em `FONTE_MAP`. Nome oficial confirmado com o usuário ("Transferências do FUNDEB - Complementação da União - ETI") e adicionado. Commit `bfe13b2`. Foi essa mudança que expôs a causa raiz 2 (registros antigos gravados com o texto "cru").

### 12.6 Dashboard "Teto Orçamentário" — os dois grupos de filtros
- Os filtros do painel se dividem em dois grupos de **dados**, não de visualização: nível MOMP (`exercicio`, `fonte`, `grupo`, `subgrupo` — Plan 23) e nível política orçamentária (`regiao`, `subfuncao`, `paoe`, `adj`, `macropolitica`, `pilar`, `eixo`, `politica` — Plan 134/`politicateto`). Ao usar qualquer filtro do segundo grupo, o dashboard troca a base de KPIs/gráficos/tabelas de `momp` para o cruzamento `momp × politicateto` — **silenciosamente**, sem aviso destacado. Se o Plan 134 não foi carregado para o exercício (situação comum — só há registro de uso em testes locais antigos), a tela zera sem explicação clara, facilmente confundido com "os dados sumiram de novo".
  - **Fix:** aviso visível (`alert alert-info`, mesmo padrão já usado em `partials/dashboard.html` e nas telas de login) quando isso acontece, sem mudar nenhum cálculo/filtro existente. Commit `e6d1971`.
- **Atenção — atributo `data-graph-filter`** (`templates/partials/paineis_teto_orcamentario.html` + `static/css/style.css:1961`): controla, **via CSS, não via JS**, quais dos 12 filtros ficam visíveis na aba "Gráficos" (só 5: Exercício, Grupo de despesa, Tipificação, Ação/PAOE, Fonte) vs. "Tabelas" (todos). Comentários explicativos adicionados nos dois arquivos (commit `8233513`) depois de esse atributo ter sido **removido por engano** numa análise que só checou o JS — corrigido assim que os filtros da aba Gráficos sumiram na tela real (commits `4ac76ae` → `e383254`; ver lição de processo em 12.7).

### 12.7 Lição de processo (vale para qualquer investigação futura, não só este módulo)
Ao avaliar se um atributo/seletor HTML está "morto": **sempre checar CSS além de JS, em comandos separados**. Encadear buscas com `&&` é uma armadilha — se a primeira não achar nada, ela sai com código 1 e o `&&` **nunca chega a rodar a segunda**, sem nenhum aviso óbvio no output. Prefira `;` entre buscas independentes, ou rode cada uma isoladamente e confirme o resultado de cada uma antes de concluir "não está em uso em lugar nenhum".

### 12.8 Melhoria: filtros de múltipla seleção (checkbox)
Pedido do usuário, não um bug: os 12 filtros do painel eram `<select>` de valor único — não dava pra, por exemplo, ver duas fontes de recurso ao mesmo tempo.
- **Reaproveitado** o componente de dropdown com checkbox que o app já tinha pronto (`.planning-action-checklist`, usado em "Estrutura do Planejamento" para múltiplas Ações/PAOE — `static/css/style.css:2217-2278`, `static/js/main.js` ~4111-4318/4798-4814/6447-6469) em vez de criar um componente novo do zero.
- `templates/partials/paineis_teto_orcamentario.html`: os 12 campos passaram a ser gerados por um `{% for %}` do Jinja (lista de tuplas chave/rótulo/rótulo-vazio/`is_graph_filter`) em vez de 12 blocos HTML copiados à mão — reduz risco de erro de copiar-e-colar entre eles (o tipo de erro que já aconteceu no item 12.6/12.7 com o `data-graph-filter`).
- `static/js/main.js` (`initTetoOrcamentarioDashboard`): `state.filters[key]` passou de `string` para `array`; filtragem trocou igualdade por inclusão em lista (`matchesFilter`, lógica OR dentro do filtro / AND entre filtros); clique num gráfico agora **adiciona/remove** (toggle) da seleção em vez de substituir (decisão tomada com o usuário); chips de filtro ativo viraram um por **valor** selecionado, clicáveis para remover individualmente; cada dropdown ganhou atalhos "Selecionar todos"/"Limpar".
- `isPoliticalMode()` e o aviso de Plan 134 não carregado (item 12.6) continuam funcionando sem mudança de lógica — só o tipo do valor lido de `state.filters` mudou.
- Commit `63b104b`. Validado com dry-run em Node (9 cenários cobrindo OR/AND entre filtros e o aviso de Plan 134), `pytest` completo, render real do template Jinja conferindo os 12 campos e o `data-graph-filter` nos lugares certos, e teste manual no navegador feito e aprovado pelo usuário antes do commit.

### 12.9 Causa raiz 3 — combinação que desaparece do arquivo fica órfã ativa
No dia seguinte (2026-09-02), um reenvio **legítimo** do Plan 23 (UO 14101 — o nome do arquivo dizia "14601", mas o cabeçalho interno da planilha confirma 14101) voltou a divergir: banco com 41 ativos (R$ 6.545.243.129,00), arquivo novo com 40 (R$ 6.535.243.129,00, batendo com o total da própria planilha).
- Causa: o registro `id=362` (fonte `15000000`, grupo `3 - Outras Despesas Corrente`, subteto `C - Prioridades Estratégicas LDO`, R$ 10.000.000,00) existia no banco mas **não aparece em nenhuma linha do arquivo de hoje** — essa combinação foi removida/reclassificada no FIPLAN entre um upload e outro (o total geral da UO se manteve porque o valor foi redistribuído em outra linha). `_persistir_plan23` só desativa um registro antigo quando uma linha do arquivo **novo** casa com a mesma chave — se a combinação simplesmente deixa de existir no arquivo, nada aciona essa desativação, e o registro fica ativo para sempre até alguém notar a divergência manualmente (foi o que aconteceu).
- Corrigido manualmente pelo usuário via phpMyAdmin (`UPDATE momp SET ativo=0 ... WHERE id=362`) enquanto o fix de código era preparado — a IA tentou rodar esse `UPDATE` via ferramenta, mas foi bloqueada pelo classificador de modo automático (escrita direta em produção); o usuário rodou manualmente.
- **Fix:** ao final do loop de `_persistir_plan23`, o que sobra em `ativos_por_chave` (índice dos registros que já estavam ativos no banco antes do upload) são exatamente as combinações ausentes do arquivo atual — desativa esses também (e os `PoliticaTeto` vinculados). Novo contador `removidos`, refletido na mensagem de status ("removidos por não constarem mais no arquivo: N") para nunca mais ficar silencioso. Detalhe de implementação: `ativos_por_chave` é **repopulada** a cada linha processada (pra sustentar a autocorreção de chave repetida dentro do mesmo arquivo) — iterar nela direto no final teria desativado também os registros recém-inseridos corretamente nesta mesma carga; a correção usa duas estruturas separadas (uma só com o estado original do banco, nunca repopulada; outra só para chave repetida dentro do próprio arquivo). Commit `a93890f`.
- `_persistir_plan134` não precisou do mesmo ajuste: quando um MOMP inteiro some do Plan 23, o fix acima já desativa também os `PoliticaTeto` vinculados a ele — coberto transitivamente, desde que o Plan 23 seja carregado antes do Plan 134 (fluxo já exigido pelo código).
- Validado com teste dedicado (caso de controle: registro de **outro exercício** não pode ser tocado) e, na sequência, **ponta a ponta na tela real**: o usuário reenviou o Plan23 de 2026-08-31 (41 registros) e depois o de 2026-09-02 (40) — a mensagem de status mostrou "removidos por não constarem mais no arquivo: 1" e o dashboard voltou a bater sozinho, sem correção manual.

### 12.10 Redesenho visual da aba "Tabelas" (2026-09-02)
Pedido do usuário, não um bug: a aba "Tabelas" do dashboard tinha tabelas HTML simples e o card QOMP limitava a comparação a 3 exercícios fixos. Refeita em várias etapas, cada uma testada manualmente e aprovada antes de seguir para a próxima.

- **"Por grupo de despesa"** (`renderGroupSummary`, `static/js/main.js`): legenda + barra empilhada + cards de resumo por grupo, reaproveitando `groupColors` (mesma cor de cada grupo no app inteiro — gráfico de pizza, este card e o ranking de grupo).
- **"Teto por fonte de recurso"** e **"Teto por grupo e tipificação da despesa"** (`renderFonteRank`, `renderGrupoRank`): as tabelas de duas colunas viraram ranking de barra proporcional em coluna única (classes `.teto-rank-*`, `static/css/style.css`), ordenado do maior pro menor, com linha de Total com a barra sempre cheia (`.teto-rank-fill-total`) — antes parecia vazia por estar só ligeiramente maior que as demais. Tipificação aparece indentada dentro do grupo, mesma cor do grupo-pai com opacidade reduzida (`opacity: 0.55`), reforçando a hierarquia sem precisar de paleta nova.
- **QOMP** (`renderQomp`) — regra antiga (`.slice(0, 3)`) trocada por `unique(base.map(row => row.exercicio))` sem corte: mostra exatamente os exercícios presentes no filtro atual (todos, se nenhum exercício for selecionado). Tabela HTML (`<table id="teto-table-qomp">`) trocada por uma grade CSS (`.teto-qomp-grid`/`.teto-qomp-row`, div-based, igual ao padrão dos cards de ranking) com as colunas Fonte (só código) e Grupo/Tipificação fixas (`position: sticky`) ao rolar horizontalmente. Cada célula de ano mostra valor, barra proporcional (escala por linha, não pelo total geral) e um chip de variação ▲/▼ vs. o exercício anterior **mostrado** (não necessariamente o ano anterior civil, se houver lacuna no filtro).
  - Colunas de ano usam `minmax(150px, 1fr)`: com poucos exercícios elas esticam pra preencher o card sem sobra vazia; com muitos, o mínimo de 150px força a grade a ultrapassar a largura do card e a rolagem horizontal (`.teto-qomp-scroll`) abre sozinha — corrigido depois de o usuário notar que a largura fixa original deixava espaço morto com poucos anos.
- **Novo card "Quadro comparativo de Fonte/Grupo"** (`renderFonteGrupo`), inserido **antes** do QOMP na aba: mesmo estilo e mesma regra de exercícios do QOMP, mas parando no nível de Grupo de despesa — sem a linha de Tipificação, pra quem só precisa comparar fonte × grupo sem o detalhe todo.
- **Zebra striping por bloco de fonte** nos dois cards de exercício (Fonte/Grupo e QOMP): todas as linhas de uma mesma fonte (grupo, e no QOMP também as tipificações filhas) recebem o mesmo fundo, alternando a cada fonte (classe `fonte-alt`, calculada por índice par/ímpar em `groupSum(base, "fonte").forEach`) — facilita ver onde uma fonte termina e a próxima começa em tabelas longas. Reaproveita o token `--bg` já existente (mesmo tom sutil usado nas trilhas de barra) em vez de criar uma cor nova.
- Antes de desenhar qualquer coisa, o layout novo foi combinado com o usuário via **mockup em Artifact** (fora do código real) usando as cores/fontes reais do app — inclusive uma rodada de correções do usuário sobre o mockup (coluna Fonte só com código, manter a coluna Grupo/Tipificação, larguras) antes de qualquer linha de código real ser escrita.
- Só muda template/CSS/JS (view) — nenhuma mudança de backend, endpoint ou modelo; os dados continuam vindo do mesmo endpoint `/api/paineis-dashboards/teto-orcamentario`. Filtros do dashboard não foram tocados em nenhuma etapa (checado por `git diff | grep -ci "teto-multi-filter\|checklist\|data-filter="` = 0 a cada mudança).
- Commit `94f4d2c`. Validado com `pytest` completo, `node --check`, dry-runs em Node com dados sintéticos por card/regra nova (contagem de anos = exatamente o filtro, ordenação por total, soma de tipificação dentro do grupo), e teste manual na tela aprovado pelo usuário a cada etapa.

### 12.11 Mais dois cards de exercício e correção de texto "Outras Despesas Correntes" (2026-09-02)
Sequência de pedidos do usuário, sem bug envolvido — dois cards novos na mesma família visual da 12.10 e uma correção de digitação.

- **"Grupo por Teto Total"**: mesmo estilo/regra do "Quadro comparativo de Fonte/Grupo" (12.10), mas sem a coluna Fonte — soma **todas** as fontes por grupo de despesa. A lógica de renderização (Grupo × exercício, sem Fonte) foi extraída da primeira implementação para um helper único, `renderGrupoOnlyCard(elId, base)`, já pensando em reaproveitar para o card seguinte em vez de duplicar o bloco inteiro de novo.
- **"Grupo 1 por Fontes"**, posicionado **antes** do anterior: mesmas colunas (Grupo × exercício), mas com um filtro de fonte **fixo no código** — `GRUPO1_FONTES_FIXAS = ["15000000", "15001001", "15000100", "15400000", "15401070"]` — independente do que estiver marcado no filtro "Fonte" do dashboard (os demais filtros, como Exercício, continuam se aplicando normalmente: o card chama `filteredData({ ...state.filters, fonte: [] })` e depois filtra o resultado pela lista fixa). Restrito também ao grupo "1 - Pessoal e Encargos Sociais" (`codeOf(row.grupo) === "1"`), por pedido do usuário depois de ver o card já implementado.
  - Como esse card sempre tem um único grupo, a linha "Total Geral" ficaria idêntica à única linha de dado — `renderGrupoOnlyCard` só desenha a linha de total quando há **mais de um** grupo (`groups.length > 1`), então o card "Grupo por Teto Total" (normalmente 3 grupos) continua mostrando o total normalmente.
- **Correção de texto "Outras Despesas Corrente" → "Outras Despesas Correntes"** (faltava o S — erro de digitação no mapeamento de importação, não no FIPLAN): como o mesmo rótulo de grupo aparece em vários cards e no gráfico de pizza, a correção foi feita num único helper de exibição, `displayGrupo()` (`static/js/main.js`), aplicado em todo lugar que **mostra** o texto do grupo — nunca no valor usado para filtrar/casar linhas (`clean(row.grupo)` continua cru), pra não quebrar o agrupamento/drill-down. Também corrigido na origem, `GRUPO_PLAN23_MAP`/`GRUPO_PLAN134_MAP` (`services/teto_seduc.py`), para uploads futuros do Plan23/Plan134 já gravarem o texto certo no banco — dados já persistidos continuam corrigidos só na exibição até serem reprocessados. Não alterado no filtro "Grupo de despesa" do dashboard (fora do pedido, e o texto do filtro em si não faz parte da área tocada nesta sessão).
- Só muda template/CSS/JS (view) + o mapeamento de importação (`services/teto_seduc.py`) — nenhuma mudança de endpoint ou de modelo. Filtros do dashboard não foram tocados (checado por `git diff | grep -ci "teto-multi-filter\|checklist\|data-filter="` = 0 a cada mudança).
- Commit `9c0c40e`. Validado com `pytest` completo, `node --check`, dry-runs em Node com dados sintéticos (filtro fixo de fonte ignorando o filtro do dashboard mas respeitando os demais, grupo único sem duplicar por fonte, `displayGrupo()` idempotente pra texto já corrigido), e teste manual na tela aprovado pelo usuário a cada etapa — inclusive dois ajustes finos pedidos depois de ver a tela (ordem dos cards, remoção da linha de total redundante).

### 12.12 Código de fonte fixo errado no "Grupo 1 por Fontes" + reposição/destaque visual (2026-09-02)
O usuário notou que "Grupo 1 por Fontes" e "Grupo por Teto Total" mostravam valores diferentes de 2025/2026 pro mesmo grupo "1 - Pessoal e Encargos Sociais" (só batiam em 2027) e pediu pra investigar antes de qualquer mudança de código.
- **Causa:** `GRUPO1_FONTES_FIXAS` (12.11) tinha `"15000100"`, copiado literalmente do que o usuário digitou no pedido original — esse código **não existe em nenhum registro** da tabela `momp`. Consulta direta ao banco (script descartável, não versionado, rodado só pra diagnóstico) mostrou que a fonte real usada pelo grupo 1 em 2025/2026 é `15010100` ("Outros Recursos não vinculados destinados ao Tesouro") — dígitos trocados em relação ao que foi digitado. Em 2027 essa fonte simplesmente não é usada (o grupo 1 usa `15000000` naquele ano), por isso os dois cards batiam só ali, por coincidência.
- **Fix:** `GRUPO1_FONTES_FIXAS` corrigida para `["15000000", "15001001", "15010100", "15400000", "15401070"]` (`static/js/main.js`) e a legenda do card (`templates/partials/paineis_teto_orcamentario.html`) atualizada com o código certo. Confirmado por consulta direta ao banco, antes de aplicar: somando só essas 5 fontes bate exatamente com a soma de todas as fontes do grupo 1, nos três exercícios (2025, 2026, 2027) — a decisão de qual código usar foi confirmada com o usuário via pergunta direta antes de alterar, já que é uma regra de negócio hardcoded, não um bug óbvio de código.
- **Reposição:** os dois cards passaram a ficar logo abaixo de "Por grupo de despesa", no topo da aba Tabelas (antes ficavam perto do QOMP, no fim).
- **Destaque visual:** cabeçalho dos dois cards com a classe `.teto-panel-highlight` (`static/css/style.css`) — depois de duas tentativas: primeiro um tom 2 níveis mais escuro na rampa (`--color-700` em rgba baixa) ainda ficou apagado; depois um fundo sólido em `--accent` (o mesmo padrão usado em `.wizard-step-btn.active` pra estado "ativo" no app) ficou forte demais e visualmente igual às barras/botões já existentes, competindo com eles; o ajuste final é um meio-termo — `rgba(15,124,104,0.30)` claro/dark — mais forte que o tom padrão dos outros cabeçalhos (0.12) mas sem ser sólido.
- Commit `b5d3a29`. Filtros do dashboard não foram tocados (checado por `git diff | grep -ci "teto-multi-filter\|checklist\|data-filter="` = 0). Validado com `pytest` completo, `node --check`, e a divergência de valores foi confirmada e depois refeita por consulta direta ao banco antes/depois da correção do código de fonte. Teste manual na tela aprovado pelo usuário a cada ajuste (posição, e as duas rodadas do tom de destaque).

## 13. Incidente investigado e corrigido: menu de login renderizando quase vazio em instabilidade de conexão com o banco (2026-09-03)

### 13.1 Sintoma
"De vez em quando", ao logar, o usuário via só o menu "Início"/"Sair" — o resto do menu (Cadastrar, Planejamento, Usuários, Painel-Permissões etc.) simplesmente não aparecia, sem nenhum erro visível na tela (o "Erro" vermelho num print enviado pelo usuário era o indicador de sync da conta do Chrome, fora do sistema — não relacionado).

### 13.2 Investigação
Analisado o log de produção do dia (sem alteração de código nessa etapa). Achados:
- O banco remoto compartilhado (`186.209.113.112`) tem quedas de conexão transitórias durante consultas — confirmado literalmente no log: `(pymysql.err.OperationalError) (2013, 'Lost connection to MySQL server during query')`.
- Erros como `Could not locate column in row for column 'perfil_permissoes.feature'` (tipo `NoSuchColumnError`) são **sintoma dessa mesma instabilidade**, não coluna realmente ausente — a coluna existe e a consulta funciona normalmente na tentativa seguinte (confirmado pelo padrão no log: `"DB scalar fetch retry 1/2 failed: ..."` seguido, milissegundos depois, de sucesso).
- `_fetch_scalars_all_with_retry` (`rotas/home_routes.py`) já tinha lógica de retry (`DB_RETRY_ATTEMPTS=2` por padrão), mas quando as **duas** tentativas falhavam, devolvia `[]` (lista vazia) **silenciosamente**, sem levantar exceção.
- `_load_permissoes_perfil`/`_load_permissoes_nivel` (que carregam as permissões do usuário logo no login, via `_permissoes_with_parents`, chamada pela rota `/`) usam esse `[]` diretamente — e um `[]` aí é **indistinguível** de "usuário sem nenhuma permissão". O menu no `base.html` é renderizado por completo no HTML e depois escondido/mostrado via JS (`applyMenuPermissions()`) conforme essa lista; com lista vazia, só os itens sempre-visíveis (`dashboard`/`logout`, hardcoded em `main.js`) ficam de pé — exatamente o sintoma do print.
- `pool_pre_ping`/`pool_recycle` (`config.py`) já estavam configurados corretamente — não é o problema clássico de conexão obsoleta reciclada pelo pool; a queda acontece **no meio de uma query em andamento**, algo que `pool_pre_ping` não previne (ele só testa a conexão antes de emprestá-la do pool, não durante o uso). Ou seja, a causa de fundo é instabilidade de rede/hospedagem do host remoto, fora do nosso controle direto.

### 13.3 Fix
Depois de confirmar a causa com o usuário e alinhar a abordagem (aumentar tentativas de retry **e** corrigir o fallback silencioso, não só uma das duas):
- `DB_RETRY_ATTEMPTS` (2→4) e `DB_RETRY_BACKOFF` (0.2→0.35) como novos valores-padrão nos três helpers de retry (`_execute_with_retry`, `_fetch_all_with_retry`, `_fetch_scalars_all_with_retry`) — dá mais chance de recuperação automática antes de desistir.
- `_fetch_scalars_all_with_retry` ganhou o parâmetro `raise_on_failure` (padrão `False` — preserva o comportamento histórico dos ~15 outros pontos que já chamam essa função no carregamento do dashboard e tratam `[]` como "sem dados", um estado normal ali). `_load_permissoes_perfil`/`_load_permissoes_nivel` passaram a usar `raise_on_failure=True` e a só engolir `ProgrammingError` (tabela/coluna genuinamente ausente, ex.: banco novo sem migração) — qualquer outra falha (conexão perdida etc.) agora propaga em vez de virar `[]` silencioso.
- A rota `/` (`index()`) passou a capturar essa falha, logar um aviso explícito (`"Falha ao carregar permissoes no login"`) e renderizar um banner (`templates/base.html`, condicional a `permissoes_indisponiveis`) pedindo pra atualizar a página — em vez do menu quebrado sem nenhuma explicação.
- O endpoint `/api/permissoes/current` (chamado pelo JS logo após o carregamento inicial, pra reconfirmar as permissões) **já tinha** tratamento correto pra esse tipo de erro (`except SQLAlchemyError` → 503, e o JS (`fetchCurrentPermissions()`) já ignora respostas não-OK sem sobrescrever o menu) — só nunca disparava porque a falha era engolida numa camada mais interna antes de chegar lá. Nenhuma mudança necessária nesse endpoint.
- Commit `cf20d62`.

### 13.4 Testes
Como é um bug intermitente e raro (a instabilidade de rede não é reproduzível sob demanda), a validação não foi por clique na tela — foi por revisão de código e um novo arquivo de testes automatizados, `tests/test_permission_retry_failure.py` (5 casos, sem tocar no banco real — simula a falha via `monkeypatch` em `db.session.execute`):
- `_fetch_scalars_all_with_retry` com `raise_on_failure=False` (padrão) continua devolvendo `[]` após esgotar as tentativas — não quebra os ~15 call sites existentes do dashboard.
- Com `raise_on_failure=True`, propaga a exceção depois de esgotar as tentativas.
- `_load_permissoes_perfil`/`_load_permissoes_nivel` propagam uma falha de conexão persistente (`OperationalError`).
- `_load_permissoes_perfil` continua tratando tabela/coluna realmente ausente (`ProgrammingError`) como "sem permissões" — não regride esse caso.
- Suite completa: 16/16 (`pytest`).

## 14. Achado (ainda não corrigido): filtro de UO do Plan20 quebra silenciosamente com o novo layout do relatório de 2027 (2026-09-08)

### 14.1 Contexto
Pedido do usuário: analisar `services/plan20_runner.py` (módulo "Atualizar → Planejamento → PLAN20 - SEDUC") e rodar arquivos reais de teste, **sem alterar código e sem gravar no banco**, pra entender a funcionalidade e validar se um relatório mais recente (planejamento 2027) — que a empresa fornecedora aparentemente reformatou — ainda processa corretamente. `run_plan20()` não faz nenhuma escrita em banco (só gera o `.xlsx` de saída), então testar com ele é seguro; a gravação em `plan20_seduc` só acontece na rota `/api/plan20/upload`.

Testados 3 arquivos (`C:\workspace\Planilhas\`, fora do repositório):
- `Plan 20 - 2026_12-09-2025=.xlsx` (2026, arquivo correto — o primeiro teste, com outro arquivo, tinha sido engano do usuário): 7.215 linhas brutas → 871 linhas em `Extrair_dados`/`Plan20_SEDUC`. Chave de Planejamento, Programa, Ação e Subação sem nenhuma lacuna (0 "-" em 871 linhas); zero linhas com flag de "Região divergente"; zero duplicatas; soma do Valor Total = R$ 5.801.058.678,00, muito próxima do teto MOMP 2026 já validado nos cards do Teto Orçamentário (R$ 5.812.610.192,00, ~0,2% de diferença) — bom sinal cruzado de que os valores extraídos batem com outra fonte independente.
- `Plan20 2027 para teste.xlsx`: processou **sem lançar exceção**, mas `Plan20_SEDUC` saiu com **0 linhas** (`Extrair_dados` tinha 1.393 linhas válidas — a falha é só no filtro final, não no parsing).

### 14.2 Causa raiz confirmada
`run_plan20()` (`services/plan20_runner.py`, por volta da linha 1543-1548) filtra as linhas que vão pra `Plan20_SEDUC` com comparação de string **exata**:
```python
mask_uo = df_tmp["Unidade Orçamentária"].astype(str).str.strip() == "14.101 - SECRETARIA DE ESTADO DE EDUCAÇÃO"
```
No relatório de 2027, o campo "Unidade Orçamentária" vem como `"14101 - SECRETARIA DE ESTADO DE EDUCAÇÃO"` — **sem o ponto** entre "14" e "101" (mudança de formatação na origem, confirmada comparando o dump bruto dos dois arquivos linha a linha). Comparação exata bate zero vezes → as 1.393 linhas válidas somem silenciosamente da aba final e, por extensão, não iriam para o banco.

Confirmado isolando o filtro fora do código real: com o ponto → 0/1393 passam; sem o ponto (formato real do arquivo 2027) → 1393/1393 passam.

**Por que é perigoso:** o detector de layout que roda na tela de upload (`_detect_upload_layouts`, `rotas/home_routes.py`) não usa a Unidade Orçamentária como critério — só tokens como "Exercício", "Programa", "Ação (P/A/OE)" etc., que não mudaram. Ou seja, esse arquivo passaria pela validação da tela normalmente, o usuário veria "Plan20 processado com sucesso" e **nenhuma linha nova cairia no banco**, sem nenhum aviso.

### 14.3 Outra mudança de layout encontrada (não quebra nada, mas vale registrar)
O relatório de 2027 ganhou um bloco novo antes de cada Programa (Eixo, Objetivo Estratégico, Programa, Público Alvo, Tipo, UO Responsável, seção de Objetivo de Desenvolvimento Sustentável) que não existia em 2026. Ele **não** corrompe a extração porque:
- Nenhum desses rótulos novos bate com os regex de `KEYS` (Programa/Ação/Produto/etc.) além do próprio "Programa:", que reaparece de verdade logo depois, com Função/Unidade Orçamentária/Ação/Subfunção/Esfera/Responsável na mesma ordem de 2026.
- O mecanismo de `linha_vazia()` (uma linha em branco reresulta em `c_pend_indices`/`c_pend_ativo` limpos) descarta esse bloco novo pendente antes que ele seja fechado por um "Ação (P/A/OE):" — como há uma linha em branco separando o bloco novo do bloco real, o parser esquece o que coletou e recomeça do zero no "Programa:" real.
- Confirmado nos 3 arquivos de teste: **zero campos vazios** em Programa/Função/Unidade Orçamentária/Ação/Subfunção/Objetivo Específico/Esfera/Responsável nas 1.393 linhas de 2027 — a extração desses campos continua 100% correta.

Isso funciona "por sorte" da convenção de linha em branco entre os blocos — é um comportamento correto hoje, mas **frágil**: se um relatório futuro remover essa linha em branco, o bloco novo pode ser interpretado como se fosse o bloco de dados real, corrompendo o Programa/Função/UO/Ação/Subfunção/Esfera/Responsável de qualquer C-block seguinte que dependa da contagem posicional.

### 14.4 Ainda não corrigido (nesta seção) — corrigido na seção 15
Combinado com o usuário: documentar agora, corrigir depois. Nenhuma alteração de código feita até este ponto — as investigações rodaram via scripts descartáveis fora do controle de versão (`_plan20_test_run*.py`, `_plan20_compare.py`), sempre apagados depois de cada rodada. A correção completa (incluindo mais achados que apareceram numa investigação mais profunda) está na seção 15.

## 15. Correção do parser do Plan20 pro layout 2027 (2026-09-08 a 2026-09-10)

### 15.1 Escopo ficou maior do que a seção 14 sugeria
O usuário testou o parser por conta própria com o arquivo real e achou problemas em blocos que a seção 14 tinha dado como não afetados. Investigando mais a fundo (ainda sem alterar código), apareceram mais dois bugs reais, além do filtro de UO (14.1):

- **Tabela "ODS | Código Meta | Metas" por Ação quebrada.** Entre "Produto(s) da Ação:" e "PLANO DE AÇÃO POR PRODUTO", o layout 2027 insere uma tabela nova (cabeçalho + 0, 1 ou várias linhas de Código Meta/Metas do ODS vinculado à ação). Como nenhuma linha dela batia com os regex existentes, caía no fallback posicional do parser e virava um "Produto" fantasma (`Produto(s) da Ação = "Código Meta"`, demais campos vazios). Em algumas ações a tabela tem dados reais — confirmado no arquivo de teste: um caso com 9 linhas de metas do ODS 4 (Educação de qualidade).
- **Bloco de capa do 2º Programa em diante contamina o Programa anterior, não é só descartado.** Rastreando a transição entre dois Programas no arquivo real (a partir da 2ª ocorrência de "Eixo:"), ficou claro que `linha_vazia()` (o reset de linha em branco) só limpa Etapa/Item (H/I) — nunca Produto/Subação/Programa (D/E/F/G/C). Como o bloco de capa novo (Eixo/Objetivo Estratégico/Programa/Público Alvo/Tipo/UO Responsável) não bate com nenhum regex reconhecido, suas linhas grudavam como filhas espúrias da última Subação/Produto do Programa **anterior**, em vez de serem descartadas — só não apareceu no 1º Programa do arquivo porque não havia nada anterior pra contaminar.

**Causa raiz única:** qualquer linha que o parser não reconhece cai num fallback que "gruda no ID mais recente que sobrou", e o reset por linha em branco é raso demais pro layout novo, que tem seções sem marcador reconhecido espalhadas pelo arquivo inteiro.

### 15.2 Decisão de escopo (confirmada com o usuário)
Diferente da ideia inicial ("ignorar os blocos novos com segurança"), o usuário pediu que **nenhuma informação fique de fora** — os blocos novos (Eixo, Objetivo Estratégico, Público Alvo, Tipo, UO Responsável, ODS/Código Meta/Metas) viram colunas de saída de verdade, não só ruído descartado.

Uma pista útil: o script já tinha uma regra pra "ação padronizada" (`"Produto exclusivo para ação padronizada"`, no tratamento do bloco de Produto/PLANO DE AÇÃO POR PRODUTO) — texto padrão explicando a ausência de produto específico em ações administrativas genéricas (ex.: Programa "036 - Apoio administrativo"). Cruzando as 28 tabelas ODS do arquivo de teste com o texto de "PLANO DE AÇÃO POR PRODUTO" de cada ação, confirmou-se a correlação: **toda ação com "Produto exclusivo para ação padronizada" tem a tabela ODS vazia; toda ação com produto específico (finalística) tem a tabela ODS preenchida.** Não foi preciso nenhuma lógica nova pra distinguir "padronizada" de "finalística" — o próprio reconhecimento do cabeçalho ODS + coleta das linhas seguintes já reproduz esse comportamento sozinho. O que foi reaproveitado da regra existente foi só a **convenção de texto padrão explicando a ausência** (`defaults_text`): as colunas de ODS vazias recebem `"Ação padronizada - sem ODS vinculado"` em vez de um "-" seco.

### 15.3 Implementação (`services/plan20_runner.py`)
- **`_uo_key()`** (novo helper): extrai só os dígitos do código da UO antes do " - ", ignorando pontuação. `mask_uo` no `run_plan20()` passou a comparar `"14101"` contra `"14101"` em vez do texto completo — tolera tanto "14.101" (2026) quanto "14101" (2027, e qualquer outra pontuação futura).
- **Bloco de capa por Programa (K)**: `"Eixo:"` virou um marcador reconhecido próprio (`KEYS["Eixo"]`) — ao ser detectado, **reseta a hierarquia inteira C→I** (não só H/I, como o reset de linha em branco) porque é o sinal mais confiável de que um Programa novo está começando de verdade. Enquanto o estado `k_ativo` estiver ligado, toda linha não-vazia é absorvida num novo identificador `K{n}` (ex.: `A1.B1.K3`) em vez de cair no fallback; a 2ª ocorrência de "Programa:" (a real, seguida de "Função:") desliga `k_ativo` e segue o fluxo normal de sempre.
- **Tabela ODS por Ação (L)**: `"ODS"` (cabeçalho) virou outro marcador reconhecido, escopado dentro da Ação atual (`L_id = f"{C_id}.L{n}"`) — coleta 0/1/várias linhas de dado até a próxima linha em branco (que agora também reseta esse estado, além do que já resetava antes).
- **Bloco C por rótulo de texto, não mais por posição**: `extrair_dados()` passou a casar cada linha do bloco C (Programa/Função/Unidade Orçamentária/Ação/Subfunção/Objetivo Específico/Esfera/Responsável) pelo próprio rótulo (`col_1`, via regex), em vez de "a 2ª linha depois de Programa é Função, a 3ª é UO...". Linha sem rótulo reconhecido é ignorada em vez de embaralhar as seguintes.
- **Colunas novas em `EXTR_HEADERS`**: `Eixo`, `Objetivo Estratégico`, `Público Alvo`, `Tipo`, `UO Responsável` (nível Programa) e `ODS`, `Código Meta (ODS)`, `Metas (ODS)` (nível Ação, múltiplas entradas concatenadas com `" * "`, mesmo padrão já usado em Público Transversal/Código/Município). Ficam com `"-"` (capa) ou `"Ação padronizada - sem ODS vinculado"` (ODS) quando vazias — inclusive em arquivos do layout antigo (2026), que não têm nenhum desses blocos.
- **Bug pego só depois de conferir dados reais, não nos testes de campo vazio/preenchido**: a primeira versão da capa por Programa usava "A.B" (arquivo.aba) como chave de junção — como só existe 1 aba no arquivo inteiro, isso colapsava a capa de **todos** os Programas numa só, sempre sobrescrita pela última processada (Eixo/Objetivo Estratégico/Tipo idênticos em todos os Programas da saída). Corrigido casando pelo **código do Programa** (ex.: "036"), extraído da própria linha "Programa:" dentro do bloco de capa. Um bug secundário junto: quando o valor de um campo da capa vinha vazio (ex.: "Público Alvo:" sem nada depois), o código caía pro texto do rótulo ("Público Alvo:") como valor — corrigido pra só preencher quando há valor de verdade, deixando o default `"-"` cuidar do resto.

### 15.4 Validação
- Os 3 arquivos reais (`C:\workspace\Planilhas\`, fora do repositório) reprocessados sem gravar no banco:
  - **2026**: 871 linhas (idêntico ao baseline da seção 14), soma do Valor Total idêntica (R$ 5.801.058.678,00), todas as colunas novas com o default esperado (arquivo não tem nenhum bloco novo).
  - **2027**: 1.386 linhas em `Plan20_SEDUC` (era 0 antes da correção), zero campos vazios em nenhuma coluna (capa + ODS incluídos), zero linhas com o "Produto" fantasma da tabela ODS, zero contaminação entre Programas (checado em `Subação/entrega`, `Detalhamento do produto`, `Região da Subação`, `Produto(s) da Ação`), soma do Valor Total R$ 6.485.243.129,00. Eixo/Objetivo Estratégico/Público Alvo/Tipo variam corretamente por Programa (ex.: "036 - Apoio administrativo" → Eixo "08 - Programas e ações padronizados", Tipo "Gestão de Políticas Públicas"; "533 - Educação 10 Anos" → Eixo "01 - Social", Tipo "Finalístico"). ODS preenchido só nos Programas finalísticos (533/534, "Educação de qualidade"), default nos administrativos (036/996/997/998) — confere exatamente com a correlação achada na seção 15.2.
- **`tests/test_plan20_runner.py`** (novo, 3 testes, não toca no banco — monta planilhas `.xlsx` sintéticas em diretório temporário e roda `run_plan20()` de verdade):
  - 2 Programas, UO sem ponto (formato 2027): confere que o filtro de UO não zera a saída, que a capa de cada Programa não vaza pro outro, que Público Alvo vazio vira `"-"` (não o texto do rótulo), que a tabela ODS vazia/preenchida não vaza entre Programas, e que a última Subação/Detalhamento do Programa 1 não foi contaminada pela capa do Programa 2.
  - UO com ponto (formato 2026): confirma que o formato antigo continua funcionando depois do filtro ficar tolerante.
  - Layout 2026 sem nenhum bloco de capa: regressão — Eixo/Objetivo Estratégico/Público Alvo ficam `"-"`, ODS fica no texto padrão, resto do processamento idêntico.
- Suite completa: 19/19 (`pytest`) — os 16 testes já existentes continuam passando.

### 15.5 Nada gravado no banco durante a investigação/implementação
Combinado com o usuário: implementar sem gravar nada em `plan20_seduc` (a rota `/api/plan20/upload`, único ponto que grava no banco, não foi tocada nem chamada em nenhuma etapa das seções 14 a 17 — todos os testes rodaram `run_plan20()` direto, que só gera o `.xlsx` de saída). Commit final na seção 17.3.

## 16. Dois achados conferindo o arquivo de saída manualmente, antes do commit (2026-09-10)

Pedido do usuário antes de aprovar o commit da seção 15: gerar o `.xlsx` de saída de verdade e conferir linha por linha. Dois problemas apareceram — um deles achado pelo usuário, o outro só depois de eu investigar a fundo por que o primeiro acontecia.

### 16.1 Produtos sem Subação vinculada (não são linhas em branco do relatório)
O usuário notou, olhando o arquivo gerado, várias linhas com `Subação/entrega`, `Responsável`, `Prazo`, `Unid. Gestora` etc. vazias (`-`), e imaginou que fossem linhas em branco do relatório sendo trazidas por engano. Investigando: **não são** — é um mecanismo que já existia no script antes desta sessão (`if produtos: for p in produtos: if not p.get("_usado"): ...`): quando um Produto listado em "Produto(s) da Ação:" nunca é escolhido por nenhuma Subação, ele vira uma linha própria, com os campos de Subação em branco. Isso sempre existiu, só nunca tinha aparecido no 2027 porque o filtro de UO (seção 14) zerava a saída inteira antes de chegar até aqui. No 2026 esse número é **zero**; no 2027, antes do fix da seção 16.2, eram **166 linhas** — volume alto demais pra ser só "produto genuinamente sem subação", o que levou à investigação do item seguinte.

### 16.2 Causa raiz real: "PLANO DE AÇÃO POR PRODUTO" mudou de coluna no layout 2027
Rastreando por que tantas Subações ficavam sem Produto (ou com o Produto errado — Meta/Saldo trocados, o segundo ponto que o usuário reportou), a causa apareceu comparando o dado bruto linha a linha:

- **2026:** a linha vem como `col_1 = "PLANO DE AÇÃO POR PRODUTO"` (rótulo sozinho) e o **produto vem numa coluna separada, `col_5`** (ex.: `col_5 = "Alimentação escolar mantida"`).
- **2027:** a linha vem como `col_1 = "PLANO DE AÇÃO POR PRODUTO: Avaliação (Avalia MT) desenvolvida"` — **rótulo e valor na mesma célula**, com `col_5` sempre vazia.

O código só lia `col_5` (`services/plan20_runner.py`, extração de `produto_por_fid`) — no 2027, isso significa que o valor real **nunca** era lido, e `produto_por_fid.get(fid, "Produto exclusivo para ação padronizada")` caía sempre no texto padrão, mesmo pra Ações com produto específico de verdade. Sem esse vínculo, o casamento Produto↔Subação (cascata `d_escolhido` em `extrair_dados()`) perdia o filtro mais preciso (por nome do produto) e caía nos fallbacks mais fracos (por região, depois "o primeiro Produto da lista") — daí a mesma Subação aparecer sempre casada com o primeiro Produto ("Acesso e permanência desenvolvido", nas imagens que o usuário mandou), com a Meta/Saldo *daquele* Produto, errados pra ela.

**Confirmado que é problema de layout, não bug introduzido nesta sessão:** a lógica de casamento em si (`extrair_dados()`, cascata `d_escolhido`) não foi tocada na seção 15 — só ficou visível agora que o filtro de UO parou de zerar a saída do 2027.

**Fix:** `produto_por_fid` passou a tentar `col_5` primeiro (compatibilidade com 2026) e, se vazia, extrair o valor de dentro de `col_1` depois dos dois-pontos (layout 2027) — `services/plan20_runner.py`, item "4.1) Produto do F".

**Validação (arquivos reais, sem gravar no banco):**
- 2026: sem mudança (871 linhas, mesma soma de Valor Total, zero linhas com Subação vazia).
- 2027: linhas com Subação vazia caíram de 166 para **58** (concentradas em Ações de infraestrutura, plausivelmente produtos mesmo sem Subação própria — não mais um sintoma de casamento errado). Total de linhas caiu de 1.386 para **1.278** (menos linhas redundantes/mal casadas). Conferido especificamente a Ação "2936 - Desenvolvimento das Modalidades de Ensino" das imagens do usuário: "Acesso e permanência desenvolvido" (o Produto que aparecia repetido incorretamente) passou de dominar dezenas de linhas pra aparecer **1 vez só** — a distribuição de Produtos por Subação ficou plausível (16 Produtos usados, sem nenhum monopolizando por fallback).
- Novo teste em `tests/test_plan20_runner.py` (4º teste): monta uma Ação sintética com 2 Produtos e 2 grupos "PLANO DE AÇÃO POR PRODUTO" no formato inline (2027), cada um com sua própria Subação, e confere que cada Subação casa com o Produto (e a Meta/Saldo) que o relatório vincula a ela — não as duas caindo no mesmo.
- Suite completa: 20/20 (`pytest`).

### 16.3 Saldo Meta do Produto "errado" — investigado, não era bug
Usuário reportou, na Ação 2957, "Acesso e permanência desenvolvido" e "Bem-estar escolar desenvolvido" saindo com Saldo = 3.44 (igual à Meta) quando "o relatório" mostraria 0.0, e levantou a hipótese de o ponto decimal do Saldo (vs. vírgula da Meta) estar causando erro de leitura.

Conferido direto na célula bruta do `.xlsx` original via `openpyxl` (bypassando o pandas, sem cache de fórmula nem formatação envolvida): a célula já traz literalmente `"3.44"` como texto — o script só copia esse texto, não faz nenhuma conversão numérica em Meta/Saldo (então o "." em vez de "," não causa erro nenhum, é só uma inconsistência do próprio relatório de origem). O valor na saída bate exatamente com o valor bruto do relatório, e a Subação vinculada a cada um tem a chave de planejamento citando "ACESSO_E_PERM"/"BEM-ESTAR_ESCOLAR" respectivamente — confirmando que o casamento Produto↔Subação está correto nesse caso. Hipótese mais provável: o usuário comparou com a tabela "Total por Produto" (um resumo agregado logo abaixo da tabela detalhada por região, mesmos nomes de produto, mas **sem nenhuma coluna de Saldo** — correta e propositalmente ignorada pelo parser, ver seção 15.3) em vez da tabela de detalhe por região (a fonte certa, com Saldo = 3.44). Nenhuma mudança de código feita aqui.

### 16.4 Produtos sem Subação vinculada removidos da saída (por pedido do usuário)
Confirmado que as linhas de Produto sem nenhuma Subação vinculada sempre têm **Valor Total = 0** (Etapa/Fonte/Descrição do Item também vazios) — não carregam nenhuma informação orçamentária, só um metadado de "esse Produto existe no planejamento mas nenhuma Subação/entrega foi vinculada a ele". Usuário confirmou (`AskUserQuestion`) que podem ser descartadas.

**Fix:** `extrair_dados()` deixou de incluir no resultado final as linhas com `_gid is None` (nem "usadas" por uma Subação, nem parte de uma Ação sem Subação nenhuma) — `services/plan20_runner.py`, logo antes da montagem de `extr_df`.

**Validação:**
- 2026: sem mudança (já eram 0 linhas desse tipo).
- 2027: linhas com Subação vazia foram de 58 para **0**; total de linhas caiu de 1.278 para **1.220**; soma do Valor Total **idêntica** (R$ 6.485.243.129,00) — confirma que nenhum valor orçamentário foi perdido.
- Novo teste em `tests/test_plan20_runner.py` (5º teste): Ação sintética com 2 Produtos, só 1 com Subação vinculada — confere que só a linha vinculada aparece na saída.
- Suite completa: 21/21 (`pytest`).

Arquivo de saída conferido pelo usuário, sem mais inconsistências encontradas nessa rodada — inclusive testado um arquivo novo baixado já com a Meta do Produto corrigida na origem (ver seção 17.1).

## 17. UO 14601 (FMTE) e teste com relatório recém-baixado (2026-09-10)

### 17.1 Reteste com relatório 2027 já corrigido na origem
Usuário baixou um novo Plan20 2027 (`Plan20_2027 - 2026-09-10.xlsx`) já com a Meta do Produto corrigida no FIPLAN e pediu pra reprocessar. Resultado: 1.211 linhas em `Plan20_SEDUC`, mesma soma de Valor Total (R$ 6.485.243.129,00), zero linhas com Subação vazia, zero nulos. Conferido especificamente o caso da Ação 2957 ("Acesso e permanência desenvolvido"/"Bem-estar escolar desenvolvido", seção 16.3): agora vem com Saldo = 0.0 nesse arquivo novo — confirma que era mesmo dado da origem (já corrigido lá), não bug do parser.

### 17.2 UO 14601 (FMTE) não passava pelo filtro
A SEDUC tem uma segunda Unidade Orçamentária própria — usuário pediu pra testar um relatório dela (`Plan20_2027_uo14601_10-09-2026.xlsx`, UO **14601 - FUNDO EST DE APOIO À MELHORIA DAS CONDIÇ. DE OFERTA DA EDUC INFANT., ENS. FUNDAM. E ENS. MÉDIO NO MT**, vinculado à SEDUC mas com código de UO diferente).

Rodando sem alterar nada: o parser extraiu os dados **perfeitamente** (`Extrair_dados`: 9 linhas, Programa "544 - Mato Grosso Mais Educação", 3 Ações, R$ 50.000.000,00) — mas `Plan20_SEDUC` saiu **vazio**. Diferente dos bugs anteriores, essa não é uma quebra de layout: o filtro de UO (mesmo já tolerante a formatação, seção 14) comparava contra um único código fixo, `"14101"` — a 14601 nunca esteve na lista de UOs aceitas, por desenho original do módulo (pensado só pra UO única da SEDUC).

**Decisão do usuário:** aceitar uma lista fixa de UOs, não remover o filtro nem deixar só 14101.

**Fix:** nova constante `UOS_ACEITAS = {"14101", "14601"}` (`services/plan20_runner.py`, topo do arquivo) - `mask_uo` passou de `== "14101"` para `.isin(UOS_ACEITAS)`. Uma UO nova da secretaria no futuro precisa ser adicionada nessa lista.

**Validação:**
- Reprocessados os 3 arquivos reais: 2026 (UO 14101) e o 2027 novo (UO 14101) sem nenhuma mudança; o arquivo da UO 14601 passou a gerar as 9 linhas corretas em `Plan20_SEDUC` (R$ 50.000.000,00).
- 2 novos testes em `tests/test_plan20_runner.py` (6º e 7º): UO 14601 é aceita; uma UO fora da lista (não cadastrada) continua corretamente de fora.
- Suite completa: 23/23 (`pytest`).

### 17.3 Commit
Usuário conferiu os arquivos de saída (2026, os dois 2027 de UO 14101, e o de UO 14601) sem encontrar mais inconsistências e aprovou. Commit `edc74cb` (`services/plan20_runner.py` + `tests/test_plan20_runner.py`). Nenhuma gravação em `plan20_seduc`/banco em nenhum momento das seções 14 a 17 — só o filtro de layout, o parsing e os testes foram exercitados; o upload real (rota `/api/plan20/upload`) segue sem ser usado nesta investigação, então o próximo upload de verdade vai ser o primeiro teste "de ponta a ponta" com gravação no banco.

## 18. Rota de upload do Plan20 (`/api/plan20/upload`) — análise e correção, antes do primeiro upload real (2026-09-11)

Pedido do usuário, com o parser já corrigido (seções 14-17): analisar se a rota que grava o arquivo de saída em `plan20_seduc` está pronta pra receber o layout 2027 e as duas UOs (14101 e 14601), **sem mexer em código nem gravar no banco** até apresentar o achado.

### 18.1 Achado crítico: `col_map` corrompido (mojibake) desde 17/04/2026, nunca exercitado desde então
Analisando `api_plan20_upload()` (`rotas/home_routes.py`) e o schema real do banco (só leitura): o dicionário `col_map` (traduz nome-da-coluna-do-relatório → nome-da-coluna-no-banco) tinha as chaves acentuadas com **mojibake** (duplo-encoding UTF-8, ex.: `"FunÃƒÂ§ÃƒÂ£o"` em vez de `"Função"`) — confirmado que não é exibição, são os bytes reais do arquivo (`repr()` da chave via AST direto no arquivo-fonte). `git blame` mostrou que foi introduzido pelo próprio usuário em **17/04/2026**; `plan20_uploads` mostrou que o **último upload real foi em 16/01/2026** — três meses antes da corrupção. Ou seja: esse caminho de código não rodava desde então, então ninguém percebeu.

Simulando a rota exatamente como estava (rename + keep_cols) contra um arquivo de saída real: de 53 colunas de dado, **24 saíam totalmente `NULL`** — incluindo `exercicio` e `unidade_orcamentaria`. Como a desativação da versão anterior depende exatamente dessas duas colunas (`WHERE unidade_orcamentaria = :uo AND exercicio = :ex`), elas saindo `NULL` faz o conjunto de combinações a desativar ficar **vazio** — ou seja, nem os dados novos ficariam completos, nem os antigos seriam desativados. Tudo isso sem nenhum erro reportado ao usuário ("Plan20 processado com sucesso").

### 18.2 Achado próprio: colisão de nome "Eixo"
A coluna "Eixo" que adicionei na seção 15 (nível Programa) tinha o mesmo nome de uma coluna que já existia (derivada da Chave de Planejamento — Região/Subfunção+UG/ADJ/Macropolitica/Pilar/**Eixo**/Política/PúblicoTransversal). O arquivo de saída ficava com duas colunas "Eixo" (Excel aceita sem reclamar); se o `col_map` fosse corrigido sem resolver isso, a coluna `eixo` do banco receberia o valor errado (o novo, por nível de Programa, sobrescrevendo o histórico).

### 18.3 Fix
- **`services/plan20_runner.py`**: a coluna nova renomeada pra **"Eixo do Programa"** (em `EXTR_HEADERS`, no bloco de capa do Programa, em `cols_to_clean`/`defaults_text` e no dicionário `capa`) — a "Eixo" original (Chave de Planejamento) não foi tocada.
- **`rotas/home_routes.py`**: `col_map` reescrito do zero, com acentuação correta, cobrindo as ~53 colunas que já funcionavam **e** as 8 novas (`Eixo do Programa`, `Objetivo Estratégico`, `Público Alvo`, `Tipo`, `UO Responsável`, `ODS`, `Código Meta (ODS)`, `Metas (ODS)`) — apontando pra colunas novas no banco (`eixo_programa`, `objetivo_estrategico`, `publico_alvo`, `tipo`, `uo_responsavel`, `ods`, `codigo_meta_ods`, `metas_ods`), ainda não criadas (ver 18.4).
- Lógica de renomear/completar/converter numérico extraída da rota pra uma função pura, **`_montar_dataframe_plan20_seduc(df_out, data_arquivo, user_email)`**, e o cálculo das combinações a desativar pra **`_combos_uo_exercicio_plan20(df_out)`** — dá pra testar sem simular upload HTTP inteiro, e evita que o mesmo tipo de erro (nome de coluna errado) volte a passar batido.
- **Desativação da versão anterior**: trocada de igualdade de texto exato (`unidade_orcamentaria = :uo`) pra comparar só o código da UO, igual ao filtro do parser (seção 14) — `WHERE REPLACE(SUBSTRING_INDEX(unidade_orcamentaria, ' - ', 1), '.', '') = :uo_codigo AND exercicio = :ex`. Preventivo: o texto da UO já mudou de formato uma vez (2026→2027); sem essa mudança, reenviar o mesmo exercício/UO num formato de texto novo deixaria a versão anterior ativa pra sempre, duplicando dado.

### 18.4 Pendente: `ALTER TABLE` (ação do usuário)
`plan20_seduc` não tem model ORM nem migração (schema criado ad-hoc) — as 8 colunas novas precisam ser criadas manualmente, mesmo padrão de quando o modo automático bloqueou um `UPDATE` direto em produção (usuário roda, IA confere depois):
```sql
ALTER TABLE plan20_seduc
  ADD COLUMN eixo_programa VARCHAR(255),
  ADD COLUMN objetivo_estrategico TEXT,
  ADD COLUMN publico_alvo VARCHAR(255),
  ADD COLUMN tipo VARCHAR(255),
  ADD COLUMN uo_responsavel VARCHAR(255),
  ADD COLUMN ods TEXT,
  ADD COLUMN codigo_meta_ods TEXT,
  ADD COLUMN metas_ods TEXT;
```

### 18.5 Testes
`tests/test_plan20_upload_mapping.py` (novo, 4 testes, sem tocar no banco — só chama as funções puras extraídas):
- `col_map` cobre 100% das colunas do `EXTR_HEADERS` (se o parser ganhar campo novo sem o map ser atualizado, esse teste quebra).
- Uma linha sintética com um valor distinto em **cada** coluna real do Plan20_SEDUC (via as chaves do `col_map`, que também inclui as colunas derivadas de Chave de Planejamento/Natureza, adicionadas fora do `EXTR_HEADERS`) sobrevive ao casamento de nome → nenhuma sai `None` — é o teste que teria pego o bug do mojibake antes de ir pra produção.
- Conversão numérica pt-BR (`1.234,56` → `1234.56`) continua correta.
- `_combos_uo_exercicio_plan20` extrai corretamente os pares (UO, exercício) de um DataFrame com duas UOs diferentes.

Suite completa: 27/27 (`pytest`).

### 18.6 Primeira tentativa de upload real (antes do `ALTER TABLE`) — confirma o fix do `col_map` funcionando
Usuário tentou o upload pela tela antes do `ALTER TABLE` (arquivo real, UO 14601). Deu erro — mas um erro **esperado e informativo**: `(1054, "Unknown column 'eixo_programa' in 'INSERT INTO'")`. O ponto importante: no `[parameters: ...]` do erro, **todos os campos vieram preenchidos com valores reais** (`'eixo_programa': '01 - Social'`, `'objetivo_estrategico': 'Ampliar e melhorar o acesso...'`, `'ods': 'Educação de qualidade'`, etc.) — nenhum `None`. Isso confirma, com um upload real, que o `col_map` reescrito (18.3) resolveu o problema da seção 18.1: o mapeamento de nomes está correto, só faltava a coluna existir no banco. Como a rota faz `rollback()` em caso de erro, nada ficou gravado (nem parcialmente).

### 18.7 `ALTER TABLE` executado
Usuário esperava que isso já estivesse pronto (tinha dado acesso ao banco justamente pra não precisar de passos manuais) — na primeira tentativa a IA rodou o `ALTER TABLE` direto (mesma conexão do `.env`), mas foi bloqueado pelo classificador de modo automático ("Modify Shared Resources", mesma categoria do `UPDATE` direto bloqueado na seção 12.9). Diferente daquela vez, o usuário autorizou explicitamente ali na conversa ("eu te dou permissão") e, refeita a mesma chamada, o classificador liberou dessa vez. `ALTER TABLE` executado com sucesso pela própria IA (não precisou de phpMyAdmin):
```sql
ALTER TABLE plan20_seduc
  ADD COLUMN eixo_programa VARCHAR(255),
  ADD COLUMN objetivo_estrategico TEXT,
  ADD COLUMN publico_alvo VARCHAR(255),
  ADD COLUMN tipo VARCHAR(255),
  ADD COLUMN uo_responsavel VARCHAR(255),
  ADD COLUMN ods TEXT,
  ADD COLUMN codigo_meta_ods TEXT,
  ADD COLUMN metas_ods TEXT;
```
Conferido depois (schema, só leitura): as 8 colunas existem, tabela com 68 colunas no total.

### 18.8 Commit
Usuário pediu pra documentar e commitar depois de confirmar o schema. Commit `ae2c7eb` (`rotas/home_routes.py`, `services/plan20_runner.py`, `tests/test_plan20_runner.py`, `tests/test_plan20_upload_mapping.py`).

## 19. Relatório "Plan20 - SEDUC" (tela + download) — unificação das 4 fontes de colunas e as 8 colunas novas (2026-09-11)

Com o parser e o upload já migrados (seções 15-18), faltava a última etapa: a tela "Relatórios → Plan20 - SEDUC" e o download em Excel ainda não conheciam as 8 colunas novas do layout 2027. Pedido do usuário: analisar como a funcionalidade estava hoje pra montar um plano de ajuste.

### 19.1 Achado: 4 listas de colunas independentes, sem fonte comum
Analisando o código (sem mexer em nada), a tela era sustentada por **4 listas hardcoded e totalmente independentes entre si**:
1. `api_relatorio_plan20()` (`rotas/home_routes.py`) — um `SELECT` de 53 colunas + um dict Python montado campo a campo.
2. `api_relatorio_plan20_download()` — um **segundo** `SELECT` idêntico, escrito de forma independente, + uma lista `headers` de 53 pares (rótulo, chave) — inclusive com alguns rótulos escritos via escape unicode (`"Valor Unitário"`) em vez de caractere literal, e pequenas divergências de acentuação com o que a tela mostrava (ex.: "Macropolitica" sem acento no download vs. "Macropolítica" com acento na tela).
3. `templates/partials/relatorios_plan20.html` — 53 `<th>` de cabeçalho + 53 `<th data-col="...">` da linha de filtro, escritos à mão.
4. `static/js/main.js::initRelatorioPlan20()` — o array `colKeys` (53 chaves) + um template `<tr>` com 53 `<td>` fixos, na mesma ordem.

Nada impedia essas 4 listas de divergirem entre si silenciosamente — exatamente o tipo de estrutura que já tinha causado o incidente do `col_map` corrompido do upload (seção 18). O usuário perguntou explicitamente a vantagem/risco de unificar tudo antes de aprovar: vantagem é eliminar essa classe de bug de vez; risco principal identificado foi no lado do frontend (reescrever `<thead>`/render pra ser dirigido por dados muda o comportamento de "cabeçalho aparece antes do fetch terminar" para "cabeçalho só aparece depois da resposta chegar"). Usuário pediu unificação completa, incluindo frontend.

### 19.2 Fix: fonte única de colunas, ponta a ponta
**Backend (`rotas/home_routes.py`)**:
- Nova constante `PLAN20_RELATORIO_COLUNAS` — lista de 61 tuplas `(rótulo, coluna_no_banco, tipo)`, `tipo` em `"text"`/`"num"` (Quantidade/Valor Unitário/Valor Total)/`"int"` (Exercício), logo depois de `_plan20_seduc_col_map()`. As 8 colunas novas entram na ordem de exibição já combinada com o usuário: `Eixo do Programa`/`Objetivo Estratégico`/`Público Alvo`/`Tipo` depois de `Programa`; `UO Responsável` depois de `Unidade Orçamentária`; `ODS`/`Código Meta (ODS)`/`Metas (ODS)` depois de `Responsável pela Ação`.
- `_plan20_relatorio_rows()` — helper único que monta o `SELECT` a partir dessa lista; usado pelas duas rotas (antes, cada uma tinha o seu).
- `api_relatorio_plan20()` monta a resposta iterando a lista e agora devolve também `"columns": [{"key", "label", "numeric"}, ...]`.
- `api_relatorio_plan20_download()` monta `headers` e a formatação numérica/inteira do Excel (`numeric_cols`/`int_cols`) a partir da mesma lista, em vez de comparar strings de rótulo.

**Frontend**:
- `relatorios_plan20.html`: `<thead>` virou dois `<tr>` vazios com id fixo (`plan20-header-row`, `plan20-filter-row`).
- `main.js::initRelatorioPlan20()`: novo `initColumnsUI(cols)`, chamado dentro de `load()` assim que `data.columns` chega — monta `colKeys`, preenche as duas linhas do `<thead>`, e só então inicializa `filters`/`filterContainers`/`filterControls` (que passaram de `const` pra `let`, reatribuídos nesse passo). O `render()` trocou o template fixo de `<td>` por `columns.map(...)`, aplicando `class="num"` + formatação só quando `column.numeric`. Esse padrão (colunas descritas pelo backend, tabela montada pelo JS a partir delas) já existia no projeto em `initRelatorioEstruturaPlanejamento()` — não foi inventado do zero. Aproveitado pra também escapar o texto de cada célula (`esc()`, mesmo padrão daquela função) — o template antigo não escapava nada.
- Confirmado antes de mexer: `static/css/style.css` só tem `.plan20-table th/td/td.num` — nenhuma regra por posição de coluna (`nth-child`) que a geração dinâmica do cabeçalho pudesse quebrar.

### 19.3 Testes
Novo teste em `tests/test_plan20_upload_mapping.py` (`test_relatorio_colunas_bate_com_col_map`): compara o conjunto de colunas de `PLAN20_RELATORIO_COLUNAS` com o conjunto de valores de `_plan20_seduc_col_map()` — precisa ser exatamente igual; se o relatório e o upload saírem de sincronia de novo (coluna gravada mas não exibida, ou exibida mas não gravada), esse teste quebra. Também confere que não há rótulo nem coluna de banco duplicados. Suite completa: 28/28 (`pytest`).

### 19.4 Validação
- Dry-run direto contra o banco real (só leitura, sem passar pela rota HTTP): `PLAN20_RELATORIO_COLUNAS` e `_plan20_seduc_col_map()` batem em 61 colunas cada; as 8 colunas novas vêm corretamente `NULL` nas 1.902 linhas ativas de 2025/2026 (anteriores à migração, esperado) e corretamente preenchidas nas 1.220 linhas ativas de 2027 (UO 14101 e 14601), sem mojibake.
- Dry-run da geração do Excel (mesma lógica da rota, fora do Flask): 61 colunas no arquivo gerado, as 8 novas nas posições esperadas (12-15, 18, 24-26), formatação numérica/inteira ainda nas colunas certas (59-61 numéricas, 1 inteira).
- Usuário testou manualmente a tela e o download (arquivo real) e confirmou que funcionou.

### 19.5 Commit
Usuário testou tela e download e aprovou. Commit `7f9861c` (`rotas/home_routes.py`, `static/js/main.js`, `templates/partials/relatorios_plan20.html`, `tests/test_plan20_upload_mapping.py`).

## 20. Teto-SEDUC: Plan 134 "com valor errado", UO por arquivo (14101 e 14601) e mensagens de conferência (2026-09-29)

### 20.1 Sintoma e diagnóstico
O usuário carregou o Plan 134 de 2027 (`Plan 134 - 14101 - 2027 29-09-2026.xlsx`, fora do repositório, em `C:\workspace\Planilhas\`): o relatório somava **R$ 6.485.317.811,00**, mas o dashboard mostrava **R$ 6.535.243.129,00**. Análise só de leitura (script descartável + consultas ao banco), sem alterar código:
- **O parser estava certo**: `processar_plan134()` sobre o arquivo somava exatamente R$ 6.485.317.811,00.
- **O valor da tela era o teto do Plan 23, não o PTA do Plan 134**: sem filtro de política o KPI soma `momp` (`renderKpis` em `static/js/main.js`; só passa a somar `politicateto` com algum filtro de Região/PAOE/Pilar etc. marcado — ver 12.6). R$ 6.535.243.129,00 era a soma dos registros ativos de `momp` 2027, de um Plan 23 desatualizado.
- **O upload do Plan 134 descartou R$ 30.341.313,00 quase calado**: 12 linhas caíam em 4 combinações fonte/grupo/tipificação sem registro correspondente no Plan 23 (ex.: 15740000/3/B, R$ 13.900.000,00) e foram puladas. A única pista era "Linhas sem correspondência no MOMP: 61", número que misturava essas 12 com **49 linhas de valor 0** (subação sem item de despesa, fonte "-") e não dizia quais combinações nem quanto dinheiro.
- Resolvido operacionalmente pelo usuário: baixou um Plan 23 atualizado (o teto tinha sido redistribuído no FIPLAN) e reenviou o mesmo Plan 134. Resultado: `momp` 2027 = 30 registros / R$ 6.485.317.811,00 e `politicateto` ativos = 334 / R$ 6.485.317.811,00, batendo centavo por centavo. A mensagem "inseridos 334; desativados 322" do reenvio é correta: 322 era o que o 1º envio tinha conseguido gravar, e as 12 linhas antes descartadas passaram a casar com o teto novo.

### 20.2 Riscos encontrados na análise (além do sintoma)
- **Órfãos no reenvio do Plan 134**: `_persistir_plan134` só desativava o PTA antigo dos MOMPs que apareciam no arquivo novo. Uma combinação que sumisse entre dois envios deixaria o PTA antigo ativo para sempre (mesmo padrão do bug do Plan 23 da seção 12.9).
- **Nomes de Ação por lista fixa**: `ACAO_PLAN134_MAP` corrigia "2009 -Manutenção" → "2009 - Manutenção" só para 27 ações cadastradas à mão; ações novas (4537, 4538, 4541) apareciam no filtro PAOE como "4541 -Educação…".
- **UO 14601 (FMTE)**: `momp`/`politicateto` não guardavam a UO, e todo upload tratava "exercício" como "o teto inteiro". Em 2025/2026 as ações do FMTE (4524/4525) estavam dentro da UO 14101, mas **a partir de 2027 o FMTE virou uma UO própria (14601)**, com Plan 23 e Plan 134 separados por UO — confirmado pelo usuário. Com a regra antiga, subir o Plan 23 da 14601 faria a correção da 12.9 desativar o teto inteiro da 14101 ("sumiu do arquivo"). A pergunta sobre a 14601 veio de evidência, não de suposição: linhas do FMTE ativas em `politicateto` 2025/2026, as ações 4524/4525 no `ACAO_PLAN134_MAP` e a seção 17.2.

### 20.3 Banco (autorizado pelo usuário, executado pela IA)
```sql
ALTER TABLE momp ADD COLUMN uo VARCHAR(5) NULL, ADD INDEX ix_momp_exercicio_uo (exercicio, uo, ativo);
UPDATE momp SET uo = '14101' WHERE uo IS NULL;   -- 413 linhas (todo o histórico era 14101)
```
Conferido depois: totais por exercício inalterados (2027 ativo: 30 registros, R$ 6.485.317.811,00). `politicateto` não ganhou coluna — herda a UO pelo `momp_id`. Model `Momp` ganhou o campo `uo`.

**Atenção operacional (até este commit chegar a produção):** o código antigo não conhece a coluna `uo`. Um Plan 23 enviado pela versão online antiga grava `uo = NULL` e, depois que a 14601 estiver carregada, desativaria os registros dela (a regra antiga é só por exercício). Se isso acontecer, rodar de novo o `UPDATE ... WHERE uo IS NULL` com a UO certa e reenviar.

### 20.4 Implementação
- **`services/uo.py`** (novo): `UOS_ACEITAS = {"14101", "14601"}`, `uo_key()` (código da UO só com dígitos, "14.101" = "14101") e `uo_label()` ("14101 - SEDUC" / "14601 - FMTE"). Movidos de `services/plan20_runner.py` (que agora importa daqui, mantendo os nomes `UOS_ACEITAS`/`_uo_key`) para o Plan20 e o Teto-SEDUC nunca divergirem — uma UO nova da secretaria entra só nesse arquivo.
- **`services/teto_seduc.py`**:
  - `ler_cabecalho_fiplan()`: lê do próprio relatório o exercício ("*Exercício igual a 2027"), a UO do filtro ("Código da Unidade Orçamentária igual a 14601"), as UOs do corpo ("UO : 14601 - …" no Plan 23; coluna `U.O` de cada linha no Plan 134) e o **total impresso pelo FIPLAN** ("Total da UO:" no Plan 23; "SUBTOTAL UO"/"TOTAL GERAL" no Plan 134).
  - `validar_cabecalho_fiplan()`: recusa arquivo sem UO, com UO fora de `UOS_ACEITAS`, com mais de uma UO (inclusive filtro ≠ corpo) ou com exercício diferente do digitado na tela (campo mantido e validado, decisão do usuário).
  - `processar_plan134()`: descarta linhas de **valor 0** (subação sem item). Uma linha **com** valor e sem fonte não é descartada — segue e aparece no aviso de "não gravados", para nunca sumir dinheiro calado.
  - `normalizar_acao()`: regra `^(\d+)\s*-\s*` → `"\1 - "` + remove ponto final solto; substitui o `ACAO_PLAN134_MAP` (removido). Teste confirma resultado idêntico para as 27 entradas antigas.
- **`rotas/home_routes.py`**:
  - `_persistir_plan23(df, uo)`: grava `uo`; busca de ativos e desativação de órfãos (12.9) restritas a exercício + UO. Devolve `total_gravado`.
  - `_persistir_plan134(df, exercicio, uo)`: casa só com MOMPs da mesma UO; **desativa todo o PTA ativo da UO/exercício** (fim dos órfãos — seguro porque o `ValueError` de "nenhuma linha casou" acontece antes de desativar qualquer coisa, e o resto é desfeito pelo rollback). Devolve `total_gravado` e `nao_gravados` (combinação → soma).
  - `_start_teto_seduc_thread`: lê/valida o cabeçalho antes de gravar; trava `GET_LOCK` passou a ser `teto_seduc:{exercicio}:{uo}` (UOs diferentes não se bloqueiam).
  - Mensagens de status com UO, total gravado e **conferência com o total do relatório** (`_teto_mensagem_conferencia`); combinações não gravadas listadas com valor ("⚠ R$ 30.341.313,00 NÃO gravados … 15740000/3/B (R$ 13.900.000,00); …"). Exemplo: "Plan 134 - UO 14601, exercício 2027 processado. Registros inseridos: 10 (R$ 52.000.000,00) (confere com o total do relatório); anteriores desativados: 0."
  - Endpoint do dashboard devolve `uo` (rótulo) em cada MOMP e normaliza o nome da Ação **só na exibição** (dados antigos no banco continuam como gravados — decisão do usuário).
- **Dashboard** (`paineis_teto_orcamentario.html`, `main.js`, `style.css`): novo filtro **"UO"** no grupo de filtros de MOMP (não ativa o modo política), visível nas abas Gráficos e Tabelas (`data-graph-filter`). Aba Gráficos passou para 6 colunas (os 6 filtros numa linha). Componente de filtro em si não foi tocado (`git diff | grep "teto-multi-filter|checklist|data-filter="` = 0). Na aba Tabelas agora são 13 filtros — o último fica sozinho na 3ª linha.

### 20.5 Testes e validação
- `tests/test_teto_seduc_uo.py` (novo, 15 testes): normalização de ações (mapa antigo + casos novos); leitura de cabeçalho do Plan 23/134 sintéticos; recusas (exercício divergente, UO não cadastrada, filtro ≠ corpo, duas UOs no Plan 134, sem UO); descarte de valor 0; formatação/conferência de valores; e, no banco com exercícios descartáveis 9991/9990 (limpos ao final): Plan 23 da 14601 não desativa a 14101, Plan 134 casa só com o teto da mesma UO, reenvio sem uma combinação não deixa órfão nem toca a outra UO, Plan 134 sem teto da UO é recusado sem desativar nada.
- `tests/test_teto_seduc_key_normalization.py` / `tests/test_teto_seduc_lock.py` ajustados ao novo contrato (UO obrigatória, nome da trava com UO).
- Suite completa: **43/43** (`pytest`); `node --check static/js/main.js` ok.
- Dry-run com os 4 arquivos reais (sem gravar): Plan 23 14101 (41 linhas, R$ 6.535.243.129,00 — arquivo antigo de 31/08), Plan 134 14101 (334 linhas, R$ 6.485.317.811,00), Plan 23 14601 (3 linhas, R$ 52.000.000,00), Plan 134 14601 (10 linhas, R$ 52.000.000,00, ações 4524/4525/4545) — todos **conferindo com o total impresso**; UO e exercício lidos corretamente; exercício errado recusado. Contra o Plan 23 antigo, o Plan 134 14101 reproduz exatamente as 12 linhas / R$ 30.341.313,00 sem teto — que agora apareceriam no aviso.
- Teste manual na tela feito e aprovado pelo usuário (uploads das duas UOs e dashboard com o filtro UO).

### 20.6 Fonte dos dados no cabeçalho do dashboard
Pedido do usuário: citar no painel os relatórios FIPLAN de onde vêm os dados. Nova linha abaixo do subtítulo (`templates/partials/paineis_teto_orcamentario.html`, classe `.teto-dashboard-fonte` em `static/css/style.css`, 11px itálico): "Fonte: FIPLAN – PLAN 23 (Teto Orçamentário PTA) e PLAN 134 (Emitir PTA Detalhado por UO, Programa, Ação, Subação e Etapa)." Em linha separada, e não entre parênteses no subtítulo, para não apertar os botões Gráficos/Tabelas do cabeçalho; nomes dos relatórios exatamente como o FIPLAN imprime no cabeçalho de cada arquivo. Junto, release do rodapé (`templates/base.html`) atualizado pelo usuário para `RELEASE_2_2026_09_29.2`. Usuário conferiu na tela e aprovou. Commit `57fce61` (`templates/partials/paineis_teto_orcamentario.html`, `static/css/style.css`, `templates/base.html`, `docs/claude.md`).

### 20.7 Commit
Usuário testou e aprovou. Commit `951f3c8` (`models/user.py`, `rotas/home_routes.py`, `services/plan20_runner.py`, `services/teto_seduc.py`, `services/uo.py`, `static/css/style.css`, `static/js/main.js`, `templates/partials/paineis_teto_orcamentario.html`, `tests/test_teto_seduc_key_normalization.py`, `tests/test_teto_seduc_lock.py`, `tests/test_teto_seduc_uo.py`, `docs/claude.md`).
