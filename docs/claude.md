# Sistema SPO — Sistema de Planejamento e Orçamento

> Documento gerado a partir de uma análise completa do repositório em 2026-08-31, e atualizado nas sessões seguintes (última atualização: 2026-11-18) à medida que os riscos identificados foram corrigidos e novas investigações aconteceram.
> Objetivo: servir de contexto rápido para quem (humano ou IA) for trabalhar neste projeto.

> **Estado em 16/11/2026:** a produção (`main`) está com o AA+ (lotes 1 a 5), os avisos modais e o Teto Orçamentário no celular e as Notas SEE com o DANFE novo da Somos, em produção desde 16/11/2026 (`RELEASE_2_2026_11_16.4`). O que entrou e o que não entrou: [CHANGELOG.md](CHANGELOG.md); lições operacionais das promoções: seção 40.13.

> **Reorganização de 16/11/2026:** a seção 41 (AA+) deixou de guardar o histórico dos lotes do dev. Jean. O que foi feito, as medições e as verificações (antigas 41.1 a 41.101) estão no [CHANGELOG.md](CHANGELOG.md), na seção "Histórico do desenvolvimento do AA+", cada subseção com o número antigo no título; a seção 41 daqui ficou só com a regra, o contrato e a instrução em vigor. Regra de contribuição: seção 40.14.

> **Estado em 10/11/2026: layout novo, menu gerado do catálogo e funcionalidades da `homologacao`/`dev/cleber` integrados na `main`** (seção 32). Novas UOs (26101, 26201 e 26202): código pronto e **bloqueado** na produção (`UOS_EXECUCAO_LIBERADAS` e `UOS_PLAN20_LIBERADAS` em `services/uo.py` = só a SEDUC; a liberação temporária da 26101 que existia na homologação **não** veio). Estado, roteiro de testes e como liberar: [plano_implementacao_novas_uos.md](plano_implementacao_novas_uos.md) e seção 31.
> **Não libere nenhuma UO nova sem a confirmação oficial e a autorização explícita do usuário** (seção 22). No início de cada sessão, pergunte se a confirmação oficial já saiu.

> **Limpeza de Histórico: em uso online desde 15/10/2026** (etapas curtas conduzidas pela tela, commit `97230e0`; seção 23.9).
> Falta observar online uma remoção que precise de várias etapas e o botão "Continuar". Deve acontecer na primeira limpeza mensal grande; ver o fim da seção 23.9.
> **Sincronizada com `homologacao`, `dev/cleber` e `dev/jean` em 16/10/2026** (seção 23.10). Falta, em cada um desses ambientes, o `atualizar_schema_sync_main.py`, o backup e a primeira limpeza.

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
│   ├── limpeza_historico_routes.py # Atualizar > Execução > Limpeza de Histórico (seção 23)
│   └── home_routes.py         # TODAS as demais rotas HTML+API (19k+ linhas)
├── services/                  # regras de negócio / parsers de planilhas por módulo
│   ├── auth.py                 # decorators login_required / role_required
│   ├── features.py             # árvore de features/menus do sistema (controle de permissão por feature)
│   ├── job_status.py           # status de jobs assíncronos em arquivos JSON (outputs/status/*.json)
│   ├── fip613_runner.py, ped_runner.py, plan20_runner.py, est_emp_runner.py, emp_record.py, teto_seduc.py
│   ├── uo.py                   # UOs aceitas (14101/14601) + uo_key/uo_label — regra única do Plan20 e do Teto-SEDUC (seção 20)
│   ├── limpeza_historico.py    # Limpeza de Histórico das cargas de execução (seção 23)
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
  - **Correção (2026-10-02):** o banco compartilhado vale para **produção (`main`) × desenvolvimento local da `main`**. Os ambientes de **homologação, `dev/cleber` e `dev/jean` têm banco próprio cada um** — mudança de schema feita na `main` precisa ser reaplicada em cada um (seção 21.4/21.7).

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

### 20.8 Grupo 3 aparecendo duas vezes no dashboard ("Corrente" × "Correntes")
O usuário notou, com 2027 já carregado, o grupo 3 duplicado no filtro "Grupo de despesa" e nos cards/tabelas ("3 - Outras Despesas Corrente" e "3 - Outras Despesas Correntes"), imaginando que o FIPLAN tivesse passado a usar o plural em 2027.
- **Causa real (consulta ao banco, só leitura):** não foi o FIPLAN. É a correção da seção 12.11: o mapeamento de importação (`GRUPO_PLAN23_MAP`) foi corrigido para "Correntes" em 2026-09-02, então o Plan 23 de 2027, carregado depois, entrou com o texto certo — mas os registros de 2025 e 2026 continuam no banco como "Corrente". Aquela correção só ajustava o texto **exibido** (`displayGrupo()` em `main.js`); filtro e agrupamento usam o valor que vem do endpoint, e aí havia dois textos para o mesmo grupo.
- **Fix:** `normalizar_grupo()` + `GRUPO_NOMES` (`services/teto_seduc.py`) — nome oficial pelo **código** do grupo (`grupo_key`: 1 → "1 - Pessoal e Encargos Sociais", 3 → "3 - Outras Despesas Correntes", 4 → "4 - Investimentos"; código desconhecido fica com o texto original). Aplicado no endpoint `/api/paineis-dashboards/teto-orcamentario` (`rotas/home_routes.py`), então filtro, gráficos, cards e tabelas recebem um valor só. **Banco não alterado** — a gravação do Plan 23 já casa pelo código (`grupo_key`, seção 12.4), então o texto antigo não afeta nada além da exibição. `displayGrupo()` no JS ficou redundante mas inofensivo.
- **Validação:** simulação com os dados reais (só leitura) — antes: "Corrente" R$ 2.315.353.352,00 + "Correntes" R$ 941.340.138,00; depois: um único "3 - Outras Despesas Correntes" R$ 3.256.693.490,00, total geral idêntico (R$ 17.871.248.953,00). Novo teste `test_normalizar_grupo_unifica_pelo_codigo` (`tests/test_teto_seduc_uo.py`); suíte completa 44/44. Usuário conferiu na tela e aprovou.
- Junto, release do rodapé atualizado pelo usuário para `RELEASE_2_2026_09_29.3`. Commit `4a17546` (`services/teto_seduc.py`, `rotas/home_routes.py`, `tests/test_teto_seduc_uo.py`, `templates/base.html`).

## 21. Sincronização `main` → `homologacao` / `dev/cleber` / `dev/jean` (2026-10-02)

### 21.1 Situação encontrada
Todo o trabalho de 31/08 a 29/09 (seções 9 a 20) foi feito direto na `main`. As três branches tinham se separado dela em 26/06 (`857eb00`) e seguiram com o **redesenho de julho** (shell/layout novo, menu gerado de `services/features.py` via `templates/_menu.html` — o menu antigo do `base.html` ficou desativado com `{% if false %}`; "Personalizar SPO", "Governança e Resultados", PTA 2027, Notas SEE, Componente da Revista, banner de ambiente, deploy por `scripts/deploy_cpanel.sh`). `homologacao` não tinha nada que as `dev/*` não tivessem; `dev/cleber` tinha a mais Receita Anexo 10/formatos FIPLAN/Notas SEE/DRE-Escola; `dev/jean`, home de status/identificação DEV. Antes de tudo, tags de backup no GitHub: `backup/homologacao-2026-10-02`, `backup/dev-cleber-2026-10-02`, `backup/dev-jean-2026-10-02`.

### 21.2 Armadilhas encontradas na simulação (`git merge-tree`, sem tocar em nada)
- **Remoção silenciosa:** a `main` tem o commit `aa1dbdc` (revert do "deploy homolog estável", 26/06). Um merge comum **apagava sem conflito** o bloco `ENVIRONMENT_LABEL`/`inject_environment_label` do `app.py` (banner HOMOLOGAÇÃO/DEV). Restaurado à mão no merge.
- **Falso conflito de pasta:** a `main` moveu `scripts/test_teto_seduc_processors.py` para `tests/`, e o Git concluiu que `scripts/` inteira virou `tests/` — sugeria mover `deploy_cpanel.sh`/`migrate_personalizar_spo_permissions.py` (e, na `dev/cleber`, `importar_escolas_see.py`). Mantidos em `scripts/`.
- **Componente da Revista implementado duas vezes:** branches (`3966ae8`, 02/07: sequência de vínculos + seleção múltipla de produtos) × `main` (`66da64f`, 27/07: versão reduzida). Mesmas tabelas e endpoints; a versão das branches é superconjunto, com as mesmas validações — **fica a das branches** (decisão delegada pelo usuário).
- **Menu "Programar PTA/LOA"** (link externo para `pta2025.projetoswebcsg.life`) só existia na `main`, no menu estático. Como nas branches o menu vem de `features.py`, entrou lá (grupo "PTA - Programação", com `external_url` em `MENU_META`) e o `_menu.html` ganhou o caso de link externo (`data-external-url`, já tratado de forma genérica no `main.js`).
- O merge automático duplicou "Componentes da Revista" no menu estático (desativado) — removida a cópia da `main`.

### 21.3 Regra de resolução dos conflitos
Layout, menu e deploy ficam da branch; regras de negócio, correções e dashboard do Teto vêm da `main`. Na prática: `.cpanel.yml`/`login.html` da branch; `base.html` da branch + aviso de "permissões indisponíveis" (seção 13) + release `RELEASE_2_2026_09_29.3`; `index()` juntando as permissões por usuário da branch (`_current_usuario_id()`) com o tratamento de falha da `main`; `features.py` com a árvore da branch; `main.js`/template da Revista da branch; `style.css` com o CSS do layout novo **e** o do redesenho do Teto (os dois lados terminavam numa regra aberta — fechada antes de emendar). `?v=` do CSS/JS atualizado para `20261002.sync-main.1` (forçar o navegador a baixar a versão nova).

### 21.4 Verificação
- Linhas que a branch acrescentou desde 26/06 e não estão no resultado: só o CSS da tabela QOMP antiga (substituída pela grade da seção 12.10) e o release antigo. Linhas da `main` fora do resultado: só as decididas acima (deploy, Revista reduzida, layout/rodapé antigos).
- `pytest` 44/44 num ambiente isolado com as dependências da branch (`pdfplumber`, das Notas SEE); `node --check`; render do menu dinâmico completo (Teto Financeiro, Programar PTA/LOA externo, Componentes da Revista, Plan20, Personalizar SPO, banner de ambiente, aviso de permissões, release) e das telas do Teto (13 filtros, filtro UO, linha de fonte), Estrutura do Planejamento e Plan20.
- Banco: **cada ambiente (homologação, dev/cleber, dev/jean) tem banco próprio** — o "banco compartilhado" da seção 8 vale só para produção × desenvolvimento local da `main`. As mudanças de schema da `main` precisam ser aplicadas em cada um: ver seção 21.7. As tabelas próprias das branches (`usuario_permissoes`, `see_catalogos`, Notas SEE, Receita etc.) não foram tocadas.
- Fluxo: branch `sync/main-para-homologacao` + pull request para `homologacao` (aprovado pelo usuário no GitHub); depois `homologacao` → `dev/cleber` e `dev/jean`, também por PR.

### 21.5 `dev/cleber` (branch `sync/main-para-dev-cleber`, a partir do resultado já resolvido da homologação)
Só 3 arquivos em conflito, mais um problema que o merge automático criaria calado:
- **"Programar PTA/LOA" duplicado e quebrado:** a `dev/cleber` já tinha levado o link para o menu dinâmico do jeito dela (`url` em `MENU_META` + caso `node.url` no `_menu.html`, abrindo nativo pelo `href`/`target`). Somado ao que entrou na homologação (21.2), o item aparecia duas vezes e — pior — a entrada repetida no dicionário `MENU_META` substituía a da `dev/cleber`, apagando a `url` dela. Mantido só o mecanismo da `dev/cleber` (removidos o item extra no grupo PTA - Programação, a entrada `external_url` e o caso `node.external_url` do `_menu.html`); no `main.js`, idem — fica o `if (externalUrl) return;` (navegação nativa) dela em vez do `window.open` da `main`.
- **`_load_permissoes_perfil`/`_load_permissoes_nivel`:** mantido o `normalize_feature_list` da `dev/cleber`, mas com o `except ProgrammingError` da `main` — o `except` amplo da branch (`OperationalError` etc.) voltaria a engolir falha de conexão e renderizar o menu vazio calado (seção 13).
- `?v=` do CSS/JS: fica `20261002.sync-main.1` (mais novo que o `20261001.notas-see-exercicio` da branch).
- `requirements.txt` mesclado sozinho e certo: `pdfplumber`/`xlrd` da branch + `pytest` da `main`, sem o `pyodbc` (removido na `main`, sem uso no código).
- Verificação: nenhuma linha da `dev/cleber` perdida além do CSS da QOMP antiga, release e `?v=` antigos; `pytest` 44/44; render do menu com o PTA/LOA uma única vez e das telas do Teto/Estrutura/Plan20.

### 21.6 `dev/jean` (branch `sync/main-para-dev-jean`, a partir do resultado já resolvido da homologação)
Só o `templates/base.html` em conflito: `?v=` do CSS/JS (fica `20261002.sync-main.1`) e o rodapé — mantida a linha própria da branch (`PATCH_DEV_SPO_2026_07_01.1`) junto com o release `RELEASE_2_2026_09_29.3`. O selo de ambiente da `dev/jean` (`lab-badge`, "Integração DEV Jean", alimentado por `inject_spo_environment` no `app.py`) ficou idêntico. Verificação: nenhuma linha da branch perdida além do CSS da QOMP antiga, release e `?v=` antigos; `pytest` 44/44; `node --check`; render do menu e das telas do Teto/Estrutura/Plan20.

**Atualização (2026-11-16):** a linha própria `PATCH_DEV_SPO_2026_07_01.1` do rodapé **não existe mais** no `templates/base.html` da `dev/jean`, e o selo `lab-badge` também não. O selo de ambiente da `dev/jean` vem das variáveis `APP_ENV_LABEL` e `APP_ENV_CLASS` do `.env` do ambiente dela (`app.py` → `ENVIRONMENT_LABEL`), como nos outros ambientes. O nome antigo `SPO_ENV_LABEL` não é mais lido.

### 21.7 Banco de cada ambiente: `scripts/atualizar_schema_sync_main.py`
O código sincronizado exige três mudanças de schema que a `main` fez só no banco dela:
1. `momp.uo` (`VARCHAR(5)`) + índice `ix_momp_exercicio_uo` + histórico marcado como `14101` (seção 20) — sem isso o Teto-SEDUC e o dashboard dão erro.
2. `plan20_seduc`: as 8 colunas do layout 2027 (seção 18) — sem isso o upload do Plan20 falha.
3. `AUTO_INCREMENT` em `logs_login.id`, `active_sessions.id` e `perfil.id` — o código não gera mais o `id` na mão (seção 9, item 5); se algum banco não tiver, **o login para de funcionar**. Em produção os três já eram `AUTO_INCREMENT`; nos outros bancos precisa ser conferido.

Cada desenvolvedor roda no seu ambiente, depois de atualizar o código (usa a conexão do `.env` local):
```
python scripts/atualizar_schema_sync_main.py            # só mostra o que falta (não altera nada)
python scripts/atualizar_schema_sync_main.py --aplicar  # aplica
```
Idempotente (o que já existe é pulado; pode rodar de novo sem efeito); tabela inexistente é pulada com aviso. Validado contra o banco de produção (já atualizado → "Nada a fazer") e por `tests/test_atualizar_schema_sync_main.py` (banco desatualizado simulado: lista as 13 pendências na ordem certa; banco atualizado: nenhuma). As tabelas novas (Revista etc.) já são criadas pelo `db.create_all()` ao subir a aplicação.

### 21.8 Incidente: PRs abertos contra a `main` (2026-10-02)
Os três PRs de sincronização foram abertos com destino `main` (padrão do GitHub — os links enviados não fixavam o destino). #13 e #14 foram mergeados na `main` antes de o erro ser notado (o #15 deu conflito e chamou atenção). Resultado: a `main` (produção) recebeu o layout de julho, o trabalho em andamento da `dev/cleber` e o `.cpanel.yml` da homologação — cujo `scripts/deploy_cpanel.sh` faz `rsync --delete` na pasta de produção (apagaria `upload/`/`outputs/`, fora do Git) e exige `pdfplumber` (que derrubaria a produção se não estiver instalado). Nenhum deploy foi feito. Correção: `main` volta a `df7a37e` por force push feito pelo próprio usuário (o modo automático da IA bloqueia force push, corretamente) (backup do estado errado na tag `backup/main-pos-merge-errado-2026-10-02`) e PRs reabertos com links que fixam o destino (`/compare/<destino>...<origem>`). Lição: **sempre enviar link de PR com o destino explícito** e conferir o campo "base" antes de aprovar.

### 21.9 Desfecho e limpeza (2026-10-02)
- **Correção dos PRs:** o usuário fez o force push da `main` de volta para `df7a37e` (`git push --force-with-lease=main:03a490a origin main`), fechou o #15 sem merge e reabriu os três PRs com o destino certo — **#16** → `homologacao` (`ccb71eb`), **#17** → `dev/cleber` (`b4b7b03`), **#18** → `dev/jean` (`ebfeab8`), todos como *merge commit*. Conferido pelo Git depois: a `main` ficou idêntica ao estado de 29/09 (0 arquivos diferentes de `df7a37e`, nenhum commit de sincronização nela, `.cpanel.yml` de produção de volta à cópia simples) e cada branch ficou com conteúdo idêntico ao validado (0 arquivos diferentes da branch `sync/...` correspondente).
- **Por que a `main` está "atrás" das branches:** de propósito. A `main` (produção) continua sem o layout de julho e sem o trabalho em andamento das `dev`; o caminho previsto é levar a `homologacao` para a `main` depois de validada no servidor de homologação, e daí em diante seguir dev → homologação → main (seção 21, Fase 4 do plano). Até lá, toda correção feita direto na `main` vai exigir uma nova sincronização como esta.
- **Limpeza feita (não desfaz nada — as mudanças já estão gravadas nas branches de destino pelos merges #16/#17/#18; as `sync/...` eram só intermediárias):** antes de apagar, confirmado que cada `sync/...` (local e remota) estava inteiramente contida no seu destino e que as pastas de trabalho não tinham nada pendente. Removidos: as pastas de trabalho (`git worktree`) `C:\workspace\projetoswebcsg_sync`, `_sync_cleber` e `_sync_jean` (com as cópias locais do `.env`); as branches `sync/main-para-homologacao`, `sync/main-para-dev-cleber` e `sync/main-para-dev-jean`, locais e no GitHub; o ambiente Python temporário usado nos testes (com `pdfplumber`/`xlrd` das branches) e os scripts auxiliares de verificação (fora do repositório). Conferido depois da limpeza: as quatro branches seguem nos mesmos commits.
- **Mantido de propósito:** as tags de backup no GitHub — `backup/homologacao-2026-10-02` (`c05dedd`), `backup/dev-cleber-2026-10-02` (`e72c14d`), `backup/dev-jean-2026-10-02` (`9db6021`) (estado das branches antes da sincronização) e `backup/main-pos-merge-errado-2026-10-02` (`03a490a`, a `main` com os merges errados). Servem para voltar atrás se algo aparecer nos testes da homologação; podem ser apagadas quando a homologação estiver validada. Também não foram tocadas as pastas antigas `projetoswebcsg_license_temp`/`projetoswebcsg_license_dev_cleber_temp` (de outro trabalho).
- **Pendente (depende do deploy de cada ambiente):** rodar `scripts/atualizar_schema_sync_main.py` (o script existe só nas branches `homologacao`/`dev/*`, não na `main` — o banco de produção já estava atualizado) (primeiro sem e depois com `--aplicar`) no servidor de homologação e em cada `dev` (seção 21.7); até isso, não usar Teto-SEDUC nem upload do Plan20 nesses ambientes, e conferir o login logo depois de aplicar.
- **Lições registradas:** (1) link de PR sempre com o destino fixo (`/compare/<destino>...<origem>`) e conferir o campo *base* antes de aprovar; (2) merge entre branches que divergiram muito: simular antes com `git merge-tree` e checar perdas silenciosas linha a linha nos dois sentidos — o pior risco aqui não estava nos conflitos, e sim no que o Git juntaria sozinho (o banner de ambiente apagado pelo revert antigo, o item de menu duplicado que apagava a `url` da `dev/cleber`); (3) o `scripts/deploy_cpanel.sh` das branches faz `rsync --delete` — nunca pode chegar à `main` sem antes excluir `upload/`/`outputs/` do rsync e instalar as dependências novas no servidor de produção; (4) o modo automático da IA bloqueia *force push* na `main` (corretamente) — esse tipo de passo fica com o usuário.

## 22. Testes com as UOs 26101, 26201 e 26202 — aguardando confirmação oficial (2026-10-13 e 14)

> **Atualização (2026-11-03):** o texto abaixo é o retrato de 13 e 14/10, antes dos ajustes. Os ajustes de gravação por UO, chave só da SEDUC e painéis foram feitos e estão na `homologacao`; o que mudou, o que foi testado e o que falta está na **seção 31**.

**O que é:** existe a possibilidade de o sistema passar a receber os relatórios de mais três UOs: **26101** (SECITECI), **26201** (UNEMAT) e **26202** (FAPEMAT). Antes de qualquer mudança, o usuário pediu testes com os relatórios reais dessas UOs em seis funcionalidades: Plan20, FIP 613, PED, EMP, NOB e EST EMP. A inclusão ainda depende de **confirmação oficial**.

**Documento completo:** [testes_uos_26101_26201_26202.md](testes_uos_26101_26201_26202.md). Leia antes de mexer em qualquer um desses módulos. Ele traz os resultados por módulo, os pontos de atenção e a lista de ajustes pendentes.

**Resumo:**
- **A leitura funciona.** Nas três UOs, todos os módulos leram e trataram os relatórios sem perder valor: as somas batem com o arquivo original e com os totais impressos pelo FIPLAN.
- **A gravação não está pronta.** FIP 613, PED, EMP, NOB e EST EMP desativam a **tabela inteira** a cada upload (`UPDATE <tabela> SET ativo = 0 WHERE ativo = 1`). Enviar um arquivo de uma nova UO apagaria os dados da SEDUC, e o envio seguinte da SEDUC apagaria os da nova UO. PED e EMP também recalculam as dotações da SEDUC.
- **O Plan20 está quase pronto.** Só falta incluir as UOs em `UOS_ACEITAS` (`services/uo.py`). Hoje ele mostra "processado com sucesso" sem gravar nenhuma linha dessas UOs.
- **Regra do PAOE 8026 (Emendas Parlamentares):** atribui uma Chave de Planejamento da SEDUC a qualquer empenho/PED desse PAOE, de qualquer UO. É intencional, mas precisará ser restrita por UO. Fica a decidir se a restrição vale só para ela ou para todas as regras de chave.
- **Ajuste já aprovado pelo usuário:** no PED, criar sempre as colunas da Dotação e da Chave de Planejamento. Hoje elas só aparecem quando alguma linha tem chave, e por isso os arquivos de saída variavam entre 50 e 67 colunas.
- **A UO é identificada pelo conteúdo do relatório, nunca pelo nome do arquivo** (decisão do usuário). Nos testes, o EST da 26101 veio salvo com o nome "26201". Use sempre a coluna `UO` das linhas e/ou o filtro impresso no cabeçalho do FIPLAN, tanto ao conferir arquivos quanto nos ajustes futuros (seção 8.1 do documento de testes).

**Como os testes foram feitos:** sem alterar o código e sem gravar no banco. As funções reais foram chamadas a partir de scripts descartáveis. As escritas no banco do EMP/NOB (Node) foram interceptadas e só registradas, e as saídas foram geradas fora da pasta `outputs/`. Os arquivos processados ficaram em `C:\workspace\Planilhas\*_SAIDA_TESTE.xlsx`, fora do repositório. Se for preciso repetir um teste, siga o mesmo cuidado: `run_fip613()`, `run_ped()`, `run_est_emp()` e os runners Node **gravam no banco** se forem chamados diretamente.

**Implementação (22/10/2026):** feita conforme [plano_implementacao_novas_uos.md](plano_implementacao_novas_uos.md) (PR #25 na `homologacao`; commits `98bbe5d` a `82d05a0` mais o merge `2f259a7`). Resumo: desativação por UO e exercício nos 5 módulos de execução; UO sempre lida do conteúdo e arquivo recusado com o motivo; FIP 613 com corte "Total UO" genérico, conferência com o total e UG como texto (`0011`); EST EMP com exercício `2026`; PED com colunas sempre presentes; recálculo de dotação por UO; Chave de Planejamento só para UOs da SEDUC (`uo_usa_chave`); painéis, relatórios e API v1 fixos na SEDUC (`?uo=`); Plan20 recusa UO não liberada e a tela virou "PLAN20". Falta: testes do usuário em homologação, liberar as UOs (Fase 3), filtro de UO nas telas, sincronizar `main`/`dev/*`.

**Regra para as próximas sessões:**
1. No início da sessão, **pergunte ao usuário se a confirmação oficial já saiu** e se os ajustes pendentes (seção 9 do documento de testes) já podem ser feitos.
2. Enquanto não houver essa autorização explícita, **não faça nenhum desses ajustes**, nem os que já foram aprovados em conceito.
3. Quando for autorizado, comece pela desativação por UO/exercício nos módulos de execução. Sem ela, os outros ajustes não resolvem o problema.

**Ajustes aplicados à mão nesta base (2026-11-03), vindos dos testes da homologação:** a produção continua só com a SEDUC (`UOS_EXECUCAO_LIBERADAS`/`UOS_PLAN20_LIBERADAS` não mudaram; a liberação temporária da 26101 da homologação **não** foi trazida). Nada da `homologacao` foi mesclado; cada ajuste foi reescrito aqui, sem layout nem menu novos.
- **Relatórios** (FIP 613, PED, EMP, EST EMP, NOB): a API devolve as UOs liberadas e a tela abre com a coluna UO pré-selecionada na SEDUC (`uos_padrao`); "(Todos)" soma tudo e "Limpar Filtros" volta à SEDUC. A coluna UO passou a ser a primeira (PED, EMP, EST EMP, NOB) e "Nome da Unidade Orçamentária" fica oculta na tela (continua no Excel e no banco). O "Baixar Excel" respeita `?uo=` e, sem parâmetro, leva só a SEDUC. Painéis, dotação e API v1 continuam fixos na SEDUC. Funções em `rotas/home_routes.py`: `_uo_liberadas`, `_uo_relatorio_download`, `_sql_uo_download`, `SQL_UO_LIBERADAS`.
- **"Última atualização" por UO** nos seis relatórios (Plan20 incluído): `_ultimas_cargas_por_uo` e `_plan20_ultimas_por_uo`; renderizador único `renderUltimasAtualizacoes` (`static/js/main.js`); CSS `relatorio-top-uo` e `meta-uo-*` com seletores adaptados a esta base (que não tem `.report-data-card`).
- **Aviso de chave só para UO que usa chave** no PED (`_find_missing_planejamento_lines`) e no EMP (`emp_runner.js`).
- **Modal de Avisos do dashboard por UO.** Diferente da homologação, aqui o PED roda na rota (sem `ProcessamentoJob`), então os avisos ficam na sessão em `ped_avisos_uo`, por UO; `run_ped` devolve a UO como 5º valor. O EMP grava `uo` no status e o dashboard lê a última carga de cada UO.
- **Operacional:** em cada ambiente, `cd node_runners && npm install --omit=dev` (erro "Cannot find module 'exceljs'"); não commitar o `package-lock.json`. Regra de correção de chave (`chave_arrumar`): a origem é o texto **errado** que aparece no Histórico do empenho.
- O `?v=` do `main.js`/`style.css` no `base.html` e o `RELEASE` do rodapé são trocados pelo usuário na entrega.

## 23. Limpeza de Histórico (Atualizar → Execução) — nova funcionalidade (2026-10-14)

### 23.1 Motivação
FIP 613, PED, EMP, NOB e EST EMP são atualizados todo dia. Cada upload desativa a carga anterior (`ativo = 0`) e grava uma cópia completa do ano. A aplicação só lê registros ativos (conferido: telas, relatórios e o recálculo de dotação filtram `ativo = 1`), então as cargas desativadas só ocupam espaço. Em 14/10/2026, mais de 99% das linhas dessas tabelas estavam desativadas, e o banco tinha cerca de **35 GB**: EMP 19,1 GB e NOB 15,8 GB. Na primeira análise o banco parecia ter 10,5 GB, porque o `information_schema` estava com estatísticas antigas (ver 23.5).

### 23.2 Regra (decisões do usuário)
O usuário informa o **exercício** e o **mês/ano da carga a manter**, e marca quais das 5 tabelas limpar. Para cada tabela marcada:
- **mantém a última carga** daquele exercício carregada no mês/ano escolhido;
- **remove as demais cargas** do mesmo exercício carregadas até o fim desse mês;
- **não toca** em cargas de outros exercícios, nem em cargas posteriores ao mês escolhido;
- **nunca remove** uma carga com registro ativo, nem carga com mais de um exercício;
- **se não houver carga** do exercício no mês escolhido, nada é removido daquela tabela.

O mês/ano é separado do exercício porque **a carga final de um exercício pode chegar no ano seguinte**: a FIP 613 de 2025 foi carregada em 14/01/2026, e o EST EMP de 2025 também em janeiro/2026. O exercício de cada carga é lido dos próprios registros. A coluna tem formatos diferentes em cada tabela: `ano` int na FIP 613, `2026` no EMP/NOB e `26` no EST EMP (`normalizar_exercicio`). A remoção trabalha sempre com a **carga inteira** (`upload_id`), nunca com linhas soltas.

### 23.3 Implementação
- **`services/limpeza_historico.py`**
  - `planejar_tabela()`: função pura que decide o que fica e o que sai.
  - `carregar_cargas()`: uma passada `GROUP BY upload_id` por tabela; leva cerca de 1 minuto no EMP e no NOB.
  - `remover_cargas()`: `DELETE` em lotes de 20.000 ids por faixa de chave primária (EMP e NOB não têm índice por `upload_id`), com `ativo = 0 AND upload_id IN (...)` em todo comando e commit a cada lote.
  - `otimizar_tabela()`: `OPTIMIZE TABLE`.
  - `espaco_mb()`: tamanho usado, livre e em disco.
  - Trava `GET_LOCK('limpeza_historico')`: só uma limpeza ou otimização por vez.
- **`rotas/limpeza_historico_routes.py`**
  - Arquivo próprio (não em `home_routes.py`), para não aumentar o arquivo monolítico e facilitar a sincronização com as branches.
  - Blueprint `limpeza_bp`, registrado em `rotas/__init__.py`.
  - Rotas: `/partial/atualizar/limpeza-historico` e `/api/limpeza-historico/{info,simular,executar,otimizar,status/<id>}`.
  - Simular, executar e otimizar rodam numa thread em segundo plano, com status em arquivo (`services/job_status.py`, tipo `limpeza_historico`), no mesmo padrão do Teto-SEDUC.
  - A execução **reanalisa as cargas na hora** e não confia na simulação da tela.
  - Executar e otimizar exigem `confirmado: true`.
- **Permissão:** feature `atualizar/limpeza-historico` em `services/features.py`, com item no menu do `base.html`. Aparece no **painel de permissões** sem nenhum perfil marcado. Por enquanto só o administrador acessa, pela regra geral de `require_feature` (nível 1 passa em tudo). Não há regra "só admin" no código; para liberar a outro perfil, basta marcar no painel.
- **Tela** (`templates/partials/atualizar_limpeza_historico.html` + `initLimpezaHistorico()` em `static/js/main.js`):
  - Formulário com exercício, mês e ano da carga, e as 5 tabelas com caixas de seleção.
  - **Simular** mostra o resultado por tabela. **Executar limpeza** só é liberado depois de uma simulação com os mesmos parâmetros e da confirmação.
  - O card **Otimizar tabelas** tem seleção própria por tabela e mostra o tamanho em disco e o espaço recuperável.
  - O card **Últimas execuções** mostra o log.
  - Durante o processamento aparece o loading padrão (`showAppLoading`), com o andamento atualizado. No fim, um modal no padrão da aplicação traz o resumo (ou o erro).
  - Se o usuário sair da tela e voltar, o acompanhamento é retomado (`sessionStorage`).
- **Log:** tabela nova `limpeza_historico_log` (model `LimpezaHistoricoLog`), criada em 14/10/2026 com autorização do usuário. Guarda ação, situação, usuário, parâmetros, tabelas, registros removidos, detalhes em JSON, início e fim. Horários gravados com `_now_local()` (America/Manaus, o fuso da aplicação) e exibidos com `formatAmazonLocalTime()`. O relógio do banco está em UTC-3.

### 23.4 Testes
- `tests/test_limpeza_historico.py` (10 testes):
  - a regra de seleção sem banco: carga final no ano seguinte, carga ativa protegida, outro exercício intocado, sem carga no mês, exercício misto, empate de data;
  - a remoção de ponta a ponta numa **TEMPORARY TABLE** do MySQL, incluindo uma carga reativada entre a análise e a execução, que não é apagada.
- Suíte completa: 54/54.
- Teste real na tela pelo usuário, em 14/10/2026:
  - PED: limpezas mantendo fev/2026 e mar/2026 (907 linhas).
  - **NOB:** mantendo set/2026, **5.395.377 linhas removidas em 1 minuto e 19 segundos**. A otimização levou 13 segundos e o NOB caiu de **~15,4 GB para 1,89 GB**.

### 23.5 Armadilhas do MariaDB (importante para quem mexer nisso)
- **Estatísticas velhas no `information_schema`.** `TABLE_ROWS` e `DATA_LENGTH` podem estar muito desatualizados: o EST EMP aparecia com 22 mil linhas e tinha 84 mil, e o EMP com 6,4 GB e tinha 19,1 GB. Por isso, toda medição na tela roda antes `ANALYZE TABLE`, que só recalcula as estatísticas, não mexe nos dados e leva cerca de 0,1 segundo.
- **O espaço não volta sozinho depois do `DELETE`.** O arquivo da tabela só diminui com `OPTIMIZE TABLE` (no InnoDB, "recreate + analyze"; `innodb_file_per_table` está ON). Logo após a limpeza, `DATA_LENGTH` passa a contar só as páginas em uso, e o espaço das linhas apagadas aparece como `DATA_FREE`. Por isso:
  - o "antes/depois" da otimização usa **tamanho em disco = usado + livre**;
  - a estimativa da simulação usa o tamanho usado.

  No primeiro teste, o "antes" saiu 1,89 GB em vez de 15,4 GB; o registro foi corrigido no log.
- **A otimização bloqueia as cargas da tabela enquanto roda.** Use fora do horário da carga automática (madrugada/início da manhã). Ela só compensa depois de uma limpeza que remova boa parte da tabela: com só 12% removido no PED, não houve redução.

### 23.6 Pendências e próximos passos
- **EMP (19,1 GB)** ainda não foi limpo. Mantendo set/2026, devem sair cerca de 5,6 milhões de linhas, e depois da otimização o EMP deve ficar perto de 2 GB. Esse teste também vai confirmar na prática o `DATA_FREE` logo após a limpeza (ver 23.5).
- **Fazer backup do banco antes de limpezas grandes.** A remoção é definitiva, e o banco da `main` é o de produção.
- ~~Sincronizar com `homologacao`, `dev/cleber` e `dev/jean`~~ — **feito em 16/10/2026**, ver 23.10.
- **Quando as novas UOs entrarem (seção 22)**, a limpeza continua correta, porque trabalha por carga. Revisar se o exercício/UO de uma carga continua único.

### 23.7 Problema no servidor online: "Processamento não encontrado" (2026-10-14) — corrigido na seção 23.8
**Sintoma:** no servidor online (cPanel), a simulação do FIP 613 e do PED terminava com o modal "Falha na simulação — Processamento não encontrado". Localmente sempre funcionou.

**Diagnóstico** (comandos de leitura no Terminal do cPanel, em `/home/proj5954/projetoswebcsg_app`):
- **O processamento em si funcionou.** Os arquivos `outputs/status/limpeza_historico_*.json` das tentativas existem no servidor e estão como "Simulação concluída", com o resultado certo (ex.: PED mantendo 30/09, 132 cargas e 5.667 linhas a remover). Não há erro da limpeza em `logs/app.log` nem em `stderr.log`.
- **O servidor não roda Passenger, e sim LiteSpeed (`lswsgi -m passenger_wsgi.py`), com vários processos da aplicação ao mesmo tempo** (4 no momento da análise). Esses processos são **reciclados em poucos minutos**: todos tinham cerca de 4 minutos de vida, e logo depois não havia nenhum rodando (`pgrep` vazio).
- **Causa:** o andamento era guardado em **arquivo** (`services/job_status.py`). Cada consulta de status cai em qualquer um dos processos, e a leitura voltou "não encontrado". Uma possível origem é ler no instante em que outro processo reescrevia o arquivo: `write_text` trunca e reescreve, e `read_status` trata conteúdo vazio como inexistente. No ambiente local há um processo só, por isso o problema nunca apareceu.
- **Risco adicional pelo mesmo motivo:** o processamento roda numa thread dentro de um desses processos. Se o LiteSpeed reciclar o processo no meio de uma limpeza demorada (EMP/NOB), a thread morre sem aviso e a tela fica esperando para sempre.
  - **Nenhum dado se perde:** cada lote é commitado, e o que sobra continua desativado e intacto.
  - **A trava `GET_LOCK` é liberada sozinha**, junto com a conexão.

**Correção combinada com o usuário (a implementar em 15/10/2026):**
1. **Guardar o andamento no banco**, na tabela `limpeza_historico_log`, em vez de arquivo. Todos os processos enxergam o mesmo dado, e o `UPDATE` é atômico. Isso resolve o "Processamento não encontrado". A simulação também passa a ter uma linha no log, só para o status. Ela não aparece em "Últimas execuções" (filtrar por `acao IN ('limpeza', 'otimizacao')`).
2. **Sinal de vida:** o processamento atualiza um horário (ex.: `atualizado_em`) a cada passo. Se ficar mais de cerca de 2 minutos sem atualização, a tela mostra "Processamento interrompido pelo servidor — rode novamente", e o log fica com a situação "interrompida". Rodar de novo continua de onde parou, porque a remoção é em lotes e sempre com `ativo = 0`.
3. Exige `ALTER TABLE limpeza_historico_log` para as colunas novas (progresso, mensagem, `atualizado_em`). Pedir autorização ao usuário antes de executar. Os outros ambientes (homologação/dev) recebem a tabela já completa pelo `db.create_all()` na sincronização.
4. **Depois de publicar, testar no servidor online:** simulação do PED e do FIP 613, e uma limpeza pequena. Só então partir para o EMP.

**Até a correção ser publicada:** não usar a Limpeza de Histórico no servidor online. No ambiente local ela funciona normalmente.

**Atenção para outras funcionalidades:** o **Teto-SEDUC** usa o mesmo mecanismo (thread + status em arquivo). Ele é curto e não houve relato de erro, mas está sujeito ao mesmo problema. Se aparecer "Processamento não encontrado" no upload do Teto, a correção é a mesma.

### 23.8 Correção: andamento no banco + sinal de vida (2026-10-15)
Implementado o plano da 23.7.
- **Banco** (autorizado pelo usuário, executado pela IA em 15/10/2026): `ALTER TABLE limpeza_historico_log ADD COLUMN progresso SMALLINT NULL, ADD COLUMN mensagem VARCHAR(500) NULL, ADD COLUMN atualizado_em DATETIME NULL`. As colunas aceitam vazio, então o código antigo publicado continuou funcionando. As 5 linhas existentes não foram alteradas. Os outros ambientes recebem a tabela completa pelo `db.create_all()` na sincronização.
- **`rotas/limpeza_historico_routes.py`**
  - O `job_id` da tela passa a ser o **id da linha do log**, criada na requisição antes de a thread começar. A simulação também ganha uma linha (`acao = 'simulacao'`), mas "Últimas execuções" mostra só `limpeza` e `otimizacao`.
  - Classe `Andamento`: cada gravação é um `UPDATE` próprio com commit (`engine.begin()`), fora da sessão do ORM. O progresso só é gravado quando muda, porque a remoção chama o callback a cada lote.
  - **Sinal de vida:** uma thread separada grava `atualizado_em` a cada `INTERVALO_SINAL` (20 s), inclusive durante comandos longos sem progresso (o `GROUP BY` de ~1 min do EMP, o `OPTIMIZE TABLE`). Sem sinal por mais de `LIMITE_SEM_SINAL` (120 s), o processo morreu. Essa regra fica em `sem_sinal_de_vida()`, em `services/limpeza_historico.py`.
  - A rota de status e a de "Últimas execuções" marcam a linha como **`interrompida`** com um `UPDATE` condicional: só vale se a linha continuar em andamento e parada. A tela recebe "Processamento interrompido pelo servidor. Rode novamente.", e na limpeza a mensagem acrescenta que a nova execução continua de onde parou. Se ninguém estiver acompanhando na tela, a linha é marcada na próxima abertura da página.
  - Na limpeza, `linhas_removidas` e `detalhes` são gravados ao fim de cada tabela. Se o processo morrer, o log mostra o que já saiu.
  - Um erro inesperado grava `falha` com a mensagem e o resultado parcial.
  - A resposta do status manteve o mesmo formato (`state`/`message`/`progress`/`resultado`), então o JS da tela não mudou.
- **`services/job_status.py` não mudou:** continua sendo usado pelos outros módulos.
- **Testes:**
  - Novo `test_sem_sinal_de_vida`. Suíte completa: 55/55.
  - Contra o banco, chamando as funções reais das rotas:
    - simulação do PED: 132 cargas e 5.667 linhas, igual ao status do servidor online de 14/10;
    - uma linha parada há 5 min virou "interrompida", e uma linha com sinal de 30 s continuou em andamento;
    - id inexistente devolve 404;
    - com intervalo de 1 s e um job parado por 5 s, `atualizado_em` avançou a cada segundo, e a linha fechou como `concluida`.
  - Ficaram no log 4 linhas de teste com `acao = 'simulacao'` e `user_email = 'teste-claude@local'`. Elas não aparecem na tela.
- **Atenção ao testar por script:** uma sessão SQLAlchemy reaproveitada mantém a transação aberta (REPEATABLE READ) e não enxerga as gravações das outras threads. Use `commit()` antes de cada leitura. As rotas não têm esse problema, porque cada requisição abre uma sessão nova.
- **Pendente:**
  - aprovação do usuário, commit e publicação;
  - no servidor online: simulação do PED e do FIP 613, depois uma limpeza pequena, e só então o EMP, com backup antes.

### 23.9 Teste online do EMP falhou → processamento em etapas curtas (2026-10-15)
**O teste online** (com a versão da 23.8 publicada, commit `49daae9`):
- **Tabelas pequenas funcionaram.** FIP 613, PED e EST EMP: simulação, limpeza (187.504 linhas em 91 s) e otimização (194,6 MB liberados).
- **O EMP parou no meio** (log 22):
  - removeu 2.920.000 das 5.624.176 linhas e parou às 16:52:59, com 66%;
  - 2 minutos depois, a tela avisou "interrompido", como planejado;
  - **mas a trava `GET_LOCK` continuou presa** pela conexão do processo online. A conexão não caiu sozinha, apesar do `wait_timeout` de 300 s. Ou seja, o processo não morreu: ficou **congelado**.
- **O servidor online removeu cerca de 15 vezes mais devagar** que o ambiente local: 2,92 milhões em 11 min, contra 2,70 milhões em 45 s.
- **Suspeita, não confirmada:** limites de recursos do CloudLinux (o MariaDB é `10.11.19-MariaDB-cll-lve`). A conta do banco não tem o privilégio PROCESS, por isso não dá para ver as conexões da aplicação online (`@localhost`).
- **Destravado** com o Restart da aplicação no cPanel, feito pelo usuário. O resto do EMP (2.704.176 linhas) foi limpo e otimizado pelo ambiente local: o EMP caiu de 18,99 GB para 2,45 GB.
- **Os dados ativos ficaram intactos o tempo todo.** As 56.693 linhas ativas eram exatamente a carga 224.

**Decisão do usuário:** a limpeza de EMP e NOB **precisa funcionar online**, pela tela. Quem herdar o acesso de administrador não terá o ambiente local.

**O Teto-SEDUC fica de fora.** Ele usa o mesmo status em arquivo, mas recebe 3 a 5 cargas por ano, com processamento curto. O usuário avaliou que não sofre o problema de volume. Se algum dia aparecer "Processamento não encontrado" no Teto, conferir o dashboard, porque a carga provavelmente gravou.

**Implementação** (nenhuma thread; o trabalho só acontece enquanto uma requisição está aberta):
- **Banco** (autorizado pelo usuário, executado pela IA): `ALTER TABLE limpeza_historico_log ADD COLUMN estado MEDIUMTEXT NULL`. A coluna guarda o JSON com o ponto de parada. As colunas da 23.8 continuam em uso: `progresso`, `mensagem` e `atualizado_em`.
- **`services/limpeza_historico.py`**
  - `executar_passo(conn, estado, prazo)` avança uma máquina de fases: `inicio` → `analise` → `remocao` → próxima tabela → `fim`. Na otimização: `inicio` → `otimizacao`, uma tabela por etapa.
  - Toda etapa faz **ao menos um pedaço** de trabalho. Com o servidor lento, ela pode passar um pouco do prazo, mas nunca fica parada.
  - **Análise por faixas de id** (`analisar_faixa` + `mesclar_cargas` + `cargas_de_agregados`). O tamanho da faixa se ajusta para ~3 s por `GROUP BY` (`FAIXA_INICIAL` = 200 mil ids, entre 20 mil e 5 milhões). O resultado é idêntico ao do `GROUP BY` único (`carregar_cargas`): conferido em teste e no banco real, no EMP e no NOB.
  - **Remoção com prazo** (`remover_lotes`): continua por carga inteira, com `ativo = 0 AND upload_id IN (...)` e commit por lote. Retomar do cursor nunca apaga nada ativo nem repete trabalho. `remover_cargas` passou a usar `remover_lotes` e continua coberta pelos testes antigos.
  - `PRAZO_PASSO` = 10 s e `LIMITE_PAUSA` = 300 s. `pausada()` substituiu `sem_sinal_de_vida()`.
- **`rotas/limpeza_historico_routes.py`**
  - `simular`/`executar`/`otimizar` só criam a linha do log (com `estado` inicial) e devolvem o `job_id`.
  - **`POST /api/limpeza-historico/passo/<id>`** faz uma etapa e responde no mesmo formato do status. Uma trava curta por execução (`limpeza_passo_<id>`) faz com que duas abas não executem a mesma etapa; a segunda recebe `ocupado`.
  - Se uma etapa der erro, a rota responde 500 com `transitorio` e **não grava o estado**: a próxima chamada refaz a etapa do último ponto gravado.
  - **Uma limpeza ou otimização por vez** (`_iniciar_exclusivo`): checa a regra e cria a linha sob um `GET_LOCK` que dura só a requisição.
    - Execução com etapa recente: responde 409.
    - Execução pausada há mais de 5 min: vira `abandonada`, com o total removido até ali registrado, e a nova começa.
  - `/info` devolve `pendente` (limpeza ou otimização não concluída), e "Últimas execuções" mostra `pausada`. Simulação parada vira `abandonada` sem aviso.
  - Saíram a thread, a classe `Andamento` e o sinal de vida da 23.8.
- **Tela**
  - `executarPassos()` substituiu o acompanhamento do status: chama `/passo` em sequência.
  - Rede fora ou erro 5xx: tenta de novo com espera crescente. Depois de 6 falhas seguidas, a execução **pausa** e aparece um modal "Execução pausada".
  - Um aviso no topo da página (`#limpeza-pendente`) mostra a execução pendente, com o botão **Continuar**.
  - Os textos de loading pedem para manter a tela aberta.
  - CSS próprio `.limpeza-pendente` (não existia classe de alerta de aviso).
- **Status novos no log:** `abandonada`. Também aparece `pausada`, calculado na hora (no banco continua `em andamento`). `interrompida` só existe nas execuções da versão com thread.

**Testes:**
- 58/58 na suíte. Novos testes:
  - `test_pausada` e `test_mesclar_cargas_soma_as_faixas`;
  - simulação em etapas igual à análise única;
  - limpeza em etapas retomando do cursor, com metade de uma carga reativada no meio. Esses dois usam `TEMPORARY TABLE` com linhas intercaladas por id e passam o estado por JSON a cada etapa, como a rota.
- No banco real, chamando as rotas:
  - simulação de EMP e NOB, mantendo out/2026: 1 etapa de 8,9 s localmente. Com o prazo forçado a 1 s, 2 etapas e o resultado igual ao `carregar_cargas`;
  - a regra de uma por vez: 409 com outra execução recente; a pausada vira `abandonada`; etapa em execução encerrada devolve a mensagem final.
  - As linhas de teste dessa última verificação foram apagadas do log. Nenhuma etapa de remoção foi chamada.

**Validação online** (15/10/2026, commit `97230e0`, `RELEASE_2_2026_10_15.2`, feita pelo usuário):

| Log | Ação | Resultado | Duração |
|---|---|---|---|
| 34 | Limpeza do NOB (2026, manter out/2026) | 741.095 linhas removidas | 5 s |
| 35 | Otimização do NOB | 2.119 MB → 184,8 MB | 2 s |
| 37 | Limpeza do EMP (2026, manter out/2026) | 774.839 linhas removidas | 6 s |
| 38 | Otimização das 5 tabelas | EMP 2.479 MB → 174,8 MB | 3 s, em 5 etapas (uma por tabela) |

- **Velocidade:** online, a remoção rodou a ~140 mil linhas/s, contra ~4.400/s à tarde com o EMP de 19 GB e a versão com thread. A causa da diferença não foi confirmada: pode ser o tamanho e a fragmentação da tabela, ou o processo congelado. Com as tabelas pequenas, cada limpeza coube numa etapa só.
- **Resultado do dia:** EMP de **19,1 GB para 175 MB**; NOB de **15,8 GB para 185 MB**.
- **Medição do banco inteiro depois da limpeza** (15/10/2026, ~18h10, com `ANALYZE TABLE` nas 95 tabelas antes):
  - **438 MB em disco** (429 MB de dados e índices, mais 9 MB livres dentro dos arquivos), contra ~35 GB antes;
  - `DATA_FREE` = 0 nas 5 tabelas otimizadas;
  - contagens reais: EMP 56.693 e NOB 69.212, todas ativas (só a carga do dia); EST EMP 9.129 (967 ativas); FIP 613 6.150 (1.673 ativas); PED 66.
- **Espaço livre no servidor:** não dá para saber pelo banco. O usuário do banco não tem privilégio para consultar o disco (nem `PROCESS` ou `INNODB_SYS_TABLESPACES`). O limite é a cota da conta no cPanel. Para ver a cota, rode no Terminal do cPanel (só leitura):
  - `uapi --output=jsonpretty Quota get_quota_info` (cota);
  - `uapi --output=jsonpretty Mysql list_databases` (tamanho de cada banco, em bytes, em cache).
- **Medição da cota em 15/10/2026, ~18h30:**
  - **cota de 100.000 MB (~100 GB); 59.348 MB usados e 40.652 MB livres.**
  - Esse "usado" ainda contava o `proj5954_spo-csg` com **22,9 GB**, valor em cache do cPanel. O real é 438 MB. Depois do recálculo do cPanel, o esperado é **~38 GB usados e ~62 GB livres**.
  - O que continua ocupando, pelo cache do cPanel:

    | Item | Tamanho |
    |---|---|
    | `proj5954_spo_csg_amb_cleber` | 11,3 GB |
    | `proj5954_spo_csg_amb_jean` | 11,0 GB |
    | `proj5954_spo_csg_homolog` | 10,9 GB |
    | Home: `projetoswebcsg_app` 1,8 GB, `repositories` 1,5 GB, `virtualenv` 1,1 GB, `db_backups` 549 MB e outros | 5,6 GB |
    | Outros bancos | ~0,1 GB |

  - O `df` mostra o disco inteiro do servidor (12 TB, compartilhado). Não é referência para a conta.
- **Próximo ganho grande de espaço:** os três bancos de homologação e dev, ~33 GB juntos. Devem ter o mesmo acúmulo de cargas diárias desativadas. O caminho é sincronizar a Limpeza de Histórico com `homologacao`, `dev/cleber` e `dev/jean` e rodar a limpeza em cada ambiente. A sincronização ficou adiada por decisão do usuário (15/10/2026).
- **Ainda não exercitado online:** a remoção em várias etapas e o botão **Continuar**. Estão cobertos pelos testes automáticos. A primeira limpeza mensal grande deve passar dos 10 s por etapa: são ~1,7 milhão de linhas por mês no EMP e no NOB. Nessa ocasião, conferir no log que a execução concluiu e, se possível, testar o Continuar fechando a aba no meio.
- **Atenção na escolha do mês:** "manter out/2026" no EMP também removeu a carga de 30/09 que tinha sido mantida antes. Está dentro da regra (dados desativados), mas a escolha do mês define qual foto do exercício fica guardada.

**Recomendação de uso:** limpeza **mensal**, fora do horário das cargas automáticas (por volta das 05h).

### 23.10 Sincronização com `homologacao`, `dev/cleber` e `dev/jean` (2026-10-16)
Os 6 commits da `main` posteriores à sincronização de 02/10 (`839afeb`, `9e29c4f`, `99a9e38`, `49daae9`, `97230e0`, `a0028cb`) chegaram às três branches — a Limpeza de Histórico completa e as seções 22 e 23 deste documento. Nada de regra de negócio estava pendente: tudo o que a `main` ganhou até 29/09 já tinha ido na seção 21.
- **Fluxo:** `main` → `homologacao` (PR #19, merge `3526551`) e, a partir da `homologacao` já resolvida, → `dev/cleber` e `dev/jean` (um PR para cada, `sync/homologacao-para-dev-cleber` e `sync/homologacao-para-dev-jean`). Cada PR foi aberto com o destino fixo no link e mergeado como *merge commit*; o PR #19 foi liberado com *bypass* de revisão (a branch exige 1 aprovação e o autor não pode aprovar o próprio PR). Depois de simular tudo com `git merge-tree`.
- **Conflitos e como foram resolvidos:**
  - `homologacao`: `services/features.py` (a branch tem o menu reorganizado em grupos — mantida a árvore dela e acrescentado "Limpeza de Histórico" no grupo "Atualizar Execução Orçamentária", depois de NOB, mais o ícone `trash3` em `MENU_META`); `static/css/style.css` (ficaram os dois blocos); `templates/base.html` (rodapé em linhas da branch com o release `RELEASE_2_2026_10_15.2` e `?v=20261016.sync-main.2`); `docs/claude.md` (os dois lados só acrescentavam).
  - `dev/cleber`: só `docs/claude.md`. `features.py` e CSS entraram sozinhos, sem duplicar o item de menu (conferido, ao contrário do "Programar PTA/LOA" de 02/10).
  - `dev/jean`: `docs/claude.md` (o lado da `homologacao` já trazia a seção 21.6, então ficou só ele) e o rodapé do `base.html` (release novo + a linha própria `PATCH_DEV_SPO_2026_07_01.1`; o selo "Ambiente de integracao DEV Jean" não mudou).
- **Armadilha de edição (final de linha):** o repositório guarda **LF**; na pasta de trabalho os arquivos podem aparecer com **CRLF** por causa do `core.autocrlf` (confira com `git ls-files --eol <arquivo>`: `i/lf w/crlf` quer dizer índice em LF e pasta de trabalho em CRLF). Quem edita por script precisa **ler o final de linha do arquivo antes** e usar o mesmo nele: um `replace` com `\n` simples não casa num arquivo em CRLF e falha **sem erro**, e gravar LF onde havia CRLF mistura os dois. Nunca misturar LF e CRLF no mesmo arquivo. Foi o que aconteceu na primeira tentativa de 16/10/2026 com `services/features.py` e `docs/claude.md`.
- **Cuidado com o banco nos testes:** a pasta de trabalho precisa de uma cópia do `.env`, então testes e `create_app()` rodados ali apontam para o banco de **produção**. Foram rodados só `tests/test_limpeza_historico.py` (14 testes, tabelas temporárias) e `node --check`; depois de subir o app uma vez, conferido que nenhuma tabela nova foi criada. A `.venv` local não tem o `pdfplumber` das branches, então a suíte completa não rodou lá.
- **Limpeza:** as pastas de trabalho e as branches `sync/...` (locais e no GitHub) foram removidas. Conferido: as três branches não têm nenhum commit da `main` faltando.
- **Pendente em cada ambiente** (homologação, `dev/cleber`, `dev/jean`):
  1. Deploy + Restart da aplicação (`limpeza_historico_log` é criada completa pelo `db.create_all()`; não precisa de `ALTER`).
  2. `python scripts/atualizar_schema_sync_main.py` (sem argumentos e depois com `--aplicar`; seção 21.7) e conferir o login.
  3. **Backup do banco** antes da primeira limpeza.
  4. Na tela, simular e só então executar a limpeza, depois otimizar. Nesses ambientes o acúmulo é de ~33 GB juntos (cache do cPanel); o esperado é voltar para perto de 0,5 GB cada.
- Daqui em diante, correção feita direto na `main` volta a exigir nova sincronização (como na seção 21.9).

## 24. Sincronização `main` → `homologacao` aplicada no ambiente local + trava do Teto-SEDUC (2026-10-16)

### 24.1 Ambiente local atualizado
- Branch local `homologacao` levada por fast-forward até `origin/homologacao` (`3526551`, PR #19, 90 commits).
- Dependências novas do `requirements.txt` instaladas no Python global: `pdfplumber==0.11.7` (usado por `services/see_notes.py`; sem ele o `app` nem importa) e `pytest==8.3.4`. O projeto local não tem venv (o `.gitignore` já ignora `.venv/`).
- **Atenção:** o `.env` local aponta para o banco de homologação (`proj5954_spo_csg_homolog`). Testar localmente grava nos dados reais da homologação.

### 24.2 Banco da homologação (autorizado pelo usuário, executado pela IA)
- `python scripts/atualizar_schema_sync_main.py --aplicar` aplicou: `momp.uo` + índice `ix_momp_exercicio_uo` + 60 registros antigos marcados como UO 14101; 8 colunas novas em `plan20_seduc`. Segunda execução: "Nada a fazer".
- O script **não cria tabelas**. Comparando `db.metadata` com o banco, faltava só `limpeza_historico_log`, criada com `checkfirst` (o `db.create_all()` do `app.py` também a criaria ao subir). Depois disso não faltou nenhuma tabela.

### 24.3 Bug: trava do Teto-SEDUC ficava presa ("já existe um processamento em andamento")
- **Sintoma:** depois de um upload da UO 14101 concluído com sucesso, o upload da UO 14601 (outro nome de trava) falhava com "Já existe um processamento do Teto - SEDUC em andamento para este exercício e UO", após ~30 s.
- **Causa:** o `GET_LOCK` do MySQL pertence à **conexão** que o adquiriu. O código pegava e liberava o lock via `db.session`, mas o `db.session.commit()` devolve a conexão ao pool; o `RELEASE_LOCK` seguinte rodava em outra conexão e não liberava nada. O lock ficava preso numa conexão ociosa do pool (`IS_USED_LOCK` mostrou `teto_seduc:2027:14101` e `:14601` presos por conexões ociosas) até ela ser fechada.
- **Correção** (`rotas/home_routes.py`, `_start_teto_seduc_thread`): o lock usa uma conexão dedicada (`db.engine.connect()`); o `RELEASE_LOCK` roda nessa mesma conexão, que é fechada em seguida. A trava da Limpeza de Histórico já era feita assim.
- **Diagnóstico, se repetir:** `SELECT IS_USED_LOCK('teto_seduc:<exercicio>:<uo>')` (devolve o id da conexão dona) e `information_schema.processlist`. Reiniciar a aplicação fecha as conexões e solta o lock.
- **Validação:** usuário reenviou os arquivos localmente e confirmou que funcionou; 61 testes passam.

- **Sintoma:** depois de um upload bem-sucedido do Plan 23 ou Plan 134, um novo upload da mesma UO/exercício esperava 30 s e falhava com "Já existe um processamento do Teto - SEDUC em andamento para este exercício e UO".
- **Causa:** o `GET_LOCK` do MySQL pertence à **conexão** que o adquiriu. `_start_teto_seduc_thread` (`rotas/home_routes.py`) pegava e soltava a trava por `db.session`, e o `db.session.commit()` devolve a conexão ao pool. O `RELEASE_LOCK` rodava em outra conexão, não liberava nada, e a trava ficava presa numa conexão ociosa do pool até ela ser reciclada.
- **Correção:** a trava passou a usar uma conexão dedicada (`lock_conn = db.engine.connect()`), usada no `GET_LOCK` e no `RELEASE_LOCK` e fechada num `finally` interno. Se o `GET_LOCK` falhar ou a trava não for obtida, a conexão é fechada antes de propagar o erro. Nome da trava (`teto_seduc:{exercicio}:{uo}`), timeout de 30 s, mensagem de erro e o resto da lógica não mudaram.
- **Validação:** `python -c "import app"` e uma prova do mecanismo com uma trava descartável (sem gravar dado): com a trava presa na conexão dedicada e um `commit()` na sessão no meio, `IS_FREE_LOCK` continuou 0; depois do `RELEASE_LOCK` na mesma conexão, passou a 1.
- **Atenção:** conferido que nenhum outro ponto usa `GET_LOCK` por `db.session`; a Limpeza de Histórico (seção 23) já usava conexão própria. Se uma trava ficar presa em produção antes desta correção chegar, o Restart da aplicação no cPanel a libera (seção 23.9).

## 25. PLAN 51 — Relação Detalhada de Créditos Adicionais (2026-11-04)

Nova funcionalidade, igual em fluxo ao FIP 613/PED: **Atualizar → Execução → PLAN 51** recebe o `.xlsx` do FIPLAN (carga diária) e **Relatórios → Execução → PLAN 51** mostra o resultado tratado.

### 25.1 Como funciona
- **Tratamento:** `services/plan51_runner.py`, portado do script `td_plan51.py` (pasta `C:\Workspace\Automacao_excel`, fora do repositório). As funções do script foram mantidas como estão; o que é do sistema fica no fim do arquivo (`run_plan51`, validação do cabeçalho, gravação). Só modo TODOS. A saída `.xlsx` formatada vai para `outputs/plan51/` (a anterior vai para `tmp/`).
- **É do estado inteiro.** O relatório traz todas as UOs (o remanejamento entre UOs é o que ele mostra): não há UO única nem validação de UO, e a tela **abre sem nenhum filtro**. A UO de cada lado está em `UO SUPL`/`UO ANUL`.
- **66 colunas** (as da saída do script, com os mesmos nomes no Excel): fonte única `PLAN51_COLUNAS` (rótulo, coluna do banco, tipo). O model, a gravação, o relatório e o Excel baixado saem dela; `tests/test_plan51.py` confere que lista e model não se afastam.
- **Cabeçalho do arquivo:** `*Exercício igual a AAAA` (vira a coluna `exercicio`) e `FLAG_STATUS igual a ...`. Arquivo sem exercício, ou com status diferente de Efetivado, é **recusado antes de gravar** (`Plan51Recusado`; o envio não fica no histórico). **Atenção:** o FIPLAN imprime o status como nome (`Efetivado`, arquivos antigos) **ou como código (`5`, arquivo de 03/11/2026)**; os dois são aceitos (`STATUS_EFETIVADO`).
- **Conferência:** a soma de `VALOR SUPL`/`VALOR ANUL` é comparada com "Total Suplementado"/"Total Anulado" impressos no rodapé (arquivo de 03/11: R$ 17.378.126.545,30 e R$ 9.557.324.441,15, conferem). Só informa na mensagem, não bloqueia. **Não usar o `resumo` do script:** ele reaplica `_as_money` em número que já é decimal e dá cerca de 34 vezes o valor real; a carga usa `_somar`.
- **Versões antigas:** cada carga entra inativa e, no fim, **numa única transação**, desativa a versão anterior **do mesmo exercício** e ativa a nova (padrão do PED). Falha no meio apaga o que a carga inseriu e a versão anterior segue ativa. A remoção física das cargas desativadas é manual, pela **Limpeza de Histórico** (`plan51` está em `TABELAS`, coluna `exercicio`; o mês/ano escolhido vale para a data do download, `data_arquivo`). Não há cron de banco; o cron do cPanel (`cleanup_xlsx.js`) já varre `upload/plan51` e `outputs/plan51`.

### 25.2 Banco
- Tabelas novas `plan51_uploads` e `plan51` (73 colunas, índices `(exercicio, ativo)` e `upload_id`), criadas em 04/11/2026 com autorização do usuário; documentadas em `db/plan51_schema.sql`. Nenhuma tabela existente foi alterada.
- **Limite de linha do InnoDB (8.126 bytes):** o primeiro DDL falhou ("Row size too large") com `VARCHAR(60)` em 64 colunas. Ficaram `VARCHAR(16)` nas 39 colunas calculadas, `24` nas fixas, `40` nas de UO/projeto e `12` na data (maior valor real: 26 caracteres).
- **`db.create_all()` roda no `create_app()`**: um DDL inválido derruba a subida do app. Validar o DDL antes de qualquer deploy.
- Cada carga real tem ~12,7 mil linhas (cerca de 4,6 milhões por ano com carga diária): usar a Limpeza de Histórico todo mês.

### 25.3 Desempenho
- A primeira versão levava 457 s só para montar o Excel formatado: `ws.max_column` do openpyxl varre todas as células a cada chamada e o script o chamava dentro do laço de cada linha. Calculado uma vez (único desvio do script, comentado no código): **36 s**, saída idêntica.
- Outro problema: a transação aberta ao ler `upload_id` segurava uma conexão parada durante o tratamento e o servidor do banco a derrubava ("MySQL server has gone away"). `update_database` faz `rollback()` antes de gravar. Lotes de 100 linhas.
- Total por carga: **~65 s** (leitura 0,5 s, tratamento 14 s, Excel 36 s, banco ~15 s). Acompanhar online (LiteSpeed): se passar do limite de tempo da requisição, mover o Excel formatado para depois da gravação.

### 25.4 Telas e rotas
- `rotas/plan51_routes.py` (blueprint próprio): `/partial/atualizar/plan51`, `/partial/relatorios/plan51`, `/api/plan51/status`, `/api/plan51/upload`, `/api/relatorios/plan51` e `/download`. Features `atualizar/plan51` e `relatorios/plan51` (`services/features.py`, menu em `templates/base.html`): aparecem no painel de permissões sem nenhum perfil marcado, só o administrador acessa até serem liberadas.
- O relatório devolve as linhas como **listas** (não dicionários) para caber: 12,7 mil x 66 colunas. A tela (`initRelatorioPlan51`, derivada da do Plan20) monta os objetos. Totais do topo: Suplementado, Anulado e Líquido do que está filtrado; sem filtro, o estado inteiro.
- Detecção de layout no upload (`_detect_upload_layouts`): cabeçalho com `TIPO CRÉDITO` + `ELEMENTO SUPLEMENTADO` + `ELEMENTO ANULADO`. Um PLAN 51 enviado em outro relatório é recusado e o contrário também.

### 25.5 Testes
`tests/test_plan51.py` (19 testes; os de banco usam exercícios descartáveis 9991/9990 e limpam ao final): cabeçalho, recusas, divisão de linha com elementos diferentes, conferência de totais, as 66 colunas x model, detecção de layout, menu/permissões/Limpeza, troca de versão só do mesmo exercício, falha no meio e Limpeza enxergando as cargas. Ponta a ponta com o arquivo real (12.667 linhas): totais idênticos aos impressos, segunda carga substitui a primeira sem duplicar; tudo removido depois.

### 25.6 Cores e subtotais da planilha tratada (2026-11-04)
O relatório e o Excel baixado **reproduzem a planilha do script** (`salvar_saida`): as cores vêm de marcações calculadas no tratamento, e por isso passaram a ser guardadas.
- **Banco:** `plan51.estilo BIGINT` (autorizado pelo usuário; coluna nova na tabela nova), uma **máscara de bits por linha** (`BIT_*` em `services/plan51_runner.py`): FLAG15 (N. FIPLAN/DECRETO/DATA/DOE/TIPO repetidos: cinza + magenta), SUPL e ANUL (blocos repetidos: azul claro e lilás), SPLIT_ANUL e "split por dimensão" (cinza), PROX_FLAG15 (a linha de cima também em magenta) e DEF em vermelho por dimensão (13). Maior bit usado: 30, cabe nos 32 bits do JavaScript. `calcular_estilos` (style_info do script -> máscaras) e `style_info_das_mascaras` (inverso) são a ponte.
- **Tela** (`initRelatorioPlan51`): `p51ClasseCelula`/`p51ClasseCabecalho` aplicam as regras **na mesma ordem do script** (a última cor vence); cabeçalho azul nas colunas (DEF) e amarelo nas de N. FIPLAN/UO/FUNÇÃO/.../REGIÃO (SUPL e ANUL); as 39 colunas calculadas centralizadas; CSS `p51-*` com texto escuro fixo nas células preenchidas (legível no tema escuro). Rodapé `<tfoot>` com **SUBTOTAL (SUPLEMENT)**, **(ANULAÇÕES)** e **(LÍQUIDO)**, que acompanham os filtros como o SUBTOTAL do Excel.
- **Excel baixado:** é o **próprio arquivo gerado pelo script no upload** da versão ativa (cores, fórmulas `SUBTOTAL` e rodapé de origem). Se esse arquivo não existir mais, `gerar_excel_do_banco` refaz a planilha do banco com a mesma `salvar_saida` (~50 s).
- **Verificação:** com o arquivo real (12.667 linhas, 836.022 células), as regras de cor extraídas do `main.js` e executadas em Node deram **0 divergências** contra a planilha do script (fundo, fonte, centralização e cabeçalhos). Teste automático equivalente: `test_planilha_regerada_do_banco_e_igual_a_do_script`.
- **Cargas feitas antes da coluna `estilo`** ficam sem cor: refazer o upload (ou preencher a partir do arquivo guardado em `upload/plan51/`, como foi feito em 04/11/2026 para a carga de teste).
- **Trava por exercício:** o teste do usuário teve dois envios quase simultâneos (um terminou sem saída gravada; o estado final ficou correto). A gravação passou a rodar sob `GET_LOCK('plan51:<exercicio>')` numa conexão dedicada (`_trava_exercicio`), como no Teto-SEDUC (seção 24): o segundo envio espera o primeiro.

### 25.7 Formato contábil e fechamento (2026-11-04)
- **Excel baixado:** VALOR SUPL, VALOR ANUL e os três subtotais (SUPLEMENT, ANULAÇÕES, LÍQUIDO) usam o formato **Contábil sem "R$"** (`_-* #,##0.00_-;-* #,##0.00_-;_-* "-"??_-;_-@_-`), definido em `salvar_saida` (vale para o arquivo do upload e para o regerado do banco). Só entra em planilhas geradas depois da mudança; o arquivo de um upload antigo continua no formato anterior até novo upload.
- **Fonte do relatório HTML:** medido no navegador, igual ao PED (cabeçalho 10px, células 8px); a impressão de fonte maior vem do negrito (células magenta, cabeçalhos coloridos, rodapé) e da centralização das 39 primeiras colunas, como no script.
- **Pendência de conferência:** `FLAG_STATUS = 5` é aceito como "Efetivado" (além do texto).

## 26. API de dados para as equipes (2026-11-04)

Objetivo: quem consome a API (Google Planilhas, Power Query, Python) atualiza as planilhas sozinho, sem baixar Excel. Antes só existiam as rotas paginadas `/api/v1/dados/<rel>` (máx. 5.000 linhas, `offset`); o log mostrou só testes com `limit=20`.

### 26.1 O que existe agora (`rotas/api_dados_routes.py`, `services/api_dados_rotulos.py`)
- Relatórios: `emp`, `nob`, `ped`, `est-emp`, `fip613` e **`plan51`** (novo escopo `dados.plan51.read`, aparece na tela Usuários > API de Acessos; o PLAN 51 é do estado inteiro, sem filtro de UO).
- `GET /api/v1/dados/<rel>/export` — carga completa, CSV (padrão) ou `formato=json`. **Cabeçalho do Excel por padrão** (mesmos `rename_map` dos downloads; PLAN 51 usa `PLAN51_COLUNAS`); `colunas=banco` dá os nomes técnicos + `id`/`upload_id`. Aceita `uo`, `fields` e filtros por coluna (nome do banco **ou** rótulo). CSV UTF-8, vírgula, decimal com ponto, datas ISO. Cabeçalhos `X-Total`, `X-Upload-Id`, `X-Gerado-Em`, `X-UO`. Lê em lotes de 5.000 por `id` (sem OFFSET).
- `GET /api/v1/dados/<rel>/meta` — última atualização: `total`, `atualizado_em`, `upload_id`, `cargas[]` (linhas, `data_arquivo`, `enviado_em`). Não expõe e-mail de quem enviou.
- `GET /api/v1/dados/<rel>/schema` — coluna, rótulo e tipo.
- Rotas paginadas antigas **mantidas** e agora com `after_id` (use o `next_after_id` da resposta; exige `order_by=id`), `upload_id` e `next_after_id` na resposta; `total` vem vazio quando se usa `after_id`. `estilo` (PLAN 51) nunca é exposto.
- UO: sem `uo`, só a SEDUC (contrato anterior); `uo=all` ou `uo=26201,26202`.

### 26.2 Correção encontrada
`NobRegistro` não tinha `elemento`, `nome_elemento_despesa` e `fonte`, que **existem na tabela `nob`** (o runner Node as grava). Por isso a API antiga nunca devolveu essas colunas. Colunas adicionadas ao model (sem ALTER: já existiam no banco).

### 26.3 Medições (banco remoto, a partir do desenvolvimento)
Export completo: NOB 71.241 linhas ~8 s (~55 MB); EMP 57.180 ~7 s (~46 MB); FIP613 e PED < 1 s. Meta: ~1 s. Para tabelas grandes o manual manda filtrar por `exercicio` e escolher `fields`. Conferir o tempo online (LiteSpeed).

### 26.4 Testes e material
`tests/test_api_dados.py` (18 testes; cliente de API e carga do PLAN 51 descartáveis, removidos ao final; os demais relatórios só são lidos). `docs/api/manual_api_google_planilhas.pdf` (manual ilustrado passo a passo para Google Planilhas; fonte em `manual_google_planilhas.html`, código para colar em `google_planilhas_varios.gs` — lista `RELATORIOS`, uma aba por relatório, função `atualizarTodos`, falha isolada por aba, só baixa carga nova; a versão de um relatório só, `google_planilhas.gs`, foi removida), `docs/api/manual_api_dados.md` (referência técnica) e `docs/api/testar_api.py` (teste de ponta a ponta contra o ambiente: token, meta, schema, CSV x meta, paginação).

### 26.5 Limites conhecidos
Os valores saem como estão no banco (o Excel de FIP613 inverte o sinal de algumas colunas; a API não). A carga completa é montada em memória antes de responder (~55 MB na NOB).

### 26.6 Carga em lotes e cascata no Google Planilhas (2026-11-06)
- **API:** `/export` aceita `limit` (1 a 100000) e `after_id`. Com `limit` a resposta traz até essa quantidade de linhas (com cabeçalho) e os cabeçalhos `X-Next-After-Id` (vazio na última parte), `X-Linhas` (linhas da resposta) e `X-Total` (total do filtro, só na 1ª parte). Em JSON: `linhas`, `total` (1ª parte) e `next_after_id`. `after_id` sem `limit` dá 400. Sem `limit` continua entregando tudo, como antes. Medido: NOB 73 mil linhas em 5 partes de 15 mil (1ª ~6 s por causa do COUNT, demais ~1,3 s; ~12 MB cada).
- **Script** `docs/api/google_planilhas_varios.gs`: fila em cascata na ordem da lista `RELATORIOS`; baixa em lotes (`LINHAS_POR_LOTE`, 10000) numa aba temporária oculta `<aba> (novo)`; perto de 5 min grava o andamento em `ScriptProperties` (`CICLO`) e agenda `continuarAtualizacao` para 1 min depois; ao concluir confere contagem e a carga (`upload_id`), copia por valores para a aba final em blocos (sem apagar a aba, então fórmulas de outras abas continuam apontando) e apaga a temporária. Carga nova no meio: recomeça o relatório (máx. 3 tentativas). Falha em um relatório não derruba os outros; o erro sai ao final.
- **Pontos de entrada:** `verificarAtualizacoes` (acionador de tempo, só baixa o que mudou; é a função que o usuário agenda), menu **SPO** (`onOpen`): *Atualizar agora*, *Refazer tudo*, *Ver andamento*. `LockService` impede execuções simultâneas; ciclo parado há mais de 3 h é descartado.
- **Testes do script:** `node tests/apps_script/teste_gs.js` simula os serviços do Apps Script (planilha, UrlFetch, Properties, Lock, gatilhos, relógio) e cobre cascata em várias execuções, ordem, conferência de dados, nenhuma baixa sem carga nova, só o relatório alterado, carga mudando no meio, falha isolada, aba temporária apagada e ciclo em andamento. **Não foi executado no Google**: limites reais (tempo de gravação por lote, cota diária de gatilhos, 10 milhões de células) dependem de teste na conta da SEDUC.
- **Limites conhecidos:** durante a carga a aba temporária e a final somam o dobro de células (limite de 10 milhões por planilha): usar `exercicio=2026` na EMP e na NOB.

## 27. Painel inicial lento (menus sem carregar) e índices nas tabelas de relatório (2026-11-09)

### 27.1 Sintoma e causa
- **Sintoma:** ao logar, os menus não carregavam. No log, `/partial/dashboard` levava **28 a 33 s** em todos os acessos (as consultas individuais somavam milissegundos) e `/api/permissoes/current` chegou a 4,7 s. Cada reabertura da tela disparava outro painel de 30 s e as requisições se acumulavam.
- **Causa:** o trecho dos avisos da EMP (`partial_dashboard`, entre `ped_dotacao_missing` e `pendentes_raw`) chamava, para cada uma das 50 últimas cargas, `SELECT uo FROM emp WHERE upload_id = :id AND uo IS NOT NULL LIMIT 1` (`_uo_da_carga`, do ajuste das novas UOs de 04/11). A tabela `emp` tem 1,2 mi de linhas (só 60 mil ativas) e **só tinha a chave primária**: cada consulta varria a tabela (~0,7 s). 50 x 0,7 s = ~30 s. Reproduzido localmente: 32,3 s.

### 27.2 Correção no código (`rotas/home_routes.py`)
- `_uos_das_cargas(tabela, upload_ids)` substitui `_uo_da_carga`: uma ida ao banco para todas as cargas (`UNION ALL` de `SELECT ... LIMIT 1`, que com o índice para na primeira linha da carga) e **cache em memória** do que já foi descoberto (a UO de uma carga não muda). Carga sem linhas (removidas pela Limpeza de Histórico) não entra no cache e só é reconsultada após 120 s (`_UO_CARGA_SEM_LINHAS_TTL_S`), porque as linhas podem ainda estar sendo gravadas.
- O laço do EMP no painel lê os status das cargas primeiro e consulta a UO só das que não têm `uo` no status.
- **Lição de medição:** a primeira versão agrupava (`GROUP BY upload_id`, ótima sem índice: 1,1 s) e ficou **pior com o índice** (3,65 s, lê todas as linhas de cada carga). A versão final (`LIMIT 1` por carga) leva 0,03 s com o índice. **Ela depende do índice**: em ambiente sem ele (homologação, dev) o painel continua lento até criar os índices.
- Testes: `tests/test_dashboard_uo_carga.py` (4).

### 27.3 Índices criados no banco de produção (autorizados pelo usuário, 09/11/2026)
Script `db/indices_upload_id_ativo.sql` (com desfazer). Aplicados com `ALGORITHM=INPLACE, LOCK=NONE` e `lock_wait_timeout=60`:

| Tabela | Índices | Tempo |
|---|---|---|
| `est_emp` | `ix_est_emp_upload_id` | **300 s** (ver abaixo) |
| `ped`, `fip613` | `ix_<t>_upload_id` | 0,3 s cada |
| `emp` (1,2 mi, 3,5 GB) | `ix_emp_upload_id`, `ix_emp_ativo` | 4 s e 3 s |
| `nob` (1,5 mi, 3,8 GB) | `ix_nob_upload_id`, `ix_nob_ativo` | 5 s e 4 s |

- `plan51` já tinha `ix_plan51_upload_id` e `ix_plan51_exercicio_ativo`. `ativo` só foi indexado em `emp` e `nob` (só ~5% das linhas ativas; os relatórios filtram `ativo = 1`). Nas demais não compensa.
- **Episódio:** o índice da `est_emp` (86 MB) levou 300 s contra 0,3 s de `ped` e `fip613`. Provável espera por trava de metadados (outra consulta/transação usando a tabela); nesse tipo de espera, consultas novas podem ficar enfileiradas. Não foi identificado impacto, mas por isso as tabelas grandes foram feitas com aviso prévio aos usuários, uma por vez e com `lock_wait_timeout`.
- Os modelos (`EmpRegistro`, `NobRegistro`, `EstEmpRegistro`, `PedRegistro`, `Fip613Registro`) declaram os índices em `__table_args__` só para documentar o esquema: **`db.create_all()` não altera tabela já existente**. Em outro banco (homologação, dev) é preciso rodar o SQL.

### 27.4 Medições (do desenvolvimento, banco remoto)
| Consulta | Antes | Depois |
|---|---|---|
| UO das 50 cargas da EMP (painel) | 32,3 s | 0,03 s |
| contar linhas de uma carga (`upload_id`) | 0,7 a 0,8 s | 0,03 s |
| contar linhas ativas (`ativo = 1`) | 0,8 a 0,9 s | 0,03 s |
| API `/meta` EMP e NOB | 1,2 a 1,4 s | 0,5 a 0,6 s |
| API `/export` completo EMP e NOB | 7,4 a 8,4 s | 4,1 s |

### 27.5 Observações (não alteradas)
- `_fetch_all_with_retry` chama `db.engine.dispose()` a cada falha: com vários painéis concorrentes isso derruba conexões de outras requisições (no log apareceu `DB fetch retry 1/4 failed: tuple index out of range`). Candidato a ajuste futuro.
- A tabela `emp` tem 1,14 mi de linhas inativas (histórico): a Limpeza de Histórico reduziria o tamanho e o custo de qualquer varredura.
- Conferir no log após o deploy: `grep "path=/partial/dashboard" <log> | tail -5` (o `elapsed_ms` deve cair de ~30000 para poucos milhares).

## 28. Notas SEE (Área UENs > SAGE) (2026-11-09)

Funcionalidade portada da branch `dev/cleber` **sem merge**: código reescrito no padrão desta base (blueprint próprio, importação tardia do pdfplumber, permissões pelo Painel). Nada de dados do dev foi trazido (catálogos e produtos nascem vazios).

### 28.1 O que faz
Lê DANFEs em PDF, extrai nº da DANFE, DRE, escola e a **quantidade de cada produto de um catálogo** (por exercício) e gera um Excel consolidado (aba *Planilha Consolidada* com cores e legenda de conferência, *Dados_Extraídos* e *Auditoria_Arquivos*). Dois layouts de fornecedor: Somos (escola/DRE/COD nas Informações Complementares) e Athos (escola = destinatário, DRE em linha própria, sem código). A quantidade é montada pela distância ao código do produto na linha (`_row_around`, tolerância 1,5 pt) e conferida: quantidade x valor unitário deve bater com o total da linha, senão `QUANTIDADE_INCONSISTENTE`.

Colunas de conferência do Excel:
- *Planilha Consolidada*: coluna **"Conferência"** no fim (motivo do destaque amarelo/laranja da linha).
- *Dados_Extraídos* e *Auditoria_Arquivos*: **"Escola (nota)"** (nome como veio no PDF), **"Origem do código"** (`nota`, `cadastro (nome + município)` ou vazio) e "Conferência".
- Os dados do cadastro ficam em `see_processamento_arquivos.dados_extraidos_json["cadastro"]` (`origem_codigo`, `destaque`, `conferencia`).

### 28.2 Arquivos
- `rotas/notas_see_routes.py` (blueprint `notas_see_bp`): catálogo/produtos, envio em lotes, iniciar, status, último, histórico, cancelar, download e `_run_see_processamento` (o que o worker executa).
- `services/see_notes.py` (extração e Excel; `pdfplumber` é importado **dentro** de `extract_pdf`) e `services/see_escolas.py` (cadastro e identificação da escola).
- `worker.py --kind see` (novo tipo; chama `_run_see_processamento`, que grava status e falha nas tabelas `see_*`). `scripts/importar_escolas_see.py`.
- Tela: `templates/partials/notas_see.html`, `initNotasSee` em `static/js/main.js`, estilos `.see-*` no fim de `static/css/style.css` (definem `--accent-rgb` e `.see-page [hidden]`).
  - Card **"DANFEs Processados"** no topo da página: filtros **Exercício** e **Catálogo**; mostra só a **versão atual** (último processamento concluído) de cada **catálogo ativo** e permite baixar o Excel de novo (`GET /api/notas-see/processamentos/historico?exercicio=&catalogo_id=`).
  - Card **"Selecionar DANFEs"**: passo **"1. Catálogo a ser utilizado"** antes dos PDFs (sincronizado com o card de catálogo); os botões de pasta/arquivos ficam desabilitados até escolher um catálogo com produtos.
  - Pergunta **"Deseja acrescentar…"** só aparece quando há PDFs novos selecionados para um catálogo já processado.
  - Ao concluir um processamento iniciado na tela: o card de envio é **limpo** e abre um **modal** de conclusão (resumo e botão Baixar Excel).
- Menu **Área UENs > SAGE > Notas SEE**; feature `area-uens/sage/notas-see` (grupo SAGE só visual, como o *Planejamento* dos painéis). **Só o admin (nível 1) acessa**: aparece no Painel de permissões para liberar aos demais perfis depois.
- `requirements.txt`: `pdfplumber==0.11.7` (traz `pdfminer.six`, `Pillow`, `pypdfium2`).

### 28.3 Fluxo de envio (limite de 1000 partes do Werkzeug)
`POST /api/notas-see/processamentos` (cria, status `enviando`; modo *acrescentar* copia as notas do processamento anterior) -> `POST .../<id>/arquivos` (lotes de até 50 arquivos ou 20 MB; duplicados por SHA-256 também entre lotes; não-PDF e conteúdo sem `%PDF` são ignorados e listados) -> `POST .../<id>/iniciar` (status `na_fila`, dispara o worker). Envio interrompido ou sem arquivos válidos: `descartado` e os PDFs são apagados. Sem a biblioteca pdfplumber, `iniciar` responde **503** com mensagem clara e o resto do sistema segue normal (no dev o `import` no topo derrubava o app inteiro).

### 28.4 Banco
- Oito tabelas `see_*`. **Já existiam, vazias, desde 02/10/2026** (criadas por uma versão antiga do modelo), e `db.create_all()` não altera tabela existente. `db/notas_see_schema.sql` (aplicado em 09/11/2026, tabelas vazias, conferido antes): coluna `see_catalogos.exercicio`, unicidade `uq_see_catalogo_exercicio_nome (exercicio, nome)` no lugar de `nome`, e índices `ix_see_processamentos_catalogo_status`, `ix_see_proc_arq_proc_sha`, `ix_see_ocorr_proc_codigo`. Os demais já têm índice nas colunas de ligação (chaves estrangeiras).
- **Não usar `db/see_catalogo_exercicio.sql`.** É um script antigo (da `dev/cleber`, 01/10/2026) e **não idempotente**: faz `ADD COLUMN` e `DROP INDEX` sem conferir antes. O script certo é `db/notas_see_schema.sql` (usa `IF [NOT] EXISTS`). O arquivo antigo fica no repositório só como histórico.
- Aprendizado: o primeiro `ALTER` deu *Lock wait timeout* porque a própria sessão da ferramenta havia feito `SELECT COUNT(*)` nessas tabelas e ainda segurava o lock de metadados. Antes de DDL: `db.session.rollback()`/`close()`.
- Cadastro de escolas: `scripts/importar_escolas_see.py` rodado com a aba `2026` de `C:\workspace\Planilhas\Antigos\RELATÓRIO_2023_A_2026 MATRÍCULAS_2026.05.12==2026.08.24.xlsx` -> **643 registros** (630 escolas + 13 códigos de DRE). Reimportar a cada ano; o que sair da planilha fica inativo (não é apagado).

### 28.5 Verificação com as 641 DANFEs reais do 3º bimestre/2026
Extração + identificação de escola em todas as notas, com catálogo sintético de 40 códigos: **0 falhas, 8.814 itens, 551 códigos conferidos na nota, 79 preenchidos pelo cadastro, 11 notas com alerta** (9 `ESCOLA_SUGESTAO`, 1 `ESCOLA_NAO_ENCONTRADA`, 1 `ESCOLA_MUNICIPIO_DRE_INCONSISTENTE`) — **idêntico ao resultado documentado no dev**. Tempo: **177 s para 641 PDFs (0,28 s por PDF)** no computador de desenvolvimento; no servidor compartilhado deve ser maior (medir no primeiro uso).

**Investigação dos 11 alertas (09/11/2026): nenhum é falha de leitura; todos são dados da própria nota** (DANFEs 439xx/440xx, layout Athos, sem código de escola):
- 9 `ESCOLA_SUGESTAO`: o destinatário vem sem o título que o cadastro tem (PROFESSORA, DESEMBARGADOR, PREFEITO, VEREADOR...) ou com variação (sobrenome a mais em 44024; grafia CAVALCANTI/CAVALCANTE em 44031). A sugestão do sistema estava correta nos 9.
- 44021 (EE POXOREO): a nota traz o município GUARANTÃ DO NORTE, mas a escola e a DRE são de Poxoréu/Primavera do Leste (erro do fornecedor) -> `ESCOLA_MUNICIPIO_DRE_INCONSISTENTE`.
- 44001 (EE SAGRADO CORAÇÃO DE JESUS, CNPJ 02.042.787/0001-80, município DENISE): o cadastro não tem essa escola em Denise (existe em Sapezal, na mesma DRE) -> `ESCOLA_NAO_ENCONTRADA`; conferir o cadastro de matrículas ou a nota.
- **Decisão: não alterar a regra.** Os alertas são erros de digitação do fornecedor e **devem** ser vistos pelo fiscal; o aviso na planilha já cumpre o papel. Mantida a regra herdada do dev: nome apenas parecido nunca preenche, só sugere. Ideias descartadas por ora: ignorar título na comparação, fallback por DRE e tabela de apelidos confirmados.

### 28.6 Testes (`tests/test_notas_see.py`, 16)
Cadastro DRE/Escola (código válido, DRE divergente, sem código só com uma escola de mesmo nome, nome parecido = sugestão, inativa), Excel (abas, cores, legenda), PDFs reais (`SEE_PDFS_DIR`; pulados se a pasta não existir), fluxo de lotes/duplicados/descarte/503 e um **processamento completo** (3 PDFs reais -> Excel -> PDFs apagados). Usam o catálogo descartável do exercício 2099 e removem tudo ao final.

### 28.7 Decisões e cuidados
- **PDFs apagados ao concluir** (`_see_limpar_pdfs`): ficam o Excel e os dados extraídos. O *acrescentar* herda as linhas já extraídas e não relê PDFs; o cron de limpeza só varre `.xlsx`. Para manter: `SEE_MANTER_PDFS=1`.
- *Acrescentar* ainda cria uma versão nova com **cópia completa** das notas anteriores (no dev: 19 versões no 2º bimestre, ~86% duplicado). Mantido igual; limpeza de versões antigas fica para depois.
- Não há tela de cadastro DRE/Escola (decisão herdada do dev: aguardar o cadastro de matrículas do outro projeto).
- Sem filtro de UO: é uma funcionalidade da SEDUC (escolas), independente das UOs de execução.

- **Código do produto só com dígitos**: o cadastro (criar/editar) remove tudo que não é número (`936.360` -> `936360`) e a extração compara só dígitos. Antes, um código com ponto nunca casava com o da nota e saía "produto não encontrado" sem erro.
- **Ordem dos produtos** (`_see_produto_ordem`, em `rotas/notas_see_routes.py`): alunos (`_al`) antes de professores (`_pr`); em cada grupo, reg, eja, esp e art; em cada tipo, pela série numérica (1º, 2º... 12º). Nomes fora do padrão `série_tipo_al/pr` vão ao fim, por código. Vale para a lista do catálogo, para as colunas da aba *Planilha Consolidada* e para a aba *Catálogo_Produtos* do Excel (planilhas antigas não mudam; reprocessar).
- **Catálogo vazio**: o aviso "ainda não tem produtos..." é neutro (não é erro) e os botões de envio ficam desabilitados até haver um produto; a rota de envio também recusa catálogo sem produtos ativos.
- **Catálogo do 3º bimestre/2026 importado** do dev em 09/11/2026 (33 produtos ativos, códigos só com dígitos) para o catálogo "3º Bimistres 2026" (id 13). Teste real: 27 notas, 0 alertas, 0 erros, 26 s.
- **Layout (menu expandido/recolhido)**: seletor de catálogo e botões Novo/Editar/Desativar empilhados (`.see-catalog-pick`, botões com quebra de linha) e quadros de números do Processamento em `auto-fit` (`.see-stats`), para não passarem da margem do card. `?v=` de `main.js`/`style.css` trocado para `20261109.notas-see.1`.

### 28.8 Deploy (checklist)
1. `pip install -r requirements.txt` no Python do cPanel (instala `pdfplumber`, `pdfminer.six`, `Pillow`, `pypdfium2`); conferir `python -c "import pdfplumber"` **antes** de usar a tela.
2. Os ajustes de banco (`db/notas_see_schema.sql`) e a carga das escolas **já foram aplicados** no banco de produção (compartilhado com o local); em outro banco, rodar o SQL e o importador.
3. Update from Remote + Deploy HEAD + Restart. Trocar o `?v=` de `main.js`/`style.css`.
4. Painel de permissões: liberar *SAGE - Notas SEE* aos perfis desejados (por enquanto só admin).
5. Primeiro uso: cadastrar o catálogo (exercício + produtos), enviar uma pasta pequena e conferir 5 a 10 notas contra o PDF. Fornecedor com layout novo: processar amostra antes de confiar em DRE/escola.

> **Não existe seção 29.** A numeração pula de propósito: renumerar mudaria as referências às seções 30 e 40, citadas em outros documentos e nas instruções de trabalho.

## 30. REGRA: merges entre branches fixas usam SEMPRE branch temporária (2026-10-16)

### 30.1 O que aconteceu
Ao levar a correção da trava (seção 24) para `dev/cleber` (PR #22) e `dev/jean` (PR #23), os PRs foram abertos com a própria `homologacao` como origem. Depois de cada merge o GitHub apagou a branch `homologacao` do remoto (apagamento automático de branches ou botão "Delete branch"). Nos syncs anteriores (seção 21) isso não aparecia porque a origem era uma branch temporária `sync/...`, que podia ser apagada sem prejuízo.

### 30.2 Recuperação
A `homologacao` foi recriada no remoto com `git push origin homologacao`, a partir da pasta local, que estava em `d0bec71` (mesma versão da homologação). Não foram usadas as versões de `dev/cleber` ou `dev/jean`, porque têm funcionalidades diferentes.

### 30.3 Regra daqui em diante
- **Nunca** abrir PR de/para `dev/cleber`, `dev/jean`, `homologacao` ou `main` usando uma delas como **origem** (head) de um merge.
- Sempre criar uma branch temporária a partir da origem (ex.: `sync/homologacao-para-dev-jean`), abrir o PR dessa temporária para o destino e, depois do merge, apagar **só a temporária**.
- Link de PR sempre com o destino fixo: `/compare/<destino>...<temporaria>`; conferir o campo *base* antes de mergear (seção 21.9).
- Em Settings → General → Pull Requests, manter "Automatically delete head branches" **desligado** (branches fixas nunca devem ser apagadas); ao mergear, não clicar em "Delete branch" se a origem for uma branch fixa.
- Se uma branch fixa sumir do remoto: conferir que a cópia local está no commit certo (`git rev-parse <branch>`) e recriar com `git push origin <branch>`; nunca recriar a partir de outra branch que tenha funcionalidades diferentes.
- Fluxo completo de desenvolvimento entre os dois desenvolvedores (branches `feat/`, squash, cherry-pick para a homologação e a `main`, arquivos que não se toca): seção 40.

### 30.4 Desfecho do dia (2026-10-16)
- Correção da trava do Teto-SEDUC presente em `homologacao`, `dev/cleber` (PR #22) e `dev/jean` (PR #23).
- Correção também aplicada na `main` pelo usuário, diretamente no projeto dela, por prompt só com o trecho da trava (sem trazer nada da `homologacao`) — informado como concluído e executado.
- Pendências de banco das `dev/*` (`scripts/atualizar_schema_sync_main.py`) informadas como concluídas pelo usuário.
- Apagamento automático de branches do GitHub desativado pelo usuário.

## 31. Novas UOs na homologação: ajustes, teste da 26101 e correções (2026-10-26 a 2026-11-03)

Tudo nesta seção foi feito e testado no ambiente de **homologação** (banco `proj5954_spo_csg_homolog`). Nada foi levado para a `main`. O plano e a lista de decisões (D1 a D8) estão em [plano_implementacao_novas_uos.md](plano_implementacao_novas_uos.md).

### 31.1 Estado do código
- **Fases 1 e 2 do plano** entraram na `homologacao` pelo PR #25 (commit `382dd2a`): desativação da carga anterior só da **mesma UO e do mesmo exercício** em FIP 613, PED, EMP, NOB e EST EMP; validador de UO nos uploads (recusa arquivo com mais de uma UO, cabeçalho diferente das linhas ou UO não cadastrada); recálculo da dotação limitado à UO do arquivo; Plan20 com lista de UOs liberadas; painéis, relatórios e API v1 fixos na SEDUC.
- **Duas listas de UO** em `services/uo.py`: `UOS_EXECUCAO` (possíveis: SEDUC + 26101/26201/26202) e `UOS_EXECUCAO_LIBERADAS` / `UOS_PLAN20_LIBERADAS` (as que aceitam upload). O Node recebe as listas do Python pelo `worker.py` (variáveis `UOS_EXECUCAO`, `UOS_EXECUCAO_LIBERADAS`, `UOS_SEDUC`), então só existe uma fonte.
- **Liberação temporária da 26101** (commit `20494fc`): as duas listas `*_LIBERADAS` incluem `26101`, com o comentário "TESTE EM HOMOLOGACAO". **Reverter antes de sincronizar com a `main`**, a não ser que a confirmação oficial já tenha saído e o usuário decida liberar de vez (nesse caso, o plano manda trocar por `set(UOS_EXECUCAO)`).

### 31.2 Como a aplicação sabe qual UO usa Chave de Planejamento
- **Lista fixa:** `UOS_SEDUC = {"14101", "14601"}` em `services/uo.py`; a função `uo_usa_chave(uo)` só confere se o código (5 dígitos) está nela. O Node espelha com `uoUsaChave()` (`node_runners/execucao_uo.js`), alimentada pela variável `UOS_SEDUC`.
- **Vale para todas as regras de chave** (decisão D2): `chaves_planejamento`, `chave_arrumar`, `forcar_chave` e o fallback do PAOE 8026. UO fora da lista fica "NÃO IDENTIFICADO" e **nunca** recebe chave.
- **A decisão é pela UO da linha, não pelo preenchimento da chave.** Um EMP/PED da SEDUC sem chave continua gerando aviso.
- **Limite:** a tabela `chave_planejamento_regra` não tem coluna de UO. Se outra UO passar a ter chave, ela entra em `UOS_SEDUC` ou (etapa 2 da decisão D2a) troca-se o interior de `uo_usa_chave` por um cadastro de UO com indicador "usa chave" e acrescenta-se a coluna `uo` às regras.

### 31.3 Correção de chave (`chave_arrumar`) no EMP
- **Sintoma:** empenho da SEDUC continuava "NÃO IDENTIFICADO" mesmo com regra cadastrada em "Correção de Erros de Chave".
- **Causa:** a regra tinha a chave **correta** em origem e destino. A **origem** precisa ser o texto **errado**, como aparece no Histórico do empenho (o runner procura a origem dentro do Histórico, ignorando maiúsculas, acentos e espaços ao redor dos asteriscos); o destino é a chave certa, com o número de partes do exercício (8 em 2026 ou mais, 7 antes; regra com destino de outro tamanho é ignorada).
- **Exemplo resolvido:** origem `* 122.6 * GAB * GESTÃO_INOVAÇÃO * P_GESTÃO_ * E_GESTÃO_INTEGRADA * _GESTÃO_INTEGR * XII *`, destino com `R9900` na frente. Depois de cadastrar, é preciso **reprocessar** (a regra só vale no processamento).
- **Outras regras suspeitas vistas no banco:** a regra 678 tem origem `* 9900 * …` (sem o `R`) e não casa com nada; a 851 tinha origem igual ao destino (inútil).

### 31.4 Erro "Cannot find module 'exceljs'" nos relatórios em Node (EMP e NOB)
- **Causa:** as dependências Node (`node_runners/package.json`: `dotenv`, `exceljs`, `mysql2`, `mssql`) não são instaladas pelo deploy (o `.cpanel.yml` só copia arquivos; ver seção 8). Aconteceu no servidor da homologação e depois no ambiente local.
- **Correção:** `cd node_runners && npm install --omit=dev`, no servidor e na máquina local. O `npm install` reescreve o `package-lock.json` inteiro (diferença de fim de linha): **não commitar essa alteração** (`git checkout -- node_runners/package-lock.json`). O `node_modules` é ignorado pelo git.
- **Lembrete:** o app local usa o banco da homologação; um upload feito localmente grava nele.

### 31.5 Teste real da 26101 na homologação
- **FIP 613:** carga ok. Banco: 263 linhas ativas da 26101 e 1.670 da SEDUC; a carga da 26101 **não** desativou a da SEDUC; os zeros das UGs foram preservados (`0000`, `0001`). Dotação Inicial 135.070.428,00, igual ao teste de 13/10; Empenhado e Pago diferem do documento de testes porque o arquivo usado é mais novo (26/10).
- **PED:** a 26101 carregou 29 registros e gerou 29 avisos de "chave não identificada" (ver 31.7), que eram esperados e não eram erro.
- **EMP:** a 26101 carregou (3.214 linhas sem chave, também esperado) depois de resolvido o erro de `exceljs` (31.4).
- **Ainda não testados com a 26101 na homologação:** EST EMP, NOB e Plan20.

### 31.6 Filtro de UO nos relatórios (só visualização)
- **Decisão do usuário:** em vez de um seletor separado (feito e desfeito), aproveitar a **coluna UO** que os relatórios já têm.
- **Como ficou** (FIP 613, PED, EMP, EST EMP e NOB): o backend devolve os dados de **todas as UOs liberadas**; a tela abre com a coluna UO **pré-selecionada na SEDUC** (`uos_padrao` na resposta da API), então os totais continuam só da SEDUC (D5); marcar outra UO no filtro da coluna inclui os dados dela; "(Todos)" soma tudo. O filtro da coluna UO sempre lista todas as UOs carregadas. "Limpar Filtros" volta à SEDUC. Funções em `rotas/home_routes.py`: `_uo_liberadas`, `_uo_relatorio_download`, `_sql_uo_download`, constante `SQL_UO_LIBERADAS`.
- **"Baixar Excel":** respeita as UOs marcadas na tela (parâmetro `?uo=14101,26101`); sem nenhuma marcada, leva todas as liberadas. **A ordem das colunas do Excel não mudou.**
- **Coluna UO na primeira posição** (PED, EMP, EST EMP, NOB; no FIP 613 já era): só na tela (cabeçalho, linha de filtros e células).
- **Coluna "Nome da Unidade Orçamentária" oculta** na tela de PED, EMP e EST EMP (continua no Excel e no banco).
- **Painéis, dashboards, dotação e API v1** continuam fixos na SEDUC.
- **Painel "Última atualização" por UO** (FIP 613, PED, EMP, EST EMP, NOB **e Plan20**): o bloco do topo mostra uma coluna por UO com "Enviado por", "Upload em" e "Data do download" da **carga ativa de cada UO** (campo `ultimas_por_uo` da API; `_ultimas_cargas_por_uo(tabela, modelo_upload)` em `rotas/home_routes.py`, que lê a UO das linhas gravadas porque as tabelas de upload não guardam UO). No Plan20, que não guarda o id do upload em `plan20_seduc`, a UO vem do texto de `unidade_orcamentaria` ("14.101 - ...") e a carga mais recente é a de maior `data_atualizacao` (`_plan20_ultimas_por_uo`); por isso o "Upload em" do Plan20 pode diferir em segundos do registro do upload. Sem dados por UO, a tela cai no último upload, como antes. O renderizador é único: `renderUltimasAtualizacoes` em `static/js/main.js`.
- **Layout compacto do topo** (mesmos seis relatórios, classe `relatorio-top-uo` no template, CSS em `static/css/style.css`): as duas metades dividem a linha (`flex-wrap: nowrap`, `flex: 1 1 0`; abaixo de 900 px empilham), os totais ficam em duas colunas e os botões "Limpar Filtros"/"Baixar Excel" ao lado deles. Só esses relatórios usam a classe; os demais (Plan21 etc.) continuam como eram.

### 31.7 Aviso de chave só para UO que usa chave
- **PED** (`_find_missing_planejamento_lines` em `services/ped_runner.py`) e **EMP** (`node_runners/emp_runner.js`): linhas de UO que não usa chave deixam de entrar na lista "Chave do Planejamento não identificada". Teste novo em `tests/test_ped_uo.py`.

### 31.8 Modal de Avisos do dashboard agrupado por UO
- **Problema:** o modal lia só o **último** job de PED e o **último** upload de EMP, de qualquer UO. Um upload da 26101 apagava os avisos da SEDUC, e o contrário também.
- **Como ficou** (`rotas/home_routes.py`, função do dashboard, e `templates/partials/dashboard.html`): vale a **última carga de cada UO** (até 50 jobs de PED e 50 uploads de EMP), e o modal mostra um bloco por UO ("UO 14101 - SEDUC", "UO 26101 - SECITECI") com os quatro avisos: dotação e chave, de PED e de EMP. UO sem aviso não aparece.
- **UO gravada em cada carga:** PED passa a gravar `uo` em `detalhes_alertas` **sempre** (mesmo sem avisos, para uma carga limpa apagar só os avisos da própria UO); `run_ped` agora devolve 5 valores (o último é a UO); o EMP grava `uo` no arquivo de status (`services/job_status`). A contagem de alertas do `worker.py` só soma listas.
- **Cargas antigas sem UO gravada:** a UO é lida das linhas que a carga inseriu (`ped`/`emp` por `upload_id`); se não houver linhas (por exemplo, removidas pela Limpeza de Histórico), vale a SEDUC.

### 31.9 Pendências e cuidados
1. **Commits:** os ajustes das seções 31.6 a 31.8 foram commitados e enviados para a `homologacao` em 2026-11-03, junto com o release do rodapé (`templates/base.html`: `RELEASE_2_2026_11_03.1` e, no commit seguinte, com o painel por UO e o layout compacto, `RELEASE_2_2026_11_03.2`). Como `rotas/home_routes.py` mistura os filtros de UO dos relatórios e o modal de avisos, o código foi em **um** commit; a documentação e o release, em outro. O `?v=` do `main.js`/`style.css` no `base.html` **não** foi trocado (continua `20261016.sync-main.2`): se alguém não vir as telas novas, limpar o cache do navegador ou trocar esse número.
2. Testar EST EMP, NOB e Plan20 com a 26101 e depois as 26201/26202 (liberar na ordem do plano: EST EMP → FIP 613 → PED → NOB → EMP); conferir após cada carga que a SEDUC não mudou.
3. Reverter a liberação temporária da 26101 (31.1) antes de qualquer sincronização com a `main`.
4. A coluna `uo` nas regras de chave e o cadastro de UO com "usa chave" (31.2) só serão feitos quando alguma UO nova tiver chave.
5. **Regra de trabalho confirmada nesta sessão:** commit e push só com autorização explícita do usuário para aquele conjunto de mudanças; a autorização de um não vale para o seguinte.

## 32. Integração do layout novo, da homologação e da dev/cleber na main (2026-11-10)

Plano e decisões: `docs/plano_migracao_branches.md`. Feito na branch temporária `integracao/layout-novo` (merge de `origin/homologacao` e depois de `origin/dev/cleber`; tags de backup `backup/*-2026-11-10` no GitHub). A dev/jean fica para o fim, sem entrar na main.

### 32.1 Veio da homologação / dev/cleber
Layout novo (casca, cores de destaque, temas claro/escuro/automático, responsivo, `login-theme.js`), **menu gerado de `services/features.py`** (`templates/_menu.html`, `MENU_META`; nomes e posições novos: Administrar SPO, Emitir/Ajustar Dotação, grupos Planejamento/Execução), Personalizar SPO, Governança e Resultados, PTA 2027, **permissão por usuário** (`usuario_permissoes`, painel novo), jobs com cancelar/reprocessar (`processamento_jobs`) para PED, EST EMP e FIP 613, rótulo de ambiente (`APP_ENV_LABEL`, vazio em produção), `scripts/atualizar_schema_sync_main.py`, **Receita Anexo 10** e `fiplan_file_reader`. O que é novo fica **só para o admin** (nível 1) até ser liberado no Painel - Permissões.

### 32.2 Mantido da main (não substituído)
PLAN 51 (tela de atualizar e de relatório com o cabeçalho novo), API de dados + Google Planilhas, painel inicial rápido (`_uos_das_cargas`) e índices (`db/indices_upload_id_ativo.sql`), **Notas SEE em blueprint próprio** (`rotas/notas_see_routes.py`, DRE/Escola, catálogo por exercício; as versões antigas dentro do `home_routes.py` e do `main.js` foram removidas), modelos `see_*` da main, atalho do admin e `.cpanel.yml` simples.

### 32.3 Decisões e cuidados da integração
- **Atalho do admin (nível 1)** mantido: `has_permission`, `require_feature` e `_permissoes_with_parents` (admin recebe todas as funcionalidades, inclusive as novas ainda não marcadas). `has_permission` filtra `ativo = 1`.
- **UO 26101**: a liberação temporária da homologação (`services/uo.py`) **foi revertida**; produção continua só com a SEDUC (seção 22).
- `scripts/deploy_cpanel.sh` (rsync `--delete` sem excluir `upload/`, `outputs/`) e o `.cpanel.yml` da homologação **não vieram**; o `CLAUDE.md` da dev/cleber também não.
- Conflitos resolvidos: `home_routes.py` (jobs/avisos por UO da homologação + versão rápida do dashboard da main), `worker.py` (`--kind` fip613/ped/est_emp/receita_anexo10/see), `features.py` (PLAN 51 e Receita Anexo 10 no menu novo), `base.html` (release da main no rodapé novo; `?v=` novo), relatórios (cabeçalho novo), `claude.md` (seções da homologação renumeradas para 30 e 31).
- Visual conferido em Chrome headless com o servidor local (admin e perfil SAGE): Início, Notas SEE, PLAN 51 (atualizar e relatório), Painel de permissões, Relatório EMP.
- **Contrato Visual Protegido** (Administrar SPO > Personalizar SPO): a tela aparecia vazia para o admin porque `canManageProtectedContract()` (main.js) só liberava quem tivesse "jean" ou "cleber" no nome/e-mail e removia o cartão dos demais. Agora o acesso é só pela permissão do servidor (`atualizar/personalizar-spo/contrato-visual-protegido`; admin sempre passa). Não há mais nenhuma trava por nome no código.
- **Cor de destaque padrão = a do layout antigo** (verde-azulado, `#0f7c68`; escuro `#3dd6b7`): nova opção **"Clássico (padrão SPO)"** (`classic`) em `ACCENT_COLORS` (`main.js`), `login-theme.js` e nos valores padrão do CSS (`:root`/`.theme-dark`), para o primeiro acesso não ter tanto impacto. É a primeira do seletor; as demais seguem a ordem do espectro (vermelho, laranja, amarelo, verde, turquesa, azul, roxo; "Verde padrão SPO" virou "Verde", agora um verde-folha `#33b865`, e "Turquesa" virou "Ciano" `#19a7cc`, para não se confundirem com o Clássico; as chaves `green` e `teal` não mudaram, então escolhas já gravadas continuam valendo). A escolha de cor/tema fica só no `localStorage` de cada navegador (`app-accent`, `app-theme`): quem já escolheu outra cor mantém; quem nunca escolheu vê o Clássico. Para mudar o padrão de todos: constante `DEFAULT_ACCENT` no `main.js` (e `login-theme.js`/CSS).
- **Seletor de cores em degradê** a partir do Clássico: Clássico, Ciano, Azul, Roxo, Vermelho, Laranja, Amarelo, Verde (fecha o círculo, o Verde termina ao lado do Clássico). E o trilho vertical do submenu "Administrar SPO" no tema escuro era um ciano fixo (`rgba(139,225,255,..)`, mais hover/ativo `82,177,210`) que ignorava a cor escolhida; agora usa `--accent-rgb`, como no tema claro.
- **"Administrar SPO" sem destaque ao passar o mouse**: as regras `.menu-admin-spo > .menu-parent` (fundo e borda transparentes, mesma especificidade do `.menu-item:hover` e declaradas depois) anulavam o `:hover` só desse item. Acrescentada regra de hover (claro e escuro) no fim do `style.css`.
- Correção visual: quadro de totais do relatório PLAN 51 (valores em bilhões) cresce em vez de quebrar os números.

## 33. Tempo de inatividade configurável e sessão derrubada por login em outro local (2026-11-10)

### 33.1 Situação anterior
- O prazo de 2 horas estava fixo em dois lugares (`app.py` e `rotas/auth_routes.py`) e ainda em `home_routes.py` (lista de sessões ativas do painel).
- Login forçado em outro local troca o token em `active_sessions`, mas o navegador antigo só era derrubado quando fazia uma requisição que chegasse ao banco. Casos em que parecia "funcionar normalmente":
  1. cache de 15 s da verificação (`ACTIVE_SESSION_CHECK_TTL_S`);
  2. só `loadPage` (troca de página) tratava o 401; as outras ~116 chamadas `fetch` do `main.js` falhavam sem redirecionar;
  3. telas já carregadas que não chamam o servidor (dashboards, iframe do PTA 2027);
  4. tolerância a falhas do banco (`keeping cached session`);
  5. `_ensure_active_session` **recriava** a linha de `active_sessions` com o token do navegador antigo quando ela não existia (por exemplo, após logout em outro local).

### 33.2 Prazo configurável
- Tabela `configuracoes_sistema` (chave/valor; modelo `ConfiguracaoSistema`; `db/configuracoes_sistema.sql`, e o `db.create_all()` também cria). Chave `sessao_timeout_minutos`; sem linha vale o padrão de **120 min**. Limites: **15 a 720 min** (12 h).
- `services/session_config.py`: `get_session_timeout()` / `get_timeout_minutos()` com cache de 30 s (outros processos do servidor enxergam a mudança em até 30 s; em falha do banco mantém o último valor), `set_timeout_minutos()` (valida, grava quem/quando alterou), `get_info()`. É a **única fonte** do prazo: `app.py`, `auth_routes.py` (conflito de login) e `home_routes.py` (painel de sessões) usam ela.
- Datas gravadas em UTC; a tela exibe no horário de Mato Grosso (`America/Cuiaba`, `_formatar_local`).
- Tela **Administrar SPO > Sessão e Login** (`atualizar/seguranca-sessao`, `templates/partials/atualizar_seguranca_sessao.html`, `initSegurancaSessao` no `main.js`); API `GET/POST /api/configuracoes/sessao` com `require_feature`. Nova funcionalidade: o admin (nível 1) vê sempre; outros perfis só se liberada no Painel de Permissões.

### 33.3 Detecção de sessão encerrada
- Endpoint `GET /api/sessao/status` (`auth_routes.py`): 200 `{ok, timeout_minutos}` ou 401 `{ok:false, reason}` com `reason` = `outro_local` (token diferente), `expirada` (inatividade), `encerrada` (linha de sessão inexistente) ou `sem_sessao`.
- No `before_request` (`app.py`), esse caminho (`HEARTBEAT_PATH`) **não usa o cache de 15 s** e **não renova `last_activity`** (a verificação automática não pode contar como atividade, senão a sessão nunca expiraria).
- `main.js` (`sessionGuard`, no topo): verificação a cada 45 s, ao voltar para a aba e ao focar a janela; e um `window.fetch` global que, ao receber 401 de uma chamada interna, confirma no servidor e redireciona. Redireciona para `/login?motivo=...`, e a tela de login mostra a mensagem (`MOTIVOS_SESSAO`). Falha de rede não derruba o usuário.
- **Mudança de comportamento:** linha de `active_sessions` inexistente (consulta bem-sucedida) agora **encerra** a sessão em vez de recriá-la. Falha da consulta (`_FETCH_ERROR`) continua mantendo a sessão do cache por tolerância.

### 33.4 Testes e implantação
- `tests/test_sessao_login.py` (linhas descartáveis em `active_sessions`; não grava em `configuracoes_sistema`).
- Produção: nenhum SQL obrigatório (a tabela é criada pelo `create_all`; o script é opcional). `?v=` do `base.html` passou para `20261110.layout-novo.2`.
- Fora do escopo: expirar a sessão enquanto o usuário só olha uma tela parada é feito pela mesma verificação de 45 s (a sessão expirada é detectada em até ~1 min).

## 34. Cards da tela Início controlados pelo Painel de Permissões (2026-11-10)
- Removido o card "Próximos passos" (`templates/partials/dashboard.html`).
- **Bem-vindo** é fixo para todos. **Status** (usuários logados) agora é a funcionalidade `dashboard/card-status` ("Card Status (usuários logados)"), filha de "Início" em `services/features.py`: aparece no Painel - Permissões e pode ser liberada por perfil, nível ou usuário (exceção individual). O admin (nível 1) vê sempre. A regra fixa antiga (`nivel in (1, 2)`) saiu; `can_view_sessions` agora é `has_permission("dashboard/card-status")` e o card inteiro some sem a permissão (a consulta de sessões só roda quando há permissão).
- Itens de catálogo com `"menu": False` não entram no menu (`build_menu_tree` os descarta), só no painel; "Início" continua um item simples.
- **Card novo no futuro:** acrescentar uma linha em `children` de `dashboard` (`{"id": "dashboard/card-<nome>", "nome": "...", "menu": False}`), proteger o bloco no `dashboard.html` com `has_permission(...)` calculado na rota `partial_dashboard`. Nasce liberado só ao admin.
- **Para manter o que o nível 2 via antes:** `db/card_status_inicio.sql` (insere `dashboard/card-status` em `nivel_permissoes` para o nível 2; idempotente). Rodar no banco de cada ambiente após o deploy; depois ajustar pelo painel. Sem rodar, só o admin (e quem receber a permissão) vê o card.
- Rodapé no celular (até 640px): fonte 11px (era 8,8px, ilegível), mais contraste, linha separadora no topo e "(c) 2025-2026 SEDUC-MT" em destaque; alinhado à esquerda, na mesma margem dos cards.
- Texto do Bem-vindo: resumo curto (2 a 3 linhas no desktop, largura até 140ch, fonte responsiva) do que é o SPO; a frase "Use o menu ao lado..." ficou no fim, em fonte pequena (`.home-welcome-hint`). Para mudar o texto: `templates/partials/dashboard.html`.
- Layout da Início (`.home-grid`): o Bem-vindo ocupa a largura toda, centralizado, como destaque; os demais cards entram em grade automática abaixo (`minmax(320px, 1fr)`), então 1, 2 ou 3 cards se reorganizam sozinhos.
- Testes: `tests/test_cards_inicio.py`.

## 35. Erro interno na Início logo após o deploy (ResourceClosedError em has_permission) (2026-11-10)
- **Sintoma:** ao reiniciar o servidor após o deploy da integração, a tela Início deu "Internal Error"; com Ctrl+F5 abriu normalmente.
- **Causa (log de produção):** `ResourceClosedError: This result object does not return rows` em `has_permission` (`rotas/home_routes.py`), chamada por `partial_dashboard` para o card Status. É a mesma instabilidade de conexão da seção 13 (a primeira consulta numa conexão recém-aberta após o restart falha e funciona na seguinte). Os carregadores de permissão do menu já tinham retry; `has_permission` só tratava `ProgrammingError`, e a Início passou a chamá-la com o card novo.
- **Correção:** `_db_retry()` (3 tentativas, rollback entre elas; trata `ResourceClosedError`, `OperationalError`, `NoSuchColumnError`, `IndexError`) nas três consultas de `has_permission` (exceção do usuário, perfil, nível) — vale para todas as telas protegidas por `require_feature`. Além disso, o card opcional **nunca derruba a Início**: se a checagem falhar de vez, o card fica oculto e a falha vai para o log.
- Teste: `tests/test_permission_retry_failure.py::test_has_permission_repete_apos_resource_closed`.
- (2026-11-10) Tema escuro: removida a sombra clara (`text-shadow` branco) do nome da aplicação no topo (`.theme-dark .subtitle`), que deixava o texto "tremido". `?v=` do `base.html`: `.7`.

## 36. Itens de menu que davam "Not Found": aviso de "em manutenção" (2026-11-10)
- "Gerenciar Vínculos" (`atualizar/estrutura-planejamento/vinculos`) e "Upload anual de chaves de planejamento" (`atualizar/chaves_planejamento_upload`) **nunca tiveram página própria**: são permissões que liberam recursos dentro de outras telas (vínculos em Componentes; upload anual em Correção de Erros de Chave, tipo "upload_chaves_planejamento"). Vieram da homologação como itens de menu e abriam "Not Found" (a rota genérica `/partial/atualizar/estrutura-planejamento/<entity>` devolve 404 para `vinculos`).
- **Upload anual de chaves** (corrigido no mesmo dia, o usuário não achou o upload): a funcionalidade **existe** (tela Correção de Erros de Chave, tipo `upload_chaves_planejamento`); por isso **não** é "em manutenção". O item de menu abre essa mesma tela já com o tipo selecionado (`_render_chave_planejamento_regra` na rota e `initRoute` no `main.js`). A permissão continua a mesma (`atualizar/chaves_planejamento_upload`).
- **Gerenciar Vínculos** é o único que de fato não tem tela própria. Decisão do usuário: **não esconder** (para não serem esquecidos). Ficam no menu com o selo "em manutenção" e, ao clicar, abrem um aviso (`templates/partials/funcionalidade_manutencao.html`); no Painel - Permissões aparecem como "(em manutenção)". A permissão continua funcionando dentro das telas de origem.
- Mecanismo reutilizável: basta `"manutencao": "texto explicando o que falta"` no item de `services/features.py` + uma rota `/partial/<id>` que chame `_render_manutencao(<id>)` (`rotas/home_routes.py`). Para tirar o aviso quando a tela ficar pronta: remover a chave e a rota. `find_feature(id)` busca o item no catálogo.
- Verificado por script que nenhum outro item de menu aponta para rota inexistente. Teste: `tests/test_cards_inicio.py`.
- (2026-11-11) Aviso na tela de login ("sessão expirou", "entrou em outro local"): o `?motivo=` permanecia na URL e um F5 mostrava o aviso de novo. Agora `templates/login.html` remove o parâmetro da URL (`history.replaceState`) logo após a página carregar; o aviso já renderizado continua na tela até recarregar. O login em si não mudou.

## 37. Testes estouravam o limite de conexões do banco (2026-11-11)
- **Relato (dev. Jean):** `test_sessao_login.py`, último da suíte, falhava com limite de conexões; rodando sozinho passava.
- **Confirmado no banco:** `max_user_connections = 15` (conta compartilhada; `max_connections` do servidor é 300, `wait_timeout` 300 s). Cada `create_app()` cria um engine/pool próprio e **deixa 1 conexão aberta** até o fim do processo (medido: 8 chamadas = +8 conexões). A suíte chamava `create_app()` ~30 vezes em 12 arquivos, então o limite era esgotado nos últimos arquivos, e a margem diminuiu conforme a suíte cresceu (61 → 160 testes). O banco é o mesmo da produção, então o limite é do usuário inteiro (produção + testes + servidores locais).
- **Correção:** `tests/conftest.py` faz a suíte reutilizar **uma única aplicação** (`create_app()` sem argumentos devolve sempre a mesma) e fecha as conexões ao terminar. Resultado medido: pico de 6 conexões durante a suíte e 52 s de duração (antes ~140 s); 160 testes passam.
- **Medição em produção (2026-11-11):** a conta tinha 5 conexões abertas (4 do sistema, paradas, mais a do script) e havia 5 processos `lswsgi` do Passenger, cada um com pool próprio. Teste de carga **local e limitado** (script fora do repositório; pool 3+2, 10 usuários simultâneos, Início, relatórios PED/FIP 613/Dotação e outras consultas): 489 requisições em 27 s, todas 200, sem erro de conexão, p95 abaixo de 0,8 s, pico de 7 conexões da conta. EMP/NOB não entraram (de fora, 18 a 30 s por causa da distância até o banco remoto, sem erro de conexão). Não fazer teste de carga contra a produção: usa as mesmas 15 conexões dos usuários.
- **Risco e ajuste (aplicado só na produção):** `config.py` usa por padrão `pool_size=5` + `max_overflow=10` = até 15 conexões **por processo** (o limite inteiro da conta), e `pool_recycle=1800` enquanto o banco fecha conexões paradas com 300 s (`wait_timeout`). Aplicado no `.env` da produção (`~/projetoswebcsg_app/.env`; backup `.env.bak-2026-11-11`): `DB_POOL_SIZE=3`, `DB_MAX_OVERFLOW=2`, `DB_POOL_RECYCLE=280`; restart feito e login conferido; conta ficou com 2 conexões logo após o restart. Para desfazer: restaurar o backup e reiniciar. O `.env` não vai no Git nem é trocado pelo deploy. Homologação e `dev/*` seguem com o padrão (5+10) até decisão. Pendente: nova contagem de conexões durante o uso normal.
- **Limite por ambiente (atualizado 2026-11-11):** os 15 valem para o banco de produção (configuração global do MariaDB da hospedagem; nosso usuário não pode alterar). Cada ambiente (homologação, dev/cleber, dev/jean) tem banco próprio, também com limite de 15 (o "30" relatado antes pelo dev. Jean foi erro de medição; ele mediu de novo e confirmou 15 para a sessão e para o servidor). Com o fluxo dev → homologação → main, testes e desenvolvimento não usam mais as conexões da produção. O ajuste do pool no `.env` é opcional fora da produção. Os 3 testes pulados fora desta máquina são os de Notas SEE que dependem da pasta de PDFs de exemplo (fora do Git).
- **Ajustes do relato do Jean:** `db/notas_see_schema.sql` também apaga o índice antigo `uq_see_catalogo_nome` (nome usado por uma versão antiga do modelo; sem isso o nome do catálogo continuaria único entre exercícios); `pdfplumber` repetido removido do `requirements.txt`.

## 38. Limite de processos da conta (NPROC): fila de um processamento pesado por vez (2026-11-11)

**Incidente (EMP de 11/11, relatado pelo dev da automação).** Às 05:07 (Cuiabá) o EMP 248 foi aceito e o worker subiu; o `GET /api/emp/status` respondeu 503 (servidor web sem thread livre). A automação reenviou o arquivo às 05:08 (EMP 249) com o 248 ainda rodando; o `Popen` do worker falhou com `BlockingIOError [Errno 11]` (NPROC=100 da conta inteira, cada thread conta) e o plano B por thread falhou com "can't start new thread" (500). O 248 terminou normalmente às 05:12 (60.198 registros); o 249 ficou preso em "em processamento". Evidência: `outputs/status/emp_249.json`, `worker_emp_249.log` com 0 bytes e `emp_248.json`. O horário do log do servidor é Brasília (1 h à frente de Cuiabá).

**Correção no SPO:**
- `services/fila_pesada.py`: trava de arquivo (`outputs/status/fila_pesada.lock`, flock) -- o `worker.py` pede a vez **antes** de `create_app()` (**correção em 12/11, seção 39:** na prática o `from app import create_app` já executava `create_app()` e importava o numpy antes da fila; agora a fila vem antes desses imports) (sem conexão ociosa no banco enquanto espera) e só um worker pesado roda por vez (EMP, NOB, FIP613, PED, EST EMP, Receita, SEE). Quem espera dorme (custo mínimo); o SO libera a trava se o processo morrer. Espera máxima 60 min (`FILA_PESADA_TIMEOUT_S`), depois o job vira falha com mensagem clara. `FILA_PESADA=0` desliga. EMP/NOB mostram "Aguardando a vez na fila".
- Upload/reprocesso recusam com **409 + `Retry-After`** se já há processamento do mesmo tipo (`_upload_em_andamento_409`, checado antes de mover/gravar o arquivo). FIP613/PED/EST EMP/Receita usam `ProcessamentoJob`; EMP/NOB usam `job_status.upload_em_andamento` (ignora status parado há mais de 90 min ou com processo morto).
- `_start_worker`: **sem plano B por thread** (falhava pelo mesmo motivo e pesava no servidor web). Falha ao iniciar grava log com traceback e marca o status como falha (não fica mais preso). O upload devolve **503 + `Retry-After`** quando a causa é falta de recurso (errno 11/12 ou "can't start new thread"), e 500 nos demais casos, sempre com log (antes o erro só ia para o cliente).
- Node: `UV_THREADPOOL_SIZE=2` por padrão e pool MySQL do runner em 3 (`NODE_DB_POOL_LIMIT`); ambos sobrescrevíveis.
- Testes: `tests/test_fila_pesada.py`.

**Para a automação (enviado ao dev):** guardar o `job_id`, nunca reenviar enquanto o anterior roda, tratar 503/409 como "esperar e consultar", processar um relatório pesado por vez, usar usuário exclusivo (uma sessão por usuário). Pendente fora do código: avaliar com a hospedagem o aumento do NPROC. Requer deploy + restart; depois conferir `ps -u $USER -L | wc -l` durante um processamento.

## 39. Trava 409 que não travava, worker morto no import e jobs órfãos (2026-11-12)

**Incidentes (Cuiabá):** 05:08 e 09:55 (EMP 253), 10:43 (EMP 255, pela tela) e 10:45 (EMP 256, automação): EMPs aceitos por cima de outro em processamento. Os workers do 253 e do 256 morreram sem gravar status ("0% — Worker externo iniciado"), e o Reprocessar do 253 reaproveitou o mesmo id. Havia 10 jobs órfãos antigos (emp 63, 65, 99, 169, 249, 256; nob 3, 49, 116, 151).

**Causas confirmadas com os logs do servidor:**
1. **A trava 409 do EMP/NOB quebrava e liberava o envio.** O runner Node grava `updated_at` com `toISOString()` ("...Z", com fuso; `node_runners/util.js`), e o Python comparava com `datetime.utcnow()` (sem fuso): `TypeError: can't subtract offset-naive and offset-aware datetimes`. Como a checagem libera o envio quando dá erro, cada aviso "Checagem de processamento em andamento falhou (emp)" no `logs/app.log` é um envio aceito por cima de outro. Só quebrava com um EMP rodando, ou seja, exatamente quando a trava era necessária.
2. **OpenBLAS com 64 threads por processo.** O servidor tem 88 núcleos (`nproc`); o numpy (pandas) abre 64 threads no import. Com o NPROC de 100 da conta, o segundo processo Python não consegue criar as threads e fica preso no `import` (`outputs/status/worker_emp_253.log`/`_256.log`: "OpenBLAS blas_thread_init: pthread_create failed ... of 64"), até receber SIGINT. Explica o "sozinho funciona, dois juntos morrem" e o 503 do status de 11/11.

**Operacional (feito pelo usuário em 12/11):** variáveis `OPENBLAS_NUM_THREADS=1`, `OMP_NUM_THREADS=1`, `MKL_NUM_THREADS=1` no Setup Python App do cPanel (produção e homologação) + Restart.

**Correção no código:**
- `app.py`/`worker.py`: as mesmas variáveis com `setdefault` antes de qualquer import (o valor do cPanel prevalece); `_start_worker` também as passa ao worker.
- `services/job_status.py`: `parse_utc()` converte horários com e sem fuso para UTC antes de comparar. Detecção de órfão do EMP/NOB (`encerrar_se_orfao`): pid morto, zumbi ou de outro programa (nome exato `worker.py`; o `pedidos_worker.py` da automação não casa), ou sem pid e parado há 90 min, vira "falha no processamento" ao consultar o status e ao checar a trava; carência de 3 min na partida. Cancelar (`solicitar_cancelamento`): worker morto encerra na hora; pedido não renova a trava; 2º clique depois de 5 min encerra. `processos_worker_spo()` lista os workers do SPO pelo `/proc`.
- `services/processamento_jobs.py` (FIP 613, PED, EST EMP, Receita): antes um órfão travava o tipo **para sempre**. `encerrar_orfaos`/`job_ativo` (idade calculada no banco com `TIMESTAMPDIFF(..., NOW())`, porque `atualizado_em` usa o relógio do banco) e `solicitar_cancelamento_job`, com as mesmas regras.
- `rotas/home_routes.py`:
  - **Trava atômica** `_upload_exclusivo(tipo)`: `GET_LOCK('spo_upload:<banco>:<tipo>', 0)` numa conexão própria, segurada da checagem até o worker iniciar, em upload e Reprocessar dos 5 tipos e da Receita. O nome leva o banco porque o `GET_LOCK` vale para o servidor MariaDB inteiro (produção, homologação e dev). Usa uma conexão a mais do pool durante o envio.
  - **Spawn:** `start_new_session=True` (o worker não morre com o `lswsgi` reciclado), `stdin=DEVNULL`, `close_fds`, log fechado no processo web, workers recolhidos (`poll`) antes de cada spawn (sem zumbis).
  - **Status por id:** `GET /api/<tipo>/status?job_id=` (opcional; sem ele, o último como antes) e `job_id` no JSON. Id inexistente: 404 JSON.
  - **Reprocessar do EMP/NOB** cria um upload novo (id novo), como os gerenciados.
- `worker.py`: pede a vez na fila antes de importar a aplicação; quem espera não carrega pandas/numpy nem abre conexão.
- `static/js/main.js`: "cancelamento_solicitado" era tratado como "cancelado" pelo renderizador de status (parava de acompanhar e desabilitava o 2º Cancelar); Reprocessar do EMP/NOB desabilitado até o status confirmar que nada roda.
- `scripts/limpar_jobs_orfaos.py`: comando oficial. Sem argumentos só lista; `--aplicar` encerra. Não toca em job com worker do SPO rodando, com pid vivo ou atualizado há menos de 3 min. Idempotente. Cobre arquivos (EMP/NOB) e banco (gerenciados).

**Contrato com a automação:** Reprocessar do EMP/NOB devolve `job_id` novo; Cancelar de job finalizado devolve 409; órfão vira `falha no processamento`/`falha`; `?job_id=` e `job_id` são acréscimos. Falha de spawn por falta de recurso continua **503 + `Retry-After: 180`** (desde 11/11). Fila sem estado novo (EMP/NOB "em processamento" com mensagem "Aguardando a vez"; gerenciados "aguardando").

**Testes:** `tests/test_trava_upload_jobs.py` (dois envios simultâneos do mesmo tipo = um 200 e um 409, com `GET_LOCK` real; horário "Z" do Node; worker morto; Reprocessar com job ativo e com id novo; Cancelar com worker morto e 2º Cancelar; spawn destacado; script), `tests/test_job_orfao.py`, `tests/test_fila_pesada.py`.

**Deploy:** sem alteração de banco. Depois do deploy, Restart no cPanel; rodar `python scripts/limpar_jobs_orfaos.py` (e `--aplicar` se listar algo); durante um EMP, conferir `ps -u "$USER" -L | wc -l` (dezenas, não perto de 100) e que o worker tem sessão própria (`ps -u "$USER" -o pid,pgid,sid,cmd`: SID do worker = PID dele).

**Validação em produção (12/11, com a automação):** às 14:49, com um EMP enviado pela tela em processamento, a automação tentou subir outro EMP e recebeu 409. Ela acompanhou o processamento até o fim e não reenviou. Não houve processamento duplicado nem job órfão; o EMP terminou com 60.280 registros. A madrugada de 13/11 foi a primeira com as correções publicadas.

## 40. REGRA: fluxo de desenvolvimento com dois desenvolvedores (Cleber e Jean) (2026-11-12)

Vale para os dois desenvolvedores e para quem (humano ou IA) trabalhar numa `dev/<nome>`. Completa a regra do §30.

### 40.1 Fluxo
```
feat/<tema> → PR (squash) → dev/<nome> → (cherry-pick em branch temporária) → homologacao → (aprovado) → main
                                                                                              │
                                                       sincronização main → dev/cleber e dev/jean (branch sync/...)
```
- **Branches de trabalho:** curtas, a partir da própria `dev/<nome>`, um tema por vez. Nomes: `feat/<tema>` (funcionalidade) e `fix/<tema>` (correção).
- **PR para a `dev/<nome>`:**
  - Destino fixo, com o link `/compare/dev/<nome>...feat/<tema>`; conferir o campo *base* antes do merge (§21.8).
  - Merge com **"Squash and merge"**: cada tema vira um commit só.
  - Depois do merge, apagar só a `feat/`/`fix/`. Nunca usar uma branch fixa como origem (§30).
- **Antes de abrir o PR:** atualizar a `feat/` com a `dev/<nome>`, que muda a cada sincronização da `main`.
- **Para a homologação e a `main`:** quem promove (hoje, o Cleber) cria uma branch temporária a partir do destino e faz o **cherry-pick só do commit do tema**. Exemplos: `pr/<tema>-para-homologacao` e `pr/<tema>-para-main`.
  - Não se faz merge da `dev/<nome>` inteira: cada `dev` tem arquivos próprios, que não podem chegar à homologação nem à `main`.
  - Na `dev/cleber`: `.cpanel.yml`, `scripts/deploy_cpanel.sh` e `CLAUDE.md`.
  - Na `dev/jean`: o selo de ambiente (vem do `.env` do ambiente dela, `APP_ENV_LABEL` e `APP_ENV_CLASS`, não do código; o nome antigo `SPO_ENV_LABEL` não é mais lido e a linha `PATCH_DEV...` do rodapé não existe mais), os `.cmd` e o `.ps1` dela e `docs/politica-seguranca-branches.md`.
- **Sincronização `main` → `dev/cleber` e `dev/jean`:**
  - Feita por quem promoveu, depois de cada subida para a `main`, com branch `sync/main-para-dev-<nome>` e PR.
  - O tema do desenvolvedor volta para a `dev/<nome>` dele como outro commit, com o mesmo conteúdo, e normalmente não dá conflito.
  - Se der conflito, mantém-se a versão mais nova do desenvolvedor, exceto a linha do `RELEASE`, que fica a da `main` (§40.8).
  - Correção urgente pode ir para todas as branches ao mesmo tempo, cada uma com a sua branch temporária, como no §39 (PRs #27, #28 e #29).

### 40.2 Arquivos de uso comum (tocar com cuidado)
- **`static/js/main.js` e `static/css/style.css`** (arquivos únicos): código novo num bloco próprio no fim do arquivo (função `init<Tema>`). Não reorganizar o que já existe.
- **`templates/base.html`:** o `?v=` muda em quase todo PR e sempre dá conflito na sincronização. Usar sufixo com o nome do desenvolvedor, por exemplo `?v=AAAAMMDD.jean-<tema>.1`. Na sincronização fica o mais novo.
- **`services/features.py` (menu e permissões) e `rotas/__init__.py` (registro de blueprint):** só acrescentar; não alterar entradas existentes.
- **`rotas/home_routes.py`** (mais de 20 mil linhas): tocar o mínimo. Código novo em blueprint próprio (`rotas/<tema>_routes.py`) e em `services/<tema>.py`.
- **`docs/claude.md` e `docs/CHANGELOG.md`:** todo tema registra entrada no [CHANGELOG](CHANGELOG.md) (§40.14). Ao `claude.md` só vai regra, contrato ou conhecimento que continua valendo, numa seção nova, **sem número**: dois PRs ao mesmo tempo pegariam o mesmo número, e quem promove numera (§40.11).

### 40.3 Não tocar sem combinar antes
- **`.cpanel.yml`, `scripts/deploy_cpanel.sh` e `CLAUDE.md` da raiz:** próprios de cada branch e de cada deploy.
- **`app.py`, `config.py` e `worker.py`.**
- **`services/job_status.py`, `services/processamento_jobs.py` e `services/fila_pesada.py`:** jobs órfãos, trava 409 e limite de processos do servidor (§38 e §39).
- **Contrato da API com a automação:**
  - Inclui as rotas de upload, Reprocessar e Cancelar, o formato do `/status`, os estados dos jobs, o 409 e o 503 com `Retry-After`.
  - A equipe da automação pediu (12/11) para ser avisada **antes** de qualquer mudança.
  - Se o tema encostar nisso, combinar entre os dois antes de começar e avisar a automação antes de publicar.

### 40.4 Banco
- **Scripts:** cada mudança de schema vai num script em `db/` que pode rodar mais de uma vez sem erro (conferir se a coluna ou o índice já existe antes de criar). `db.create_all()` só cria tabelas novas.
- **Bancos:** cada ambiente tem banco próprio (homologação, `dev/cleber` e `dev/jean`); a `main` divide o banco com a produção (§8 e §21).
- **Ordem:** o script é executado à mão em cada ambiente **antes** do código. O PR deve listar os scripts a rodar.
- **Limite de conexões:** 15 por usuário do banco (§37). Nos testes, usar a aplicação única do `tests/conftest.py` (não chamar `create_app()` de novo) e apagar as linhas que o teste criar.

### 40.5 Testes e entrega
- **Antes do PR:**
  - Testar com dado real no banco da própria `dev`.
  - Suíte (`pytest`) passando duas vezes seguidas.
  - `node --check static/js/main.js` quando mexer no JS.
- **No PR:** escrever o que foi testado e com quais dados, os scripts de banco e se há mudança de contrato da API.

### 40.6 Publicação
- **Produção (`main`): só durante o dia, até ~18:00, nunca à noite.** A automação roda de madrugada e precisa validar antes.
- **Depois do deploy:** Restart no cPanel e conferência rápida (login, a tela do tema e, se mexeu em jobs, `python scripts/limpar_jobs_orfaos.py`).

### 40.7 Promoção para a homologação e para a `main`: pasta separada
- **Uma pasta separada por promoção** (`git worktree`), nunca na pasta principal: `git worktree add -b pr/<tema>-para-homologacao C:\workspace\projetoswebcsg_<tema> origin/homologacao` (ou `pr/homologacao-para-main` a partir da `origin/main`). A pasta principal fica só para leituras e o `fetch`.
- Logo depois de criar: `git branch --unset-upstream`, para que um `git push` sem argumento nunca vá para a `homologacao` nem para a `main`.
- **Cherry-pick só dos commits dos temas**, na ordem em que entraram, nunca merge da branch inteira (§40.1). O conflito esperado é só a linha `RELEASE` do `templates/base.html` (fica o `RELEASE` de destino, §40.8); conflito no `docs/claude.md` ou em código: parar e analisar.
- **Conferência antes de testar:** `git diff --stat origin/<destino>...HEAD`; os arquivos levados idênticos aos da origem, menos o `RELEASE`; `services/uo.py`, `.cpanel.yml` e `scripts/deploy_cpanel.sh` iguais aos do destino (UOs novas seguem bloqueadas, §22); `requirements.txt`, `node_runners/package*.json` e `db/` sem mudança, ou os scripts de banco listados no PR.
- **Testes na pasta separada:** copiar só o `.env`, `npm install --omit=dev` em `node_runners` (descartar a reescrita do `package-lock.json`), `pytest` duas vezes e `node --check`; teste visual rápido e teste manual de quem promove antes do push.

### 40.8 Como mergear cada tipo de PR
- **Tema para a `dev/<nome>`** (`feat/` ou `fix/`): **"Squash and merge"**, um commit por tema (§40.1).
- **Lote para a `homologacao` e `homologacao` → `main`:** **"Create a merge commit"**, para os commits dos temas ficarem separados no histórico.
- **Sincronização `main` → `dev/cleber` e `dev/jean`:** **"Create a merge commit"**, **nunca squash**: o squash apaga o histórico e faz a próxima sincronização repetir os mesmos conflitos.
- **Conflito só na linha do `RELEASE` do `templates/base.html`:** fica o da `main` (sincronização) ou o `RELEASE` de destino (promoção). É o único conflito que pode ser resolvido sem parar; qualquer outro: parar, listar os arquivos e explicar.
- Em todos: link do PR com o destino fixo (`/compare/<destino>...<origem>`) e conferir o campo *base* antes de mergear (§21.8).

### 40.9 RELEASE conta por dia, para todas as branches
- O `RELEASE_2_AAAA_MM_DD.N` do rodapé (`templates/base.html`) conta **por dia e para todas as branches juntas** (o `N` soma `dev/cleber`, `homologacao` e `main`): o próximo `N` do dia, qualquer que seja a branch ou o ambiente. Dia diferente: troca a data e volta para `.1`; mesmo dia: soma 1 ao maior `N` já usado em qualquer branch.
- Em 16/11/2026: `.1` (lote 4), `.2` (lote 5), `.3` (Teto no celular) na homologação, `.4` na produção e `.5` nesta atualização do `docs/claude.md`.
- Conferir antes: `git log --all -S'RELEASE_2_AAAA_MM_DD.N' --oneline`. O `?v=` do CSS e do JS segue a regra do §40.2 e não é a mesma coisa.

### 40.10 Exceção combinada: correção urgente em cima da homologação
- Quando a correção é urgente e a `dev/cleber` ainda **não tem o código** em que ela mexe (por exemplo, o AA+ do Jean, que chegou à `dev/cleber` só pela sincronização da `main`), a correção é feita numa branch `pr/<tema>-para-homologacao` **em cima da homologação**, e não numa `feat/` a partir da `dev/cleber`.
- Ela segue o fluxo de promoção do §40.7 e volta para a `dev/cleber` e a `dev/jean` **pela sincronização da `main`**, como qualquer outro tema.
- Exemplos de 16/11/2026: avisos modais no celular (§41.100, no PR #154) e Teto Orçamentário no celular (§41.101, PR #155).
- O PR precisa dizer que é essa exceção, e a seção do tema deve dizê-lo também.

### 40.11 Seções novas do dev. Jean: subseções da 41
- O dev. Jean descreve cada tema numa seção **sem número** (§40.2). Quem promove as **numera como subseções da 41** (`### 41.N`), em ordem, **depois da última 41.N e antes da `## 42.`**, rebaixando os subtítulos `###` internos para `####` e sem número. Só o nível e o número do título mudam; o texto dele não é reescrito.
- Exemplos: 41.33 a 41.97 (lote 4) e 41.98 a 41.99 (lote 5). As seções do próprio promotor (41.100 e 41.101) seguem a mesma ordem.
- No `docs/claude.md` das `dev/*`, depois da sincronização, fica a versão numerada da `main`.

### 40.12 Testes do padrão AA+ (valem para os dois desenvolvedores)
- **`test_tamanho_fonte`:** `font-size` no `static/css/style.css` só por `var(--spo-fs-*)`; valor em px novo ou contagem que sobe reprova.
- **`test_cor_literal`:** sem cor literal nova (`#hex`, `rgb`, `hsl`) fora da definição de um token; referência em `tests/referencia_cor_literal.json`.
- **`test_style_inline`:** sem cor ou tamanho de texto em `style="…"` nos templates nem escritos à mão no `main.js`.
- **`test_kit_mockup`:** o kit (`static/kit/`) continua lendo do `main.js` e segue o próprio padrão; o `?v=` do CSS no kit precisa ser igual ao do `base.html`.
- Quando uma regra antiga passar a usar o token ou a variável, a contagem desce e o teste avisa para baixar a referência.

### 40.13 Lições operacionais das promoções
- Uma **pasta nova (`git worktree`) nasce sem `node_modules` e sem `.env`**: copiar só o `.env` e rodar `npm install --omit=dev` em `node_runners` (sem commitar o `package-lock.json`). **Apagar a cópia do `.env`** das pastas temporárias ao remover a pasta.
- **Servidor com `--debug`** (Flask) cria um **processo filho** que segura a porta (5010): ao encerrar, encerrar a árvore inteira (janela, pai, filho do `--debug` e o launcher do `.venv`) e confirmar a porta livre.
- **Shell ou servidor dentro da pasta impede apagá-la**: o `git worktree remove` desregistra mas pode falhar ao apagar ("Permission denied" ou "Invalid argument") e deixar a pasta vazia ou pela metade. Sair da pasta, parar o que a usa e repetir; sem `--force` e sem apagar à mão antes de conferir.
- **Caminho longo** (`node_modules` fundo): `cmd /c rmdir /s /q "\\?\<pasta>"`.
- No Git Bash, argumentos como `origem:.arquivo` sofrem conversão de caminho (`MSYS_NO_PATHCONV=1`); e o Python do Windows não enxerga `/tmp` do Git Bash.
- Testes locais usam sempre o banco do próprio ambiente (`proj5954_spo_csg_amb_cleber` ou `proj5954_spo_csg_amb_jean`), nunca o da produção: conferir o nome em `.env` antes de rodar.

### 40.14 Todo tema registra uma entrada no CHANGELOG; o `claude.md` guarda só o que continua valendo
- **Todo tema** (`feat/` ou `fix/`, de qualquer desenvolvedor) registra, **no mesmo PR**, uma entrada no [docs/CHANGELOG.md](CHANGELOG.md): data do dia (a mais nova em cima), branch ou tema, **o que mudou** (uma linha por mudança, com o número do PR quando houver), **scripts de banco** (ou "nenhum") e **mudança no contrato da API com a automação** (ou "nenhuma"). Cada lote do dia usa o `RELEASE_2_AAAA_MM_DD.N` do §40.9.
- **Ao `claude.md` só vai o que for regra, contrato ou conhecimento duradouro**: como a funcionalidade funciona hoje, o que não pode quebrar, como testar, decisões que ainda valem. **Histórico** (problema encontrado, medição antes e depois, verificação, `?v=`, lista de arquivos, o que cada PR fez) fica **só** no CHANGELOG.
- A seção nova do `claude.md` continua **sem número** (§40.2 e §40.11). A seção 41 deixou de ser "uma subseção por tema" em 16/11/2026: o histórico das 101 subseções foi para o CHANGELOG ("antiga 41.N") e a seção 41 ficou com a parte em vigor. Tema que nasceu como seção do `claude.md` e virou só histórico segue o mesmo caminho: o texto é movido **sem reescrever** para o CHANGELOG, com o número antigo no título.
- Quem promove (§40.11) numera só a parte que ficou como regra; o histórico não ganha número novo.
- **Cabeçalho da entrada no CHANGELOG:**
  - `dev/cleber`: "AAAA-MM-DD — dev/cleber, RELEASE_...", com o `RELEASE` do commit.
  - `dev/jean`: "AAAA-MM-DD — dev/jean, ainda não publicado", sem `RELEASE` (o Jean não mexe no `RELEASE`).
  - Quem promove completa o cabeçalho: na homologação, "AAAA-MM-DD — homologação, RELEASE_... (lote N)"; na `main`, só "AAAA-MM-DD — produção, RELEASE_..." (sem lote e sem o `RELEASE` da homologação).

## 41. AA+ (acessibilidade) — dev. Jean (2026-11-13)

Reorganizada em 16/11/2026 (tema `fix/docs-changelog-secoes-41`): o **histórico** dos lotes do dev. Jean (problema, medição, verificação, `?v=`, o que cada PR fez) foi para o [CHANGELOG.md](CHANGELOG.md), na seção "Histórico do desenvolvimento do AA+ (antiga seção 41)", agrupado por data e com o número antigo em cada título ("antiga 41.N"). Aqui ficam só a **regra, o contrato, a decisão e a instrução em vigor**, com o número de sempre. O texto das subseções foi movido sem reescrever.

- Subseções que ficaram inteiras: 41.26, 41.29, 41.79.
- Subseções **mistas** (a parte em vigor está abaixo; o histórico está no CHANGELOG): 41.2, 41.6, 41.11, 41.14, 41.17, 41.24, 41.30 a 41.32, 41.35, 41.41, 41.45, 41.49, 41.52 a 41.53, 41.55 a 41.57, 41.59 a 41.60, 41.63 a 41.68, 41.75 a 41.77, 41.83, 41.85, 41.88 a 41.98, 41.100 a 41.101.
- Subseções que **só tinham histórico** e foram para o CHANGELOG: 41.1, 41.3 a 41.5, 41.7 a 41.10, 41.12 a 41.13, 41.15 a 41.16, 41.18 a 41.23, 41.25, 41.27 a 41.28, 41.33 a 41.34, 41.36 a 41.40, 41.42 a 41.44, 41.46 a 41.48, 41.50 a 41.51, 41.54, 41.58, 41.61 a 41.62, 41.69 a 41.74, 41.78, 41.80 a 41.82, 41.84, 41.86 a 41.87, 41.99.
- Os documentos do padrão visual continuam em [padrao-visual-spo-aa-plus.md](padrao-visual-spo-aa-plus.md); a regra para novos temas está na seção 40.14.

### 41.2 Fontes e tamanho do texto no Personalizar Temas SPO (dev. Jean, 2026-11-12)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.2._

**Fontes hospedadas no SPO** (`static/fonts/`, pasta nova), não pelo Google Fonts: nenhum dado do usuário vai a terceiros. Só o subconjunto latino (cobre o português), ~180 KB no total; o navegador só baixa a fonte escolhida. Todas sob a **SIL Open Font License 1.1**, com o `OFL.txt` em cada pasta; origem (Fontsource 5.3.x via jsDelivr), bytes e sha256 de cada arquivo em `static/fonts/LEIA-ME.md`.

### 41.6 Contraste AA+ (7:1) no tema claro (dev. Jean, 2026-11-12)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.6._

**Correção (só CSS, bloco `[jean]` no fim do `style.css`, só no tema claro, dentro de `@media (forced-colors: none)`):**
- `--muted` e `--bs-secondary-color` = **`#48464e`** (≥ 7,7:1 em todos os fundos claros medidos);
- variável **`--spo-accent-text`**: o tom mais claro de cada paleta que passa de 7:1 — `--color-700` (Clássico, Azul, Roxo), `--color-800` (Ciano, Verde, Laranja, Vermelho), `--color-900` (Amarelo), escolhido pelo `body[data-accent-preference]`; usada no texto de `.subtitle`, `.eyebrow`, `.report-eyebrow`, `.module-eyebrow`, `.modal-header-title`, links de `.noticia-box`, "Administrar SPO" aberto e números do Teto;
- botões com texto branco na cor de destaque (`.btn-primary`, "Baixar Excel", prévia do Personalizar): fundo em degradê desse mesmo tom;
- `.btn-secondary` (Bootstrap): `#495057`;
- rodapé (`.footer .footer-line`): de 9,2 para **11 px** (nos dois temas).

### 41.11 Semântica e teclado: menu operável, título da aba, "Pular para o conteúdo", avisos modais, abas e filtros (dev. Jean, 2026-11-12)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.11._

**Correção (bloco `initSemanticaTeclado` no fim do `static/js/main.js`; CSS em bloco `[jean]`; nenhuma função existente alterada):**
- grupos do menu com `role="button"`, `tabindex="0"`, `aria-controls` e `aria-expanded` sincronizado; Enter/Espaço disparam o mesmo clique que já abre/fecha;
- a cada troca de tela (filhos do `<main id="content-area">`): título da aba "‹título› — SPO" (sem `h1`, o nome do item do menu), título principal (`h1`, ou `role="heading" aria-level="1"` no título visível), foco no título e aviso "Página carregada: …" (`role="status"`); com um aviso modal aberto, o foco não sai dele;
- "Pular para o conteúdo" como primeiro item do Tab (visível só no foco), levando ao `<main>`;
- avisos modais: `role="dialog"`, `aria-modal`, `aria-labelledby` no título, foco no primeiro controle, Tab preso, Esc aciona "Fechar/Cancelar", e o foco volta a quem abriu (ou ao título da página);
- abas (`role="tablist"`): `aria-controls`/`role="tabpanel"`, `tabindex` itinerante, setas/Home/End;
- filtros: `aria-haspopup` e `aria-expanded` conforme a lista aberta;
- ✓ nas opções escolhidas do Personalizar (a seleção já tinha `aria-pressed` e uma barra; agora também o sinal);
- o mesmo anel de foco do A4 para elementos focados por script (`[tabindex="-1"]`).
- `templates/base.html`: `?v=20261112.jean-teclado.1` no CSS e no JS.

**Fora daqui:** Esc nas listas dos filtros e navegação por setas dentro do menu (o Tab já percorre tudo); telas de cadastro com modais próprios não foram exercitadas uma a uma.

### 41.14 Contornos dos controles na cor de destaque, em três níveis (dev. Jean, 2026-11-13)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.14._

**Correção (só CSS, bloco `[jean]` no fim do `style.css`):**
1. **Campos, filtros e botões secundários** (onde a borda é o próprio controle): `--spo-control-border` vira um tom do destaque, o mais claro que passa em 3:1.
   - Claro: um passo da paleta — `--color-400` (Clássico, Azul, Vermelho), `--color-500` (Ciano, Verde, Laranja), `--color-700` (Amarelo), `--color-300` (Roxo).
   - Escuro: `color-mix(in srgb, var(--accent) 65%, var(--card))`; no Roxo, 85%.
   - "Contraste reforçado": ≥ 4,5:1 (claro `--color-400`…`--color-800` conforme o destaque; escuro `--accent`, Roxo `--accent-strong`).
2. **Linhas de opção** (`.preference-row`, `.visual-pref-toggle`, `.visual-pref-option`), em três níveis:
   - **em repouso:** suave, no tom da prévia (`--spo-option-border`, destaque a ~30% sobre o cartão). Ícone, texto e seta já identificam o controle;
   - **mouse, foco ou grupo aberto:** `--spo-control-border` (≥ 3:1);
   - **selecionada:** destaque cheio (`--spo-selected-border`), com 2 px por `box-shadow`, sem mudar o tamanho; continua com o fundo tingido e o ✓.

   As linhas de opção saíram da regra geral de contorno daquele bloco anterior.
3. **"Contraste reforçado":** todas as linhas ficam fortes (≥ 4,5:1), e a selecionada continua a mais forte.

Cartões que só exibem informação continuam suaves no padrão, como antes.

### 41.17 Plano de fundo da página mais neutro e com mais separação dos cartões (dev. Jean, 2026-11-13)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.17._

**Correção (só CSS, bloco `[jean]` no fim do `style.css`):** fundo igual para todos os destaques.
- **Claro:** `#e6e9ed`, cinza neutro (1,22:1 contra o cartão).
- **Escuro:** `#121212` (1,21:1).
- Valem para `--bg`, `--body-bg` e `--footer-bg`, inclusive na tela de login.
- Os botões das Preferências comuns ficam sem tinta em repouso; a tinta fica no grupo aberto e na opção escolhida.
- **Azul no claro:** o texto de destaque passa ao `--color-800`. O `--color-700` dava 6,71:1 sobre o fundo novo; agora dá 8,64:1.
- O alto contraste e as cores forçadas do sistema não mudam.

### 41.24 Controles dos formulários: campo desabilitado no escuro e botões de decisão mais vivos (dev. Jean, 2026-11-13)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.24._

**Correção (só CSS, bloco `[jean]` no fim do `style.css`):**
- **Campo desabilitado:**
  - no escuro, fundo `rgba(255,255,255,.05)`, um degrau acima do cartão, texto `#b5b5b5` e borda cinza neutra `#4a4a4a`;
  - no claro, texto `#5f6670` (de 4,29 para 5,15:1) e borda `#c9ced6`;
  - a regra leva `:not(#spo-desab)`, que só soma peso de id, para vencer a regra dos contornos (#44).
- **Botão principal** (`.btn-primary` e equivalentes): usa o tom **mais vivo** de cada destaque que ainda dá 7:1, na variável `--spo-btn-fill`.
  - **Claro**, com texto branco: `--color-600` (Clássico, Roxo), `--color-700` (Azul, Turquesa, Vermelho) ou `--color-800` (Verde, Amarelo, Laranja).
  - **Escuro**, com texto `#111`: a cor de destaque cheia (`--accent`); no Roxo, `--accent-strong`.
- **Vermelho cheio** (Excluir, Desativar, Cancelar processamento): `#a51d2d` no claro e `#ff7b7b` no escuro.
- **"Limpar Formulário"** (`.btn-danger[id$="-clear"]`, nos 4 cadastros): contorno vermelho, sem preenchimento, para o Salvar ficar como a ação principal.
- **Mouse e foco no claro:** o botão escurece (`brightness(.92)`) em vez de clarear, para o texto não cair abaixo de 7:1.

### 41.26 Tela de login com o mesmo `?v=` do CSS que o restante do SPO (dev. Jean, 2026-11-13)

**Problema:** o `login.html` carregava `style.css?v=20261113.jean-fundo.2`, do #47. O CSS mudou depois em vários PRs (#49–#58), mas a linha do login não acompanhou. Quem abriu a tela de login desde o #47 podia continuar vendo nela uma cópia antiga do CSS, guardada no navegador.

**Correção:** o `login.html` passa a usar o **mesmo** `?v=` do `base.html` (`20261113.jean-controles.3`). As duas páginas compartilham a mesma entrada de cache do `style.css`.

**Regra daqui em diante:** sempre que o `style.css` mudar, o `?v=` do CSS muda **no `base.html` e no `login.html`**, com o mesmo valor.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean.
- A página de login servida (pedido sem cookies) traz `?v=20261113.jean-controles.3`, igual ao `base.html`; o `login-theme.js` continua em `?v=20260706.1`.
- Visual conferido no escuro. O que muda é só o que o CSS atual já fazia para quem não tinha cópia antiga: o botão "Entrar" passa do pastel à cor de destaque, com texto escuro (10,3:1), pela regra dos botões do #57.
- `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

Sem script de banco. Só a linha `?v=` do CSS no `templates/login.html`.

### 41.29 Padrão visual "spo-aa+" documentado (dev. Jean, 2026-11-13)

**O quê:** documento novo [padrao-visual-spo-aa-plus.md](padrao-visual-spo-aa-plus.md), com as regras de cor, contraste, temas e componentes que o AA+ (#31–#61) estabeleceu. Ele vale para **toda tela nova ou alterada**. O `contrato-visual-spo.md` ganhou um link para ele.

**Por quê:** as regras estavam espalhadas em 25 seções deste arquivo e em comentários do `style.css`. O `style.css` tem 303 cores literais (235 diferentes) e 111 variáveis, sem uma regra dizendo qual usar para quê. Sem o padrão escrito, cada tela nova pode reabrir diferenças, e a troca das cores fixas (N5) não teria critério.

**Conteúdo:**
1. Metas: texto 7:1, elementos que não são texto 3:1, foco, alvos, movimento, zoom.
2. Temas, níveis de contraste e destaques.
3. Tokens com os valores e contrastes **medidos** nos dois temas e nos níveis Suave e Reforçado.
4. Componentes: hierarquia de botões, campos, listas, navegação.
5. Como medir (bateria).
6. Regras de manutenção e checklist de PR.
7. A dívida conhecida (as cores literais por área), que é o roteiro do N5.

**Próximas fases** (a combinar):
- **B:** um teste na suíte que impede **cor literal nova** no `style.css`, comparando com uma linha de base.
- **C:** trocar os literais pelos tokens, área por área, junto com a divisão do CSS.

Sem mudança de código. Sem script de banco.

### 41.30 Padrão de fontes do SPO: Inter, Intel One Mono e tabelas a 12 px (dev. Jean, 2026-11-13)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.30._

**Decisão do dev. Jean**, depois de comparar telas: o padrão SPO passa a ser
- **Inter** na interface;
- **Intel One Mono** nas tabelas e dados (monoespaçada, números alinhados; as tabelas ficam mais largas);
- **Legível (12 px)** no texto das tabelas;
- tamanho geral **100%**.

As duas fontes já eram hospedadas no SPO (#32) e aparecem iguais em qualquer sistema.

### 41.31 Login: "Entrar" na cor de destaque e "×" do Alerta redondo no celular (dev. Jean, 2026-11-13)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.31._

**(1) "Entrar" verde no login, em qualquer destaque claro (laranja, vermelho, azul…).**
- **Causa:** o login carrega só parte da paleta (`login-theme.js`: `--accent`, `--accent-strong`, `color-400/500/600`). A regra do botão principal do #57 (`--spo-btn-fill`, que vem de `color-700/800`) tem a **mesma especificidade** (0,4,1) da regra do login do A9 e vem depois no `style.css`. Por isso vencia, e o `color-700/800` caía no verde do `:root`.
- **Correção** (bloco `[jean]` no fim do `style.css`): no login claro, `--spo-btn-fill` sai do próprio `--accent` escurecido (50%; 40% no Amarelo), e o gradiente do "Entrar" ganha `:not(#spo-login)` para vencer a regra do #57. Vale também para o "Continuar e entrar" do aviso de sessão (#61). O escuro já estava certo (usa o `--accent`).

### 41.32 Login com a paleta inteira do destaque: foco dos campos na cor escolhida (dev. Jean, 2026-11-13)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.32._

- **Causa:** a mesma do #66, num ponto a mais. O `login-theme.js` só põe no `<body>` o `--accent` e os tons 400/500/600. O contorno de foco do AA+ (#34) é `--spo-focus-ring: var(--color-700)` no claro, e o 700, o 800 e o 900 caíam no verde do `:root`. O #66 tinha tratado só o botão.
- **Correção** (bloco `[jean]` no fim do `style.css`): no login, `body.login-body[data-accent-preference=…]` recebe os tons 50–300 e 700–900 de cada destaque, com **os mesmos valores do `ACCENT_COLORS` do `main.js`**. Com isso, qualquer regra do AA+ que use esses tons fica igual no login e no resto do SPO. **Se a paleta mudar no `main.js`, mudar também aqui.** O `login-theme.js` não foi tocado.
- **Verificação:** SPO local.
  - Foco nos campos, clicando de verdade: o contorno sai no tom 700 de cada destaque no claro (Laranja #a33400, Azul #1a4f91, Clássico #0b4c44) e no `--accent` no escuro, de 5,66:1 a 10,26:1 sobre o cartão (mínimo exigido: 3:1).
  - Varredura de todos os elementos do login, nos destaques não verdes e nos dois temas: nenhuma cor da paleta Clássica sobrou.
  - `pytest` completo duas vezes seguidas.

### 41.35 "Restaurar padrão" também volta as fontes e os tamanhos (dev. Jean, 2026-11-13)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.35._

- **Causa:** as fontes (#32/#63) ficam numa chave própria do navegador (`spo-font-preferences`), que o botão `spo-theme-reset` não conhecia.
- **Correção** (só no bloco `[jean]` `initFontesPersonalizar` do `main.js`; o código do botão não muda): ao clicar em `#spo-theme-reset`, a chave das fontes é apagada e o `<body>` volta ao padrão SPO (Inter, Intel One Mono, 100%, tabelas 12 px). O cartão de fontes mostra as opções do padrão marcadas e o aviso "Fontes e tamanhos de volta ao padrão SPO."
- **Verificação:** SPO local, logado.
  - Escolhidos Atkinson, "Maior (125%)" e "Compacto (8 px)". Depois do clique: `ui=inter`, `data=intel-mono`, `size=default`, `table=legible`, a chave apagada, o cartão com as quatro opções do padrão marcadas e a interface em Inter.
  - `node --check`.
  - `pytest` completo cinco vezes. Uma falha na 1ª rodada, logo depois de desligar o servidor local, sem o nome registrado. As quatro seguintes passaram inteiras. A mudança é só no JS, que a suíte não executa.

### 41.41 Login: com sessão já aberta, a segunda tentativa de entrar encerra a outra (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.41._

- **Mudança, só na página de login** (`templates/login.html`):
  - o aviso perde os botões e passa a `role="status"`, anunciado pelo leitor de tela. Texto: "Para entrar mesmo assim, clique em **Entrar** de novo: a outra sessão será encerrada";
  - na 1ª tentativa, o servidor responde 409 e a página só mostra o aviso, com o foco no "Entrar";
  - na nova tentativa **com o mesmo e-mail**, o pedido já vai com `force_login: true`;
  - se o e-mail mudar, o aviso some e a tentativa vai normal;
  - se a 2ª tentativa falhar por outro motivo (senha errada), o aviso some, fica só a mensagem de erro, e a próxima tentativa recomeça do zero;
  - sem o aviso na página, continua a pergunta do `window.confirm`.
- **A rota `/login`, o 409 e o `force_login` não mudam** (`rotas/auth_routes.py` e `app.py` não foram tocados), nem o fluxo sem JavaScript. A automação não é afetada.
- **CSS:** saem as regras dos botões do aviso (`.login-sessao-acoes` e a borda do "Cancelar", ambas do #61), e o texto do aviso passa a margem 0 e entrelinha 1,5.
- **Verificação:**
  - o script da página rodou com um `fetch` falso (nenhum pedido ao servidor, nenhuma senha real) em quatro cenários: 409 → aviso sem `force_login`; 2ª tentativa com o mesmo e-mail → `force_login: true` e entrada; troca de e-mail → aviso some, sem forçar; senha errada na 2ª → aviso some, erro aparece, a 3ª recomeça sem forçar;
  - `pytest` completo duas vezes seguidas;
  - o teste com duas sessões reais fica para o teste humano: entrar com o mesmo usuário em dois navegadores.

### 41.45 Personalizar: opção "Espaçamento entre linhas" (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.45._

- **Personalizar Temas SPO**, cartão "Fontes e tamanho do texto": novo grupo **"Espaçamento entre linhas"** com **Padrão (1,5)**, **Amplo (1,75)** e **Mais amplo (2,0)**, na mesma marcação dos outros grupos de fontes.
- **`main.js`, bloco `[jean]` `initFontesPersonalizar`:** a opção entra em `OPTIONS`, `DEFAULTS` (`line: "default"`) e `ATTRS` (`data-spo-line-height` no `<body>`). Ela fica no navegador, na mesma chave das fontes (`spo-font-preferences`), e o **"Restaurar padrão"** (#72) volta para 1,5 sem código novo, porque restaura todo o `DEFAULTS`.
- **`style.css`, bloco `[jean]`:** com `ample`/`larger`, o texto corrido do conteúdo (`p`, `li`, `dd`, `blockquote`, `.alert`, `.muted`, `small`, metadados da "Última atualização", descrições dos cartões de Governança, dica da Início) passa a 1,75/2,0. **Títulos, tabelas, botões, rótulos e campos não mudam**, para não desmontar o layout. A regra usa `:not(#spo-entrelinha)` para vencer a do passo 1 (#75).
- Atende ao WCAG 1.4.12 (espaçamento de texto, AA) e ao 1.4.8 (apresentação visual, AAA).
- **Verificação:** SPO local, logado.
  - Escolhendo cada opção no próprio cartão: subtítulo, `p.muted` e amostra a 1,5 / 1,75 / 2,0; título (1,2), célula de tabela (1,5) e botões sem mudança.
  - "Restaurar padrão": volta a "Padrão (1,5)" e apaga a escolha salva.
  - Com "Mais amplo (2,0)": FIP 613 (com dados), Teto Orçamentário, Meta Física e Limpeza de Histórico sem texto cortado e sem rolagem lateral. O botão "+" da Meta Física tem o mesmo recorte de 4 px com a opção no padrão, que já existia e não tem relação com esta mudança.
  - `node --check`; `pytest` completo duas vezes seguidas.
- **Padrão spo-aa+:** a §2.1 ganhou a linha da opção, e a §2.2 marca os passos 1–3 da aplicação como feitos.

### 41.49 Gráficos na escala tipográfica e legíveis em tela estreita (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.49._

- **Correção:** bloco `[jean]` `initGraficosEscala` no fim do `main.js`. Ele envolve `Plotly.react`/`Plotly.newPlot` e ajusta o layout e os dados **de qualquer gráfico antes de desenhar**, sem mudar o código de cada gráfico:
  - fonte da interface (a do `<body>`), com mínimo de **12 px** no texto, nos eixos, na legenda e nas notas;
  - eixos com `automargin`;
  - legenda **vertical no pé do gráfico** (`yref: "container"`), um item por linha;
  - pizza com `automargin` (rótulos de fora inteiros);
  - gráfico estreito (≤ 520 px): margens laterais mínimas, eixo X inclinado e nomes longos da legenda quebrados entre palavras (`<br>`).
  - Ao redimensionar, o gráfico que muda de faixa é redesenhado a partir do layout e dos dados originais.
- **Limite conhecido:** trocar a fonte no Personalizar vale para os gráficos a partir do próximo desenho (filtro, aba ou recarga).

### 41.52 Login com as fontes e o tamanho escolhidos no Personalizar (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.52._

As opções de "Fontes e tamanho do texto" ficam no navegador (`localStorage`, chave `spo-font-preferences`) e eram aplicadas só pelo `main.js`, que a tela de login não carrega. Por isso o login aparecia sempre com o padrão (Inter, 100%, entrelinha 1,5), mesmo para quem escolheu outra fonte ou texto maior.

- **`static/js/login-theme.js`:** lê a mesma chave e grava no `<body>` os mesmos atributos do `initFontesPersonalizar` (`data-spo-font-ui`, `-font-data`, `-text-size`, `-table-text`, `-line-height`). Valor desconhecido fica no padrão SPO. **Se as opções mudarem no `main.js`, mude também lá.**
- **CSS** (bloco `[jean]` no fim do `style.css`): a fonte da interface já valia pelas regras do `body`. Agora o **tamanho geral** amplia o cartão de login (112% e 125%, como o `.main` nas outras telas) e a **entrelinha** vale para o texto corrido do cartão.
- **Verificação:** o login foi aberto (sem sessão) com várias combinações, em 1280, 375 e 320 px. Os atributos ficam certos e um valor inválido volta ao padrão. A fonte aplicada é a escolhida; a entrelinha fica em 21, 24,5 e 28 px. O cartão é ampliado sem rolagem lateral nem corte, e sem escolha salva nada muda. `pytest` completo duas vezes seguidas.

### 41.53 Opção "Leitura" no Personalizar (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.53._

- **Opção "Leitura"** (cartão "Fontes e tamanho do texto", atributo `data-spo-reading`, também lida no login):
  - **Padrão SPO:** largura livre, como antes do #92;
  - **Intermediário:** o #92 (72ch e 1 entrelinha entre parágrafos);
  - **Estrito (WCAG AAA):** no máximo 80 caracteres reais por linha e 1,5 entrelinha depois de cada parágrafo.
- **Largura do Estrito, medida por fonte** (o `ch` é a largura do "0", mais larga que a média das letras): Inter 53ch, Atkinson 48ch, Source Sans 3 56ch, Do sistema 52ch. Com 80ch, o pior caso dava 117–130 caracteres; com esses valores, 78 no máximo. Fonte nova no Personalizar precisa dessa medição.
- **Espaço entre parágrafos no Estrito:** quem reserva o espaço é o parágrafo de cima (`p:has(+ p)`), com 1,5 da própria entrelinha, e o de baixo fica sem margem. Em cartões flex/grid as margens não se sobrepõem: somar as duas dava até 3 entrelinhas, e regra só no de baixo dava 1,34 quando o de cima tinha letra maior (boas-vindas da Início).
- **Verificação** (SPO local, contagem exata de caracteres por linha):
  - 14 tamanhos de tela (celulares e tablets em pé e deitados, notebooks, desktop, ultrawide, monitor em retrato, zoom de 200%) na Limpeza: Estrito ≤ 78 em todos, sem rolagem lateral;
  - Início, Sessão e Login e Contrato Visual: Estrito com ≥ 1,5 entrelinha entre parágrafos, também a 390 px;
  - Padrão SPO com `max-width: none`;
  - opção no Personalizar (abre, marca, grava, volta);
  - login respeitando a escolha;
  - `pytest` completo duas vezes seguidas.

### 41.55 "Visão e cores": cores dos gráficos seguras para daltonismo (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.55._

- **Cartão novo "Visão e cores"** no Personalizar Temas, com a opção **"Cores dos gráficos"**: Padrão SPO / Seguras para daltonismo.
  - A preferência fica na mesma chave das fontes (`spo-font-preferences`, campo `cvd`, atributo `data-spo-cvd`) e o login também a lê.
  - O cartão usa a mesma lógica do "Fontes e tamanho do texto" (`initFontesPersonalizar`, agora para os dois cartões). Os próximos ajustes de visão (foco, links, 150%, brilho, movimento) entram nele.
- **Paleta** (função `[jean]` `spoTemaDiagramaComDestaque`, que já ajustava o tema dos diagramas): com a opção ligada, a paleta vira a **Okabe-Ito**, distinta nos três tipos de daltonismo.
  - No escuro, o azul-escuro e o azul-claro trocam de lugar e o preto vira branco.
  - Positivo/negativo da cascata passam a azul × laranja. O acumulado do Pareto usa a mesma paleta.
  - As tabelas coloridas por grupo (aba Tabelas do Teto) seguem, porque leem o mesmo tema (`getTetoChartColors`).
- **Padrões, contorno e marcadores** (envoltório `initGraficosEscala`):
  - barras, fatias e blocos ganham **contorno** na cor do texto. É o contorno que garante 3:1 com o fundo (WCAG 1.4.11);
  - barras e fatias ganham **padrão por cor**: a mesma cor tem o mesmo padrão em todos os gráficos e na legenda;
  - linhas ganham marcador e traço próprios.
- **Validador** `window.spoValidarPaletaDaltonismo()` (bloco `[jean]` no fim do `main.js`): simula as três deficiências (matrizes de Machado, 2009) e mede a diferença entre séries e o contraste do contorno.
- **Verificação** (SPO local):
  - **validador:** menor diferença entre séries 12,2 (protanopia), 11,6 (deuteranopia) e 10,9 (tritanopia); contorno com 13,1:1 no claro e 16,6:1 no escuro;
  - **Teto, claro e escuro:** os 5 gráficos com a paleta e os padrões certos, e a aba Tabelas com as mesmas cores;
  - **captura com deuteranopia simulada:** as três séries do gráfico por exercício continuam distintas;
  - **cartão:** sem corte a 390, 820, 1080 (retrato), 1366 e 1920 px; a opção grava e volta ao padrão;
  - **login:** respeita a escolha, e um valor inválido volta ao padrão;
  - `pytest` completo duas vezes seguidas e `node --check`.
- **Teste humano:** na homologação, o colega com daltonismo compara o Teto com e sem a opção.

### 41.56 "Visão e cores": foco reforçado e links sublinhados (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.56._

- **Realce do foco** (`data-spo-focus`): Padrão SPO / Reforçado.
  - **Padrão:** o anel de 3 px de `feat/foco-visivel` continua.
  - **Reforçado (anel duplo):** 4 px na cor do anel de foco entre duas faixas de 2 px na cor do fundo (branco no claro, preto no escuro). O anel contrasta com a própria faixa (≥ 4,69:1 nos 8 destaques, no claro e no escuro), então aparece sobre qualquer superfície, inclusive sobre botões na cor de destaque (WCAG 2.4.13, AAA).
  - **Áreas que cortam o que passa da borda:** o anel vai para dentro. Uma varredura de 9 telas achou os controles cortados: grupos do Personalizar (`.visual-pref-group`, 13 botões), filtros das colunas dos relatórios (`.table-responsive`, 21 no FIP 613), botões de ação das tabelas de Usuários e API e um botão de aviso modal. O anel padrão de 3 px também é cortado ali, mas esses controles têm uma faixa interna própria no foco.
- **Links** (`data-spo-links`): Padrão SPO / Sempre sublinhados.
  - Valem os links de texto do conteúdo e do login. Menu, abas, botões e links com cara de botão não mudam.
  - Hoje os links de texto estão nas 4 telas institucionais (17 links). Sem a opção, eles se distinguem do texto pelo negrito; a cor sozinha dá só 1,47:1 contra o texto em volta.
- **Verificação** (SPO local):
  - **Tab real**, esperando a transição dos controles: 7 tipos de controle com o anel duplo (linhas do modo de aparência, cores, grupos, botões, menu, filtros das colunas, botões do relatório), no claro e no escuro, com capturas;
  - **links:** os 17 links das telas institucionais sublinhados, e nenhum botão ou aba afetado;
  - **Personalizar:** a opção abre, marca, grava e volta;
  - **login:** respeita as duas escolhas, e o "Esqueci a senha" fica sublinhado;
  - **cartão:** sem corte em 390, 820, 1080 (retrato), 1366 e 1920 px;
  - `pytest` completo duas vezes seguidas e `node --check`.

### 41.57 "Visão e cores": texto 150%, brilho reduzido e reduzir movimento (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.57._

- **"Muito grande (150%)"** no grupo "Tamanho do texto" (`data-spo-text-size="xlarge"`): amplia a área de conteúdo e o cartão de login, como o 112% e o 125%. Medição em 5 telas (Limpeza, Teto, Personalizar, Usuários, FIP 613) e até 14 tamanhos de tela, comparando com o tamanho padrão:
  - **celular (< 480 px):** o 150% deixava só 240 px úteis, abaixo dos 320 px de referência do WCAG 1.4.10. A palavra "Espaçamento" passava da borda no Personalizar. Ali o 150% vale **125%**;
  - **Teto a 360 px:** o "Teto total" (#89, sem quebra de linha, ~222 px) passava da borda. Isso **já acontecia com o 125%**. Quando o painel tem até 300 px, o valor pode quebrar entre o "R$" e o número;
  - **telas com cabeçalho e duas colunas** (Usuários, Cadastrar, API, Permissões): o `@media` olha a largura da janela, não a da área ampliada. A 820 px e 150%, Usuários ficava com 547 px úteis em duas colunas e rolava para o lado; o mesmo valia para o 112% e o 125% em algumas larguras. Os limites de uma coluna (900, 1100 e 720 px) agora são multiplicados pelo fator de cada tamanho;
  - **resultado:** sem rolagem lateral nova e sem texto cortado novo em nenhuma das medições.
- **"Brilho do tema claro: Reduzido"** (`data-spo-brightness`, só no claro): cartões `#eceef1` (cerca de 15% menos luz que o branco) e página `#dadee4`. Para não perder contraste, o texto secundário escurece (`#4a4850`) e o texto de destaque passa ao tom 800. Comparação de todo o texto em 6 telas: nenhuma falha nova de AA ou AAA; o menor caso nas telas de dados continua ≥ 7:1.
- **"Movimento: Reduzir sempre"** (`data-spo-motion`): faz o mesmo que a preferência do sistema já fazia (bloco "Movimento reduzido"), mesmo sem ela.
  - **Medido:** de 117 elementos com transição ou animação no Teto, ficaram 0. A rolagem suave pedida pelo JS fica imediata.
  - **Achado:** a rolagem da página é feita pelo `<html>`, que rola suave pelo CSS, e pedir `behavior: "auto"` seguia o CSS. O `initMovimentoReduzido` agora pede `"instant"`, e a regra do CSS também vale no `<html>`.
- **Também verificados:** o Personalizar (as três opções abrem, marcam, gravam e voltam), o login (as três opções; o 150% vale 125% a 360 px, sem rolagem lateral), `pytest` completo duas vezes seguidas e `node --check`.
- **Defeito antigo anotado, fora deste PR:** a 360 px e no tamanho padrão, Usuários, Cadastrar e API rolam para o lado. A regra que põe essas telas em uma coluna até 900 px perde, por especificidade, para a que define as duas colunas delas.

### 41.59 Densidade automática (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.59._

Decisão do dev. Jean na discussão de 14/11. Em vez de trocar o preset do Contrato Visual pelo tipo de aparelho ("Pressão operacional" no celular, "Compacto gerencial" no tablet), a densidade passa a ser escolhida pela **largura da janela**. A largura acompanha zoom, redimensionamento e janela dividida; já retrato × paisagem não diz se a tela é de celular (um monitor em retrato tem 1080 px e mouse).

- **Opção nova "Automática" (padrão SPO)** no grupo "Densidade visual" do Personalizar Temas:
  - mais de 1200 px: confortável;
  - de 721 a 1200 px: compacta;
  - até 720 px: ampla.
- **Como funciona:** as 34 regras de CSS de cada densidade são as mesmas de sempre. Muda só o valor que vai para `data-visual-density`: com "Automática", o bloco `[jean]` `initDensidadeAutomatica` (fim do `main.js`) escolhe pela largura e atualiza ao redimensionar. A escolha da pessoa fica em `data-visual-density-pref`. O alvo de toque de 44 px continua vindo da regra de ponteiro grosso (#35).
- **Código do Cleber tocado (4 linhas, marcadas `[jean]`):** a opção `auto` em `COMMON_VISUAL_OPTIONS.density`, o padrão `density: "auto"` em `COMMON_VISUAL_DEFAULTS` (era `"comfortable"`) e a linha de `applyCommonVisualPreferences` que grava a densidade (agora grava a escolhida e a em uso). O `login-theme.js` resolve do mesmo jeito.
- **Quem já tem preferências visuais salvas no navegador** continua com a densidade gravada (em geral "confortável", o padrão antigo, porque o SPO grava todas as preferências juntas quando se muda qualquer uma). Para passar à automática, basta escolhê-la ou usar "Restaurar padrão".
- **Verificação** (SPO local):
  - **faixas:** os 14 tamanhos da matriz caem na faixa certa e mudam na hora ao redimensionar. Ampla: celulares em pé e zoom de 200% (640 px). Compacta: celulares deitados, tablets em pé e deitados, monitor em retrato de 1080 px. Confortável: de 1280 px em diante;
  - **layout:** 9 telas (Início, Limpeza, Teto, FIP 613 relatório e upload, Personalizar, Usuários, Parceiros, Estrutura) a 390 px (ampla) e a 1024 px (compacta), comparadas com a confortável: nenhuma rolagem lateral nem corte novos;
  - **Personalizar:** escolher uma densidade fixa grava e vale; voltar a "Automática" recalcula;
  - **login:** ampla a 390, compacta a 1024 e confortável a 1440;
  - `pytest` completo duas vezes seguidas e `node --check`.

### 41.60 Busca de telas (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.60._

- **Como abrir:** pelo item **"Buscar tela"** do menu lateral, logo abaixo de "Início" (inserido pelo JS, sem mexer no `base.html`; com o mesmo tamanho e o mesmo comportamento dos outros itens, inclusive com o menu recolhido e no menu do celular), ou pelos atalhos **Ctrl+K** (Cmd+K no Mac) e **/** (este, fora de campos de texto).
- **Visual no padrão dos avisos do SPO:** cartão `modal-card`, título `modal-header-title` (na cor de destaque) com o subtítulo das teclas, campo `.field` com rótulo e o botão secundário **"Fechar"** no rodapé. O fundo copia o visual do `.modal-overlay` sem usar a classe: com ela, o tratamento genérico dos avisos (#41) também prendia o Tab e o foco voltava sempre ao "Fechar".
- **O que procura** (bloco `[jean]` `initBuscaTelas` no fim do `main.js`):
  - só as **telas do menu que a pessoa vê**: o menu já vem filtrado pelas permissões (`applyMenuPermissions` esconde com `display: none`), e a busca segue o mesmo filtro. Não muda nada no servidor;
  - ignora acentos e maiúsculas e aceita parte da palavra. Compara com o nome, o caminho no menu e o **glossário** de siglas e assuntos (`GLOSSARIO`: empenho → EMP, PAOE → Ações/PAOE, senha → Alterar Senha, daltonismo → Personalizar, teto → Teto Financeiro e Teto Orçamentário…);
  - cada resultado mostra o caminho ("Relatórios › Execução"), e os links externos aparecem marcados "abre em outra aba";
  - Enter abre a tela pelo mesmo caminho do clique no menu.
- **Telas recentes:** com a caixa vazia, aparecem as 5 últimas telas **do próprio usuário**, abertas pela busca ou pelo menu. Ficam no navegador, numa chave por conta (`spo-telas-recentes:<conta>`), e não acompanham a pessoa em outro computador.
- **Acessibilidade:**
  - diálogo modal (`role="dialog"`, `aria-modal`), com caixa de busca `combobox` e lista `listbox` (padrão WAI-ARIA, `aria-activedescendant`);
  - setas escolhem, Enter abre, Esc fecha e devolve o foco a quem abriu (ou ao item "Buscar tela" do menu);
  - o Tab fica preso no diálogo, e a quantidade de resultados é anunciada (`role="status"`);
  - a busca fica acima dos avisos modais (camada 10001).
- **Fase 2, com o Cleber:** glossário administrável no banco, com tela de administração e permissão própria, e registro das **buscas sem resultado**. A fase 1 deixa o glossário no mesmo formato (tela → termos) para virar dados iniciais.
- **Verificação** (SPO local):
  - **busca:** "empenho", "emp", "paoe", "senha", "daltonismo", "fip", "teto", "consultar" e "dotação" acham as telas certas;
  - **teclado real:** Ctrl+K, "/", setas, Enter (abriu o Relatório EMP e o foco foi ao título), Tab preso, Esc;
  - **resultado vazio:** "Nenhuma tela encontrada";
  - **permissões:** escondendo o grupo de Usuários e o Teto Orçamentário como as permissões fazem, "senha" deixa de achar e "teto" acha só o Teto Financeiro;
  - **recentes por conta:** outra conta no mesmo navegador não vê as recentes da primeira;
  - **telas:** 12 tamanhos (celular em pé e deitado, tablets, notebooks, desktop, ultrawide, retrato, zoom 200%), com a caixa dentro da tela, sem rolagem da página e sem resultado cortado;
  - `pytest` completo duas vezes seguidas e `node --check`.

### 41.63 Declaração de acessibilidade "SPO-AA+" (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.63._

- **Cartão "Acessibilidade do SPO"**, no fim do Personalizar Temas, ocupando a largura inteira (`templates/partials/atualizar_personalizar_spo.html`):
  - **padrão SPO-AA+:** WCAG 2.2 nível AA como meta mínima e critérios AAA oferecidos como opção. Segue também o eMAG, em atenção à Lei Brasileira de Inclusão (Lei 13.146/2015). O texto diz que é uma declaração do que foi verificado, não uma certificação;
  - **três blocos:**
    - **Atendido**, com os critérios WCAG citados;
    - **Atendido por opção**, com o nome de cada opção da página;
    - **Parcial ou em avaliação**: leitor de tela, Firefox e Safari, cores forçadas, o mockup PTA 2027 e listas nativas no celular;
  - **links para as fontes, abrindo em nova aba** (ícone ↗ e "(abre em nova aba)" para leitor de tela; `rel="noopener noreferrer"`):
    - WCAG 2.2 e cada critério citado (1.1.1, 1.4.1, 1.4.3…) na **tradução oficial do W3C Brasil** (`w3c.br/traducoes/wcag/wcag22-pt-BR/#<critério>`), que leva direto ao critério em português;
    - eMAG (`emag.governoeletronico.gov.br`), Lei 13.146/2015 (Planalto), paleta Okabe-Ito (Color Universal Design) e fonte Atkinson Hyperlegible (Braille Institute);
  - **como foi avaliado:** a bateria de 14/11 e o teste com os colegas na homologação;
  - **contato para barreiras:** nger@edu.mt.gov.br.
- **Manutenção:** a cada avaliação ou opção nova, atualizar o cartão no mesmo PR (padrão visual §5.1).

### 41.64 Atalhos do teclado (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.64._

- **Ajuda "Atalhos do teclado"** (bloco `[jean]` `initAtalhosTeclado` no fim do `main.js`):
  - **visual:** diálogo no mesmo padrão da busca de telas (#106); teclas em `<kbd>`;
  - **seções:** Navegar, Menu lateral, Abas/diagramas/mapa, Busca/avisos/filtros e Navegador;
  - **como abre:** tecla **?**, item "Atalhos do teclado" no menu (abaixo de "Buscar tela", inserido pelo JS) e links com `data-spo-atalhos` (cartão Teclado e declaração de acessibilidade);
  - foco no "Fechar", Tab preso entre a lista rolável e o botão, Esc fecha e o foco volta.
- **Voltar ao menu:**
  - **Alt+M** leva o foco ao item da tela atual no menu; se ele estiver num grupo fechado ou no menu recolhido, ao grupo visível que o contém;
  - link de pulo **"Ir para o menu (Alt+M)"** ao lado do "Pular para o conteúdo" (#41).
- **Cartão "Teclado" no Personalizar**, com a opção **"Atalhos de uma tecla"** (Ligados, o padrão / Desligados), atributo `data-spo-keys`:
  - **por quê:** WCAG 2.1.4 (nível A) pede que atalhos de uma tecla só possam ser desligados (atrapalham comando de voz);
  - desligada, barra o **/** da busca (uma condição a mais na linha do atalho em `initBuscaTelas`) e o **?** da ajuda; **Ctrl+K** e **Alt+M** continuam;
  - entra também no `login-theme.js`, como as demais opções.
- **Dica dos componentes com setas:** ao chegar pelo teclado ao menu, a uma aba ou a um cartão do mapa/diagrama, aparece uma faixa curta no rodapé da tela (ex.: "← → trocam o cartão · Home/End: primeiro/último · Tab sai"). O leitor de tela recebe o mesmo texto pelo `aria-describedby`.

### 41.65 Alt+M: anel visível e Esc para sair do menu (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.65._

- **"O Alt+M só funciona depois da ?":** um registro das teclas no navegador mostrou que o SPO recebia e tratava todos os Alt+M. O foco ia ao menu, mas **sem anel e sem a dica**. O Chrome só mostra o `:focus-visible` quando a última ação foi uma tecla "comum": depois de um clique de mouse, foco movido por combinação com Alt/Ctrl fica sem anel. Depois da **?** (tecla comum), o anel aparecia.
  - **Correção:** o foco levado por atalho passa por `focarComAnel` (`focus({ focusVisible: true })` + classe `spo-foco-atalho`, com anel igual ao do teclado, inclusive no foco reforçado), e a dica aparece.

### 41.66 O padrão SPO-AA+ para o usuário: resumo e documento técnico no servidor (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.66._

- **Janela "O padrão SPO-AA+"** (bloco `[jean]` `initPadraoSpoAaPlus` no fim do `main.js`), no visual da busca e da ajuda dos atalhos. Abre por links com `data-spo-padrao` (hoje: "SPO-AA+" na declaração). Duas vistas:
  - **Resumo**, escrito para quem usa o SPO: o que o padrão garante em todas as telas (metas do §1 do documento técnico), o que se pode ajustar no Personalizar e como é verificado. **Mantido à mão:** mudou uma meta ou opção, atualizar o `RESUMO` no mesmo PR (regra no §5.1 do documento);
  - **Documento técnico completo**, lido do servidor, com sumário que rola dentro da janela.
- **Rota nova** `GET /api/documentos/<nome>` (`rotas/documentos_routes.py`, blueprint `documentos_bp`, só acrescentado ao `rotas/__init__.py`; `login_required`):
  - devolve `titulo`, `html` e `sumario` do documento convertido; nome desconhecido → 404; arquivo ilegível → 503;
  - hoje só `padrao-spo-aa-plus` → `docs/padrao-visual-spo-aa-plus.md`. Como o deploy copia o repositório, cada PR que muda o documento atualiza o que o usuário vê.
- **Conversor de markdown próprio** (`services/documentos.py`), para não acrescentar dependência no servidor:
  - cobre títulos, parágrafos, listas (aninhadas, numeradas, de marcar), citações, tabelas, `código`, **negrito** e links http(s);
  - todo texto é escapado; links só http/https, abrindo em nova aba;
  - testes em `tests/test_documentos.py` (7), inclusive o documento real: HTML balanceado e sem markdown cru.

### 41.67 Ponte de preferências para o PTA 2027 (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.67._

- **No SPO** (bloco `[jean]` `initPontePta2027` no fim do `main.js`): uma mensagem própria, `spo-preferencias`, com os atributos `data-spo-*`/`data-visual-*` do `<body>` e os tons calculados de foco e destaque (`--spo-focus-ring`, `--spo-focus-halo`, `--spo-accent-text`, `--spo-accent-graphic`). Vai quando o mockup carrega ou pede, e a cada mudança de preferência (observador próprio dos atributos; o do código original só olha `class`/`style`).
- **No mockup** (arquivos novos, `[jean]`; o `index.html` ganhou só duas linhas, o `<link>` e o `<script>`):
  - `assets/spo-preferencias.js`: grava os atributos no `<body>` (classe `spo-ponte`), valida os valores e os tokens;
  - `assets/spo-preferencias.css`: fontes do SPO (o mockup pedia "Inter", que não carregava, e caía na Segoe UI), fonte e tamanho das tabelas, entrelinha, leitura (72ch/estrita), anel de foco do SPO e foco reforçado, links sublinhados, contraste médio/reforçado (no escuro, a borda segue a lógica do mockup: destaque a 42%/64%), brilho reduzido e movimento reduzido.
- **O que fica fora:**
  - **tamanho do texto:** já funcionava, porque o zoom da área do SPO (`.main`) amplia o iframe (medido: escala 1,25 com "Maior"). Ampliar de novo dobraria;
  - densidade e estilo dos cartões (o mockup tem layout próprio, em px) e daltonismo nos diagramas (imagens SVG prontas);
  - a escala tipográfica do próprio mockup (há textos de 9 a 11 px): fica para um PR de adequação do mockup ao SPO-AA+.

### 41.68 Mockup PTA 2027 no padrão SPO-AA+ (dev. Jean, 2026-11-14)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.68._

- **Cores** (`static/pta2027_v63r/assets/spo-preferencias.css`, seções "Cores do PTA 2027" e "Tema escuro"):
  - **claro:** `--teal`, `--teal-dark` e `--spo-accent-deep` passam ao tom de texto do destaque do SPO (`--spo-accent-text`), o mesmo dos botões principais do SPO; texto secundário `#3f4e52` (8,3:1), com variações para brilho reduzido e contraste reforçado; 5 cores literais do mockup (×, avisos, "Não definida", textos semitransparentes) trocadas;
  - **escuro, como o SPO:** texto no destaque usa o tom claro do destaque; botões e marcadores com fundo no destaque passam a texto `#111111`; badges numerados sobre o tingido do destaque e o texto do cartão selecionado na cor do texto;
  - abas inativas na cor do texto (Vermelho e Roxo davam 6,9:1 sobre o tingido).
- **Tamanho do texto:** `static/pta2027_v63r/assets/spo-escala.css`, **gerado** por `scripts/gerar_escala_pta2027.py`:
  - 72 regras do mockup com fonte de 8 a 11 px passam a 12 px, e a 11 px nos rótulos em maiúsculas;
  - mesma especificidade da regra original (`:where`), vencendo por carregar depois, para não encolher as regras do mockup que aumentam o tamanho num caso;
  - teste `tests/test_escala_pta2027.py`: o arquivo gravado tem de ser igual ao gerado (se o `pta-2027.css` mudar, rode o script de novo) e nenhuma regra gerada fica abaixo de 11 px nem soma especificidade.
  - O `<small>` sem tamanho próprio vai a 12 px (`:where`, as regras do mockup vencem).

### 41.75 Movimento reduzido: rolagem, cartões parados e aberturas do mockup PTA/OpR (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.75._

- **Rolagem suave por `scrollTo`/`scrollBy`:** o `initMovimentoReduzido` só tratava o `scrollIntoView`; a edição de Etapa (`window.scrollTo({ behavior: "smooth" })`) rolava suave mesmo com "Reduzir sempre". Agora `window` e `Element` (`scrollTo`, `scroll`, `scrollBy`) também viram rolagem imediata quando a opção (ou o sistema) pede; vale para chamadas futuras.
- **Mockup PTA/OpR:** o ajuste do `main.js` não vale dentro do iframe; o `spo-preferencias.js` do mockup ganhou o mesmo tratamento (4 chamadas a `scrollIntoView` suaves no `pta-2027.js`, que não foi tocado).

### 41.76 Telas estreitas: rolagem automática do mockup e janelas no celular (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.76._

- **Rolagem automática do mockup PTA/OpR só em tela estreita.** O ganho da rolagem suave ao abrir as metas de um pilar ou os eixos de uma macropolítica aparece no celular e na janela dividida, onde o painel costuma abrir fora da tela. Agora, com "Movimento: Automático", a rolagem suave original do mockup só vale quando a janela do SPO tem até 1200 px de largura (as faixas compacta e ampla da densidade automática) ou com o preset "Pressão operacional" do Contrato Visual (`data-visual-contract-preset="mobile"`, que realça o modo móvel e split). Em tela larga, a página não rola; só se o começo do painel ficar fora da tela é que ela rola, suave, até ele (para o clique não parecer sem efeito). Com "Reduzir sempre", nada muda (rolagem contida e imediata do #121). O mockup lê a largura e o preset na página do SPO (mesma origem); `spo-preferencias.js`, `telaEstreita()` e `comRolagemContida()`.

### 41.77 Busca de telas e ajuda dos atalhos por cima de um aviso (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.77._

Correção (`main.js`):
- enquanto uma dessas janelas estiver aberta (`janelaPorCima()`), o aviso de baixo não toma o foco ao aparecer e não trata as teclas;
- ao fechar a janela com um aviso ainda aberto, o foco vai para o aviso (`window.spoFocarAvisoAberto`, chamado pelo `fechar()` das três janelas), e não para quem abriu a janela, que fica atrás dele; sem aviso, o foco volta para quem abriu, como antes.

### 41.79 Janela dividida sem largura mínima (dev. Jean, 2026-11-15)

O dev. Jean testou o SPO no Chrome com o modo de aparelho do DevTools (iPhone 393 × 852 e 440 × 956) e no painel estreito do app, e a página inteira (topo, conteúdo, cartões e o quadro do PTA/OpR) passava da borda. Causa: o modo "split sob pressão" do contrato visual de 2026-07 (`syncSplitPressureMode` no `main.js`; `html.spo-split-pressure` no `style.css`). Ele liga com a área da página até 480 px dentro de uma janela de navegador pelo menos 180 px mais larga (pensado para o Chrome com a janela dividida) e fixava `--spo-operational-min-width` em 560 px (520 com o preset "Pressão operacional"), com rolagem lateral. No celular de verdade não liga (janela e página têm a mesma largura); liga no DevTools e no painel estreito. Não veio dos PRs #121–#124.

Decisão do dev. Jean: desligar a largura mínima. Bloco `[jean]` no fim do `style.css`: com `spo-split-pressure`, `--spo-operational-min-width` vai a 0 (inclusive com o preset "Pressão operacional"), a página não rola para o lado e o quadro do PTA/OpR acompanha a largura. A classe continua sendo posta pelo JS (que não mudou); a página se reorganiza como no celular (WCAG 1.4.10; padrão SPO-AA+: sem rolagem lateral a 320 px).

Medido no Chrome do dev. Jean (sessão dele, tema escuro, destaque roxo), com o SPO em quadros de 393 e 440 px dentro da janela de 1080 px (o modo liga): antes, a página tinha 560 px e 12+ elementos passavam da borda; agora, Início e Programação PTA/OpR com a largura do quadro, nenhum elemento além da borda, e o mockup do PTA/OpR com 276 e 323 px, sem rolagem lateral. Também a 390 px (a largura das baterias do SPO) e a 320 px (WCAG 1.4.10): nada além da borda; mockup com 273 e 203 px. `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `login.html`: só a linha `?v=` do CSS (`20261115.jean-sem-largura-minima.1`).

### 41.83 Fonte Mulish como opção da interface (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.83._

- **Arquivos:** `static/fonts/mulish/` (`mulish-latin-wght-normal.woff2`, 29.968 bytes, e `OFL.txt`), do pacote oficial `@fontsource-variable/mulish@5.3.0` pelo jsDelivr, com sha256 no `static/fonts/LEIA-ME.md`. Nada é carregado de fora; o navegador só baixa a fonte quando ela é escolhida.
- **Opção:** Personalizar → Fontes e tamanho do texto → Fonte da interface → "Mulish" (template, `OPTIONS.ui` no `main.js`); também no login (`login-theme.js`) e no mockup PTA/OpR (`spo-preferencias.css`). O padrão continua a Inter.
- **"Leitura → Estrito":** largura média das letras ÷ largura do "0" medida (Mulish 0,801; Inter 0,787) dá 54ch; ficou 53ch, como a Inter, pelo lado conservador (no máximo 80 caracteres por linha). A mesma conta reproduz os valores já calibrados (Atkinson 47,7 → 48; Source Sans 3 57,8 → 56).

### 41.85 "Densidade do texto" no lugar de "Texto das tabelas" (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.85._

- **Padrão SPO: Intermediário.** O dev. Jean testou as três densidades no FIP 613 e as demais telas no Intermediário e decidiu: o Intermediário é o padrão; o Legível fica para quem precisa de letra maior e entra na "Leitura confortável" da Atkinson (`initSugestaoAtkinson`, agora com cinco ajustes).
- **Conversão da escolha gravada** (uma vez só, marca `spo-densidade-v2` no `localStorage`, no `main.js` e no `login-theme.js`): a densidade de mesmo tamanho nas tabelas — Legível antigo (12 px) → Intermediário (12), Intermediário antigo (10) → Compacto (10), Compacto antigo (8) → Compacto (10). Testado com armazenamento simulado (sem escolha, cada valor antigo, segunda execução).
- **Variáveis** (bloco `[jean]` no fim do `style.css`), definidas no `body` por densidade: `--spo-fs-dado` 13/12/10 px, `--spo-fs-controle` 13/12/11, `--spo-fs-texto` 14/13/12, `--spo-fs-leitura` 16/14/13, `--spo-fs-titulo` 16/14/13, `--spo-fs-rotulo` 11, `--spo-fs-secao` 20, `--spo-fs-pagina` 24 (Legível/Intermediário/Compacto). Sem atributo vale o Intermediário.
- **Tabelas já nos valores novos** (decisão do dev. Jean): tabelas dos relatórios, filtros de múltipla escolha, paginação, demais tabelas do conteúdo e a amostra do Personalizar usam `--spo-fs-dado` — Legível 13 px (antes 12), Intermediário 12 (antes 10), Compacto 10 (antes 8). As regras antigas por densidade ficaram no arquivo, sobrepostas pelo bloco novo (`:not(#spo-dens)`). Mockup PTA/OpR: tabelas 13/12/10 no `spo-preferencias.css`.
- **Textos da opção:** "Compacto — tabelas com 10 px", "Intermediário (padrão SPO) — tabelas com 12 px…", "Legível — tabelas com 13 px…; indicado para baixa visão (entra na Leitura confortável da Atkinson)"; legenda da amostra e resumo do SPO-AA+ ajustados.

### 41.88 Densidade do texto, etapa 3 (c): formulários e cadastros (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.88._

Cuidado registrado: a regra dos campos dos avisos (`.modal-card input`) é mais específica que a do campo da busca de telas (etapa a) e o baixaria de 13 para 12 px; as janelas de consulta ficam fora dela (`.modal-card:not(.spo-busca-caixa)`), e os botões delas ganharam regra própria (texto).

### 41.89 Densidade do texto, etapa 3 (d): painéis, cartões dinâmicos e Início (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.89._

Achado: a regra do "mínimo de 12 px" (escala, passo 1) tem na lista um seletor com id (`#meta-fisica-tbody td.muted`), o que a deixa mais específica que as regras da densidade para os itens dela que não ganham contexto extra; a dica da Início ficava fixa em 12 px e recebeu um seletor mais específico. Os demais itens dessa lista que ainda não seguem a densidade (filtros do Teto fora de `.field`, `.teto-dashboard-fonte`, `.contract-version-pill`, `td > code`, `.planning-status`, `.status-badge`) ficam para a etapa 4.

### 41.90 Densidade do texto, etapa 3 (e): gráficos (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.90._

`initGraficosEscala` (`main.js`), que já ajustava todo `Plotly.react`/`newPlot`, trocava o tamanho do texto dos gráficos por um mínimo fixo de 12 px. Agora o tamanho base é o de dado da "Densidade do texto", lido da variável `--spo-fs-dado` do `<body>` (Compacto 10, Intermediário 12, Legível 13 px); texto que o gráfico já pede maior que 12 px sobe ou desce na mesma diferença. No Intermediário fica como antes. Um observador do atributo `data-spo-table-text` redesenha os gráficos abertos quando a densidade muda no Personalizar (com os dados e o layout de base guardados pelo próprio bloco).

### 41.91 Densidade do texto, etapa 4: teste contra `font-size` em px (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.91._

- **Itens fixos em 12 px:** a regra "mínimo de 12 px" (escala, passo 1; bloco `[jean]`) tinha um id na lista e ficava mais específica que as regras da densidade; os itens dela (rótulos dos totais, linhas da atualização, rótulos dos indicadores e filtros do Teto, dica da Início, pílula de versão do Contrato, códigos em tabela, selos de situação…) passam a `var(--spo-fs-controle)` (Intermediário 12, como antes). O rótulo da paginação passa a `var(--spo-fs-dado)`. A linha "Fonte: FIPLAN…" do Teto, que é um `<p>` do cabeçalho, ganhou regra própria (controle) para não subir a 13 px como a descrição da página.
- **Teste** `tests/test_tamanho_fonte.py` (3 testes): conta `font-size: <n>px` no `style.css` (sem comentários) por valor — 310 declarações, 27 valores em 15/11 — e reprova valor novo ou contagem que sobe; se a contagem desce (regra antiga passou à variável), pula com o aviso de baixar a referência; e confere que as três densidades definem as variáveis. Teste negativo feito: acrescentar `15px` e `19px` reprovou com "Valores novos: ['19']; contagens que subiram: ['15px: 8 -> 9']".
- Padrão SPO-AA+: §2.2.1 com o teste e item novo no checklist do §6.1.

### 41.92 Navegação pelo teclado nas tabelas (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.92._

- **Tab** entra na tabela numa célula só, a última visitada (na primeira vez, a primeira célula de dados): tabindex itinerante, leve em tabelas grandes (só a célula da vez tem `tabindex="0"`; as outras ganham `-1` quando visitadas). Um observador da área de conteúdo prepara as tabelas desenhadas depois (troca de página, filtro); se a escolha automática foi feita antes de os dados chegarem (só o cabeçalho), ela passa à primeira célula de dados.
- **Setas** de célula em célula (também cabeçalho e linha dos filtros); **Home/End** na linha; **Ctrl+Home/End** primeira/última célula; **PageUp/PageDown** 10 linhas; **Enter** foca o primeiro controle da célula (filtro, campo, botão, link); **Esc**, dentro dele, volta para a célula. Só linhas visíveis (a linha escondida de "nenhum resultado" recebia a marca e tirava a tabela do Tab — corrigido antes do PR).
- **A tabela é uma parada só do Tab** (achado do dev. Jean no PED: o Tab atravessava os 64 filtros do cabeçalho e saía para a barra lateral): os botões, links e filtros das células (no cabeçalho, também os campos) ganham `tabindex="-1"` (o valor original fica em `data-spo-tab-orig`) e são alcançados por setas + Enter. Campos de digitação das linhas de dados continuam no Tab; nada dentro de uma lista de filtro (`.mf-panel`, checklists) é tocado.
- **Paginação** (achado do dev. Jean no PED: com o foco num botão de página, as setas não andavam e não havia como voltar à tabela): `initTecladoPaginacao`, para os botões de toda `.pagination` do conteúdo (as 20 telas usam o mesmo contêiner; a FIP 613 não usa a classe `.page-btn` nos botões — a primeira versão só a reconhecia, achado do dev. Jean). ← → andam pelos botões ativos, Home/End primeiro/último, ↑ ou Esc voltam à célula da vez da tabela do mesmo cartão. A paginação é redesenhada a cada troca de página e o foco caía no início da página; agora ele volta ao mesmo número (ou à página atual).
- **Clique numa célula** (fora de um controle e sem texto selecionado) torna-a a da vez e começa a navegação por ali.
- **Setas a partir de um filtro ou botão da célula** (com a lista fechada) andam como se o foco estivesse na célula — da linha dos filtros, ↓ desce para os dados. Dentro de um campo de digitação, ou com a lista do filtro aberta, as setas continuam do campo/da lista. A tabela não ganha `role="grid"`.
- **Rolagem lateral:** o foco sozinho só rolava o bastante para a célula aparecer em parte — voltando para a esquerda, a 1ª coluna ficava cortada e a barra não voltava ao início (achado do dev. Jean no teste). Agora a célula vai inteira à vista e, na 1ª coluna, a rolagem lateral volta ao começo (Usuários a 1000 px: → até a última coluna rola 110 px; ← até a 1ª volta a 0; Home também).
- Anel de foco por dentro da célula (`outline-offset: -3px`; por fora seria cortado pela rolagem). Seção "Tabelas" na ajuda dos atalhos e dica na faixa de teclado.

### 41.93 Kit de mockup SPO-AA+ (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.93._

- Usa o próprio `style.css` e lê do `main.js` (por `fetch` + extração do trecho) a paleta `ACCENT_COLORS`, as opções do Personalizar e as funções de teclado das tabelas: não há cópia para manter. Abre pelo SPO local sem login (`/static/kit/mockup-spo-aa-plus.html`).
- Painel com as opções **e as regras** do Personalizar: escolher a Atkinson abre a mesma oferta "Leitura confortável" (constantes lidas do `initSugestaoAtkinson`); abre com as preferências vigentes do SPO neste navegador (pedido do dev. Jean no teste: o kit deve reproduzir o comportamento exato do SPO); o que mudar vale só na página, nada gravado. Componentes de exemplo com a marcação real (cabeçalho, `module-card`, `flow-node`, formulário, `alert-info`, relatório com filtros `.mf-*`, teclado e paginação).
- `tests/test_kit_mockup.py` (4 testes): marcadores do `main.js` que o kit lê (inclusive a regra da oferta da Atkinson); funções de teclado completas; toda opção do Personalizar aparece no kit; CSS do kit sem px nem cor literal.

### 41.94 Achados do kit de mockup resolvidos: densidade dos cartões e pos/neg (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.94._

- No SPO (bloco `[jean] O padrão SPO-AA+` do `main.js`): o pedido do documento manda `X-Requested-With`, então sem sessão a rota responde 401 em vez de redirecionar para o login, e a janela diz "entre no SPO neste navegador e abra de novo" (antes: "tente de novo em instantes", que não ajudava). Vale também para sessão expirada no SPO.
- Para o agente: o comentário do topo do kit e o passo 0 do "Como usar" apontam `docs/padrao-visual-spo-aa-plus.md` (§6.1 checklist, §6.4 kit).

### 41.95 Teste contra cor literal no style.css — Fase B (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.95._

- `tests/test_cor_literal.py`: conta as cores literais do `style.css` (hexadecimal e `rgb/rgba/hsl/hsla` com números; `rgba(var(--accent-rgb), .16)` não conta) **fora de definição de token** (`--nome: …` fica de fora: é onde a cor deve estar), sem comentários. Hoje: **376 cores, 191 valores**. A referência fica em `tests/referencia_cor_literal.json` (gerada pelo próprio teste). Reprova valor novo e contagem que sobe; quando desce, o segundo teste avisa (skip) para baixar a referência com `python tests/test_cor_literal.py` — que se recusa se a contagem subiu (exceção nova do §6.3 só com `--forcar`).
- Conferido que reprova: cor nova (`#1a5fb4`) e mais um uso de cor existente (`#ffffff`; o comando de atualizar também recusou); e que aceita: token novo definido com cor e usado por `var()`.
- Cores por nome (`white`, `black`…) não são contadas.
- Padrão: regra de ouro (§3), checklist (§6.1) e roteiro do N5 (§7) atualizados.

### 41.96 Teste contra cor e tamanho em style="…" nos templates e no JS (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.96._

- **Templates** (`templates/**/*.html`, sem comentários Jinja): declarações de cor (`color`, `background`, `border*`, `outline`, `fill`, `stroke`, `box-shadow`) e de texto (`font`, `font-size`, `font-family`, `line-height`) dentro de `style="…"`; definição de variável (`--swatch`, as amostras do Personalizar, §6.3) não conta. Medição: só **6** antigas — links "Esqueci a senha"/"Voltar ao login" (`login.html`, `forgot_password.html`, `reset_password.html`; do dev. Cleber), título "UO …" do aviso da Início (`dashboard.html`) e o bloco monoespaçado de Acessos à API (2) — em `REFERENCIA`, por arquivo. Reprova arquivo novo ou contagem que sobe; quando desce, pede para baixar (skip).
- **`main.js`:** `.style.color/background/backgroundColor/borderColor/outlineColor/fill/stroke = "…"` e `setProperty("color"…)` só com `var(--…)` (hoje: 4 `borderColor = "var(--primary)"`; a sonda `probe.style.color = name` usa variável e não conta); `.style.fontSize = "…"` nunca; HTML montado com `style="…"` só sem cor (`#…`, `rgb(…)`, `hsl(…)`) ou tamanho escritos à mão — valor calculado (`${colorFor(…)}`, a cor do grupo vinda da paleta do tema, §3.5) e `var(--…)` são aceitos (4 casos no Teto).
- Padrão: §6.1 (item do `style=`) e §7 (os 6 antigos como dívida).

### 41.97 Bateria 3: SPO e kit de mockup (dev. Jean, 2026-11-15)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.97._

- **Achados e correções:**
  1. *Cartão de documento do Institucional* com "Diagramas" no modo oposto ao tema: fundo pelos diagramas e título, legenda e link pelo tema, 1,0–1,6:1. No bloco `[jean]` dos cartões dinâmicos: `h5` e `small`/`.text-muted` nas cores dos diagramas; o link passa a `--spo-diagram-text` sublinhado só quando os modos divergem.
  2. *Preset selecionado do Contrato Visual* no tema escuro: `.theme-dark .contract-preset.active` (0,3,0) vencia a regra do cartão dinâmico e, com "Diagramas: claro", a descrição ficava a 1,12:1. A regra ganhou `:not(#spo-preset)`.
  3. *Kit:* `<code>` fora do `#content-area` ("Como usar", painel) com a cor rosa do Bootstrap (4,5:1 no claro, 3,45:1 no escuro); regra própria no `kit-mockup.css`.
  4. *Kit:* os presets do Contrato (marcação do Contrato) entraram no catálogo, comparados com o template, para o kit cobrir o componente do achado 2.
- Padrão §6.1: item novo (quem usa as cores dos diagramas usa em tudo; medir com os diagramas no modo oposto ao tema). `?v=` do CSS: `20261115.jean-bateria-3.1` (`base.html` e `login.html`).

### 41.98 ✓ na cor de destaque ativa (dev. Jean, 2026-11-16)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.98._

- Bloco `[jean]` no fim do `style.css`, no **desenho do laboratório** (preferido pelo dev. Jean depois de comparar):
  - amostra de **44 px** (alvo do AA+; antes 34 px, e 44 px só no toque), com o halo claro na cor da amostra e o ponto na cor real (regras de base inalteradas);
  - escolhida: **anel suave na própria cor** (55%, com vão na cor do cartão), agora decorativo, e o `::after` vira um **selo neutro com ✓** (fundo `--card`, borda `--border`, ✓ em `--text`, `--spo-fs-rotulo`) no canto inferior direito. Quem carrega o estado é o ✓.
  - Dois desenhos anteriores foram descartados na conferência do dev. Jean: anel e selo na cor do texto (pesado) e anel em `--spo-selected-border` (destoava do laboratório).
  - `:not(#spo-check)` vence `.accent-swatch.active` e a regra de alvos do AA+. Com cores forçadas do Windows, contorno e selo em `Highlight`.
- **Kit de mockup com o `?v=` do SPO:** o kit carregava `../css/style.css` sem versão, e o navegador continuou mostrando as amostras antigas do cache. O link do kit passou a levar o mesmo `?v=` do `base.html`, e `test_kit_carrega_o_style_css_da_mesma_versao_do_spo` (`tests/test_kit_mockup.py`) reprova quando os dois divergem: **ao mudar o `?v=` do CSS, mudar também em `static/kit/mockup-spo-aa-plus.html`**.

### 41.100 Avisos modais no celular: cartão com margem (dev. Cleber, 2026-11-16)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.100._

**Regra aplicada** (só CSS, bloco `[cleber]` no fim do `style.css`, dentro de `@media (max-width: 600px), (max-height: 560px)`: celular em pé ou deitado; no computador nada muda):
- todo `.modal-overlay`, menos o "Carregando" (`.app-loading-overlay`), passa a ter a altura visível (`height: 100vh` e, em seguida, `100dvh`) e margem de 16 px somada às áreas seguras (`max(16px, env(safe-area-inset-*))`, com `padding: 16px` de reserva);
- `.modal-card` e `.emp-record-card` ficam com `width: 100%`, `max-height: 100%` do overlay (a altura visível de verdade) e `overflow-y: auto`: o cartão cabe na tela e rola por dentro;
- o cabeçalho (`.modal-header`) e o rodapé (`.modal-footer`, `.see-done-footer`) ficam fixos (`position: sticky`, com fundo `var(--surface)`): nos avisos sem "×" o "Fechar" fica sempre visível, sem rolar até o fim;
- no Alerta, o "×" (`.emp-record-close-icon`) fica fixo no topo do cartão enquanto ele rola, sem ocupar linha (margens negativas); a área de toque de 44 px e o foco visível do #66 continuam;
- o `top`/`bottom` do `sticky` conta a partir da área de conteúdo, sem o padding do cartão; por isso os valores negativos (−20, −18 e −6 px), que deixam cabeçalho, rodapé e "×" rentes à borda;
- só `var(--…)`: sem cor literal, sem `font-size` em px e sem `style="…"` (`test_cor_literal`, `test_tamanho_fonte` e `test_style_inline` continuam passando); nenhum texto, conteúdo ou comportamento dos avisos mudou, e o `main.js` não foi tocado;
- fora do bloco, a busca, a ajuda e "O padrão SPO-AA+" (`.spo-busca-fundo`) seguem as regras do #124.

**Como testar:** na Início, com a janela em 360 × 740, 390 × 844 e 740 × 360 (celular deitado), recarregar:
1. o Alerta fica num cartão com 16 px de margem, rola por dentro, e o "×" fica a ~11 px do topo do cartão, visível também com a rolagem no fim; um toque a 5 px do círculo ainda fecha (área de 44 px);
2. fechado o Alerta, as pendências aparecem num cartão com margem, com o cabeçalho no topo e o "Fechar" (44 px de altura) visível sem rolar, em qualquer posição da rolagem; Esc fecha e o foco volta para a página;
3. avisos de conclusão (Notas SEE), Limpeza de Histórico e Metas Físicas seguem a mesma regra (conferidos com o mesmo HTML montado pelo `main.js`, sem processar nada);
4. a 1366 × 768 tudo igual ao de antes: Alerta com 900 × 483 px e "×" absoluto, `.modal-card` com `max-height: 85vh`, cabeçalho e rodapé estáticos.

### 41.101 Teto Orçamentário no celular: tabelas sem corte (dev. Cleber, 2026-11-16)

_Parte em vigor. O histórico (problema, medição, verificação) está no [CHANGELOG](CHANGELOG.md), antiga 41.101._

**Regra aplicada** (só CSS, bloco `[cleber]` no fim do `style.css`; nenhum cálculo, filtro, dado ou endpoint mudou, e o `main.js` não foi tocado). Opção "rótulo acima dos valores", aprovada pelo dev. Cleber:
- **Quadros por exercício, só até 600 px de largura** (`@media (max-width: 600px)`): a grade passa a ter só as colunas dos exercícios, e o rótulo vai para linhas próprias acima dos valores (`grid-row` 1: código da fonte; 2: nome do grupo/tipificação; 3: valores). O rótulo ocupa a largura do quadro (`100cqw`, do `container-type: inline-size` no `.teto-qomp-scroll`) e acompanha a rolagem lateral (`position: sticky; left: 0`), inteiro e quebrando em linhas. Sem coluna fixa por cima, a rolagem chega à última coluna inteira (184 px, cabem os valores de até "R$ 99.999.999.999,99"). O cabeçalho dos exercícios (ano e total) continua alinhado com os valores, e o "Total Geral" e as faixas alternadas por fonte continuam. Os rótulos "Fonte" e "Grupo de despesa" do cabeçalho somem no celular (não têm mais coluna); a célula do código da fonte vazia (`:empty`) não ocupa linha.
- **Se o navegador não suportar `container-type` ou `cqw`:** o `width: 100%` de reserva faz o rótulo ocupar a linha toda; ele deixa de acompanhar a rolagem lateral, mas não some nem corta.
- **Ranking, até 600 px de largura ou com a janela baixa até 560 px de altura** (celular em pé ou deitado, onde o cartão tem ~600 px e os nomes ainda saíam cortados): o rótulo (código e nome) fica inteiro na linha de cima, quebrando em linhas, e a barra e o valor ficam na de baixo; a tipificação continua recuada.
- Só `var(--…)`: sem cor literal, sem `font-size` em px e sem `style="…"` (`test_cor_literal`, `test_tamanho_fonte` e `test_style_inline` continuam passando). Acima desses limites nada muda.
- Esta correção foi feita em cima da homologação (mesma exceção dos avisos modais, 41.100: a `dev/cleber` ainda não tem o AA+) e chega à `dev/cleber` e à `dev/jean` depois, pela sincronização da `main`.

**Como testar:** na aba Tabelas do Teto Orçamentário, com a janela em 360 × 740, 412 × 915 e 740 × 360, nas densidades Compacto, Intermediário e Legível e com Intel One Mono e Inter (Personalizar → Fontes e tamanho do texto):
1. nos quatro quadros por exercício, o rótulo fica inteiro acima dos valores; rolando para o lado, o rótulo acompanha e a última coluna aparece inteira (scrollWidth = 552 px com 3 exercícios); os 14 códigos de fonte têm 8 dígitos;
2. no ranking das fontes e dos grupos, o nome aparece inteiro acima da barra e do valor;
3. a 1366 × 768 tudo igual ao de antes (colunas fixas de 86 e 250 px, ranking de 3 colunas com reticências).

### Avisos modais com fonte maior: botão de fechar sempre visível (dev. Cleber, 2026-11-16)

**Problema** (Chrome no Android, na homologação): com "Fonte maior" no Personalizar SPO, o modal de pendências (PTA Gerencial - Metas Físicas e Etapas Aguardando Aprovação) mostrava o cabeçalho com o logo e o título, mas o botão "Fechar" sumia.

**Causa** (medida no navegador do app a 360 × 800 e 1366 × 768):
- A fonte maior é um `zoom` no `.main` (`body[data-spo-text-size] .main`: 112,5%, 125% e 150%; 125% no "Muito grande" abaixo de 480 px). Os avisos modais ficam dentro do `.main`.
- O `zoom` multiplica as medidas em unidades da janela (`vw`, `vh`, `dvh`). A 360 × 800 com 125%, o `height: 100dvh` do bloco "Avisos modais no celular" (41.100) virava 1.000 px numa tela de 800 px: o cartão passava da janela e o rodapé, com o "Fechar", ficava fora dela (903 a 958 px).
- No computador acontecia o mesmo com o `max-height: 85vh` do `.modal-card` (a 1366 × 768 com 125%, o cartão ia de -24 a 792 px) e com o `width: min(900px, 92vw)` do Alerta.
- A densidade visual não interfere: comfortable e spacious deram medidas idênticas. Só o tamanho da fonte importa.

**Regra aplicada** (só CSS, bloco `[cleber] Avisos modais com fonte maior` no fim do `style.css`; o bloco 41.100 não foi alterado):
- `--spo-zoom` guarda o mesmo fator do zoom do `.main` (1, 1,125, 1,25 e 1,5; 1,25 no "Muito grande" abaixo de 480 px). **Os fatores espelham as regras de zoom do `.main`:** ao mudar um fator lá, mude também o `--spo-zoom`. `tests/test_modal_zoom.py` reprova se divergirem.
- Dentro do `.main`, as medidas em `vw`/`vh`/`dvh` dos avisos são divididas por `var(--spo-zoom, 1)`: a altura do `.modal-overlay` no celular (`100dvh`), o `max-height` do `.modal-card` (`85vh`) e a largura do Alerta (`92vw`). Depois do zoom valem a janela visível, e o conteúdo continua ampliado. Com a fonte normal o fator é 1 e nada muda.
- Alerta no computador com a fonte ampliada (janela maior que 600 × 560): ganhou `max-height` (janela menos 16 px de margem em cada lado) e rolagem, e o "×" acompanha o topo, como no celular. A 700 × 620 e 1366 × 600 o cartão passava da janela e o "×" saía dela.
- Só `var(--…)` e `calc()`: sem cor literal, sem `font-size` em px e sem `style="…"`. O `main.js` não foi tocado.

**Verificação** (modal de pendências e Alerta; fonte normal, Grande, Maior e Muito grande; densidade confortável e ampla; 360 × 800, 412 × 915, 800 × 360, 700 × 620, 1366 × 600 e 1366 × 768): cartão dentro da janela, botão de fechar visível, sem cobertura por outro elemento e com clique que fecha, conteúdo rola até o fim, sem rolagem lateral da página e sem tabela cortada sem rolagem. Com a fonte normal as medidas ficaram iguais às de antes.

`templates/base.html`, `templates/login.html` e `static/kit/mockup-spo-aa-plus.html`: só o `?v=` do CSS, agora `20261116.cleber-modal-fonte-maior.1` nos três. `RELEASE_2_2026_11_16.6`.


## 42. Notas SEE: DANFE novo da Somos (4º bimestre/2026) (2026-11-13)

**Sintoma (produção):** catálogo "4° Bimestre", 647 notas, todas com alerta. Produtos e quantidades certos (8.818 itens, soma 339.835), mas a escola não era identificada: 639 notas sem DRE/código/escola e 8 com lixo no campo DRE (`DRE_FORA_DO_CADASTRO`).

**O que mudou no DANFE da Somos** (a partir das notas de 09/2026):
- Destinatário é sempre a SECRETARIA DE ESTADO DE EDUCACAO (não é mais a escola). A nota não traz DRE nem código da escola.
- A escola só aparece nas Informações Complementares:
  - `Solicitação:` em três formas: `REF. SOLICITACAO Nº <n> - <escola>`, `<n> - SECRETARIA DE ESTADO DE EDUCACAO DE MATO - <escola>` e `REF. SOLICITACAO Nº <n> - DRE <nome>` (entrega na própria DRE). O nome termina antes do texto fiscal ("NÃO INCID. ICMS ...").
  - `Endereço de entrega: <logradouro> - <bairro> - <MUNICÍPIO> - MT - BR - <CEP>`: o município é o trecho antes de "- MT - BR".
  - `Razão Social:` vem cortada (35 caracteres) e quase sempre é o nome do conselho (CDCE): não é usada.
- As quebras de linha caem em qualquer ponto (até no meio do rótulo e do nome); a leitura junta as linhas antes.
- Produto: código novo em CÓD. PROD. (`500000065xxx`) e "Cod. Legado" na linha de baixo. Já funcionava com o catálogo do 4º bimestre (códigos novos); não mudou.

**Causa dos 8 valores-lixo:** a nota caía no caminho genérico de `_metadata`, cuja busca do rótulo DRE não exigia início de palavra nem parava no texto fiscal: "ALEXAN**DRE** GOMES...", "PA**DRE** AMADEU..." (endereço), "AN**DRE** ANTONIO MAGGI NÃO INCID. ICMS..." e "DRE PONTES E LACERDA NÃO INCID. ICMS...".

**Correção** (`services/see_notes.py`, `services/see_escolas.py`; sem mudança de banco, rotas, worker ou tela):
- **Layout novo** (`metadata["layout"] = "somos_solicitacao"`, função `_somos_solicitacao`): reconhecido por `Solicitação:` + `Endereço de entrega:` quando não há o bloco "ESCOLA: ... DRE: ... COD:" (Somos antigo) nem escola no destinatário + linha de DRE (Athos). Nome da escola pela Solicitação; município pelo endereço de entrega (não o CUIABÁ do destinatário). Alertas da extração nesse layout: nome e município (`CAMPO_NOME_ESCOLA_AUSENTE`, `CAMPO_MUNICIPIO_AUSENTE`); DRE e código não são cobrados da nota, vêm do cadastro.
- **Casamento com o cadastro** (`_identificar_por_municipio`): procura nas escolas ativas do município (em todas as DREs dele). Exatamente uma com o mesmo nome (`nucleo_nome`): preenche código, nome oficial e DRE, origem `cadastro (nome + município)`. Nome parecido: `ESCOLA_SUGESTAO`. Sem escola igual ou mais de uma igual: `ESCOLA_NAO_ENCONTRADA`. A Conferência diz o que a nota traz e o que o cadastro traz (inclusive escola com o mesmo nome em outro município), sem preencher. Entrega na DRE: código da DRE no cadastro (1 a 14), origem nova `cadastro (DRE)`, e confere se o município de entrega é da DRE (`ESCOLA_MUNICIPIO_DRE_INCONSISTENTE`).
- **Regra do usuário:** o DANFE é documento oficial; o sistema nunca corrige nem completa dado do fornecedor por aproximação. Divergência vira sugestão ou alerta na Conferência, para o fiscal decidir. Exemplo: "EE 7 DE SETEMBRO" (nota) × "EE 07 DE SETEMBRO" (cadastro, Juína) fica como sugestão (zero à esquerda não é ignorado).
- **Município só na comparação** (`chave_municipio`): sem espaços e com D'OESTE / D OESTE / DO OESTE iguais (as notas trazem "MIRASSOL D OESTE"; o cadastro, "MIRASSOL D'OESTE"). O texto da nota não é alterado; "Escola (nota)" mostra sempre o nome da Solicitação, mesmo quando a escola é identificada.
- **Proteção para todos os layouts:** o rótulo DRE do caminho genérico exige início de palavra (`\bD\.?R\.?E\.?`), e DRE ou escola com texto fiscal (`_TEXTO_FISCAL`: NÃO INCID, ICMS, RICMS, IPI, RIPI, LEI COMPLEMENTAR, ART. n) é descartado; a nota fica com o alerta de campo ausente.

**Os três layouts valem ao mesmo tempo** (Somos antigo, Athos e Somos novo): o novo não substitui o antigo, e um mesmo processamento pode misturar notas dos dois modelos; cada nota é reconhecida pelo próprio conteúdo.

**Validação** (sem gravar no banco, com o cadastro de 630 escolas):
- 4º bimestre, 647 PDFs: itens e quantidades idênticos nota a nota (8.818, soma 339.835). 627 escolas identificadas por nome + município, 13 entregas na DRE, 1 sugestão (EE 7 DE SETEMBRO, Juína) e 6 não encontradas: EE GUSTAVO DUTRA (nota: Santo Antônio do Leverger; cadastro: Campo Verde), EE CRISTIANO ARAUJO PIRES (nota: Sorriso; cadastro: Boa Esperança do Norte), EE SAGRADO CORAÇÃO DE JESUS (Denise, mesmo caso do 3º bimestre), EE PAULO FREIRE (Barra do Bugres), EE PROFESSORA ALZIRA DA SILVA SANTOS (Juscimeira) e EE LUIZ CARLOS CECONELLO (Sinop), estas três sem cadastro. Nas 640 com código, a DRE do cadastro bateu com a pasta da DRE em 100%. Nenhum texto fiscal em DRE/escola.
- 3º bimestre, 641 PDFs (Somos antigo + Athos): resultado **idêntico** ao anterior nota por nota (DRE, código, escola, município, alertas, itens e quantidades).
- Pela tela (rotas reais e worker), no banco da `dev/cleber`: catálogo "4º Bimestre 2026 (teste)" (id 64, 24 produtos) e processamento novo 78 com 25 cópias; mesmo resultado da extração isolada.
- Testes em `tests/test_notas_see.py`: textos sintéticos das três formas da Solicitação (com quebras de linha), D'OESTE, entrega na DRE, os 8 trechos-lixo, casamento (identificada, mesmo nome em outro município, sugestão, duas iguais, município fora do cadastro, DRE), amostra real do 4º bimestre (`SEE_PDFS_DIR_4BIM`) e **lote misto** 3º + 4º bimestre pelo processamento completo (cada nota no layout certo e com o mesmo resultado que teria sozinha). Os testes com PDFs são pulados se as pastas não existirem.

**Operacional:** as notas do 4º bimestre já processadas em produção com o defeito devem ser **reenviadas num processamento novo** ("Não, iniciar uma nova lista"), e não com "acrescentar": o *acrescentar* copia as notas anteriores como estão e não relê os PDFs (§28.7).

## Notas SEE: envio robusto dos PDFs (2026-11-18)

Complementa a §28. Vale para a tela "Notas SEE" (`initNotasSee` no `main.js`) e para as rotas de `rotas/notas_see_routes.py`.

**Envio em lotes, com tempo limite e novas tentativas (navegador):**
- Os PDFs sobem em lotes (até 50 arquivos ou 20 MB) para `POST /api/notas-see/processamentos/<id>/arquivos`. Cada lote tem tempo limite de **60 s + 15 s por MB**.
- Tempo esgotado, falha de rede, erro 5xx e 429 reenviam **o mesmo lote**, até **3 tentativas no total**, esperando 5 s e depois 15 s. Outros erros (4xx) não são repetidos.
- Depois da 3ª falha, a tela não descarta nada: mostra o erro com "Tentar novamente" (continua do lote que falhou) e "Cancelar envio" (cancela o processamento **do envio**, pelo id criado nele, nunca o do painel).
- Enquanto há envio, o `beforeunload` avisa ao recarregar ou fechar a aba. O texto é do navegador e não pode ser personalizado; o painel do app (Electron) não mostra o aviso.

**"Já recebido" (servidor):** no mesmo processamento, arquivo com o mesmo SHA-256 **e** o mesmo caminho relativo é tratado como reenvio do mesmo arquivo: não é gravado de novo, não soma ignorado e volta no campo `already` da resposta (`{"ok", "added", "accepted", "already", "ignored"}`). O navegador soma `accepted` e `already` para saber o que já chegou. Mesmo conteúdo em **outro** caminho continua sendo duplicado e é ignorado como antes. A ocorrência de ignorado guarda o `path` para não ser registrada duas vezes num reenvio.

**Arquivos que não são PDF:** o navegador não os envia. A tela mostra "N arquivo(s) ignorado(s) por não ser PDF: …" (até 5 nomes, depois "e mais N") e manda os nomes no campo opcional `ignorados_navegador` (JSON) de `POST /api/notas-see/processamentos`. O servidor aceita só lista de textos, guarda só o nome do arquivo (sem pasta), descarta vazio, nome com mais de 255 caracteres ou com caractere de controle, limita a 500 nomes e registra cada um como a ocorrência `ARQUIVO_IGNORADO` ("Extensão diferente de PDF."), igual à que o servidor gerava quando o arquivo subia. Sem o campo, a criação funciona como antes. No "acrescentar", entram só os ignorados do envio novo.

**Como testar:** `tests/test_notas_see.py` (reenvio idempotente, duplicado em outro caminho, envio depois do cancelamento, contagem do `iniciar`, ignorados do navegador, limite e campo opcional, acrescentar). Para simular resposta perdida ou erro 5xx na tela, use um proxy local temporário **fora do repositório** na frente do servidor de desenvolvimento.
