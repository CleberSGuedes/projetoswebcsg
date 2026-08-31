# Sistema SPO — Sistema de Planejamento e Orçamento

> Documento gerado a partir de uma análise completa do repositório (sem alterações de código) em 2026-08-31.
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
- **PyMySQL 1.1.1** (driver MySQL) e **pyodbc 5.3.0** (driver SQL Server) — o projeto suporta os dois engines, ver seção 4.
- **pandas / numpy / openpyxl / XlsxWriter / rapidfuzz** — processamento de planilhas Excel (upload/tratamento/relatórios), fuzzy matching de chaves de planejamento.
- **python-dotenv** para carregar `.env`.
- Sem framework de testes (não há `pytest`/`unittest` nas dependências). Existe apenas um script manual de smoke test: `scripts/test_teto_seduc_processors.py` (roda com `python scripts/test_teto_seduc_processors.py`, não é descoberto por um test runner).

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
├── node_runners/               # workers Node.js p/ EMP e NOB (processamento pesado de planilhas)
├── db/                          # scripts .sql de schema (CREATE TABLE) por módulo — não são migrations versionadas (tipo Alembic)
├── scripts/                     # utilitário único de smoke test manual
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
- `config.py` suporta **dois engines** de banco: MySQL (via PyMySQL) e SQL Server/MSSQL (via pyodbc), decidido pela variável `DB_ENGINE`:
  - Se `DB_ENGINE=mssql` **e** existirem variáveis `DB_*_HMG` preenchidas → usa SQL Server.
  - Caso contrário (mesmo que `DB_ENGINE=mssql`) → cai no branch MySQL, usando as variáveis `DB_*_CSG` (com fallback para `DB_USER`/`DB_PASSWORD`/etc. genéricos).
  - **Achado importante:** no `.env` atual, `DB_ENGINE=mssql` está definido, mas **não existem variáveis `DB_*_HMG`** — portanto, na prática, a aplicação está rodando em **MySQL** (usando as credenciais `*_CSG`), apesar do comentário no `.env` dizer "BANCO DE DADOS (MySQL remoto)" e do `DB_ENGINE` sugerir SQL Server. Isso é uma inconsistência de nomenclatura/config que pode confundir quem for mexer no projeto — vale confirmar a intenção real antes de qualquer alteração de config.
  - O mesmo padrão de resolução de engine existe duplicado em `node_runners/db.js` (JS reimplementa a mesma lógica do `config.py` em paralelo — **duas fontes de verdade para a mesma regra**, risco de dessincronização).
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
- Alguns PKs usam `autoincrement=False` com geração manual de próximo ID em Python (`_next_pk`, `_next_pk_active_session` em `app.py`/`auth_routes.py`) — herança aparente de compatibilidade com SQL Server antigo (comentário no código: "Gera próximo ID para tabelas sem IDENTITY/auto_increment (SQL Server 2008)"), mas a tabela hoje roda em MySQL. Esse padrão gera **condição de corrida** em cenário de concorrência (dois inserts simultâneos podem calcular o mesmo `MAX(id)+1`), pois não há lock/transação serializável nem uso de sequence.
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
4. **Lógica de resolução de engine duplicada** em Python (`config.py`) e JavaScript (`node_runners/db.js`) — mesma regra de negócio mantida em dois lugares, risco de dessincronização futura.
5. **Geração manual de PK (`MAX(id)+1`)** em `active_sessions` e `logs_login` (código legado para SQL Server 2008 sem IDENTITY) rodando hoje sobre MySQL, que já suporta `AUTO_INCREMENT` nativamente — potencial condição de corrida sob concorrência real, já que não há lock explícito nem transação serializável em torno do cálculo.
6. **Sem migrations formais** (Alembic/Flask-Migrate) — schema evolui via `db.create_all()` automático (só cria tabelas novas, não altera colunas existentes) + scripts `.sql` soltos em `db/` aplicados manualmente + script standalone `gerar_migracao_chaves.py`. Não há histórico versionado e reproduzível de alterações de schema.
7. **Arquivos monolíticos muito grandes:** `rotas/home_routes.py` (19.214 linhas) concentra praticamente todas as rotas HTML/API/partial do sistema; `templates/home.html` tem ~741 KB; `models/user.py` concentra ~65 classes de todas as entidades do sistema. Isso dificulta navegação, revisão de código e aumenta o risco de conflitos de merge entre desenvolvedores trabalhando em módulos diferentes.
8. **Sem suíte de testes automatizada.** `requirements.txt` não inclui `pytest` (ou similar); o único artefato de teste (`scripts/test_teto_seduc_processors.py`) é um script manual com `assert`s, executado via `python scripts/...py`, sem integração a CI (não há pasta `.github/workflows` nem outro CI configurado no repositório).
9. **Sem proteção CSRF explícita** nas rotas que alteram estado via formulário/POST, e sem CORS configurado — coerente com uma aplicação same-origin, mas vale revisão se a API `/api/v1/...` ou `/api/` vier a ser consumida por outros domínios.
10. **`SESSION_COOKIE_SECURE` tem default `false`** — só fica `True` se a variável de ambiente homônima for setada explicitamente; se o servidor de produção já serve por HTTPS, vale confirmar se essa variável está de fato configurada lá (não está no `.env` local).
11. **`app.run(..., debug=True)`** está hardcoded no bloco `if __name__ == "__main__":` do `app.py`. Isso só é relevante para quem rodar `python app.py` diretamente (em produção via Passenger/WSGI esse bloco não executa, pois o import usa `application`/`app` diretamente) — mas é um risco caso alguém suba o servidor de outra forma.
12. **Sem limite explícito de tamanho de upload** (`MAX_CONTENT_LENGTH` não configurado no Flask) — uploads de planilhas grandes dependem apenas dos limites default do stack (Flask/Werkzeug/servidor WSGI/cPanel), sem um teto de negócio definido no código.
13. **Volume de armazenamento local crescente sem rotina de limpeza aparente:** `upload/` (~985 MB) e `outputs/` (~1.3 GB) acumulam milhares de arquivos `.xlsx` temporários e de histórico (subpastas `tmp/`) sem expurgo automático visível no código.
14. **Nomenclatura inconsistente** entre português/inglês para colunas de auditoria (`criado_em` vs `created_at`, `alterado_em` vs `updated_at`) espalhada pelas ~65 tabelas — não é um bug, mas exige atenção redobrada ao escrever queries/joins novos.
15. Comentário em `app.py` menciona compatibilidade com **IIS/wfastcgi**, mas não há `web.config` nem dependência `wfastcgi` no projeto — parece ser vestígio de uma tentativa de deploy alternativa (Windows/IIS) que não está mais em uso; o deploy real observado é via cPanel (`.cpanel.yml`).

---

## 10. Convenções úteis para quem for mexer no código

- O ponto de entrada da aplicação Flask é sempre `app.py::create_app()` — tanto o `worker.py` quanto `scripts/test_teto_seduc_processors.py` reutilizam essa factory (`from app import create_app`) para ter acesso a `db`/config dentro de um `app.app_context()`.
- Toda rota HTML "parcial" (carregada via AJAX no `home.html`) segue o padrão de URL `/partial/<módulo>/<ação>`, servida por `rotas/home_routes.py`, e a contraparte de dados fica em `/api/<módulo>/...`. Ao adicionar uma nova tela, normalmente é necessário: (1) uma entrada em `services/features.py` (menu + controle de permissão), (2) uma rota `/partial/...` retornando um template em `templates/partials/`, (3) uma ou mais rotas `/api/...` para os dados.
- Regras de acesso por papel usam `login_required`/`role_required` de `services/auth.py`, combinadas com o nível do perfil (`g.user_nivel`) carregado no `before_request` de `app.py`.
- Alterações de schema devem considerar que o banco é **compartilhado com o ambiente online** (seção 8) — qualquer novo `CREATE TABLE`/`ALTER TABLE` deveria, idealmente, ser adicionado tanto ao model SQLAlchemy quanto (se seguir o padrão dos módulos de upload existentes) a um script em `db/*.sql`, documentando a intenção antes de rodar contra o banco remoto.
