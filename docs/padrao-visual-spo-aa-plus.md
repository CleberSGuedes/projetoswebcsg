# Padrão visual "SPO-AA+" do SPO

Regras de cor, contraste, temas e componentes da interface do SPO. Valem para **qualquer tela nova
ou alterada**, por qualquer desenvolvedor. Completa o `docs/contrato-visual-spo.md`, que cuida de
layout, responsividade e preferências.

> Versão 4, 2026-11-15: escala tipográfica com **"Densidade do texto"** (Compacto, Intermediário,
> Legível) para a interface inteira e passo **Leitura** (§2.2); a aplicação vem em PRs seguintes.
> Versão 3, 2026-11-15 (a versão 2, de 14/11, cobria até o #86; a 1, de 13/11, até o #61). Valores
> **medidos** no SPO da `dev/jean` até o #119 e conferidos pela bateria 2 (4 perfis × 66 telas × 3
> larguras, §5), com o destaque Clássico (os outros 7 destaques seguem as mesmas regras; ver §3.3).
> Quando uma regra mudar, atualize este documento no mesmo PR.

## 1. Metas

| Item | Meta | De onde vem |
|---|---|---|
| Texto comum (inclui texto de botões, rótulos, tabelas) | **≥ 7:1** contra o fundo real atrás dele (AA+, WCAG 1.4.6) | #33, #36, #37 |
| Texto grande (≥ 24 px, ou ≥ 18,7 px em negrito) | ≥ 4,5:1 | WCAG 1.4.6 |
| Elementos que não são texto: borda de campo, contorno de botão sem preenchimento, ícone que informa, faixa de aviso, anel de foco | **≥ 3:1** contra o fundo vizinho (WCAG 1.4.11) | #37, #44 |
| Foco visível | anel de 3 px em todo elemento focável pelo teclado | #34 |
| Alvo de toque | ≥ 24 px sempre; ≥ 44 px em tela de toque (`pointer: coarse`) | #35 |
| Movimento | respeitar `prefers-reduced-motion` (sem transições nem rolagem suave) | #38 |
| Zoom e telas estreitas | sem corte nem rolagem lateral da página a 320 px e a 200% | #43 |
| Espaçamento de texto | nada se perde com entrelinha 1,5, parágrafos 2×, letras 0,12 em e palavras 0,16 em forçados em todo o texto (WCAG 1.4.12) | bateria de 14/11 (§5) |
| Tamanho e entrelinha | escala do §2.2: mínimo de 12 px para leitura, entrelinha 1,5 no texto corrido | #73, #75, #76 |
| Informação | nunca só pela cor: título, ícone, negrito, ✓ ou `aria-*` junto | #49, #54 |

**Exceções da WCAG que valem aqui:** campos e botões **desabilitados** não precisam de contraste
mínimo, mas, no SPO, o texto deles fica ≥ 4,5:1 (§4.2). Logotipos e decoração, também.

## 2. Temas, níveis e destaques

| Dimensão | Valores | Onde se escolhe |
|---|---|---|
| Tema | Claro, Escuro, Automático (segue o sistema) | Personalizar → Temas |
| Nível de contraste | **Suave** (padrão SPO), **Médio** (contornos das opções e dos diagramas na cor dos controles), **Reforçado** (texto secundário mais forte e contornos dos controles na cor de destaque) | Personalizar → Preferências comuns |
| Destaque | 8: Clássico (turquesa), Azul, Turquesa claro, Verde, Amarelo, Laranja, Vermelho, Roxo | Personalizar → Temas |
| Densidade | **Automática** (padrão SPO): confortável acima de 1200 px, compacta de 721 a 1200 px, ampla até 720 px (pela largura da janela, que acompanha zoom e janela dividida); ou fixa: Confortável, Compacta, Ampla | Personalizar → Preferências comuns |
| Alto contraste do Windows | `@media (forced-colors: active)`: o sistema impõe as cores; o SPO não deve brigar com ele | automático — **ainda não medido** |
| `prefers-contrast: more` | só os diagramas reagem hoje | automático — **ainda não medido** |

A classe `theme-dark` no `<body>` indica o tema escuro; `data-visual-contrast` (`standard`,
`medium`, `enhanced`) e `data-accent-preference` (`classic`, `blue`, …) indicam nível e destaque.
`data-visual-density` traz a densidade **em uso** (`comfortable`, `compact`, `spacious`, que é o que o
CSS lê) e `data-visual-density-pref`, a escolha da pessoa (`auto` ou uma das três).

### 2.1 Tipografia

Fontes **hospedadas no SPO** (`static/fonts`, licença OFL 1.1): aparecem iguais em qualquer
computador ou celular. O navegador só baixa a fonte em uso.

| Uso | Padrão SPO | Alternativas no Personalizar | Atributo no `<body>` |
|---|---|---|---|
| Interface (textos, menus, botões, campos) | **Inter** | Atkinson Hyperlegible, Source Sans 3, **Mulish** (formas arredondadas, leitura corrida; I e l parecidos — em avaliação, 15/11), Do sistema | `data-spo-font-ui` |
| Tabelas e dados (tabelas, totais, KPIs) | **Intel One Mono** (monoespaçada, números alinhados) | Inter com números alinhados, IBM Plex Sans, **Atkinson Hyperlegible Mono** (monoespaçada da família Atkinson, oferecida junto com ela; bateria 2), Do sistema | `data-spo-font-data` |
| **Densidade do texto** (antes "Texto das tabelas"; §2.2.1). Nesta fase vale nas tabelas do conteúdo, filtros e paginação; as demais telas passam a segui-la por etapas | **Intermediário** (tabelas 12 px) | Legível (13 px; entra na "Leitura confortável" da Atkinson), Compacto (10 px) | `data-spo-table-text` (nome mantido; a escolha gravada antes é convertida para a de mesmo tamanho) |
| Tamanho geral | **100%** | 112%, 125%, 150% (amplia a área de conteúdo; em tela com menos de 480 px, o 150% vale 125%) | `data-spo-text-size` |
| Espaçamento entre linhas (texto corrido) | **Padrão (1,5)** | Amplo (1,75), Mais amplo (2,0); títulos, tabelas, botões e campos não mudam | `data-spo-line-height` |
| Cores dos gráficos (cartão **"Visão e cores"**) | **Padrão SPO** | Seguras para daltonismo (paleta Okabe-Ito, §3.5) | `data-spo-cvd` |
| Realce do foco ("Visão e cores") | **Padrão SPO** (anel de 3 px) | Reforçado: anel duplo (§4.4) | `data-spo-focus` |
| Links ("Visão e cores") | **Padrão SPO** (cor de destaque e negrito) | Sempre sublinhados | `data-spo-links` |
| Brilho do tema claro ("Visão e cores") | **Padrão SPO** | Reduzido: cartões `#eceef1`, página `#dadee4`, texto secundário `#3e3c44` (`#38363d` com o Reforçado), texto de destaque no tom 800 e "Dispensar" em `#861523` (só no claro; bateria 2, 15/11) | `data-spo-brightness` |
| Movimento ("Visão e cores") | **Automático** (segue o `prefers-reduced-motion` do sistema) | Reduzir sempre: sem animações, transições nem rolagem suave | `data-spo-motion` |
| Atalhos de uma tecla ("Teclado") | **Ligados** ("/" abre a busca e "?" a ajuda, fora de campos de texto) | Desligados (WCAG 2.1.4); Ctrl+K e Alt+M continuam | `data-spo-keys` |
| Leitura (largura do texto corrido e espaço entre parágrafos) | **Padrão SPO (largura livre)** | Intermediário (72ch, 1 entrelinha entre parágrafos), Estrito (WCAG AAA: até 80 caracteres por linha, 1,5 entrelinha depois de cada parágrafo) | `data-spo-reading` |

- **"Do sistema"** é a pilha antiga (`Cantarell, Segoe UI, system-ui…`): **varia com o computador**
  (Segoe UI no Windows, a do sistema no Mac, Cantarell no Linux). Não é mais o padrão.
- A escolha de cada pessoa fica no navegador (`localStorage`, chave `spo-font-preferences`). Sem
  escolha salva, vale o padrão acima.
- **Tela de login:** o `login-theme.js` lê a mesma chave e grava os mesmos atributos; lá valem a
  fonte da interface, o tamanho geral (o cartão de login é ampliado) e a entrelinha do texto do
  cartão. **Se as opções mudarem no `main.js` (`initFontesPersonalizar`), mude também no
  `login-theme.js`** (como a paleta, §3.3).
- **Troca consciente:** a Intel One Mono deixa as tabelas **mais largas** (textos longos quebram em
  mais linhas); em troca, os números ficam alinhados e as letras, bem distintas.
- Texto de componente novo herda a fonte do `<body>`; tabela nova deve usar as classes de tabela do
  SPO para receber a fonte e o tamanho de dados.

### 2.2 Escala tipográfica (tamanhos e entrelinhas)

Medição de 2026-11-13 (66 telas, padrão de fontes SPO, 100%): **20 tamanhos diferentes nas telas** e,
no `style.css`, **37 valores de `font-size` e 23 de `line-height`**. Nenhum texto das telas fica abaixo
de 10 px, mas há texto de leitura em 10–11 px e descrições com entrelinha 1,25. A referência de
hierarquia é a das telas de **Governança e Resultados** (avaliadas pelo dev. Jean como adequadas em
títulos, subtítulos, ênfases e espaçamentos).

| Passo | Tamanho | Entrelinha | Peso | Uso |
|---|---|---|---|---|
| Rótulo | **11 px** | 1,5 | ≥ 700, **maiúsculas** | sobretítulo da página ("RELATÓRIOS · EXECUÇÃO"), rótulo curto em maiúsculas |
| Pequeno | **12 px** | 1,5 | 400–700 | rótulos de campo e de totais, metadados, ajuda, paginação, selos de situação, dados das tabelas |
| Texto | **14 px** | 1,5 | 400 | texto corrido, subtítulo da página, descrição de cartão |
| Destaque | **15–16 px** | 1,4 | 600–700 | título de cartão ou de bloco |
| Seção | **20 px** | 1,25 | 700 | título de seção |
| Página | **24 px** | 1,2 | 700 | título da página (`h1` ou equivalente) |

Regras:
- **Mínimo de 12 px** para qualquer texto que se lê; 11 px só no passo "Rótulo" (curto, maiúsculas,
  negrito). **Exceção, informação secundária:** o nome e o "Logados" do topo (`.user-meta`) e o
  rodapé ficam com **10 px** (decisão do dev. Jean, 2026-11-13). O "Tamanho do texto" do Personalizar (112%, 125%) multiplica esta escala.
- **Entrelinha 1,5** em todo texto que pode ocupar mais de uma linha (referência: WCAG 1.4.8, nível
  AAA; e o 1.4.12 pede que o conteúdo aguente 1,5 sem quebrar). Títulos usam 1,2–1,25.
- Tamanhos fracionados (11,52; 12,25; 13,44; 16,71; 25,6 px) vêm de `em`/`rem` herdados: usar px da
  escala.
- **Largura do texto corrido** (`p`, `li`, `dd` e avisos `.alert`, fora de tabelas, menus e botões):
  **livre por padrão**, ocupando o cartão (decisão do dev. Jean, 14/11: o limite fixo deixava muito
  espaço vazio). O WCAG 1.4.8 (AAA) é atendido **pelo mecanismo** da opção "Leitura" do Personalizar
  (§2.1):
  - **Intermediário:** 72ch (média medida de 84–110 caracteres por linha) e 1 entrelinha entre
    parágrafos;
  - **Estrito:** no máximo **80 caracteres reais** por linha e **1,5 entrelinha depois de cada
    parágrafo**. O `ch` é a largura do "0", mais larga que a média das letras, então a largura é
    **medida por fonte**: 53ch na Inter e na Mulish, 48ch na Atkinson, 56ch na Source Sans 3 e 52ch na do sistema
    (pior caso medido: 78 caracteres). **Fonte nova no Personalizar precisa dessa medição.**
- O título da página é **negrito (700), 24 px, entrelinha 1,2** em todas as telas com cabeçalho de
  página (decisão do dev. Jean, 2026-11-13; aplicado no #76).
- **Dados** (valores, totais, KPIs, códigos) usam a fonte de dados do §2.1, inclusive fora de
  `<table>` (ex.: aba Tabelas do Teto, #82). Componente novo que mostra número deve entrar na lista
  de seletores da fonte de dados.
- O texto **dentro dos gráficos** (SVG do Plotly) usa a fonte da interface e o tamanho de dado da
  "Densidade do texto" (`--spo-fs-dado`: Compacto 10, Intermediário 12, Legível 13 px; texto pedido
  maior que 12 px acompanha a mesma diferença), aplicado a todo gráfico por `initGraficosEscala`
  (`main.js`); trocar a densidade redesenha os gráficos abertos.

### 2.2.1 Densidade do texto (versão 4, decisões do dev. Jean, 2026-11-15)

Medição de 15/11 (Chrome, 100%, Inter): o SPO tinha **dois registros de texto** na mesma tela — telas de
trabalho e cartões de opção em **12 px** (rótulos, campos, células, resumos) e janelas e prévias (busca,
atalhos, "O padrão SPO-AA+", "Fontes e tabelas") em **14–16 px**. O passo "Texto" (14 px) quase não era
usado fora das janelas, por isso o 100% parecia pequeno. No CSS, 330 declarações de `font-size` com 25
valores, sem variável comum. Plano completo: `docs_jean/PLANO_NORMALIZACAO_TIPOGRAFIA.md` (ambiente do
dev. Jean).

Decisões:
1. **Compacto / Intermediário / Legível** deixam de ser só o "Texto das tabelas" e passam a ser a
   **"Densidade do texto"** da interface inteira: cada opção escolhe um conjunto de valores para as
   variáveis abaixo.
2. O **padrão SPO é o Intermediário** (decisão do dev. Jean depois de testar as três densidades no FIP 613
   e as telas no Intermediário, 15/11; a primeira decisão era o Legível). O **Legível** fica para quem
   precisa de letra maior e entra na sugestão "Leitura confortável" da Atkinson.
3. No **Compacto**, as tabelas ficam com **10 px** (sai o 8 px).
4. Novo passo **Leitura**: texto de janelas de ajuda, documentos (SPO-AA+), declaração e avisos longos,
   **16 px** no Legível.

| Variável (CSS) | Uso | Compacto | **Intermediário (padrão)** | Legível |
|---|---|---|---|---|
| `--spo-fs-dado` | células de tabela, paginação, totais | 10 px | **12 px** | 13 px |
| `--spo-fs-controle` | rótulos de campo, campos, resumo das opções, metadados | 11 px | **12 px** | 13 px |
| `--spo-fs-texto` | texto corrido, descrições, itens de lista, nome de opção, **botões**, legenda de grupo de campos | 12 px | **13 px** | 14 px |
| `--spo-fs-leitura` | janelas de ajuda, documentos, declaração, avisos longos | 13 px | **14 px** | 16 px |
| `--spo-fs-titulo` | título de cartão e de janela | 13 px | **14 px** | 16 px |
| `--spo-fs-rotulo` | sobretítulo em maiúsculas | 11 px | **11 px** | 11 px |
| `--spo-fs-secao` / `--spo-fs-pagina` | título de seção / de página | 20 / 24 px | **20 / 24 px** | 20 / 24 px |

- O **"Tamanho do texto"** (112/125/150%) continua por cima de tudo, como hoje (amplia a área).
- **Mínimo de 12 px:** vale no Legível e no Intermediário. No **Compacto**, escolhido pela pessoa,
  tabelas (10 px) e rótulos (11 px) ficam abaixo — exceção registrada, como o 10 px do topo e do rodapé.
- **Entrelinhas** não mudam: 1,5 no texto, 1,2–1,25 nos títulos (e a opção "Espaçamento entre linhas").
- **Componente novo** usa as variáveis, nunca um `font-size` em px escrito à mão. O teste
  `tests/test_tamanho_fonte.py` guarda a contagem de `font-size` em px do `style.css` por valor (310 em
  15/11, regras antigas) e reprova valor novo ou contagem que sobe; quando uma regra antiga passa à
  variável, a contagem desce e o teste pede para baixar a referência.

Etapas (um PR por etapa): (1) esta seção — feito; (2) criar as variáveis e trocar "Texto das tabelas"
por "Densidade do texto", com login e mockup PTA/OpR — feito: as **tabelas já usam** `--spo-fs-dado`
(Legível 13 / Intermediário 12 / Compacto 10 px; decisão do dev. Jean de aplicar os valores novos nelas
já nesta etapa) e o padrão passa ao **Intermediário**; (3a) Personalizar e janelas — feito: cartões,
opções, prévias, declaração, busca, ajuda dos atalhos e "O padrão SPO-AA+" usam as variáveis (sobretítulo
= rótulo; título de cartão e de janela = título; descrição, nome de opção e itens = texto; resumo, rótulo de
campo e status = controle; ajuda dos atalhos e documento = leitura; títulos de seção dessas janelas no
tamanho do texto ao lado, em negrito, nunca maiores que o título da janela); (3b) relatórios e cabeçalho
de página — feito: cabeçalho de todas as telas (sobretítulo = rótulo, título = página, descrição =
texto); "Última atualização" e "Totais" = texto em negrito; nome da UO, linhas da atualização e rótulos
dos totais = controle; valores dos totais = dado; (3c) formulários e cadastros — feito: título de cartão =
título; rótulos de campo e o texto dentro dos campos (também nas tabelas e nos avisos) = controle; botões e
legendas de grupo = texto (botões dos formulários: 14 → 13 px no Intermediário); (3d) painéis, cartões
dinâmicos e Início — feito: Governança (títulos = título, descrições = controle, contexto = leitura),
cartões dinâmicos (título = título, texto = controle), Teto (títulos = título, rótulos = controle, valores e
ranking = dado; **valores em destaque** um passo acima do título: indicadores +2 px, cartões por grupo +4 px)
e Início (boas-vindas = leitura + 2 px); no Intermediário, os valores ficam como antes; (3e) gráficos —
feito: o texto do Plotly segue `--spo-fs-dado` e redesenha ao trocar a densidade — **etapa 3 concluída**; (3)
migrar as telas por grupo, medindo antes e depois, **começando pelo Personalizar** (decisão do dev.
Jean), depois janelas, relatórios e tabelas, formulários, painéis e Início, gráficos; (4) teste que
reprova `font-size` em px fora das variáveis — feito (`tests/test_tamanho_fonte.py`; a lista do "mínimo de 12 px" passou à variável de controle). A troca das cores literais por tokens (Fase C) vem em PR
separado, depois. As duas mexem em regras do dev. Cleber no `style.css`; ele orientou fazer o que for
preciso **antes** da divisão/simplificação dos arquivos monolíticos, então não há espera — cada PR
avisa o que mudou nas regras dele. Com o Intermediário como padrão, a etapa 3 deixa
o texto das telas de trabalho em 12–13 px (perto do de hoje) e as janelas em 14 px: o ganho é a
coerência entre elas.

Aplicação (por PRs, medindo antes e depois): (1) os casos abaixo de 12 px e as descrições com
entrelinha 1,25 (§7) — feito (#75, #82); (2) títulos de página iguais — feito (#76); (3) opção
"Espaçamento entre linhas" no Personalizar — feito (§2.1); (4) a troca dos valores por tokens
(`--spo-fs-*`, `--spo-lh-*`) na divisão do CSS. Medição completa, por
tela: `docs_jean/auditoria_aa/TIPOGRAFIA_MEDICAO_2026-11-13.md` (ambiente do dev. Jean).

## 3. Tokens (a fonte única de cor)

**Regra de ouro:** em componente, **só variável** (`var(--…)`). Cor literal (`#…`, `rgb(…)`) só
na **definição** de um token (§3.1–3.4) ou numa exceção registrada (§6.3). **Verificado pela suíte**
(`tests/test_cor_literal.py`, desde 15/11): guarda a contagem atual de cores literais fora de definição de
token (376 em 191 valores, regras antigas) em `tests/referencia_cor_literal.json` e reprova valor novo ou
contagem que sobe; definição de token (`--nome: #…`) não conta.

### 3.1 Superfícies, texto e bordas

| Token | Uso | Claro | Escuro | Contraste medido (sobre o cartão) |
|---|---|---|---|---|
| `--bg` | fundo da página | `#e6e9ed` | `#121212` | cartão × fundo: 1,22 (claro), 1,21 (escuro) |
| `--card` / `--surface` | cartões, campos, listas abertas | `#ffffff` | `#242424` | — |
| `--text` | texto principal | `#1d1d1d` | `#f6f5f4` | 16,86 / 14,26 |
| `--muted` | texto secundário (legendas, metadados) | `#48464e` (Reforçado `#38363d`) | `#d0cfcc` (Reforçado `#e2e1de`) | 9,29 / 9,96 (Reforçado 11,91 / 11,87) |
| `--border` | **só separadores decorativos** (linhas entre seções, contorno de cartão) | `#deddda` | `#3d3d3d` | 1,36 / 1,43 — **não serve para contorno de controle** |
| `--spo-control-border` | contorno de campo, lista, filtro, botão secundário | tom do destaque (Clássico `#1f9b7e`) | tom do destaque (Clássico `#349884`) | 3,47 / 4,40 (Reforçado 5,12 / 8,50) |

### 3.2 Destaque

| Token | Uso | Claro (Clássico) | Escuro (Clássico) | Contraste |
|---|---|---|---|---|
| `--accent` / `--accent-rgb` | cor de destaque "pura": anel de foco no escuro, contornos, tintas (`rgba(var(--accent-rgb), .16)`) | `#0f7c68` | `#3dd6b7` | — |
| `--accent-strong` | variante do destaque (hover, ênfase) | `#1f9b7e` | `#22b58f` | — |
| `--spo-accent-text` | **texto** na cor de destaque (títulos de seção, links, KPIs) | `#0b4c44` | `#a9e0d2` | 9,85 / 10,56 |
| `--spo-btn-fill` | **fundo do botão principal** | o tom mais claro da paleta com 7:1 para texto branco (`--color-600/700/800`, por destaque) | `--accent` (Roxo: `--accent-strong`) | texto branco 7,36–10,19 (claro); texto `#111` 7,53–14,61 (escuro) |
| `--color-100` … `--color-900` | paleta do destaque (definida em `ACCENT_COLORS`, `main.js`) | — | — | usar **por meio** dos tokens acima |
| `--spo-focus-ring` | anel de foco | `--color-700` | `--accent` | ≥ 3:1 |

### 3.3 Por destaque

Os 8 destaques trocam `--color-*`, `--accent*` e, por consequência, `--spo-control-border`,
`--spo-accent-text` e `--spo-btn-fill`. **Tela de login:** o `login-theme.js` só põe o `--accent` e os
tons 400–600; o bloco `[jean] Login com a paleta inteira` (#67) completa os tons 50–300 e 700–900
com os mesmos valores do `ACCENT_COLORS`. **Se a paleta mudar no `main.js`, mude também lá.** Cada combinação destaque × tema foi medida (#33, #36, #37,
#57). Quando um destaque não atingia a meta, a correção foi **no token, por destaque** (ex.: Azul
no claro usa `--color-800` como texto; Roxo no escuro usa `--accent-strong` no botão). Faça o mesmo:
nunca corrija um destaque dentro de um componente.

### 3.4 Estados

| Estado | Token / valor | Claro | Escuro | Uso |
|---|---|---|---|---|
| Perigo (preenchido) | `#a51d2d` / `#ff7b7b` | texto branco 7,46 | texto `#111` 7,53 | Excluir, Desativar, Cancelar processamento |
| Perigo (contorno) | `#a51d2d` / `#ffa3a3` | texto e borda 7,46 | 8,15 | Limpar Formulário, Cancelar/Remover das consultas |
| Aviso | âmbar `#b45309` / `#fbbf24` | faixa 4,63; ícone 4,28 | 8,28; 6,96 | faixas e ícones de aviso (o texto do aviso fica em `--text`) |
| Desabilitado | fundo `#eef2f4`, texto `#5f6670`, borda `#c9ced6` | texto 5,15 | fundo `rgba(255,255,255,.05)`, texto `#b5b5b5` (6,53), borda `#4a4a4a` | todo `:disabled` em `.form-grid` e as opções indisponíveis das Notas SEE (`.see-upload-option.is-disabled`, antes `opacity .45`) |
| Valores positivos/negativos (classes `pos`/`neg`) | tokens **`--spo-positive`** / **`--spo-negative`**: azul `#0000ff` / vermelho `#a00000` | 8,37 / 8,20 | `#94baff` / `#ff9999` (7,71 / 7,38) | totais de todos os relatórios e **qualquer célula numérica do conteúdo** (`td.num.pos`/`.neg`, em negrito 600; antes só o FIP 613 — achado do kit de mockup, 15/11) (#70). O vermelho puro `#ff0000` daria 4,0:1, abaixo do AA |
| Informação (`.alert-info`) | texto `#0f4c4c` sobre fundo do destaque claro | 8,86 | texto `--text`, fundo do destaque a 10%, borda dos controles (11,65) | avisos informativos, ex.: "Regras de segurança" da Limpeza (#79) |

> **A fazer (N5):** estes estados ainda são literais repetidos em blocos diferentes. Na divisão do CSS,
> viram tokens (`--spo-danger`, `--spo-danger-outline`, `--spo-warning`, `--spo-disabled-*`),
> definidos uma vez por tema. `--spo-positive` e `--spo-negative` já existem (15/11).

### 3.5 Diagramas e gráficos

Paletas por modo de diagrama (`data-visual-diagrams`: auto, claro, escuro, alto contraste) em
`COMMON_DIAGRAM_THEMES` (`main.js`) e nas variáveis `--spo-diagram-*`. Regras (#40, #45):
- fundo do gráfico **transparente** (herda o cartão); nada de fundo azul-marinho fixo. Vale também
  no **alto contraste escuro** (#100) e no **quadro em volta do gráfico** (`.teto-chart`, que pinta
  `--spo-diagram-bg`; #119): no `highDark`, `--spo-diagram-bg` = cor do cartão (`#242424`) e
  `--spo-diagram-node-bg` = `#262626`; só as cores das séries, o texto e a moldura são as do alto
  contraste (texto 14,8:1). O alto contraste claro mantém o fundo branco. **Lição do #119:** ao mudar o
  fundo de um gráfico, confira também o **contêiner** dele (o #100 deixou o gráfico transparente e o
  azul do quadro de trás reapareceu);
- **1ª cor de série = destaque** (tom com ≥ 3:1); as demais são **cores categóricas** para distinguir
  séries — podem ser azuis, rosas etc. de propósito;
- todo gráfico tem resumo para leitor de tela e "Ver dados em tabela";
- **layout comum a todo gráfico** (`initGraficosEscala`): fonte da interface ≥ 12 px, eixos com
  `automargin`, legenda vertical no pé, pizza com `automargin`; em gráfico estreito (≤ 520 px),
  margens mínimas, eixo X inclinado e nomes longos da legenda quebrados entre palavras;
- **a cor de um grupo é a mesma em gráficos e tabelas**: tabelas e cartões que colorem por categoria
  usam a mesma paleta do tema dos gráficos (`getTetoChartColors()` no Teto, #81), nunca uma paleta fixa;
- **cores seguras para daltonismo** (opção do Personalizar, `data-spo-cvd="safe"`): paleta **Okabe-Ito**
  (`window.spoPaletaDaltonismo`), distinta nos três tipos de daltonismo:
  - **claro:** `#0072B2 #E69F00 #009E73 #D55E00 #56B4E9 #CC79A7 #F0E442 #000000`;
  - **escuro:** o azul-escuro e o azul-claro trocam de lugar, e o preto vira branco;
  - **positivo/negativo:** azul × laranja, **nunca verde × vermelho**;
  - **contorno:** barras, fatias e blocos têm contorno na cor do texto, e é o **contorno** que dá o
    3:1 com o fundo (WCAG 1.4.11; 13,1:1 no claro e 16,6:1 no escuro). Algumas cores de preenchimento
    (amarelo, laranja) ficam abaixo de 3:1 sozinhas;
  - **padrão por cor** (liso, `/`, `\`, `x`, `-`, `|`, `+`, `.`): a mesma cor tem o mesmo padrão em
    todos os gráficos e na legenda;
  - **linhas:** ganham marcador e traço próprios.

  Menor diferença entre duas séries simuladas (CIEDE2000): 12,2 na protanopia, 11,6 na deuteranopia e
  10,9 na tritanopia. Na paleta padrão, o azul e o roxo davam 0,4 na deuteranopia, ou seja, iguais.
  Tudo é aplicado no tema dos diagramas e no `initGraficosEscala`: **gráfico novo que passa pelo
  Plotly já recebe tudo isso**. A **prévia dos componentes** do Personalizar também segue a opção: os
  nós do mini diagrama usam as quatro primeiras cores, com contorno, pela cor do fundo do diagrama
  (`data-visual-diagram-theme`, #117).
- **texto secundário dos nós** nos diagramas claros (`--spo-diagram-muted`): `#3e3d44` (≥ 8,9:1 em nós brancos e tingidos; antes `#4a4950`, abaixo de 7:1 no nó selecionado com o brilho reduzido; bateria 2).

## 4. Componentes

### 4.1 Botões — hierarquia

| Nível | Aparência | Exemplos | Classes |
|---|---|---|---|
| **Principal** | cheio, `--spo-btn-fill`; texto branco (claro) / `#111` (escuro) | Salvar, Consultar, Adicionar, Aprovar, Imprimir, Baixar Excel | `.btn-primary` |
| **Perigo** | vermelho cheio (§3.4) | Excluir, Desativar, Cancelar processamento | `.btn-danger` |
| **Dispensar** | contorno vermelho, sem preenchimento | Limpar Formulário, Cancelar/Remover das consultas | `.btn-danger` com `id` `*-clear`, `*-filtro-cancel`, `*-filtro-remove` |
| **Secundário** | contorno neutro (`--spo-control-border`), texto `--text`, sem preenchimento | Limpar critérios, Editar | `.btn-secondary` |

- **Mouse/foco:** no claro, escurecer (`brightness(.92)`); no escuro, clarear. Nunca baixar o texto de 7:1.
- **Um só "Principal" por grupo de ações** sempre que possível; o vermelho cheio fica para o que
  **apaga ou interrompe**.
- Botão novo de "dispensar" (cancelar, fechar, tirar um item): use o nível **Dispensar**; se não
  couber no padrão de `id`, crie uma classe (`.btn-dismiss`, a combinar) em vez de mais um `id`.

### 4.2 Campos

| Estado | Regra |
|---|---|
| Normal | fundo `--surface`, texto `--text`, borda `--spo-control-border` (≥ 3:1) |
| Foco | anel `--spo-focus-ring` de 3 px |
| Desabilitado | §3.4: apagado, borda **cinza neutra** (não a do destaque), cursor "não permitido" |
| Selo "editando" | classe `.spo-editing-badge` (cor `--text`), nunca `style="color:…"` no HTML (#52) |
| Arquivo (`<input type="file">`) | botão nativo em contorno, como o Secundário: borda `--spo-control-border`, texto `--text`, 12 px (#79) |

### 4.3 Listas

| Lista | Regra |
|---|---|
| Filtros `.mf-*` (relatórios) | painel `--card`, borda `--spo-control-border`; **Esc** = Cancelar e foco volta ao botão (#51) |
| Listas de marcar `.planning-action-checklist-*` | opção sob o mouse: 16% do destaque; Esc fecha (#51) |
| `<select>` nativo | `appearance: base-select` (Chrome/Edge): lista no tema, opção sob o mouse 16% do destaque, ✓ + negrito na escolhida, até 40rem e quebra de linha; campo fechado numa linha com seta desenhada no fundo (#54, #55). Firefox/Safari: lista nativa |

### 4.4 Navegação e estrutura

| Componente | Regra |
|---|---|
| Menu lateral | grupos com `aria-expanded`; Enter/Espaço abrem; ↓↑ Home End → ← (#41, #58) |
| Abas | `role="tab"` + `aria-selected` + `aria-controls` → `tabpanel`; setas trocam de aba, **uma** rotina de teclado por lista (#41, #49) |
| **Tabelas (teclado)** | padrão "grade" do WAI-ARIA, sem `role="grid"` (o leitor de tela segue lendo como tabela): Tab entra numa célula só (a última visitada; na primeira vez, a primeira de dados — tabindex itinerante), setas andam de célula em célula, Home/End na linha, Ctrl+Home/End na tabela, PageUp/PageDown 10 linhas, Enter entra no controle da célula e Esc volta; só linhas visíveis; a tabela é uma parada só do Tab (filtros, botões e links das células saem do Tab e são alcançados por setas + Enter; campos de digitação dos dados ficam); clique numa célula começa a navegação por ali; na paginação, ← → andam pelos botões e ↑/Esc voltam à tabela (o foco fica na paginação depois de trocar de página); de um filtro ou botão da célula, as setas andam pelas células (dentro de um campo ou com a lista aberta, são do campo/da lista). Alt + setas não: no navegador volta/avança a página. `initNavegacaoTabelas` (`main.js`); seção "Tabelas" na ajuda dos atalhos e dica na faixa |
| Paginação | página ativa com borda `--accent`, negrito e `aria-current="page"` (#42, #49) |
| Avisos modais | `role="dialog"`, foco dentro, Tab preso, Esc fecha, foco volta (#41) |
| **Cabeçalho de página** | cartão `header.module-header` (ou `report-header`): sobretítulo `.module-eyebrow` (11 px, maiúsculas), `<h1>` 24 px negrito, descrição em `<p>`, e à direita o **ícone do item do menu** (`MENU_META`, `services/features.py`) num quadrado na cor de destaque (#84). Tela nova usa essa marcação |
| Busca de telas | item **"Buscar tela"** no menu lateral, abaixo de "Início", e atalhos **Ctrl+K** e **/**. A busca é um diálogo modal no visual dos avisos (`modal-card`, título na cor de destaque, campo `.field`, botão "Fechar"), com caixa de busca `combobox` + lista `listbox` (WAI-ARIA). Fica centralizada no desktop e, no celular (até 600 px), é um cartão com 16 px de margem no alto da tela (o teclado virtual não cobre o campo), acima dos avisos modais. As outras janelas de consulta (ajuda dos atalhos, "O padrão SPO-AA+") seguem o mesmo cartão com margem, centralizado; nenhuma ocupa a tela inteira. Cada resultado mostra o **caminho no menu**. A opção ativa tem fundo e borda à esquerda, para não depender só da cor. Quem ganha a lista são as telas do menu já filtrado pelas permissões, mais o **glossário** de siglas (`GLOSSARIO` em `initBuscaTelas`). **Tela nova no menu:** se o nome não diz o assunto, acrescente os termos no glossário |
| Atalhos do teclado | ajuda no item **"Atalhos do teclado"** do menu (abaixo de "Buscar tela"), na tecla **?** e em links com `data-spo-atalhos`; mesmo visual da busca. **Alt+M** leva ao item da tela atual no menu (ou ao grupo visível que o contém) e há o link de pulo "Ir para o menu"; no menu, **Esc** volta para onde a pessoa estava e fecha o menu aberto por cima da tela. Foco levado por atalho usa `focarComAnel` (classe `spo-foco-atalho`): depois de um clique, o Chrome não mostra o `:focus-visible` para foco movido por combinação com Alt/Ctrl. Componente com setas mostra uma **dica** no rodapé ao receber foco pelo teclado (`DICAS` em `initAtalhosTeclado`, com `aria-describedby`). **Atalho novo:** entra na lista `SECOES` da ajuda; se for de uma tecla só, respeita `data-spo-keys="off"` (WCAG 2.1.4). **Componente novo com setas:** entra em `DICAS` |
| Foco reforçado (opção) | anel de **4 px** na cor do anel de foco (`--spo-focus-ring`) entre duas faixas de 2 px na cor do fundo (branco no claro, preto no escuro): anel × faixa ≥ 4,69:1 nos 8 destaques (WCAG 2.4.13). Dentro de áreas que cortam o que passa da borda (menu, grupos do Personalizar, tabelas com rolagem, avisos modais), o anel vai **para dentro** do controle. Medir com Tab real e **esperar a transição** dos controles (~0,2 s) antes de ler o estilo |
| Foco levado ao título | ao abrir tela ou fechar aviso, o foco vai ao `<h1>` (A11) **sem anel**: título e `<main>` não são controles (#69). Controles mantêm o anel |
| **Cartão dinâmico** | o cartão clicável segue o `.flow-node` de Governança e Resultados: fundo, borda e textos pelas cores dos diagramas (`--spo-diagram-*`), cantos de 12 px, espaçamento de 12 px, ícone de 20 px na cor de destaque no alto, título de 14 px em negrito, descrição de 12 px; com o mouse ou o foco, borda na cor de destaque; selecionado, borda e barra de 3 px à esquerda na cor de seleção e fundo tingido. Cartão que leva a um documento é clicável **inteiro** (o link do texto cobre o cartão; um só ponto de Tab). Em uso: Governança, Institucional, envio das Notas SEE, presets do Contrato Visual. **Cartão novo clicável:** acrescentar a classe ao bloco "Cartões dinâmicos" do `style.css` (o mockup PTA/OpR será revisto à parte) |
| Totais dos relatórios | 2 colunas sempre que os pares cabem numa linha; senão 1; se rótulo e valor não cabem lado a lado, o **valor desce inteiro** (#31, #56). PLAN 51: o mesmo comportamento quando o **cartão** tem até 640 px (container query `spo-plan51`; até 900 px os botões descem) (#77) |
| Quadros comparativos do Teto | colunas de ano com mínimo de 184 px (cabe "R$ 99.999.999.999,99" em 14 px mono); com pouco espaço, o quadro rola para o lado (#82) |
| Personalizar | o resumo da escolha atual de cada grupo quebra linha, nunca reticências (#86). **Prévias** em dois cartões de uma coluna: "Componentes do SPO" e "Fontes e tabelas" (`#spo-previa-fontes`, com a mesma borda da prévia: classes `personalizar-preview`/`preview-surface`; #117). Escolher a **Atkinson Hyperlegible** oferece, toda vez, aplicar também 112%, entrelinha 1,75 e leitura 72ch ("Leitura confortável"; sobe só o que estiver abaixo; #118). Desde a bateria 2 a oferta inclui a Atkinson Hyperlegible Mono nas tabelas e dados e lista só o que falta (botão "Aplicar", "Aplicar as duas"… até "as quatro"). Opção que muda o visual deve aparecer numa das prévias |
| Programação PTA/OpR (mockup no iframe) | recebe do SPO, na mensagem `spo-preferencias` (`initPontePta2027`), os atributos `data-spo-*`/`data-visual-*` e os tons de foco e destaque; `static/pta2027_v63r/assets/spo-preferencias.js`/`.css` aplicam fontes, tabelas, entrelinha, leitura, foco, links, contraste, brilho e movimento. O **tamanho do texto não** entra: o zoom da área do SPO já amplia o iframe. A **densidade** vale no espaço dos cartões (seção "Dimensões" do `spo-preferencias.css`); estilo dos cartões e daltonismo nos diagramas (SVG prontos) ficam fora. **Dimensões:** cartão principal = cabeçalho de página (título 24 px), títulos 24/20/16 px, cartões com cantos 14 (pequenos 12) e espaço pela densidade, campos 40 px, botões ≥ 40 px. Os atalhos do SPO digitados dentro do mockup voltam ao SPO (`spo-atalho`). **Opção nova no Personalizar:** acrescentar em `ATRIBUTOS` nos dois lados e a regra no `.css` do mockup. **SPO-AA+ no mockup:** cores (tokens `--teal`/`--muted` e literais) na seção "Cores do PTA 2027" do `spo-preferencias.css`; escala tipográfica **gerada** em `spo-escala.css` por `scripts/gerar_escala_pta2027.py` (o teste `tests/test_escala_pta2027.py` avisa se o `pta-2027.css` mudar sem gerar de novo) |
| Login | sem diálogos nativos (`window.confirm`/`alert`). Com sessão já aberta: aviso `role="status"` sem botões; a **segunda tentativa com o mesmo e-mail** encerra a outra sessão (#61, #80). O login usa a paleta inteira do destaque (§3.3) |

## 5. Como medir (bateria)

1. **Uma tela por vez**, esperando os dados chegarem (o banco de dev tem 15 conexões e tabelas de
   milhões de linhas). Para várias larguras, **redimensione a mesma tela**.
2. Meça o contraste contra o **fundo real** atrás do elemento (componha as transparências até um
   fundo opaco). Fundos em `linear-gradient` não aparecem em `backgroundColor`.
3. Espere o fim das **transições** (0,2 s nos botões) antes de medir.
4. Converta cores `color(srgb a b c)` (0–1) para 0–255 antes de calcular.
5. Cubra **2 temas × 8 destaques** quando a cor depende do destaque; os níveis Suave e Reforçado
   quando o componente usa `--muted` ou `--spo-control-border`.
6. Para `<select>`: meça com o **valor mais longo** escolhido e a lista **aberta**.
7. Larguras de referência: 1920, 1600 e 1366 (menu aberto e fechado), 1024, 768, 390, 320.
8. Teclado real (Tab, setas, Esc, Enter) para foco e navegação; leitor de tela quando houver
   `aria-*` novo. **Foco com clique ou Tab reais**: o `.focus()` por script não vale quando a página
   não tem o foco do navegador.
9. **Tipografia:** colete, por tela, tamanho, entrelinha, peso, maiúsculas e se o texto ocupa mais de
   uma linha; compare com a escala do §2.2. Inclua as abas e os estados que não abrem primeiro
   (ex.: aba Tabelas do Teto).
10. **Espaçamento de texto (WCAG 1.4.12):** em cada tela, compare o normal com um estilo forçado
    (`* { line-height: 1.5 !important; letter-spacing: .12em !important; word-spacing: .16em !important }`,
    `p { margin-bottom: 2em !important }`). Conte só o que **piora**: texto que passa da borda de um
    elemento que corta (`overflow: hidden/clip`) ou que passa a se sobrepor a outro. Descarte
    sobreposição com aviso modal aberto e com rodapé `position: sticky`. **Gráficos (SVG) não entram
    na medição automática: olhe na captura.** Teste também as combinações do Personalizar
    ("Maior (125%)" + "Mais amplo (2,0)") a 375 px.
11. **Login:** depois de mudar cor ou botão em componente geral, confira a tela de login nos 8
    destaques (ela carrega a paleta de outro jeito, §3.3).
12. **Opacidade e contêineres:** componha a **opacidade** do elemento e dos ancestrais no contraste
    (a dica de boas-vindas tinha `opacity: .7` e passava despercebida); e, ao mudar o fundo de um
    gráfico ou diagrama, meça também o **contêiner** dele (o #100 deixou o gráfico transparente e o
    quadro azul-marinho de trás reapareceu; #119). Procure fundos azul-marinho (`#111827`, `#0f172a`,
    `#050816`) em toda tela.
13. **Bateria completa (2026-11-15):** roteiro local que abre cada tela do menu num quadro, uma por
    vez, espera os dados e mede a mesma tela a 1366, 1080 e 390 px (o quadro se apresenta como janela
    do próprio tamanho, para não cair no modo "janela dividida"), em 4 perfis: padrão; escuro +
    Amarelo + Reforçado + diagramas em alto contraste + foco reforçado; acessibilidade máxima
    (Atkinson, 150%, entrelinha 2,0, leitura estrita, daltonismo, links, brilho reduzido, movimento);
    denso (compacta, Roxo, médio, plano, Source Sans, 112%, tabelas de 8 px). Mede rolagem lateral,
    texto cortado, contraste (AA e 7:1), azul-marinho, gráficos desenhados e erros no console. As
    preferências de quem testa são guardadas antes e devolvidas no fim.
14. **Daltonismo:** `window.spoValidarPaletaDaltonismo()`, no navegador, simula protanopia,
    deuteranopia e tritanopia e mede:
    - a menor diferença entre séries (CIEDE2000, ≥ 10);
    - o contraste do contorno com o fundo (≥ 3:1).

    Para ver como uma pessoa com daltonismo enxerga, aplique à área de conteúdo um filtro SVG
    `feColorMatrix` com a matriz de Machado. Tire a captura com e sem a opção.

## 5.1 Declaração de acessibilidade

O cartão **"Acessibilidade do SPO"**, no fim do Personalizar Temas, é a declaração pública do padrão
**SPO-AA+**. O rodapé leva a ele pelo link "Acessibilidade". Ele lista o que é atendido, o que é atendido
por opção e o que é parcial ou está em avaliação. **A cada avaliação (bateria, teste na homologação) ou
mudança de opção, atualize o cartão no mesmo PR.** Regra: declarar só o que foi medido.
Contato para barreiras: nger@edu.mt.gov.br.

**Publicação para o usuário:** "SPO-AA+", na declaração, abre a janela "O padrão SPO-AA+"
(`initPadraoSpoAaPlus`, fim do `main.js`) só com o **resumo** escrito para quem usa o SPO (metas do §1,
opções do §2.1 e como se verifica). O **documento técnico completo** abre pelo Contrato Visual Protegido
(link `data-spo-padrao="tecnico"`, desde a bateria 2), lido deste arquivo pelo servidor
(`/api/documentos/padrao-spo-aa-plus`, conversor em `services/documentos.py`). Este documento atualiza
sozinho; o **resumo, não**: mudou uma meta do §1 ou uma opção do §2.1, atualize o `RESUMO` no mesmo PR.
Elemento de markdown novo neste arquivo (link, bloco de código…): confira se o conversor o cobre
(`tests/test_documentos.py`). Cada norma, critério e recurso citado tem link para a fonte
(critérios na tradução oficial das WCAG 2.2 do W3C Brasil), abrindo em nova aba com ícone e aviso para
leitor de tela; critério novo citado no cartão entra com o link no mesmo formato.

## 6. Regras de manutenção

### 6.1 Ao criar ou mudar uma tela

- [ ] **Desde o primeiro mockup:** a tela nova começa do **Kit de mockup SPO-AA+** (§6.4), com as classes
      dele, e é vista no kit nos dois temas, em mais de um destaque, nas três densidades do texto e a 150%
      antes de virar código. Nada fica para "ajustar ao padrão depois".
- [ ] Nenhuma cor literal em componente; use os tokens (§3). Precisa de cor nova? Crie um token
      (definido por tema) e registre aqui. O `tests/test_cor_literal.py` reprova cor escrita à mão.
- [ ] Botões na hierarquia do §4.1; campos, listas e navegação como no §4.
- [ ] Nada de `style="color:…"`/`background`/`font-size` no HTML (#52) nem cor ou tamanho escritos à mão pelo
      JS (`.style.color = "#…"`, `style="color:#…"` em HTML montado): o `tests/test_style_inline.py` reprova
      (valor calculado da paleta do tema, `${…}`, e `var(--…)` podem).
- [ ] Nada de `window.alert`/`confirm`/`prompt` (bloqueados em alguns navegadores; #61).
- [ ] Cabeçalho de página no padrão do §4.4, com o ícone do item do menu.
- [ ] Textos na escala do §2.2 (mínimo 12 px, entrelinha 1,5 no texto corrido); números na fonte de dados.
- [ ] Valores positivos/negativos com as classes `pos`/`neg`, nunca cor própria.
- [ ] Texto na cor de destaque: `--spo-accent-text`; ícone na cor de destaque: `--spo-accent-graphic`. **Nunca** `--accent`/`--accent-strong` direto como cor de texto (com o Azul dá 3,77:1; com o Amarelo no claro, ~1,5).
- [ ] Colunas sem mínimo fixo em px: `minmax(min(100%, Npx), …)` ou `minmax(0, …)`. Lembre que o texto ampliado (112/125/150%) divide a largura útil.
- [ ] `<code>` e outros elementos com cor própria do Bootstrap: conferir no escuro e com o brilho reduzido.
- [ ] Layout de colunas que muda por `@media` de largura: lembrar que a área de conteúdo pode estar ampliada (112/125/150%) e a largura útil é a da janela dividida pelo fator. Prefira `@container` ou repita o limite multiplicado (ex.: 900 px × 1,5 = 1350 px).
- [ ] Gráfico novo pelo Plotly (passa pelo `initGraficosEscala`) e com as cores do tema dos diagramas, para seguir a opção de daltonismo; cor que distingue informação nunca é a única pista (padrão, marcador, rótulo).
- [ ] Rótulo e valor que podem não caber lado a lado: o valor desce inteiro (nunca reticências em dado).
- [ ] Tamanho de texto novo usa as variáveis da "Densidade do texto" (`var(--spo-fs-texto)`, `-dado`, `-controle`, `-leitura`, `-titulo`…; §2.2.1), nunca `font-size` em px: o `tests/test_tamanho_fonte.py` reprova.
- [ ] Movimento novo (animação, rolagem suave, cartão que sobe ao passar o mouse) respeita "Reduzir sempre" e o sistema: no CSS, sob `body[data-spo-motion="reduce"]` e `prefers-reduced-motion`; no JS, `scrollIntoView`/`scrollTo`/`scrollBy` já viram um salto com esmaecimento da área de chegada (0,22 s, só opacidade), sozinhos (também no mockup). Troca de conteúdo sem deslocamento: esmaecer, nunca deslizar.
- [ ] Painel que abre ao clicar num cartão (metas, eixos, detalhamento): abre logo abaixo do cartão ou do grupo dele; com "Reduzir sempre", a página só rola se o começo do painel não estiver à vista, e só até ele (sem esmaecer); o cartão aberto mostra o estado (borda na cor de destaque) e tem `aria-expanded`. Com "Automático", a rolagem suave até o painel vale só em tela estreita e baixa (até 1200 × 1000 px; celular até 720 px de largura sempre; ou preset "Pressão operacional"); em tela larga ou alta (monitor em retrato, tablet em pé), a página só rola se o começo do painel ficar fora da tela.
- [ ] Texto **nunca** apagado com `opacity` < 1: use a cor (`--muted`, ou o desabilitado do §3.4). A opacidade derruba o contraste e escapa de medições que não a compõem (bateria 2: pílula de versão a 4,26:1, opções indisponíveis das Notas SEE a 2,2:1, dica da Início).
- [ ] Texto dentro de forma de tamanho fixo (círculo, pílula, selo): a forma cresce com o texto (`fit-content`, `aspect-ratio`), nunca o contrário; meça com a fonte mais larga (indicador de avanço do PTA/OpR, bateria 2).
- [ ] Texto sobre painel **semitransparente** (tingido do destaque, degradê): meça contra o fundo da página que aparece por trás, também com o brilho reduzido.
- [ ] Teclado: tudo alcançável com Tab; componente com setas tem dica (`DICAS`) e sai com Tab; atalho novo está na ajuda (§4.4).
- [ ] Medido como no §5, nos dois temas, e anotado no PR.

### 6.2 CSS e cache

- **Especificidade:** regras antigas com `background:` ou a regra geral dos contornos (0,6,2) vencem
  blocos novos. Quando o novo precisa ganhar, use `:not(#nome)` (soma peso de id sem selecionar nada
  a mais) e **comente o motivo**. Evite `!important`.
- **`?v=` do CSS:** mudou o `style.css`, muda o `?v=` no `base.html` **e** no `login.html`, com o
  mesmo valor (#59). Mudou o `main.js`, muda o `?v=` do JS no `base.html`.
- No `base.html` e no `login.html` de uma branch `dev/*`, mude **só as linhas `?v=`** (o selo de
  ambiente e o rodapé são próprios de cada branch).

### 6.3 Exceções registradas (cor literal permitida)

| Onde | Por quê |
|---|---|
| Faixa de ambiente (`.environment-banner`, `.environment-inline`) | identidade de cada ambiente (dev/homologação) |
| Amostras de destaque no Personalizar (`.accent-swatch`) | mostram a cor real de cada destaque |
| Paletas de diagrama (§3.5), `ACCENT_COLORS` e a paleta Okabe-Ito (`PALETA_DALTONISMO`) | são definições de token |
| Paleta do login (bloco `[jean] Login com a paleta inteira`) | cópia do `ACCENT_COLORS` para a tela que não carrega o `main.js` (§3.3) |
| Definição de `--spo-positive`/`--spo-negative` (§3.4) e `.alert-info` escuro | são definições de token (o `.alert-info` escuro, literal até o N5) |
| Logotipo e imagens | não são interface |

### 6.4 Kit de mockup SPO-AA+

`static/kit/mockup-spo-aa-plus.html` (com `kit-mockup.css` e `kit-mockup.js`): página de partida de toda
tela nova. Abre pelo SPO local, sem entrar: `http://127.0.0.1:5000/static/kit/mockup-spo-aa-plus.html`.
- Usa o **próprio `style.css`** do SPO e lê do `main.js` a paleta dos destaques (`ACCENT_COLORS`), as
  opções do Personalizar (`OPTIONS`, `COMMON_VISUAL_OPTIONS`) e a navegação pelo teclado das tabelas
  (`initNavegacaoTabelas`, `initTecladoPaginacao`) e a regra da oferta "Leitura confortável" da Atkinson
  (`initSugestaoAtkinson`) e os **presets do Contrato Visual Protegido** (`PROTECTED_CONTRACT_PRESETS`:
  Padrão SPO, Compacto gerencial, Contraste técnico, Pressão operacional): **nada é cópia**, o kit
  acompanha o SPO sozinho.
  Aberto do disco (`file://`), o navegador bloqueia a leitura do `main.js`; o kit avisa e fica no padrão.
- Painel **"Preferências do kit"** com **os próprios cartões do Personalizar** (Tema, Cor de destaque,
  Preferências comuns, Fontes e tamanho do texto, Visão e cores — mesma marcação do template, descrições
  de cada opção incluídas) e um cartão "Preset do Contrato Visual" com os presets em **cartões dinâmicos**
  e o **painel de contexto** logo abaixo (escopo, estado, regras e detalhes do preset escolhido, lidos do
  `PROTECTED_CONTRACT_PRESETS`), como em Governança e Resultados. O cabeçalho do painel é o cabeçalho de
  página do SPO (§4.4). Comportamento igual ao do SPO
  (escolha com `.active`/`aria-pressed`, resumo do grupo, um grupo aberto por vez, o grupo fecha ao
  escolher) e **as mesmas regras**
  (escolher a Atkinson abre a oferta "Leitura confortável", como no Personalizar). O kit **abre com as
  preferências vigentes** no SPO deste navegador, inclusive o preset do Contrato; o que mudar vale só
  na página (nada é gravado); "Padrão SPO" volta ao padrão e recarregar a página volta às vigentes.
- **Kit × Contrato Visual Protegido:** papéis diferentes. O Contrato é a ferramenta de administração que
  **aplica** o visual do SPO (presets, presets personalizados, auditoria, pacotes), dentro do SPO e com
  permissão; o kit é o **ponto de partida** de uma tela nova, fora do login, e não muda nada. O kit cobre os
  presets do Contrato para que o mockup seja visto em todas as combinações. Integração futura a combinar
  com o dev. Cleber: uma aba "Componentes" no Contrato que abra o próprio kit.
- Logo abaixo dos cartões de preferências (e recolhidas com eles), **as duas prévias do Personalizar**
  ("Componentes do SPO" e "Fontes e tabelas", mesma marcação do template) e **gráficos** de pizza e de
  barras (Plotly do SPO com o tema dos diagramas, o destaque, o daltonismo, a escala e a tabela de dados:
  `COMMON_DIAGRAM_THEMES`, `initDiagramasDestaque`, `initGraficosAcessiveis` e `initGraficosEscala` lidos
  do `main.js`): mostram o efeito de cada escolha.
- Na tela de exemplo, componentes com a marcação real: cabeçalho de página, cartão `module-card`,
  o **catálogo de padrões** (cada exemplo recortado do template de origem: cartões dinâmicos com painel de
  contexto e fluxo com setas de Governança e Resultados — com o comportamento do `initGovernancaSemanticFlows`
  lido do `main.js` —, cartões de painel, cartão de documento do Institucional, indicadores do Teto e
  opções de envio das Notas SEE; os cartões do mockup PTA/OpR entram quando ele for integrado), formulário (campos,
  desabilitado, hierarquia dos botões), aviso `alert-info` e uma **tela de relatório** com a marcação dos
  relatórios do SPO (FIP 613, PLAN20, PED): `report-header`, cartão `report-data-card`, topo
  `relatorio-top relatorio-top-uo` com "Última atualização por UO" (o HTML do `renderUltimasAtualizacoes`),
  totais com `pos`/`neg` e os botões; filtros `.mf-*`, navegação pelo teclado e paginação.
- **O padrão dentro do kit:** os botões "O padrão SPO-AA+" (resumo) e "Documento técnico" no cabeçalho do kit
  abrem a mesma janela do SPO (`initPadraoSpoAaPlus`, lido do `main.js`); o documento técnico vem da rota
  `/api/documentos/padrao-spo-aa-plus` e pede o SPO logado neste navegador (sem sessão, a janela diz para
  entrar). Para o agente ou quem lê o código, a fonte é este arquivo, citado no comentário do kit e no
  "Como usar".
- **Como usar** (o mesmo passo a passo está na caixa "Como usar o kit", no alto da página):
  0. **Consultar o padrão:** botões do cabeçalho (resumo e documento técnico) ou este arquivo; checklist de
     tela nova no §6.1.
  1. **Criar:** copie `mockup-spo-aa-plus.html` para a mesma pasta (`static/kit/`) com outro nome, por exemplo
     `mockup-pta-opr.html`, e abra pelo SPO local (`/static/kit/mockup-pta-opr.html`); na mesma pasta, os
     caminhos do `style.css`, do `kit-mockup.js` e do `main.js` continuam valendo.
  2. **Montar:** troque só o conteúdo de `<main id="content-area">`, copiando os blocos de exemplo e mudando
     os textos; mantenha o painel e o `<script>` do fim.
  3. **CSS próprio**, se precisar: num `<style>` do mockup, só com tokens de cor e `var(--spo-fs-*)` (§6.1).
  4. **Revisar pelo painel:** claro e escuro; dois ou três destaques (o Amarelo é o mais exigente no claro);
     Compacto, Intermediário e Legível; 150%; a Atkinson com a "Leitura confortável"; os presets do Contrato;
     daltonismo, se houver gráfico; Tab, setas e Esc; janela estreita. O que quebra ou não muda se corrige
     ainda no mockup.
  5. **Levar ao SPO:** o conteúdo do `<main>` vira `templates/partials/<tela>.html`; o `<style>` vai para um
     bloco `[jean]` no fim do `style.css`. Teclado nas tabelas, gráficos acessíveis, densidade e temas já vêm
     do SPO.
  6. **Guardar:** o mockup fica em `static/kit/` enquanto está em desenho e vai ao Git só junto com o PR da
     tela.

  Com o Claude: "faça um mockup da tela X a partir do kit" — ele cria o arquivo, mede nas combinações do
  passo 4 e mostra as capturas antes de qualquer código no SPO. O kit também **mede o próprio SPO**:
  componente que não muda ao trocar a densidade ou o tema no painel é dívida (§7).
- Regra nova do Personalizar que liga uma opção a outra (como a oferta da Atkinson): o kit precisa
  reproduzi-la, lendo do `main.js`, para continuar mostrando o comportamento exato do SPO.
- `tests/test_kit_mockup.py` reprova se os trechos que o kit lê do `main.js` mudarem de nome ou de forma,
  se os cartões do painel deixarem de ser iguais aos do Personalizar (cartão novo ou opção nova no
  template precisam ser copiados para o kit) e se o CSS do
  kit escrever px ou cor literal; também se a marcação das prévias do Personalizar mudar sem ir para o kit,
  se os blocos dos gráficos mudarem de forma ou passarem a depender de outra função global e se o Plotly
  do kit não for o mesmo do `base.html`; e se um exemplo do catálogo deixar de ser igual ao template de
  origem. **Padrão novo de cartão no SPO:** acrescentar ao catálogo (recorte do template + linha na lista
  `CATALOGO` do teste). **Opção nova no Personalizar:** copiar o cartão do template para o
  painel do kit (o teste aponta qual).

## 7. Dívida conhecida (roteiro do N5)

Medição de 2026-11-13 no `style.css` (fora dos blocos `[jean]`): **303 cores literais, 235
diferentes**, em 3.709 declarações (695 já usam `var(--)`); 143 regras `.theme-dark` espalhadas.

| Tipo | Áreas (cores literais) | Tratamento |
|---|---|---|
| Definição de token (legítimo) | `:root`/`.theme-dark` (≈ 53), paletas de diagrama `data-visual-diagram*` (≈ 55) | mover para o arquivo de tokens na divisão do CSS |
| Componentes com cor própria | Meta Física (30), prévia do Personalizar (27), tabela do PLAN 20 (16), registro do EMP (15), tabela do PLAN 21 (12), alertas (7), Teto QOMP (6), limpeza de histórico (6), carregamento (5) | trocar pelos tokens do §3, área por área, medindo antes e depois |
| Já sobrepostos pelo AA+ | filtros `.mf-*`, paginação, `.btn`, totais, tabelas | apagar o literal antigo quando a área for dividida (hoje o bloco `[jean]` vence) |

Ordem sugerida: na divisão do CSS (depois do AA+ na `main`), cada área vira um arquivo; no mesmo
PR da área, os literais viram tokens. **Fase B — feita (15/11):** `tests/test_cor_literal.py` impede
literais novos; a contagem de hoje (376 cores, 191 valores, fora de definição de token) só desce. Quando uma
área passar aos tokens, rode `python tests/test_cor_literal.py` para baixar a referência (o comando se recusa
se a contagem tiver subido; exceção nova do §6.3 só com `--forcar`).

**Tipografia** — a tabela abaixo é a medição de 2026-11-13. **Resolvidos:** os casos de 10–11 px e as
entrelinhas apertadas (#75; topo e rodapé ficam em 10 px por decisão do dev. Jean), os títulos de
página (#76) e a aba Tabelas do Teto (#82). **Resta:** os tokens `--spo-fs-*`/`--spo-lh-*` (o texto dos gráficos foi
resolvido por `initGraficosEscala`).

| Caso | Onde | Telas |
|---|---|---|
| 9 px | nome do usuário e "Logados" no topo (`.user-meta`) | todas |
| 10 px | "Itens por página" da paginação; rótulos do Teto Orçamentário; pílula de versão do Contrato Visual | 17 / 4 / 1 |
| 10,5 px | identificadores em `<code>` (API acessos) | 1 |
| 11 px | metadados da "Última atualização por UO" (entrelinha 1,25); selos "Ativo/Ativa"; rótulos dos totais e dos filtros; textos de ajuda | 21 / 16 / 22 / 7 |
| entrelinha 1,25–1,35 em texto de várias linhas | descrições `muted` dos cabeçalhos (12 px); cartões de Governança (12,25 px); fonte do Teto (11 px) | 9 / 6 / 1 |
| título da página | 23 px (43 telas, peso 700, entrelinha 1,2 ou 1,5), 24 px peso 500 (9), 22 px (1), 25,6 px (1) | — |

**Outros pontos conhecidos** (detalhe no inventário do dev. Jean, `docs_jean/INVENTARIO_AA_PLUS_2026-11-14.md`):

| Ponto | Situação |
|---|---|
| Legendas dos gráficos (Plotly) com espaçamento forçado (1.4.12) | **resolvido** (legenda vertical, nomes quebrados no estreito). Resta, a 375 px e só com espaçamento imposto depois do desenho, um rótulo inclinado de eixo a 5 px da borda; a tabela de dados cobre |
| 11 telas de formulário/consulta sem cabeçalho de página (§4.4) | a decidir (mudança de template) |
| Programação PTA/OpR com cabeçalho próprio | depende do futuro do mockup (dataPTA) |
| Ícone do cabeçalho repete o do `MENU_META`; paleta do login repete o `ACCENT_COLORS` | na divisão, gerar de uma fonte só |
| Cores forçadas do Windows, `prefers-contrast: more`, leitor de tela, Firefox/Safari | não medidos |
| `style="…"` antigos com tamanho de texto (15/11; travados pelo `tests/test_style_inline.py`): links "Esqueci a senha"/"Voltar ao login" (12 px) no login e nas telas de senha (templates do dev. Cleber), título "UO …" do aviso da Início (0,95rem) e o bloco monoespaçado de Acessos à API (12 px) | migrar para classes com as variáveis da densidade; ao migrar, baixar a `REFERENCIA` do teste |
| **Achado do kit de mockup (15/11):** componentes comuns fora da "Densidade do texto" — título do cartão `.module-card-title h2` (15 px fixo), descrição dele (12), texto corrido e listas de cartão (14, herdado do `body`) e avisos `.alert` (14) | **resolvido** (`fix/densidade-cartoes-pos-neg`): título = `--spo-fs-titulo`, descrição = `-controle`, texto e listas = `-texto`, avisos = `-leitura` (os da Limpeza de Histórico têm tamanho próprio). No Intermediário, título 15 → 14 e texto do cartão 14 → 13 nas 11 telas com cartão de seção |
| **Achado do kit de mockup (15/11):** `pos`/`neg` só pintam nos totais (`.totais-row`) e na tabela do FIP 613 (`.fip-table td.num`); numa tabela nova, o negativo fica sem cor | **resolvido**: tokens `--spo-positive`/`--spo-negative` e regra para qualquer `td.num.pos/.neg` do conteúdo (§3.4) |

## 8. Histórico

| Data | Mudança |
|---|---|
| 2026-11-13 | Versão 1: consolida o AA+ da `dev/jean` (#31–#61). |
| 2026-11-15 | **Versão 4:** §2.2 "Densidade do texto" (Compacto/Intermediário/Legível para a interface inteira; padrão Legível; Compacto com tabelas de 10 px; passo Leitura de 16 px) e as variáveis `--spo-fs-*`; §2.1 Mulish na largura do "Estrito". Só documentação: a aplicação vem nas etapas 2–4. |
| 2026-11-15 | §4.4: **cartão dinâmico** (o `.flow-node` de Governança) como sistema visual dos cartões clicáveis: Institucional, envio das Notas SEE e presets do Contrato Visual. |
| 2026-11-15 | **Versão 3:** opções novas (leitura, daltonismo, foco, links, brilho, movimento, 150%, densidade automática, atalhos de uma tecla; §2.1); fundo do alto contraste escuro no gráfico e no quadro (§3.5); busca de telas, atalhos do teclado, Programação PTA/OpR, prévias e sugestão da Atkinson (§4.4); declaração e padrão publicados para o usuário (§5.1); bateria 2 e lições de opacidade e contêineres (§5); brilho reduzido com `--muted` `#3e3c44` e "Dispensar" `#861523`. Até o #119 e a bateria 2. |
| 2026-11-14 | **Versão 2:** metas de espaçamento e escala (§1); dados fora de tabela e texto dos gráficos (§2.2); paleta do login (§3.3); valores `pos`/`neg` e `.alert-info` (§3.4); cor de grupo igual em gráficos e tabelas (§3.5); campo de arquivo (§4.2); cabeçalho de página com ícone do menu, foco no título, totais e PLAN 51, quadros do Teto, Personalizar e login (§4.4); baterias de tipografia, 1.4.12 e login (§5); checklist (§6.1); exceções (§6.3); dívida atualizada (§7). Até o #86. |
| 2026-11-13 | §2.1 Tipografia: padrão SPO passa a Inter (interface), Intel One Mono (dados) e tabelas a 12 px; a antiga "Clássica" vira "Do sistema". |
| 2026-11-14 | §2.1: opção "Espaçamento entre linhas" (1,5 / 1,75 / 2,0) no Personalizar. |
| 2026-11-13 | §2.2 Escala tipográfica (11/12/14/15–16/20/24 px; entrelinha 1,5 no texto) a partir da medição de 66 telas; casos fora da escala no §7. Só documentação: a aplicação vem em PRs seguintes. |
