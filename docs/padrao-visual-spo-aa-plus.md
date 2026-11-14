# Padrão visual "spo-aa+" do SPO

Regras de cor, contraste, temas e componentes da interface do SPO. Valem para **qualquer tela nova
ou alterada**, por qualquer desenvolvedor. Completa o `docs/contrato-visual-spo.md`, que cuida de
layout, responsividade e preferências.

> Versão 2, 2026-11-14 (a versão 1, de 13/11, cobria até o #61). Valores **medidos** no SPO da
> `dev/jean` depois do #86, com o destaque Clássico (os outros 7 destaques seguem as mesmas regras;
> ver §3.3). Quando uma regra mudar, atualize este documento no mesmo PR.

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
| Alto contraste do Windows | `@media (forced-colors: active)`: o sistema impõe as cores; o SPO não deve brigar com ele | automático — **ainda não medido** |
| `prefers-contrast: more` | só os diagramas reagem hoje | automático — **ainda não medido** |

A classe `theme-dark` no `<body>` indica o tema escuro; `data-visual-contrast` (`standard`,
`medium`, `enhanced`) e `data-accent-preference` (`classic`, `blue`, …) indicam nível e destaque.

### 2.1 Tipografia

Fontes **hospedadas no SPO** (`static/fonts`, licença OFL 1.1): aparecem iguais em qualquer
computador ou celular. O navegador só baixa a fonte em uso.

| Uso | Padrão SPO | Alternativas no Personalizar | Atributo no `<body>` |
|---|---|---|---|
| Interface (textos, menus, botões, campos) | **Inter** | Atkinson Hyperlegible, Source Sans 3, Do sistema | `data-spo-font-ui` |
| Tabelas e dados (tabelas, totais, KPIs) | **Intel One Mono** (monoespaçada, números alinhados) | Inter com números alinhados, IBM Plex Sans, Do sistema | `data-spo-font-data` |
| Texto das tabelas (todas as tabelas do conteúdo; Legível mantém o tamanho próprio, ≥ 12 px, das tabelas fora dos relatórios) | **Legível (12 px)** | Intermediário (10 px), Compacto (8 px) | `data-spo-table-text` |
| Tamanho geral | **100%** | 112%, 125% (amplia a área de conteúdo) | `data-spo-text-size` |
| Espaçamento entre linhas (texto corrido) | **Padrão (1,5)** | Amplo (1,75), Mais amplo (2,0); títulos, tabelas, botões e campos não mudam | `data-spo-line-height` |

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
- **Largura do texto corrido:** `p`, `li`, `dd` e avisos `.alert` (fora de tabelas, menus e botões)
  com no máximo **72ch** (média medida de 84–110 caracteres por linha; decisão do dev. Jean, entre
  o WCAG 1.4.8, que pede ≤ 80, e o uso do espaço nos cartões largos). Parágrafos seguidos com
  **1 entrelinha** de espaço (`1lh`; decisão do dev. Jean; o AAA pediria 1,5).
- O título da página é **negrito (700), 24 px, entrelinha 1,2** em todas as telas com cabeçalho de
  página (decisão do dev. Jean, 2026-11-13; aplicado no #76).
- **Dados** (valores, totais, KPIs, códigos) usam a fonte de dados do §2.1, inclusive fora de
  `<table>` (ex.: aba Tabelas do Teto, #82). Componente novo que mostra número deve entrar na lista
  de seletores da fonte de dados.
- O texto **dentro dos gráficos** (SVG do Plotly) usa a fonte da interface com mínimo de 12 px,
  aplicado a todo gráfico por `initGraficosEscala` (`main.js`).

Aplicação (por PRs, medindo antes e depois): (1) os casos abaixo de 12 px e as descrições com
entrelinha 1,25 (§7) — feito (#75, #82); (2) títulos de página iguais — feito (#76); (3) opção
"Espaçamento entre linhas" no Personalizar — feito (§2.1); (4) a troca dos valores por tokens
(`--spo-fs-*`, `--spo-lh-*`) na divisão do CSS. Medição completa, por
tela: `docs_jean/auditoria_aa/TIPOGRAFIA_MEDICAO_2026-11-13.md` (ambiente do dev. Jean).

## 3. Tokens (a fonte única de cor)

**Regra de ouro:** em componente, **só variável** (`var(--…)`). Cor literal (`#…`, `rgb(…)`) só
na **definição** de um token (§3.1–3.4) ou numa exceção registrada (§6.3).

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
| Desabilitado | fundo `#eef2f4`, texto `#5f6670`, borda `#c9ced6` | texto 5,15 | fundo `rgba(255,255,255,.05)`, texto `#b5b5b5` (6,53), borda `#4a4a4a` | todo `:disabled` em `.form-grid` |
| Valores positivos/negativos (classes `pos`/`neg`) | azul `#0000ff` / vermelho `#a00000` | 8,37 / 8,20 | `#94baff` / `#ff9999` (7,71 / 7,38) | totais de todos os relatórios e células do FIP 613 (#70). O vermelho puro `#ff0000` daria 4,0:1, abaixo do AA |
| Informação (`.alert-info`) | texto `#0f4c4c` sobre fundo do destaque claro | 8,86 | texto `--text`, fundo do destaque a 10%, borda dos controles (11,65) | avisos informativos, ex.: "Regras de segurança" da Limpeza (#79) |

> **A fazer (N5):** estes estados ainda são literais repetidos em blocos diferentes. Na divisão do CSS,
> viram tokens (`--spo-danger`, `--spo-danger-outline`, `--spo-warning`, `--spo-disabled-*`,
> `--spo-positive`, `--spo-negative`), definidos uma vez por tema.

### 3.5 Diagramas e gráficos

Paletas por modo de diagrama (`data-visual-diagrams`: auto, claro, escuro, alto contraste) em
`COMMON_DIAGRAM_THEMES` (`main.js`) e nas variáveis `--spo-diagram-*`. Regras (#40, #45):
- fundo do gráfico **transparente** (herda o cartão); nada de fundo azul-marinho fixo;
- **1ª cor de série = destaque** (tom com ≥ 3:1); as demais são **cores categóricas** para distinguir
  séries — podem ser azuis, rosas etc. de propósito;
- todo gráfico tem resumo para leitor de tela e "Ver dados em tabela";
- **layout comum a todo gráfico** (`initGraficosEscala`): fonte da interface ≥ 12 px, eixos com
  `automargin`, legenda vertical no pé, pizza com `automargin`; em gráfico estreito (≤ 520 px),
  margens mínimas, eixo X inclinado e nomes longos da legenda quebrados entre palavras;
- **a cor de um grupo é a mesma em gráficos e tabelas**: tabelas e cartões que colorem por categoria
  usam a mesma paleta do tema dos gráficos (`getTetoChartColors()` no Teto, #81), nunca uma paleta fixa.

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
| Paginação | página ativa com borda `--accent`, negrito e `aria-current="page"` (#42, #49) |
| Avisos modais | `role="dialog"`, foco dentro, Tab preso, Esc fecha, foco volta (#41) |
| **Cabeçalho de página** | cartão `header.module-header` (ou `report-header`): sobretítulo `.module-eyebrow` (11 px, maiúsculas), `<h1>` 24 px negrito, descrição em `<p>`, e à direita o **ícone do item do menu** (`MENU_META`, `services/features.py`) num quadrado na cor de destaque (#84). Tela nova usa essa marcação |
| Foco levado ao título | ao abrir tela ou fechar aviso, o foco vai ao `<h1>` (A11) **sem anel**: título e `<main>` não são controles (#69). Controles mantêm o anel |
| Totais dos relatórios | 2 colunas sempre que os pares cabem numa linha; senão 1; se rótulo e valor não cabem lado a lado, o **valor desce inteiro** (#31, #56). PLAN 51: o mesmo comportamento quando o **cartão** tem até 640 px (container query `spo-plan51`; até 900 px os botões descem) (#77) |
| Quadros comparativos do Teto | colunas de ano com mínimo de 184 px (cabe "R$ 99.999.999.999,99" em 14 px mono); com pouco espaço, o quadro rola para o lado (#82) |
| Personalizar | o resumo da escolha atual de cada grupo quebra linha, nunca reticências; a amostra empilha como os totais (#86) |
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

## 6. Regras de manutenção

### 6.1 Ao criar ou mudar uma tela

- [ ] Nenhuma cor literal em componente; use os tokens (§3). Precisa de cor nova? Crie um token
      (definido por tema) e registre aqui.
- [ ] Botões na hierarquia do §4.1; campos, listas e navegação como no §4.
- [ ] Nada de `style="color:…"`/`background` no HTML (#52).
- [ ] Nada de `window.alert`/`confirm`/`prompt` (bloqueados em alguns navegadores; #61).
- [ ] Cabeçalho de página no padrão do §4.4, com o ícone do item do menu.
- [ ] Textos na escala do §2.2 (mínimo 12 px, entrelinha 1,5 no texto corrido); números na fonte de dados.
- [ ] Valores positivos/negativos com as classes `pos`/`neg`, nunca cor própria.
- [ ] Rótulo e valor que podem não caber lado a lado: o valor desce inteiro (nunca reticências em dado).
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
| Paletas de diagrama (§3.5) e `ACCENT_COLORS` | são definições de token |
| Paleta do login (bloco `[jean] Login com a paleta inteira`) | cópia do `ACCENT_COLORS` para a tela que não carrega o `main.js` (§3.3) |
| Valores `pos`/`neg` e `.alert-info` escuro (§3.4) | literais até virarem tokens no N5 |
| Logotipo e imagens | não são interface |

## 7. Dívida conhecida (roteiro do N5)

Medição de 2026-11-13 no `style.css` (fora dos blocos `[jean]`): **303 cores literais, 235
diferentes**, em 3.709 declarações (695 já usam `var(--)`); 143 regras `.theme-dark` espalhadas.

| Tipo | Áreas (cores literais) | Tratamento |
|---|---|---|
| Definição de token (legítimo) | `:root`/`.theme-dark` (≈ 53), paletas de diagrama `data-visual-diagram*` (≈ 55) | mover para o arquivo de tokens na divisão do CSS |
| Componentes com cor própria | Meta Física (30), prévia do Personalizar (27), tabela do PLAN 20 (16), registro do EMP (15), tabela do PLAN 21 (12), alertas (7), Teto QOMP (6), limpeza de histórico (6), carregamento (5) | trocar pelos tokens do §3, área por área, medindo antes e depois |
| Já sobrepostos pelo AA+ | filtros `.mf-*`, paginação, `.btn`, totais, tabelas | apagar o literal antigo quando a área for dividida (hoje o bloco `[jean]` vence) |

Ordem sugerida: na divisão do CSS (depois do AA+ na `main`), cada área vira um arquivo; no mesmo
PR da área, os literais viram tokens. Um teste da suíte deve impedir **literais novos** (Fase B).

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
| Programação PTA 2027 com cabeçalho próprio | depende do futuro do PTA 2027 |
| Ícone do cabeçalho repete o do `MENU_META`; paleta do login repete o `ACCENT_COLORS` | na divisão, gerar de uma fonte só |
| Cores forçadas do Windows, `prefers-contrast: more`, leitor de tela, Firefox/Safari | não medidos |

## 8. Histórico

| Data | Mudança |
|---|---|
| 2026-11-13 | Versão 1: consolida o AA+ da `dev/jean` (#31–#61). |
| 2026-11-14 | **Versão 2:** metas de espaçamento e escala (§1); dados fora de tabela e texto dos gráficos (§2.2); paleta do login (§3.3); valores `pos`/`neg` e `.alert-info` (§3.4); cor de grupo igual em gráficos e tabelas (§3.5); campo de arquivo (§4.2); cabeçalho de página com ícone do menu, foco no título, totais e PLAN 51, quadros do Teto, Personalizar e login (§4.4); baterias de tipografia, 1.4.12 e login (§5); checklist (§6.1); exceções (§6.3); dívida atualizada (§7). Até o #86. |
| 2026-11-13 | §2.1 Tipografia: padrão SPO passa a Inter (interface), Intel One Mono (dados) e tabelas a 12 px; a antiga "Clássica" vira "Do sistema". |
| 2026-11-14 | §2.1: opção "Espaçamento entre linhas" (1,5 / 1,75 / 2,0) no Personalizar. |
| 2026-11-13 | §2.2 Escala tipográfica (11/12/14/15–16/20/24 px; entrelinha 1,5 no texto) a partir da medição de 66 telas; casos fora da escala no §7. Só documentação: a aplicação vem em PRs seguintes. |
