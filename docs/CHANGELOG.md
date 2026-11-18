# Registro de mudanças (CHANGELOG) — SPO

Este arquivo lista o que foi publicado, por data. Formato:

- Uma entrada por data de publicação, a **mais nova em cima**.
- Dentro da entrada, as mudanças ficam agrupadas em **Adicionado**, **Alterado**, **Corrigido** e **Removido** (grupo sem mudança é omitido).
- **Uma linha por mudança**, com o número do PR e, quando houver, a seção do [claude.md](claude.md) ou do [padrao-visual-spo-aa-plus.md](padrao-visual-spo-aa-plus.md) que explica.
- Regras do fluxo de desenvolvimento e de promoção: seção 40 do [claude.md](claude.md).

---

## 2026-11-17 — dev/jean, ainda não publicado

### Adicionado
- Siglas com explicação em qualquer aparelho (`feat/siglas-explicadas`; achado do dev. Jean): `<abbr title>` só mostrava o significado ao passar o mouse, e no celular e pelo teclado não aparecia nada. Agora toda sigla vira um botão com balão (`initSiglas`, bloco `[jean]` no fim do `main.js` e do `style.css`): toque ou clique fixa; mouse por cima e foco pelo teclado mostram; Esc, clicar fora, sair ou tocar numa moldura (iframe) fecham; o leitor de tela lê o significado (`aria-describedby`). Sigla dentro de um link ganha um "ⓘ" logo depois do link ("O que é WCAG?"). Vale nas 6 siglas de hoje (PTA e OpR na Programação PTA/OpR e no mockup dela; WCAG e eMAG no Personalizar) e nas telas carregadas depois. O mockup PTA/OpR tem uma cópia exata do bloco, conferida pelo `test_kit_mockup.py`; o kit roda o bloco do `main.js` e ganhou o exemplo e a ficha. Testado com mouse, clique, teclado, Esc, toque (dentro e fora da moldura) e a 360 px (balão dentro da tela): 17/17. A comparação antes × depois achou que a regra de 44 px dos botões em tela de toque esticava o título no celular (de 29 para 44 px): a sigla ficou fora dela. Também: a bateria trata o botão de sigla numa frase como os links (exceção do alvo de 24 px), e a "segunda olhada" da comparação passou a casar as diferenças pelo caminho do elemento (com o id no nome, um id sorteado de novo fazia diferenças legítimas sumirem). Padrão SPO-AA+ 5.3 (§4.4). `?v=` do CSS e do JS `20261118.jean-siglas.1` (`base.html`, `login.html`, kit) e do `spo-preferencias.css/.js` do mockup.
- Índice e fichas no kit de mockup (`feat/kit-indice`; achado do dev. Jean): o alto do kit tem um índice com os padrões em 5 grupos (itens e subitens, com âncoras), e cada um dos 21 padrões ganhou uma ficha logo abaixo do título: quando usar, onde no SPO, classes do `style.css`, funções do `main.js`, seção do padrão SPO-AA+ e PRs. Fonte única: `static/kit/catalogo.json`, que pessoa ou agente lê direto; o `tests/test_kit_mockup.py` confere que tudo o que ele cita existe e que todo título de padrão do kit está nele. "Proposta: avisos da Início como cartões" virou padrão no kit e no padrão (§4.4; aceita no lote 7). Padrão SPO-AA+ versão 5.2 (§6.4). Só `static/kit/`, o teste e o documento do padrão.
- Bateria SPO-AA+: comparação antes × depois (`feat/bateria-comparar`; Fase 0 do frontend da modularização): `npm run comparar:gravar` grava antes de uma mudança um retrato de cada tela (estilo calculado de todos os elementos, caixas, `::before`/`::after`, tokens e captura), em 11 telas × 3 vistas e o kit de mockup; `npm run comparar` lista depois cada diferença (tela, elemento, propriedade, antes → depois). Dado ao vivo, medida de até 1 px e ids sorteados dos gráficos saem como aviso. Provado: `style.css` cortado em 3 partes na mesma ordem, sem diferença em 33/33; partes fora de ordem e borda do cabeçalho com alfa 0,10 → 0,11, acusado em 33/33; +2 px num cartão do kit, acusado só no kit. Achados: o `kit-mockup.js` procura o CSS do SPO pelo nome do arquivo e quebra com o CSS dividido (mudar junto na divisão); o menu lateral mede a largura antes de a fonte carregar (16–19 px ao acaso; `fix/` à parte). Só `tests/navegadores/`.
- Teste do mapa de rotas (`feat/teste-mapa-rotas`; Fase 0 do backend da modularização do `rotas/home_routes.py`, sugestão do dev. Cleber de 13/11): `tests/test_mapa_rotas.py` compara URL, métodos e endpoint das 258 rotas da aplicação com `tests/referencia_mapa_rotas.json` (uma rota por linha) e reprova se alguma some, aparece ou muda; rota nova ou mudança de propósito se registra rodando `python tests/test_mapa_rotas.py`. Conferido movendo uma rota de verdade (experimento desfeito): para outro arquivo, registrada no mesmo `home_bp`, passa sem diferença; para um blueprint novo, acusa `home.api_clients_list → usuarios.api_clients_list`.

### Alterado
- Bateria SPO-AA+: comparação antes × depois mais rápida (`feat/bateria-comparar-rapida`; pedido do dev. Jean): retrato compacto (cada combinação de estilo guardada uma vez só; o PLAN 51, com 61 mil elementos e 144 estilos diferentes, passou de 71 MB), espera pela tela pronta no lugar dos tempos fixos (nenhuma requisição pendente, fontes e imagens carregadas, nenhum elemento novo por 0,7 s) e `--telas=` com uma lista escolhida (ex.: `--telas=inicio,kit`). Gravar as 11 telas e o kit em 3 vistas: de 358 s para 138–211 s (varia com o banco de desenvolvimento), referência de 329 MB para 45 MB; a dirigida, ~40 s para gravar e ~1 min para comparar. Também: abrir uma vista tenta duas vezes e, não abrindo, registra a falha e segue (antes, a comparação parava); o vizinho que só se desloca dentro de uma linha com texto que mudou (a hora que encolhe empurra o nome) conta como dado. Conferido: a versão nova contra uma referência gravada pela antiga, 36/36 sem diferença de estilo; uma borda com alfa 0,10 → 0,11, acusada em 6/6 com exatamente uma diferença cada. Só `tests/navegadores/`.

### Corrigido
- Linha do usuário no cabeçalho (`fix/linha-usuario-cabecalho`; achado do dev. Jean na homologação): "Nome - data, hora | Logados: N" quebrava em qualquer ponto nas telas estreitas (o `overflow-wrap: anywhere` do #43), com "Nome -" numa linha e a data na outra, ou a hora separada da data. Agora a linha se divide em nome e "data, hora | Logados", que nunca se partem ao meio: cabendo, uma linha; não cabendo, o nome numa linha e a sessão na de baixo, sem o traço. Vale para o cabeçalho principal e para o da Programação PTA/OpR. Medido de 320 a 1366 px: com o nome do usuário de teste (42 caracteres), duas linhas até 440 px e uma a partir de 600 px; com um nome de 28 caracteres, uma linha já a 440 px. Comparação antes × depois (6 telas × 7 vistas): a única diferença são as três partes novas dentro da linha. Blocos `[jean]` no fim do `main.js` (`initLinhaUsuario`; o texto da linha não muda) e do `style.css`. `?v=` do CSS e do JS `20261117.jean-linha-usuario.1` (`base.html`, `login.html` e kit).
- Largura do menu lateral (`fix/largura-menu-fontes`; achado pela comparação antes × depois da Bateria SPO-AA+): era medida uma vez, logo depois das permissões, e saía com a fonte que estivesse carregada naquele instante. Com a Inter, a coluna ficava com 341 ou 357 px ao acaso (o certo é 360), e trocar a fonte no Personalizar não mudava a largura. Agora mede de novo quando as fontes terminam de carregar (`document.fonts.ready` e o evento `loadingdone`): 360 px em todas as cargas, e 344 px ao trocar para a Atkinson com a página aberta (igual a uma carga nova com a Atkinson). Comparação antes × depois em 5 telas e o kit × 3 vistas, 3 cargas: nenhuma diferença de estilo. `?v=` do JS `20261117.jean-largura-menu.1` (`base.html`).
- Bateria SPO-AA+ com tempos-limite (`fix/bateria-tempo-limite`; pedido do dev. Cleber depois de rodar o completo na homologação, onde travou uma vez em silêncio no Firefox, no passo do tema "Automático"): cada passo (tela, valor do Personalizar, checagem de recurso) tem limite de 180 s (`BATERIA_LIMITE_PASSO_S`) e, se estourar, falha com o que estava rodando, fecha o navegador e segue; um vigia para a bateria, grava o relatório e sai com código 1 se nenhum resultado aparecer por 6 min (`BATERIA_LIMITE_SEM_PROGRESSO_MIN`); limites também no Playwright (30 s nas ações, 60 s na navegação) e no pytest das travas (5 min). Conferido de propósito: com limite de 1 s, as 20 telas do básico falham com mensagem e a bateria chega ao fim (código 1); com um passo que nunca termina (`BATERIA_TESTE_TRAVA=1`), o vigia para em ~30 s, com o relatório. O primeiro teste do vigia achou que ele não podia estar solto do processo (`unref`): o Node saía com o código 13, sem relatório. Intermediário normal: 159/159. Só `tests/navegadores/` (e o README).
- **Scripts de banco:** nenhum. **Mudança no contrato da API com a automação:** nenhuma.

---

## 2026-11-17 — produção, RELEASE_2_2026_11_17.4

### Corrigido
- Menu lateral no celular (`fix/hover-toque`): o item tocado antes ficava destacado junto com o da tela atual (dois itens destacados), porque o navegador mantém o `:hover` no último toque. Em telas sem mouse (`@media (hover: none)`), o `:hover` dos itens do menu volta ao estado normal; a tela atual e o grupo aberto continuam destacados; no desktop nada muda. Medido no Playwright com toque (Chromium e WebKit): de 2 itens destacados para 1; com mouse (Chromium, Firefox e WebKit) o destaque continua. Regra nova no `padrao-visual-spo-aa-plus.md` §6.1. `?v=` do CSS `20261117.jean-hover-toque.1` (`base.html`, `login.html` e kit).
- Mockup "Programação PTA/OpR", Visão processual (`fix/hifenizacao-processos`): no celular os quadrinhos dos processos ficavam em duas colunas de 65 a 91 px e até 9 de 12 nomes saíam da caixa. Agora são duas colunas só quando cada uma tem ao menos ~9,5rem (desktop e iPad iguais), senão uma; hifenização em português e quebra forçada só como último recurso. Medido no Playwright (S26, Edge 50 Pro, iPhone 16, Firefox com fonte ampliada, desktop e iPad): de até 9 nomes saindo da caixa para 0. Só `static/pta2027_v63r/assets/spo-preferencias.css` (`?v=20261117.jean-hifenizacao-processos.1` no `index.html` do mockup).
- Lista nativa aberta e filtro SPO com a mesma linguagem (`fix/lista-nativa-visual`): no desktop com Chrome/Edge, a lista aberta de um `<select>` (ex.: Cadastrar Dotação) passa a ter a borda do filtro SPO dos relatórios (`--spo-control-border`, que segue o destaque); os dois usam a mesma sombra (`--shadow` de cada tema) e o mesmo destaque ao passar o mouse (12% do destaque; antes 16% na lista e 4–6% quase invisível no filtro). Medido no Chromium, claro e escuro. Firefox e Safari seguem com a lista do sistema. Regra "nativo × componente SPO" registrada no `padrao-visual-spo-aa-plus.md` §4.3. `?v=` do CSS `20261117.jean-lista-nativa.1` (`base.html`, `login.html` e kit).
- Alvos de toque com no mínimo 24 px (`fix/alvos-toque-24`; WCAG 2.5.8): as opções "Não/Sim" dos seis formulários com opções (Dotação, Estorno, Etapa, Meta Física, Subação, Notas SEE) tinham o rótulo com 18 px de altura e o círculo com 13 px; o link "Acessibilidade" do rodapé, 15 px. Agora o rótulo da opção tem ao menos 24 px (44 px em tela de toque) com o círculo de 18 px, e o link do rodapé ganha área de toque invisível sem mudar o desenho. Medido no Playwright (Chromium e WebKit no celular, Chromium e Firefox no desktop): de 3 alvos abaixo de 24 px para 0. Regra no `padrao-visual-spo-aa-plus.md` §6.1. `?v=` do CSS `20261117.jean-alvos-24.1` (`base.html`, `login.html` e kit).
- Cabeçalho da Programação PTA/OpR igual ao do SPO (`fix/menu-cabecalho-pta`; achados e desenho do dev. Jean no Chrome: Samsung A55 360 × 800, iPhone 16, Surface 10 960 × 1440). Ao programar um eixo, o cabeçalho do SPO dá lugar ao da tela (`special-context-focus`, contrato visual), que estava sem a lupa e o "?" e com o "›" do menu em cima do título no celular (a medida do logotipo, escondido, dava 0) e, no tablet, abaixo do cabeçalho, com um canto vazio acima da coluna do menu. Agora a lupa e o "?" do SPO entram nesse cabeçalho enquanto ele está na tela (os mesmos botões, que voltam ao sair; `moverAcoes` no bloco `initMenuCompacto`), lado a lado, à direita: no celular na linha de baixo, ao lado do usuário e do "Voltar" (o título fica com a largura inteira); do tablet para cima, depois da linha do usuário. A barra ficou mais baixa (pedido do dev. Jean): o "Voltar" tem 24 px visíveis com a área de toque de 44 px por fora (`::after`); A55 157 → 120 px, iPad/Surface 95 → 75 px. **Padrão SPO para telas em tela cheia** (pedido do dev. Jean): "barra de título da tela (secundária)" no `docs/padrao-visual-spo-aa-plus.md` (§4.4, regra no §6.1, kit no §6.4, nomes genéricos das classes na dívida do §7) e no kit, com um exemplo vivo numa moldura que roda o `style.css` e o `initMenuCompacto` do SPO (`tests/test_kit_mockup.py`: marcação recortada do template e o marcador novo). No kit, o conteúdo passa a usar a largura inteira no celular (sobravam 64 px à direita, resto da coluna do menu que ele não tem). O "›" fica no centro da coluna dos dois botões: no celular pelo `--spo-seta-topo`; com a coluna do menu à vista, ela passa a começar no alto da tela (`--spo-seta-foco`). Medido no Chromium (A55, deitado 852 × 393, Surface 10, desktop), WebKit (iPhone 16, iPad 11) e Firefox (fonte 18; desktop): centro do "›" = centro dos botões, nada embaixo do "›" nem dos botões, título inteiro, sem rolagem lateral, a lupa abre a busca, e ao voltar à cadeia os botões e o "›" voltam ao lugar. `?v=` do CSS e do JS: `20261117.jean-menu-cabecalho-pta.4`.
- Filtro SPO dos relatórios no desktop (`fix/filtro-desktop`; achado do dev. Jean no kit, medido também no FIP 613): o painel tinha 260 px fixos, opções sem quebra de linha e em negrito (herdado do cabeçalho da coluna); no FIP 613, 28 de 30 nomes de uma coluna ficavam cortados, e nas últimas colunas o painel passava até 110 px da área da tabela, escondendo o "Aplicar". Agora as opções ficam em peso normal e quebram linha (em todas as larguras), o painel cresce com os nomes até 420 px e, quando sairia da área visível, abre alinhado pela direita da coluna (bloco `[jean]` `initFiltroPosicao`, também rodado pelo kit; CSS no fim do `style.css`). Medido no kit e no FIP 613 com Chromium, Firefox e WebKit a 1366 × 768: nenhum nome cortado, "Aplicar" à vista em todas as colunas; celular sem mudança (cartão do #172, 6 perfis). Padrão §4.3. `?v=` do CSS e do JS: `20261117.jean-filtro-desktop.1`.
- Mockup PTA/OpR, rolagem com "Movimento: Automático" (`fix/rolagem-automatica`; achado do dev. Jean no Chrome, iPhone 16): em tela estreita, abrir os eixos de uma macropolítica levava a página **para cima** (762 px no iPhone; 148 px a 1100 × 650) e o cartão tocado saía da tela: a rolagem original do mockup centralizava (`block: "center"`) uma lista de eixos mais alta que a tela, dentro do iframe. Agora a rolagem suave só desce, até o cartão tocado ficar logo abaixo do cabeçalho do SPO com o painel embaixo (`rolarAteOCartao` no `spo-preferencias.js`); e toda rolagem logo depois de abrir espera a página crescer (o SPO aumenta o iframe um instante depois; perto do fim, a rolagem parava em 0, também com "Reduzir sempre"). Medido no Chromium, Firefox e WebKit (iPhone 16, 1100 × 650; Chromium também 1366 × 768 e 1060 × 1900), com o cartão no meio da tela: "Automático" em tela estreita desce 324 px (iPhone; cartão a 128 px do alto, painel a 273) e 252 px (1100 × 650); em tela larga ou alta, no máximo o começo do painel (0–34 px); "Reduzir sempre" sem rolagem, ou só até o começo do painel, de uma vez (0–40 px). Padrão §6.1. `index.html` do mockup: só o `?v=` do `spo-preferencias.js` (`20261117.jean-rolagem-automatica.1`).
- Bateria 4 entre navegadores (`fix/bateria-4`; pedido do dev. Jean, com o usuário temporário da bateria só no banco de desenvolvimento): todas as opções do Personalizar e os presets do Contrato no SPO e no kit, os recursos do lote 6 e a varredura de ~55 telas em Chromium, Firefox e WebKit (desktop, celulares e tablets). Achados corrigidos: (1) no kit, "Espaçamento entre linhas" e "Leitura" não valiam nas prévias do Personalizar (fora do `#content-area`): as regras passam a valer também em `.spo-area-leitura`, com a mesma especificidade; (2) o kit não marcava `data-forced-colors` no `<body>`; (3) no desktop, clicar fora não fechava o filtro no FIP 613, no PLAN 51 e nos demais relatórios abertos depois do primeiro (o "clicar fora" de cada relatório é registrado uma vez só, sob a marca global `multiFilterClickBound`): tratador geral no `initFiltroPosicao`; (4) as caixas de marcar com rótulo ao lado (Usuários: Cadastrar, Editar, Perfil, Acessos da API) tinham 18 px de altura no desktop: agora ≥ 24 px (44 no toque), como as opções Não/Sim (#169). Padrão §4.2, §4.3, §6.4. `?v=` do CSS e do JS: `20261117.jean-bateria-4.1`.

### Alterado
- Ajuda e busca no cabeçalho (`feat/ajuda-cabecalho`): a janela "Atalhos do teclado" virou **"Ajuda"**, com seções que se abrem (Buscar telas; Acessibilidade e preferências; Atalhos do teclado, fechada em tela de toque; Sobre e contato, com versão, ambiente e o e-mail do NGER). A **lupa ("Buscar tela") e o "?" ("Ajuda") saíram do menu lateral** e foram para o cabeçalho, numa coluna própria (no celular, em pé, à direita do título e da linha do usuário). Atalhos iguais (Ctrl+K e /; ?; Alt+M). O menu ficou com 2 itens a menos (de 13 para 11): a coluna de ícones parou de rolar por dentro no S26 e no Edge 50 Pro. Medido no Playwright (Chromium, Firefox e WebKit; desktop, celulares e iPad): Ajuda abre pelo botão e pela tecla, Tab preso na janela, Esc devolve o foco ao botão, lupa abre a busca, sem erros; cabeçalho igual ao de antes no desktop, no iPad, no Edge 50 Pro e no iPhone 16 (no S26, a 360 px, o título quebra em 3 linhas até a coluna de ícones sair do celular, no próximo tema). Padrão §4.4. `?v=` do CSS e do JS `20261117.jean-ajuda-cabecalho.3`.
- Menu no celular (`feat/menu-celular`): no celular (até 600 px de largura ou 560 px de altura) e sempre que a coluna de ícones não cabe na altura, a coluna some e fica só o "›" no canto do cabeçalho; o conteúdo ganha a largura inteira (S26: 296 → 360 px; Edge 50 Pro: 343 → 407). Um toque abre o menu inteiro por cima da tela, agora **opaco** (antes translúcido) e com a **altura da tela** (deitado e na tela externa do Razr: 3 → 6 itens visíveis, o resto rola). Fecha com toque fora, Esc ou ao escolher uma tela, como antes. O cabeçalho do S26 voltou a 118 px. Desenho aprovado pelo dev. Jean: o "›" no estilo dos botões redondos do SPO e **alinhado ao centro do logotipo** (diferença 0 px, também com fonte ampliada e deitado; o "‹" da gaveta aberta fica no mesmo lugar), e **a lupa e o "?" discretos** (só o ícone, na cor do texto secundário, contraste 9,29:1 no claro e 9,96:1 no escuro; área de toque igual). O menu aberto por cima da tela fica opaco também nos tablets. Desktop, iPad e Fold aberto com a coluna como antes. Medido no Playwright (Chromium, Firefox e WebKit; 10 perfis). Token novo `--spo-veu`. Padrão §4.4. `?v=` do CSS e do JS `20261117.jean-menu-celular.5`.
- Filtros dos relatórios no celular (`feat/filtro-celular`): até 600 px de largura ou 560 px de altura, o filtro aberto vira um cartão no alto da tela (16 px de margem, largura da tela, lista rolando por dentro, "Cancelar"/"Aplicar" sempre à vista, nomes longos quebrando linha), com um véu atrás que só fecha o filtro, sem acionar o que está embaixo. Antes o painel abria preso à coluna e saía cortado ou abaixo da borda, sem o "Aplicar" à vista (PED, FIP 613, PLAN 51; deitado, nas três). Medido no Playwright (Chromium, Firefox e WebKit; 5 perfis de celular × 3 telas): cabe e "Aplicar" tocável em todos; tocar sobre outro filtro só fecha o aberto. Desktop sem mudança. Padrão §4.3. `?v=` do CSS e do JS `20261117.jean-filtro-celular.1`.
- Mockup PTA/OpR, detalhe logo abaixo do cartão (`fix/detalhe-abaixo-cartao`): no Mapa da cadeia (grupos empilhados, até 1180 px) a lista de eixos abre logo depois da linha do cartão tocado, dentro da grade, e não mais no fim do grupo; na Visão processual, o detalhe do processo abre logo depois da linha do cartão (antes, depois dos três cartões). Distância entre o cartão e o detalhe: de até 481 px (S26; 508 px no Firefox com fonte ampliada) para 13 px no Mapa da cadeia, e de até 373 px para 10 px na Visão processual (Chromium, Firefox e WebKit; celulares e iPad). O "×" que fecha a Visão processual e a estratégica ganha o nome "Fechar" e fica na mesma linha do "Exportar diagrama PNG", com 44 px (antes quebrava sozinho para a linha de baixo, esticado). Desktop com os grupos lado a lado sem mudança. Só arquivos do mockup (`spo-preferencias.js`/`.css`, `?v=` `20261117.jean-detalhe-cartao.1` no `index.html` do mockup).
- **PROPOSTA ao dev. Cleber** — Início, cartão "Chave do planejamento" no lugar do Alerta (`feat/alerta-cartao`): a janela "Alerta" (#emp-record-modal), que abria sozinha ao entrar e cobria até 87% da tela no celular (41% no desktop), vira um cartão da Início, abaixo do "Bem-vindo": o sinal de alerta do Alerta (triângulo branco com borda vermelha, cores de placa fixas), placas dos dias no estilo SPO (algarismo na cor do texto), "N dias sem erros", recorde com o selo "recorde atual", as datas da última atualização e, no pé, "Colabore para melhorar este índice" com a placa "homens trabalhando" redesenhada em SVG (o JPG tinha cantos brancos no tema escuro). Mesmos dados (`emp_record`). Ao entrar, abrem só as pendências, como antes depois de fechar o Alerta (as pendências são o PR seguinte, também proposta). A grade da Início deixa de transbordar com menos de ~350 px de conteúdo (`minmax(min(320px, 100%), 1fr)`). Medido no Playwright (Chromium, Firefox e WebKit; 7 perfis). Se recusada, sai por um PR de reversão. `?v=` do CSS e do JS `20261117.jean-alerta-cartao.1`.
- **PROPOSTA ao dev. Cleber** — Início, pendências em cartão (`feat/pendencias-cartao`): a janela das pendências (#dotacao-aguardando-modal), que abria sozinha ao entrar, vira o cartão "Pendências" da Início, com a contagem por tipo (e os avisos de carga por UO em destaque, quando houver); só aparece quando há pendência. "Ver detalhes" abre o detalhe no cartão flutuante da busca e de "O padrão SPO-AA+" (fora do `.main`): um grupo recolhível por tipo com as mesmas tabelas (no celular, cada linha vira "rótulo: valor"), "Fechar" sempre à vista, Esc e toque fora fecham, o foco volta ao botão. Com o #174, nada mais abre sozinho na Início. Medido no Playwright (Chromium, Firefox e WebKit; 8 perfis). Teste novo `tests/test_inicio_avisos.py`; o `test_style_inline` baixa a referência da Início (o título "UO …" com `style=` saiu). Se recusada, sai por um PR de reversão. `?v=` do CSS e do JS `20261117.jean-pendencias-cartao.1`.
- Kit de mockup em dia com o modo móvel (`feat/kit-movel`): o kit deixa de guardar a margem de 64 px da coluna do menu (que ele não tem) e passa a ocupar a tela inteira no celular, sem cortes (S26, Edge 50 Pro com fonte ampliada, iPhone 16, iPad, desktop). Seção nova **"Seu aparelho"** (janela útil, área visível, tela, densidade, texto padrão e mínimo desenhados, toque, navegador, preferências do sistema e a faixa do SPO que vale na janela; botão "Copiar"; nada é enviado), para o teste com os colegas no celular real. No catálogo: cabeçalho com busca e Ajuda, lista nativa e filtro SPO (que abre de verdade, como cartão com véu no celular) e as opções "Não/Sim" de 24 px (recortadas do Cadastrar Dotação e conferidas pelo `test_kit_mockup`). Só arquivos do kit e o teste.
- Declaração de acessibilidade e padrão SPO-AA+ em dia (`fix/declaracao-acessibilidade`). Documento técnico (`docs/padrao-visual-spo-aa-plus.md`) na **versão 5**: metas de interrupções (2.2.4), ordem (1.3.2, 2.4.3) e navegadores e aparelhos (§1); avisos da Início e detalhe junto do cartão (§4.4); bateria entre navegadores (§5, item 15); regras "nada abre sozinho" e "linha do cartão" (§6.1); kit no celular e "Seu aparelho" (§6.4). Declaração, no Personalizar: "Atendido" passa a dizer alvos de 24 px em tudo e 44 px no toque (2.5.8; meta 2.5.5), e ganha a ordem de leitura e de foco (1.3.2, 2.4.3), "nada abre sozinho por cima da tela" (2.2.4, AAA; vale enquanto as propostas #174/#175 ficarem) e o modo móvel dos filtros e do menu (1.4.10); em "Parcial", o leitor de tela continua em avaliação, mas o visual já é medido no Chrome/Edge, Firefox e Safari, e os celulares reais entram como item próprio; "Como foi avaliado" ganha a bateria de 17/11 (170 telas, três motores, celulares e tablets, fonte ampliada). O resumo de "O padrão SPO-AA+" ganha "nada abre sozinho" e os três navegadores. Âncoras das WCAG conferidas na tradução do W3C Brasil. `?v=` do JS `20261117.jean-declaracao.1`.
- Busca nas listas longas dos formulários (`feat/busca-listas-longas`; Etapa 5 do plano de readequação): toda lista de uma escolha com **40 opções ou mais** ganha logo abaixo o campo "Filtrar a lista" (bloco `[jean]` `initBuscaListasLongas`, sem mexer nos templates). Hoje: Chave do Planejamento (296 opções), Subação/Entrega (470), Etapa (438) e Subelemento (51) no Cadastrar Dotação, e Subação (470) na Etapa. O `<select>` continua sendo o campo (valor, cascatas e envio iguais; o filtro só tira opções da lista, que voltam ao escolher, ao sair, com Esc e no `reset`); Enter escolhe quando sobra uma, ↓ abre a lista filtrada, contagem "12 de 296" anunciada; o texto escolhido que não cabe na lista fechada aparece inteiro logo abaixo. Medido no Playwright: em 34 telas, escolher pela busca dá a mesma cascata que escolher direto (Chromium, Firefox, WebKit no iPhone), sem erros; Cadastrar Dotação nos 10 perfis sem rolagem lateral nem corte. Kit: exemplo "Lista longa com busca" rodando o mesmo bloco; padrão §4.3 com o funcionamento; declaração: o item "listas longas cortadas no celular" sai de "Parcial". `?v=` do CSS e do JS `20261117.jean-busca-listas.1`.
- Kit de mockup no Contrato Visual Protegido (`feat/kit-no-contrato`; pedido do dev. Jean: o kit só abria pelo endereço, e o dev. Cleber ainda não o tinha visto): cartão "Kit de mockup SPO-AA+" logo abaixo do título do contrato, com o que o kit tem, como testar e como usar numa tela nova (link para o documento técnico do padrão), e o botão principal "Abrir o kit de mockup", que abre numa aba nova. Medido no Chromium e Firefox (1366), WebKit (iPad, iPhone 16), Chromium S26 e Firefox com fonte 18: cabe, nada cortado, sem rolagem lateral, botão abre o kit; contraste ≥ 7:1 no claro e no escuro (destaque Amarelo). Padrão §6.4. `?v=` do CSS e do JS: `20261117.jean-kit-no-contrato.1`.
- Contrato Visual Protegido sem a nota repetida (`fix/contrato-nota-repetida`; achado do dev. Jean): a linha "Regras de acessibilidade e de visual que todo preset respeita: documento técnico do padrão SPO-AA+" saiu, porque o cartão do kit (#182) já leva ao mesmo documento; saiu junto a regra de CSS dela (`.contract-padrao-note`). O documento técnico continua abrindo pelo Contrato, pelo link do cartão. Padrão §5.1. `?v=` do CSS: `20261117.jean-contrato-nota.1`.
- Bateria SPO-AA+ no repositório (`feat/bateria-spo-aa-plus`; pedido do dev. Jean): `tests/navegadores/` junta as baterias 1 a 4 do AA+ (contraste, tipografia, Personalizar, modo móvel, equivalência SPO × kit) numa ferramenta Playwright em três níveis — básico (~3 min, a cada PR), intermediário (~7 min, a cada lote) e completo (~30 min, antes de enviar um lote) —, com relatório em Markdown e JSON e saída com código 1 se houver falha. Verificações: travas (pytest), telas, contraste (AA reprova, AA+ avisa), equivalência e recursos-chave. Usa um usuário de teste do banco de desenvolvimento (credenciais em `.env.bateria` ou variáveis de ambiente, fora do git); só lê e toca. Dependência só da pasta: `playwright` 1.64.1 (`package.json` e `package-lock.json` próprios; `node_modules/`, `sessoes/` e `resultados/` ignorados). Padrão §5 (item 16) e kit ("Como usar"). **Achado da própria bateria, corrigido:** com o menu aberto por cima da tela (celular ou gaveta até 1200 px), o Esc só fechava com o foco dentro dele, e no Safari tocar no "›" não leva o foco ao botão (iPad/iPhone com teclado: o Esc não fazia nada); agora fecha esteja o foco onde estiver, se nenhuma janela estiver aberta por cima (`initMenuCompacto`). `?v=` do `main.js`: `20261117.jean-bateria-aa-plus.1`.
- Bateria SPO-AA+ como passo oficial (`docs/bateria-no-padrao`): padrão SPO-AA+ na versão 5.1 — checklist de tela nova (§6.1) com o nível de bateria de cada momento (básico a cada PR, intermediário a cada lote, completo antes de enviar um lote); §5 item 15 aponta a bateria oficial (item 16) e deixa a pasta local como laboratório; §8 com a versão 5.1. Declaração de acessibilidade (Personalizar), "Como foi avaliado": desde 17/11 cada mudança passa pela Bateria SPO-AA+ (660 verificações no nível completo). Só texto.

- **Scripts de banco:** nenhum. **Mudança no contrato da API com a automação:** nenhuma.

---

## 2026-11-17 — dev/cleber, RELEASE_2_2026_11_17.1

### Alterado
- Ajustes nas regras da seção 40 do `claude.md` (`fix/docs-regras-40-ajustes`), por sugestão do dev. Jean: a 40.2 aponta para a 40.14 (todo tema registra entrada no CHANGELOG; ao `claude.md` só vai o que continua valendo); a 40.13 vale para os bancos dos dois ambientes; a 40.14 descreve os formatos do cabeçalho da entrada; a 40.9 diz que o N soma `dev/cleber`, `homologacao` e `main`; a 40.8 define o único conflito liberado (linha do `RELEASE`), e a 40.1 e a 40.7 foram ajustadas para não contradizê-la.
- Cabeçalho do `claude.md` com a data de 17/11/2026. Só documentação e o `RELEASE` do rodapé; nenhum código.
- **Scripts de banco:** nenhum. **Mudança no contrato da API com a automação:** nenhuma.

---

## 2026-11-16 — dev/cleber, ainda não publicado (RELEASE_2_2026_11_16.7)

### Alterado
- Reorganização da documentação (`fix/docs-changelog-secoes-41`): o histórico dos lotes do dev. Jean (antigas 41.1 a 41.101 do `claude.md`) foi movido, **sem reescrever**, para a seção "Histórico do desenvolvimento do AA+" deste arquivo, agrupado por data e com o número antigo em cada título. A seção 41 do `claude.md` passou de 1.898 para cerca de 630 linhas e ficou só com a regra, o contrato e a instrução em vigor (3 subseções inteiras e 44 mistas). Nova regra de contribuição: seção 40.14 do `claude.md`.
- Cabeçalho do `claude.md` com o aviso da reorganização. Só documentação e o `RELEASE` do rodapé; nenhum código.
- **Scripts de banco:** nenhum. **Mudança no contrato da API com a automação:** nenhuma.

---

## 2026-11-16 — dev/cleber, ainda não publicado (RELEASE_2_2026_11_16.6)

### Corrigido
- Avisos modais com fonte maior (dev. Cleber, `fix/modal-pendencias-fonte-maior`): o botão "Fechar" do modal de pendências sumia no celular com "Fonte maior" no Personalizar SPO, porque o zoom da fonte multiplicava a altura do aviso. O botão de fechar fica visível em qualquer tamanho de fonte, no celular e no computador; fonte normal sem mudança — seção "Avisos modais com fonte maior" do [claude.md](claude.md) e `tests/test_modal_zoom.py`.

---

## 2026-11-16 — Publicação em produção (RELEASE_2_2026_11_16.4)

Promoção da homologação para a `main` (PR #156): AA+ lotes 1 a 5 (PRs #65, #68, #153 e #154), avisos modais e Teto Orçamentário no celular (#154 e #155). Sincronização da `main` com a `dev/cleber` (#157) e com a `dev/jean` (#158).

### Adicionado
- AA+ (acessibilidade), do dev. Jean, lotes 1 a 5 (#65, #68, #153, #154): fonte e fundo novos (Inter na interface, Intel One Mono nos dados), Personalizar Temas SPO (fontes, densidade do texto, espaçamento entre linhas, "Leitura", cores seguras para daltonismo, foco, brilho e movimento, ✓ na cor de destaque), contraste AA/AA+, alvos de toque de 44 px, gráficos acessíveis (inclusive a linha separadora, #152) — antigas seções 41.1 a 41.99 (a parte em vigor está na seção 41 do `claude.md`; o histórico, na seção "Histórico do desenvolvimento do AA+" deste arquivo) e `padrao-visual-spo-aa-plus.md`.
- Atalhos do teclado: busca de telas (Ctrl+K ou "/"), ajuda ("?"), Alt+M para o menu e setas no menu e nas tabelas (#153) — antigas 41.25, 41.60, 41.64, 41.65 e 41.92 (regra em vigor na seção 41 do `claude.md`; histórico na seção "Histórico do desenvolvimento do AA+" deste arquivo).
- Declaração de acessibilidade "SPO-AA+" no Personalizar, com link no rodapé (#153) — antigas 41.63 e 41.66 (regra em vigor na seção 41 do `claude.md`; histórico na seção "Histórico do desenvolvimento do AA+" deste arquivo).
- Rota `GET /api/documentos/<nome>` (#112, entrou no #153): exige login, lista fechada de documentos (hoje só o do padrão SPO-AA+) e lê só dentro de `docs/`.
- Kit de mockup em `/static/kit/` (#139, entrou no #153): **sem login também na produção**, sem dado real; só lê o `main.js` do SPO.
- `scripts/gerar_escala_pta2027.py` (#114, entrou no #153): gerador manual de CSS do mockup PTA/OpR, sem banco, rede ou deploy.
- Testes que viram regra (#153): `test_tamanho_fonte`, `test_cor_literal`, `test_style_inline` e `test_kit_mockup` (seção 40.12 do `claude.md`).

### Alterado
- Login (#61 e #80, entraram no #65 e no #153): com sessão já aberta a página mostra um aviso e **a 2ª tentativa de entrar encerra a outra sessão** (antes, janela de confirmação do navegador). A rota `/login`, o 409 e o `force_login` são os mesmos.
- Nome da tela "Programação PTA 2027" para **"Programação PTA/OpR"** em `services/features.py`, com o mesmo id — exceção à regra 40.2 (#153).
- Avisos modais no celular (#154, dev. Cleber): cartão com margem, altura visível, "×" e "Fechar" sempre visíveis; desktop sem mudança — seção 41.100.
- Teto Orçamentário, aba Tabelas, no celular (#155, dev. Cleber): rótulo acima dos valores e ranking com o nome inteiro; desktop sem mudança — seção 41.101.
- **Aviso à automação:** contrato da API sem mudança (rotas de upload, Reprocessar e Cancelar, `/status`, estados dos jobs, 409 e 503 iguais). O login pela tela passa a encerrar a outra sessão na 2ª tentativa; a automação deve usar **usuário exclusivo**.

### Corrigido
- Notas SEE: reenviar as notas do 4º bimestre num **processamento novo** (o reconhecimento do DANFE novo só vale para processamentos novos). Das 647 notas, 627 identificam a escola, 13 são entrega na DRE e **7 ficam para o fiscal** (1 sugestão e 6 não encontradas) — seção 42.

### Não entrou
- UOs novas (26101, 26201 e 26202): seguem bloqueadas, `services/uo.py` igual ao da `main` (só SEDUC).
- Arquivos próprios da homologação (`.cpanel.yml` e `scripts/deploy_cpanel.sh`) e o commit de sincronização `3374e53`.

---

## 2026-11-13

### Corrigido
- Notas SEE: identifica a escola no DANFE novo da Somos (Solicitação e endereço de entrega) (#64) — seção 42.
- `claude.md`: referências da seção 31, detalhes do Notas SEE e nota da seção 29 (#48).

---

## Histórico do desenvolvimento do AA+ e dos avisos modais (antiga seção 41 do claude.md)

Estas entradas vieram da seção 41 do [claude.md](claude.md), reorganizada em 16/11/2026. O texto foi movido **sem reescrever**; cada título leva o número antigo ("antiga 41.N"). Está agrupado pela **data de desenvolvimento** (a data que consta no título de cada subseção), da mais nova para a mais antiga. Nas subseções **mistas**, a parte que ainda vale (regra, contrato, instrução) continua no claude.md; aqui está o resto. Os PRs aparecem no próprio texto de cada subseção, quando ela os cita.

Abertura original da seção 41 (mantida): As subseções vêm dos PRs #31 a #63 da `dev/jean`, na ordem em que entraram.

### 2026-11-16 — antigas 41.98 a 41.101

#### ✓ na cor de destaque ativa (dev. Jean, 2026-11-16) (antiga 41.98)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.98)._

Gate C10 do AA+ v3.1 do laboratório, com o desenho confirmado na comparação visual de 16/11 (o laboratório reabilitado por cópia tinha o ✓). No Personalizar, a amostra da cor escolhida tinha um anel na **própria cor da amostra**: nas cores claras ele ficava abaixo de 3:1 com o cartão (5 de 16 combinações; Amarelo no claro 1,47:1), e a escolha dependia só da cor (WCAG 1.4.1 e 1.4.11).
- Medido nos 8 destaques × 2 temas (marcando a amostra só na página, com as transições desligadas): **✓ a 16,86:1 (claro) e 14,26:1 (escuro)** em todos; anel de 1,25 a 4,19:1 e borda do selo de 1,36 a 1,43:1, suaves de propósito (decorativos). Antes, o anel era a única marca, de 1,47 a 6,05:1. O kit de mockup mostra o mesmo (usa o `style.css`).
- `?v=` do CSS: `20261116.jean-cor-ativa-check.3` (`base.html`, `login.html` e o kit de mockup).
- Junto, no ambiente local do dev. Jean (sem código): a etiqueta de ambiente voltou ao topo com `APP_ENV_LABEL`/`APP_ENV_CLASS` no `.env` local — o mecanismo do `app.py` já existia.

#### Linha separadora nos gráficos (dev. Jean, 2026-11-16) (antiga 41.99)

Vinda da comparação visual com o laboratório v4.3.1 (16/11): lá as fatias, os blocos e as barras tinham um vão entre si, nos dois temas. No Teto, os grupos 1 e 4 da paleta clara são dois azuis; sem o vão, a pizza e as barras viravam um bloco só (duas cores vizinhas abaixo de 3:1 entre si, WCAG 1.4.11).
- `initGraficosEscala` (`main.js`), função `comSeparador`: pizza, árvore (treemap), sunburst e barras ganham `marker.line` de **1 px** no modo normal (claro e escuro) e de **2 px com os diagramas em alto contraste** (`highLight`/`highDark`, também no automático quando o sistema pede mais contraste) **ou com o "Contraste: Reforçado"** (`data-visual-contrast="enhanced"`), na **cor do fundo do próprio gráfico**. Trocar o contraste redesenha os gráficos abertos (o observador que já redesenhava na troca de densidade passou a olhar também o `data-visual-contrast`). Uma primeira versão com 2 px nas fatias e blocos ficou mais pesada que a do laboratório (conferência do dev. Jean). O fundo vem do layout (`plot_bgcolor` nas barras, `paper_bgcolor` nos outros) e, quando ele é transparente (caso do Teto), do primeiro ancestral com fundo pintado (o quadro `.teto-chart`). Assim acompanha o tema dos diagramas, que pode estar no modo oposto ao da página. O `react`/`newPlot` envolvidos passaram a montar o layout antes dos dados, para o separador ler o fundo.
- Linhas, pontos e cascata (PAOE) ficam sem: as barras do cascata já não se tocam.
- Com as **cores seguras para daltonismo** continua o contorno na cor do texto (`comPadroes`), mais forte.
- Conferido no kit de mockup, nos temas claro e escuro e com os diagramas claro, escuro e em alto contraste (inclusive no modo oposto ao tema): a linha tem exatamente a cor do quadro nas 8 combinações (2 temas × 4 modos dos diagramas), com 2 px só em alto contraste; e nos 3 níveis de contraste × 2 temas, com 2 px só no Reforçado. No SPO real, os 5 gráficos do Teto Orçamentário (pizza, árvore, dois de barras e o cascata). Padrão §3.5: item novo.
- `?v=` do `main.js`: `20261116.jean-graficos-separador.4` (`base.html`). CSS sem mudança.

#### Avisos modais no celular: cartão com margem (dev. Cleber, 2026-11-16) (antiga 41.100)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.100)._

**Problema:** no celular (Moto Edge 50 Fusion, Firefox no Android), na homologação, os avisos modais ocupavam a tela inteira e o "×" de fechar ficava fora da área visível: não dava para fechar o "Alerta" da chave de planejamento na Início. A busca de telas e a ajuda dos atalhos já tinham sido resolvidas no #124 (cartões com margem), mas os outros avisos ficaram de fora.

**Causa** (medida no navegador do app a 360 × 740 e 390 × 844):
- Todo aviso usa o mesmo mecanismo: `.modal-overlay` (`position: fixed; inset: 0`, centralizado) com um cartão dentro, e `prepararModais` (`main.js`) cuida de foco, Tab e Esc.
- O **Alerta da Início não usa `.modal-card`**, e sim `.emp-record-card`, que só tinha largura: sem altura máxima e sem rolagem. O "×" é `position: absolute` no topo do cartão. A 360 × 740 o cartão tinha 779 px: começava em −19 px e o "×" ficava em −8 a 20 px, cortado; o overlay não rola, então não havia como alcançá-lo (só o Esc fechava). O #66 só tinha deixado o "×" redondo e com área de toque de 44 px.
- O `.modal-card` comum tinha `max-height: 85vh` e rolava o cartão inteiro: cabeçalho e rodapé rolavam junto, e o "Fechar" ficava no fim da rolagem (a 390 × 844, nas pendências da Início, a 1.133 px dentro de um cartão de 717 px). Os avisos desse tipo não têm "×". Além disso `vh` não acompanha a barra de endereço do navegador do celular (hipótese: o navegador do app não tem barra dinâmica, então isso fica a confirmar no aparelho).
- O CSS não tinha `dvh`, `svh` nem `env(safe-area-inset-*)`.

Sem script de banco e sem mudança de back-end. `templates/base.html`, `templates/login.html` e `static/kit/mockup-spo-aa-plus.html`: só o `?v=` do CSS, agora `20261116.cleber-modais-celular.1` nos três (há teste que reprova se divergirem). O `RELEASE` continua `RELEASE_2_2026_11_16.2` (ainda não publicado). Esta correção é feita em cima do lote 5 por exceção combinada (a `dev/cleber` ainda não tem o AA+) e chega à `dev/cleber` e à `dev/jean` depois, pela sincronização da `main`.

#### Teto Orçamentário no celular: tabelas sem corte (dev. Cleber, 2026-11-16) (antiga 41.101)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.101)._

**Problema** (Android, ~412 px, na homologação; Painéis/Dashboards → Teto Orçamentário → aba "Tabelas"):
1. "Grupo 1 por Fontes" e "Grupo por Teto Total": a coluna fixa do grupo ocupava mais da metade da largura, os valores dos exercícios apareciam cortados ("R$ 4.", ".320.9") e a rolagem lateral não chegava ao fim da coluna.
2. "Teto por grupo e tipificação da despesa" (e "Teto por fonte de recurso"): o nome do grupo sumia ("3 C", "4 Ir") e a tipificação ficava espremida contra a barra.
3. "Quadro comparativo de Fonte/Grupo" (e o QOMP): as colunas dos exercícios não apareciam e o código da fonte saía cortado ("1540107" em vez de "15401070"), isto é, um número errado na tela.

**Causa** (medida no navegador do app a 360 × 740 e 412 × 915; o cartão tem 230 e 282 px de largura útil):
- **Quadros por exercício:** as colunas fixas (`position: sticky`) somavam 190 px (só Grupo) ou 260 px (Fonte 70 + Grupo 190), de 282 px, e ficavam por cima das colunas dos exercícios (184 px cada). Na rolagem até o fim, a última coluna ocupava de 98 a 282 px e a coluna fixa cobria de 0 a 190 px: só 92 dos 184 px apareciam. Para uma coluna fixa deixar a última coluna inteira, a conta é `coluna fixa + coluna do exercício ≤ largura do cartão`; a 360 px (230 px) isso dá menos de 58 px de coluna fixa, que não comporta nem o código da fonte.
- **Código da fonte:** a coluna da Fonte tinha 70 px fixos (50 px de conteúdo), e o código de 8 dígitos precisa de 49 px (Intel One Mono, Compacto) a 67 px (Inter, Legível). Passando da coluna, o final ficava por baixo da coluna do Grupo (também fixa e mais alta). A fonte mais larga do AA+ agrava, mas a causa é a largura fixa.
- **Ranking:** `.teto-rank-row` tinha 3 colunas (rótulo, barra de 70 px no mínimo, valor de 90 px no mínimo e até 147 px); o rótulo ficava com 25 a 67 px, e `.teto-rank-name` tem `overflow: hidden; text-overflow: ellipsis; white-space: nowrap`.
- Os outros cartões da aba (inclusive "Por grupo de despesa") não tinham corte.

Medido nas 6 combinações de densidade e fonte, a 360 × 740, 412 × 915 e 740 × 360: nenhum texto cortado, nenhum rótulo fora do cartão, última coluna inteira, valores dentro do cartão.

Sem script de banco e sem mudança de back-end. `templates/base.html`, `templates/login.html` e `static/kit/mockup-spo-aa-plus.html`: só o `?v=` do CSS, agora `20261116.cleber-teto-celular.1` nos três (há teste que reprova se divergirem). `RELEASE_2_2026_11_16.3`.

### 2026-11-15 — antigas 41.72 a 41.97

#### Sugestão "Leitura confortável" ao escolher a Atkinson (dev. Jean, 2026-11-15) (antiga 41.72)

Pedido do dev. Jean: quem escolhe a Atkinson Hyperlegible tende a precisar também de texto maior, linhas mais espaçadas e linhas mais curtas (o site da fonte a mostra em tamanho grande). Decidido com ele: **sugerir, não mudar sozinho** (cada opção do Personalizar faz uma coisa só; e há quem escolha a Atkinson só pelas letras distintas, sem precisar de texto maior).

- **Comportamento** (bloco `[jean]` `initSugestaoAtkinson` no fim do `main.js`): ao escolher a Atkinson na "Fonte da interface", aparece abaixo dela a oferta de aplicar também **texto Grande (112%)**, **entrelinha Ampla (1,75)** e **leitura Intermediária (72ch)**:
  - **"Aplicar as três":** sobe só o que estiver abaixo do sugerido (quem já usa 125% continua em 125%), clicando nos próprios botões do cartão (grava e aplica pelo mesmo caminho); o aviso do cartão diz o que mudou; o foco volta para "Fonte da interface";
  - **"Agora não":** só fecha a sugestão desta vez;
  - aparece **toda vez** que a Atkinson é escolhida (inclusive depois de trocar de fonte, de "Restaurar padrão" ou de recarregar), exceto se as três opções já estiverem no sugerido ou acima; some se outra fonte for escolhida. (A primeira versão lembrava o "Agora não" no navegador e não oferecia mais; o dev. Jean pediu que oferecesse sempre.)
  - anunciada ao leitor de tela (`role="status"`), sem tirar o foco de onde a pessoa está.
- **Verificação** (SPO local, preferências do dev. Jean guardadas antes e devolvidas depois): aparece abaixo da fonte; aplicar grava 112%/1,75/72ch e mostra o aviso; reaparece depois de "Agora não", de trocar de fonte e de recarregar; não aparece com as três já aplicadas; com 125% já escolhido, não reduz (só entrelinha e leitura sobem); `pytest` completo duas vezes seguidas e `node --check`.

Sem script de banco. `templates/base.html` e `login.html`: só as linhas `?v=` (CSS `?v=20261115.jean-atkinson.1`, JS `?v=20261115.jean-atkinson.2`).

#### Alto contraste escuro sem azul-marinho também no quadro dos gráficos (dev. Jean, 2026-11-15) (antiga 41.73)

Relatado pelo dev. Jean: no tema escuro com "Diagramas: alto contraste", o gráfico "Teto por grupo de despesa" voltou a aparecer sobre azul-marinho, depois da correção do #100.

- **Causa:** o #100 deixou transparente o fundo do gráfico Plotly no modo `highDark`, mas o **quadro** em volta (`.teto-chart`) pinta `--spo-diagram-bg`, que nesse modo ainda era `#111827`. Com o gráfico transparente, aparecia o azul do quadro.
- **Correção** (bloco `[jean]` no fim do `style.css`), com a mesma decisão do #100: no `highDark`, `--spo-diagram-bg` = fundo do cartão (`--card`, `#242424`), `--spo-diagram-node-bg` = cinza neutro do escuro comum (`#262626`, nós do Mapa Conceitual e afins) e a prévia do Personalizar igual. Cores das séries, texto e bordas do alto contraste não mudam.
- **Verificação** (SPO local, Teto Orçamentário, modo aplicado só na página): quadro do gráfico com a mesma cor do cartão (`rgb(36, 36, 36)`); texto do alto contraste 14,8:1; `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `login.html`: só a linha `?v=` do CSS (`?v=20261115.jean-ac-quadro.1`).

#### Bateria 2 do Personalizar: correções e documentação técnica conferida (dev. Jean, 2026-11-15) (antiga 41.74)

Pedido do dev. Jean: depois de reaparecer um defeito já corrigido (o azul-marinho do alto contraste escuro, #119), repetir a bateria completa do Personalizar em todas as telas, de forma automatizada, corrigir o que fosse achado e conferir a documentação técnica.

- **Bateria** (roteiro local, fora do Git; método no padrão §5, itens 12 e 13):
  - **Fase 2, telas:** 66 telas do menu × 4 perfis (padrão; escuro + Amarelo + Reforçado + diagramas em alto contraste + foco reforçado; acessibilidade máxima com brilho reduzido; denso com Roxo) × 3 larguras (1366, 1080, 390) = 792 medições, uma tela por vez, a mesma tela nas três larguras; mede rolagem lateral, texto cortado, contraste (AA e 7:1), fundos azul-marinho, gráficos desenhados e erros no console. Duas rodadas completas (a segunda compondo a **opacidade**, que a primeira não via) e uma conferência das 24 telas com achados.
  - **Fase 1, opções:** as 33 opções de "Fontes e tamanho do texto", "Visão e cores" e "Teclado" e as 14 das preferências comuns, clicadas pela tela: gravadas e aplicadas; sugestão da Atkinson aparece toda vez; prévias acompanham daltonismo e fontes. Também: busca de telas, ajuda dos atalhos, janela do padrão, link "Acessibilidade", "Ir para o menu", rota do documento técnico e o mockup PTA/OpR por dentro (brilho reduzido).
  - **Sem achado:** nenhum texto abaixo do AA no fim, nenhum azul-marinho (a correção do #119 vale em todas as telas), nenhum gráfico sem desenhar, nenhum texto cortado, nenhum erro no console.
- **Correções** (blocos `[jean]` no fim do `style.css`; uma regra no `spo-preferencias.css` do mockup):
  1. **Brilho reduzido, texto secundário:** `--muted` de `#4a4850` para `#3e3c44` (painéis semitransparentes mostravam o fundo `#dadee4`: 6,3–6,98:1 → ≥ 7,58); com o Reforçado, volta a valer `#38363d` (o brilho reduzido o anulava).
  2. **Brilho reduzido, "Dispensar":** vermelho `#861523` (era `#a51d2d`, 6,42:1 no cartão `#eceef1`); também no "×" do mockup.
  3. **Início:** texto e dica de boas-vindas (cartão de degradê transparente; `--muted` e `opacity .7`) passam ao texto principal, sem opacidade.
  4. **Contrato Visual Protegido:** `<em>` das prévias no texto principal (escuro + Amarelo: 6,65 → 7,98); pílula de versão sem `opacity .72` (4,26:1).
  5. **Notas SEE:** opções de envio indisponíveis no estilo de desabilitado do padrão (§3.4) em vez de `opacity .45` (2,2:1 → 5,15 claro / 6,53 escuro).
  6. **Diagramas claros:** texto secundário dos nós (`--spo-diagram-muted`) `#3e3d44`.
  7. **Programação PTA/OpR (mockup), achados do dev. Jean na conferência:** o indicador de avanço (círculo fixo de 68/62 px) ficava menor que "concluído" na escala de 12 px; agora acompanha o texto (`fit-content` + `aspect-ratio: 1`, mínimo 68 px; ~80 px, folga de 1,5 px nas três larguras e nas cinco fontes). Texto do cartão inicial: "o valor que a **política** pretende gerar" (era "a SEDUC").
  8. **Prévia "Fontes e tabelas":** a legenda da tabela quebra antes de "(segue \"Texto das tabelas\")".
  9. **Documento técnico do SPO-AA+ sai da janela da declaração e vai para o Contrato Visual Protegido** (pedido do dev. Jean): a janela aberta por "SPO-AA+" mostra só o resumo; o Contrato ganhou a linha "Regras de acessibilidade e de visual que todo preset respeita: documento técnico do padrão SPO-AA+" (`data-spo-padrao="tecnico"`, link no destaque e sublinhado), que abre a mesma janela já no documento. Sem o botão de alternar entre as vistas.
  10. **Atkinson Hyperlegible Mono** (pedido do dev. Jean): nova opção em "Fonte de tabelas e dados" — a monoespaçada da família Atkinson (Braille Institute, OFL 1.1, 17,7 KB, baixada do pacote `@fontsource-variable/atkinson-hyperlegible-mono@5.3.0` como as demais; `static/fonts/LEIA-ME.md`). Mesmos alvos da Intel One Mono (tabelas, totais, KPIs e valores do Teto), também no login (`login-theme.js`) e no mockup PTA/OpR. A sugestão "Leitura confortável" (#118) passa a oferecer a combinação inteira — Atkinson Mono nos dados + texto Grande + entrelinha Ampla + leitura 72ch — listando só o que falta ("Aplicar", "Aplicar as duas"… "Aplicar as quatro"). Medida em 22 telas com tabelas × 3 larguras com Atkinson + Atkinson Mono + 150% + entrelinha 2,0 (sem rolagem lateral, corte, contraste abaixo de 7:1 nem erro no console).
- **Não reproduzido:** Processos, Riscos e Projetos rolou para o lado a 390 px numa medição da primeira rodada (perfil de acessibilidade máxima); não se repetiu em quatro tentativas nem nas rodadas seguintes.
- **Documentação técnica** (`docs/padrao-visual-spo-aa-plus.md`, **versão 3**): fundo do alto contraste escuro no gráfico e no quadro (§3.5), prévias e sugestão da Atkinson (§4.4), valores do brilho reduzido (§2.1), desabilitado das Notas SEE (§3.4), texto secundário dos nós (§3.5), método da bateria e lições de opacidade e contêineres (§5), dois itens novos no checklist (§6.1), dívida e histórico (§7, §8). A declaração de acessibilidade diz que a bateria foi repetida em 15/11.
- `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `login.html`: só as linhas `?v=` (CSS `20261115.jean-atkinson-mono.1` nos dois; JS `20261115.jean-atkinson-mono.1` no `base.html`; `login-theme.js` `20261115.jean-atkinson-mono.1` no `login.html`); `index.html` do mockup: o `?v=` do `spo-preferencias.css` (`20261115.jean-atkinson-mono.1`) e o texto do cartão inicial.

#### Movimento reduzido: rolagem, cartões parados e aberturas do mockup PTA/OpR (dev. Jean, 2026-11-15) (antiga 41.75)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.75)._

Pedido do dev. Jean: a opção "Movimento" (Personalizar → Visão e cores) parecia não ter efeito. Avaliação: ela funciona, mas o SPO tem pouco movimento — indicadores de carregamento, abertura do menu lateral, rolagem suave e transições curtas de cor —, então o efeito é discreto; a descrição da opção fica como está (vale para animações que vierem com painéis novos). Dois pontos escapavam:
- **Chegada com esmaecimento** (pedido do dev. Jean depois de testar: o salto imediato trocava a tela de uma vez, como um "flash", ao abrir os eixos de uma macropolítica e ao ir para o "Direcionamento estratégico"): com "Reduzir sempre" (ou o sistema pedindo), a página continua indo direto ao destino e a área de chegada esmaece (opacidade 0,35 → 1 em 0,22 s, Web Animations, sem deslocamento) — como o "Reduzir movimento" do iPhone/Mac. Esmaece o alvo do `scrollIntoView`, ou a área de conteúdo (`#content-area`) quando a página inteira rola por `scrollTo`; nada esmaece se a página não se moveu. Mesmo tratamento no mockup.
- **Cartões que sobem 1–3 px ao passar o mouse** (notícias da Início, opções de envio das Notas SEE, "Entrar" do login, presets do Contrato Visual, nós dos diagramas; no mockup, macropolíticas, pilares, nós da EAP e políticas): com a transição desligada o salto continuava; agora ficam parados (borda, fundo e sombra continuam marcando).

- **Aberturas do mockup PTA/OpR** (testes do dev. Jean em tela baixa, "paisagem"), só no `spo-preferencias.js`/`.css` do mockup:
  - **Eixos junto da macropolítica (nos dois modos):** com os grupos A/B/C um embaixo do outro (até 1180 px), a lista de eixos abria no fim do último grupo, até ~1000 px abaixo do cartão clicado. Agora `#policy-expansion` vai para logo depois do grupo do cartão (`expandMacro` envolvida); antes do `renderMacros` (que redesenha os grupos e apagaria a caixa) ela volta ao lugar original. Em tela larga, com os grupos lado a lado, nada muda.
  - **Rolagem contida (só com "Reduzir sempre"):** ao abrir eixos ou metas de um pilar, o mockup rolava o painel inteiro para a tela (`block: "center"`/`"nearest"`), até 275 px em tela baixa, e cada rolagem esmaecia o painel (a "cintilância" do grupo C, cuja lista é maior que a tela). Agora a página não rola se o começo do painel (título + 1ª linha, ~170 px) já está à vista; senão, rola de uma vez só até ele, sem esmaecer (`rolarSoOComeco`). Com "Automático" fica a rolagem suave original (pedido do dev. Jean: nesse modo ela guia o olho até o que abriu).
  - **Cartões da Visão processual:** no tema claro não mostravam a seleção (a regra geral do modo embutido, com `!important`, apagava o `:hover`/`.active`). Em teste, seguem o desenho dos cartões do Mapa EAP: fundo claro, borda fina; aberto ou com o mouse em cima, borda na cor de destaque e sombra leve.
  - **Contexto ativo em coluna estreita:** com a escala de 12 px, o cabeçalho dos componentes media 196 px numa coluna de 183 px e a lista passava 13 px da borda; agora o cabeçalho quebra a linha e a grade fica presa à coluna. O painel aberto (componentes ou metas) tinha `margin-top: -10px` e subia sobre o cartão; agora fica colado a ele, dividindo a borda. As metas desse painel seguem o desenho dos componentes (sem a barra lateral e o fundo tingido); a aberta ganha a borda na cor de destaque.
  - **Leitor de tela:** pilares, macropolíticas e processos dizem "expandido/recolhido" (`aria-expanded`, pela classe `.active` que o mockup já põe).

Medido com a aba na frente: com "Reduzir sempre", `scrollTo` e `scrollIntoView` chegam ao destino no primeiro quadro (no SPO e no mockup) e o cartão fica com `transform: none`; macropolítica → eixos esmaece a área dos eixos (`.policy-expansion`), "Programar" → "Direcionamento estratégico" esmaece a área do PTA, a volta ao topo esmaece a área de conteúdo, e sem movimento real nada esmaece; com "Automático", a rolagem continua suave (76 px no primeiro quadro) e o cartão sobe 1–2 px como antes. No mockup (iframe numa página, 1129 × 560): macropolítica 01 com os eixos a 180 px do cartão (antes ~1000 px); com "Reduzir sempre", rolagem imediata de 291 px (pilar), 348 px (macro 01) e 206 px (macro 10), sem esmaecer, e nenhuma quando o começo do painel já está à vista; com "Automático", rolagem suave (~60 px nos primeiros 60 ms). Contexto ativo a 1300 × 650: nenhum elemento além da borda, painel colado (1 px de encontro). Cartão selecionado da Visão processual com a mesma borda e sombra de um nó aberto do Mapa EAP. Sem erro no console. `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `login.html`: só as linhas `?v=` (`20261115.jean-movimento.1`); `index.html` do mockup: só os `?v=` do `spo-preferencias.css` e do `.js` (`20261115.jean-movimento.7`); o `main.js` também está em `.2`.

#### Telas estreitas: rolagem automática do mockup e janelas no celular (dev. Jean, 2026-11-15) (antiga 41.76)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.76)._

Pedidos do dev. Jean depois de testar os dois modos de movimento e o celular no Chrome (iPhone 16 Pro Max 440 × 956, iPhone 393 × 852 e menores):
- **Janelas no celular sem tela cheia.** Até 600 px, a busca de telas e a ajuda dos atalhos ocupavam a tela inteira; a janela "O padrão SPO-AA+" já era um cartão com margem. Agora as três seguem o cartão: 16 px de margem, cantos de 14 px, até 72% da altura. A busca fica no alto (o teclado virtual sobe por baixo e não cobre o campo); a ajuda, no centro. Só CSS (bloco no fim do `style.css`).

Medido: mockup dentro de uma página (iframe de 1004 px), cartão no meio e no pé da tela. Com "Automático" a 1500 px: pilar no meio 0 px, macropolítica no meio 27 px; no pé, 285 e 342 px (só até o começo do painel). A 1100 px: rolagem suave original (327–657 px). "Reduzir sempre": igual nas duas larguras (0–348 px, imediata). Janelas a 393 × 852: busca de 16 a 377 px na horizontal, no alto (16 px); ajuda centralizada (119 a 733 px); a 320 × 640, sem rolagem lateral e com o "Fechar" dentro do cartão. `pytest` completo duas vezes seguidas.

Achado sem relação, para um `fix/` à parte: abrir a busca com Ctrl+K enquanto um aviso modal está aberto (o "Alerta" da Início) deixa o foco no botão do aviso, atrás da busca, e o Esc não fecha nenhum dos dois.

Sem script de banco. `templates/base.html` e `login.html`: só a linha `?v=` do CSS (`20261115.jean-janelas-celular.1`); `index.html` do mockup: só o `?v=` do `spo-preferencias.js` (`20261115.jean-janelas-celular.1`).

#### Busca de telas e ajuda dos atalhos por cima de um aviso (dev. Jean, 2026-11-15) (antiga 41.77)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.77)._

Achado no teste do celular (#122): abrir a busca com Ctrl+K com um aviso modal aberto (o "Alerta" da Início, que aparece depois de a página carregar) deixava o foco no botão do aviso, atrás da busca, e o Esc não fechava a janela da frente. Causa: o tratamento geral dos avisos (`prepararModais`, #41) levava o foco para o aviso quando ele aparecia e, com a captura do teclado no `document`, tomava o Esc e o Tab, sem saber que a busca, a ajuda dos atalhos e "O padrão SPO-AA+" (`.spo-busca-fundo`) abrem por cima dos avisos.

Medido no SPO local: aviso aberto → Ctrl+K → o Tab fica na busca, o Esc fecha a busca e o foco vai ao aviso, outro Esc fecha o aviso; Ctrl+K logo depois de carregar (o aviso aparece depois) → o foco continua no campo da busca, Esc fecha a busca e o foco vai ao aviso; o mesmo com a ajuda dos atalhos; sem aviso, o foco volta para onde estava. Sem erro no console. `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html`: só a linha `?v=` do `main.js` (`20261115.jean-busca-aviso.1`).

#### Rolagem automática do mockup sem o monitor em retrato (dev. Jean, 2026-11-15) (antiga 41.78)

Pergunta do dev. Jean depois do #122: num monitor Full HD em retrato (~1060 × 1900 com o navegador maximizado), a regra "até 1200 px de largura" ligava a rolagem automática do mockup PTA/OpR. Lá a tela é estreita mas muito alta, e o painel aberto quase sempre cabe à vista. Decisão: a regra passa a olhar também a altura. Com "Movimento: Automático", a rolagem suave ao abrir metas e eixos vale com a janela do SPO até 1200 px de largura **e** até 1000 px de altura; o celular (até 720 px de largura) entra sempre, qualquer que seja a altura; o preset "Pressão operacional" também. Ficam de fora a tela larga, o monitor em retrato e o tablet em pé (nesses, a página só rola se o começo do painel ficar fora da tela, e só até ele). `spo-preferencias.js`, `telaEstreita()`.

Medido com o mockup dentro de uma página (com `meta viewport`, como o SPO), macropolítica 01 no meio da tela, "Automático": 1060 × 1900 (retrato) não rola; 800 × 1280 (tablet em pé) não rola; 1100 × 650 (janela dividida/notebook pequeno) rolagem suave de 342 px; 440 × 956 (iPhone 16 Pro Max) rolagem suave de 844 px; 1500 × 650 só 27 px (o começo do painel). `pytest` completo duas vezes seguidas.

Sem script de banco. `index.html` do mockup: só o `?v=` do `spo-preferencias.js` (`20261115.jean-sem-retrato.1`).

#### Janela dividida sem largura mínima (dev. Jean, 2026-11-15) (antiga 41.79)

_Esta subseção continua inteira no [claude.md](claude.md), seção 41 (41.79), por ser regra em vigor._

#### Declaração de acessibilidade com os tamanhos dos outros cartões (dev. Jean, 2026-11-15) (antiga 41.80)

O dev. Jean notou, no Personalizar, que o cartão "Acessibilidade do SPO" parecia ter letra maior que os vizinhos e perguntou se faltava fixar o tamanho em px (como no "Texto das tabelas"). Avaliação: os tamanhos do SPO já são todos em px; as opções 112/125/150% ampliam a área de conteúdo inteira (`zoom` no `.main`), na mesma proporção. A diferença era de valor: a Declaração usava o passo "Texto" da escala (título 18 px, abertura/itens 14 px, títulos internos 14 px), e os outros cartões usam título 14 px (`.card-title`), descrição 12 px (`p.muted`), nome da opção 13 px e resumo 12 px.

Decisão do dev. Jean: igual aos outros cartões. No bloco `[jean]` da Declaração (`style.css`): título 14 px, abertura e itens 12 px (entrelinha 1,5), títulos internos ("Atendido"…) 13 px; rodapé continua 12 px; a cor do texto não muda. Medido na tela: Declaração 14/12/13/12 px, igual ao cartão "Fontes e tamanho do texto" (14/12/13/12). `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `login.html`: só a linha `?v=` do CSS (`20261115.jean-declaracao-tamanhos.1`).

#### Cartões dinâmicos no padrão de Governança e Resultados (dev. Jean, 2026-11-15) (antiga 41.81)

Pedido do dev. Jean: os cartões das telas do Institucional (Diretrizes e Procedimentos, Repositório de Arquivos, Legislação e Normas, Rede de Parceiros) não seguiam o padrão dos cartões dinâmicos de Governança e Resultados; fazer uma varredura e aplicar esse sistema visual aos cartões dinâmicos do SPO (o mockup PTA/OpR será revisto depois).

Varredura (cartões clicáveis com ícone/título/descrição): `.noticia-box` do Institucional (17 cartões em 4 telas, sem ícone, só o texto do link clicável), `.see-upload-option` das Notas SEE (2, borda tracejada, centralizados), `.contract-preset` do Contrato Visual (4, cantos de 8 px). Ficaram de fora as linhas de opção (Personalizar, replicar exercício), que são listas, e os cartões informativos (não clicáveis).

Implementação (bloco `[jean]` "Cartões dinâmicos" no fim do `style.css`): o desenho do `.flow-node` medido na tela — fundo, borda e textos pelas variáveis dos diagramas (`--spo-diagram-*`, que seguem a opção "Diagramas" e o alto contraste), cantos de 12 px, espaçamento de 12 px, altura mínima de 132 px, ícone de 20 px na cor de destaque, título 14 px negrito, descrição 12 px; mouse/foco: borda na cor de destaque; selecionado (preset escolhido na prévia, `.active`): borda, barra de 3 px à esquerda e fundo tingido; o preset ativo local (`.contract-preset-active-local`) só ganha a borda na cor de seleção, para não se confundir com a prévia; contraste Médio/Reforçado e preset "Contraste técnico": contorno no tom de controle (≥ 3:1), como o `.flow-node`; opção desabilitada das Notas SEE continua no estilo da bateria 2; movimento reduzido sem levantar. No Institucional, os 4 templates ganharam um ícone por cartão (Bootstrap Icons, `aria-hidden`) e o link do texto cobre o cartão inteiro (`a::after` com `inset: 0`; um só ponto de Tab; anel de foco no cartão).

Medido na tela: cartões do Institucional com o mesmo fundo, borda, cantos, espaçamento e altura do `.flow-node` (132 px), ícone 20 px, título 14 px, descrição 12 px, nas 4 telas (4, 3, 5 e 5 cartões, todos com ícone); clique nos cantos e no centro de cada cartão cai no link; preset ativo do Contrato com o mesmo fundo, borda e barra do nó selecionado de Governança; opções das Notas SEE (desabilitadas nesta conta) no estilo de desabilitado. `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `login.html`: só a linha `?v=` do CSS (`20261115.jean-cartoes-dinamicos.2`).

#### Rótulos do gráfico de árvore com contraste garantido (dev. Jean, 2026-11-15) (antiga 41.82)

Pergunta do dev. Jean: com "Cores dos gráficos: seguras para daltonismo", nos temas claro e escuro, os textos dos gráficos do Teto Orçamentário estão adequados? Medido (contraste WCAG de cada rótulo sobre a cor do bloco): a pizza está bem (rótulos fora das fatias, na cor do texto sobre o cartão); no gráfico de árvore ("Teto por ADJ"), o Plotly escolhe o texto de dentro do bloco entre um cinza (#444) e o branco por critério próprio, e **5 dos 8 blocos ficavam abaixo de 4,5:1** no claro com a paleta Okabe-Ito: rosa 3,18, verde 3,42, vermelhão 3,87, azul-céu 4,22, laranja 4,32.

Correção (`main.js`, `ajustarDados` da escala dos gráficos, que já passa por todo `Plotly.react`/`newPlot`): em todo gráfico de árvore com cores por bloco, o texto passa a ser **preto ou branco, o que der mais contraste** com a cor de cada bloco (`textfont.color` por bloco). Vale para qualquer paleta e tema.

Medido na tela (Teto por ADJ, 8 blocos): paleta segura para daltonismo, claro 5,19–21:1 e escuro 5,19–21:1; paleta padrão, claro 5,18–10,35:1 e escuro 5,6–17,03:1. O pior caso (5,19:1, branco sobre o azul Okabe-Ito #0072B2) passa o AA (4,5:1); o AAA (7:1) dependeria de mudar a cor do bloco, o que tiraria a paleta Okabe-Ito do padrão. `pytest` completo duas vezes seguidas; `node --check`.

Sem script de banco. `templates/base.html`: só a linha `?v=` do `main.js` (`20261115.jean-rotulos-arvore.1`).

#### Fonte Mulish como opção da interface (dev. Jean, 2026-11-15) (antiga 41.83)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.83)._

O dev. Jean achou confortável a leitura de uma página em Mulish (16 px, entrelinha 1,5, texto cinza-escuro) e pediu para compará-la com a Inter no próprio SPO. Avaliação registrada no plano de tipografia (`docs_jean/PLANO_NORMALIZACAO_TIPOGRAFIA.md`): boa parte do conforto vem da composição (tamanho, entrelinha, espaço entre parágrafos, linha de ~80 caracteres); a Mulish tem I maiúsculo e l minúsculo parecidos, o que pesa num sistema com códigos e siglas, por isso entra como **opção**, não como padrão.

Medido na tela: a opção aparece no Personalizar; com ela, a interface e a prévia "Fontes e tabelas" passam à Mulish (a tabela da prévia continua na fonte de dados); a fonte carrega do próprio SPO. A preferência do navegador de teste foi devolvida à Inter. O login não foi aberto (com sessão ativa ele redireciona); a mudança ali é só incluir o valor na lista aceita. `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html`: `?v=` do CSS e do `main.js` (`20261115.jean-mulish.1`); `login.html`: `?v=` do CSS e do `login-theme.js` (idem); `index.html` do mockup: `?v=` do `spo-preferencias.css` (idem).

#### Padrão SPO-AA+ versão 4: "Densidade do texto" (dev. Jean, 2026-11-15) (antiga 41.84)

_Atualizou o `docs/padrao-visual-spo-aa-plus.md` (só documentação); o conteúdo em vigor está nesse documento._

Só documentação (etapa 1 do plano de normalização da tipografia). O dev. Jean notou que o 100% parecia pequeno: as janelas (busca, atalhos, "O padrão SPO-AA+") e a prévia "Fontes e tabelas" ficavam maiores que os cartões de opção. Medição: telas de trabalho e cartões em 12 px; janelas e prévias em 14–16 px; 330 `font-size` com 25 valores, sem variável comum.

Decisões do dev. Jean, registradas no `docs/padrao-visual-spo-aa-plus.md` §2.2 (versão 4): Compacto/Intermediário/Legível passam a ser a "Densidade do texto" da interface inteira; padrão Legível; Compacto com tabelas de 10 px (sai o 8 px); passo novo "Leitura" de 16 px no Legível (janelas de ajuda, documentos, declaração). Variáveis `--spo-fs-dado`, `-controle`, `-texto`, `-leitura`, `-titulo`, `-rotulo`, `-secao`, `-pagina`, com os valores das três densidades. Etapas seguintes: (2) variáveis com os valores de hoje e a opção "Densidade do texto"; (3) migração das telas por grupo, começando pelo Personalizar (a Fase C das cores vem em PR separado, depois; o dev. Cleber orientou fazer as mudanças antes da divisão do código); (4) teste contra `font-size` em px fora das variáveis. §2.1: Mulish com 53ch no "Leitura → Estrito".

Sem mudança de código. `pytest` completo duas vezes seguidas (o documento é lido pela rota `/api/documentos/padrao-spo-aa-plus`).

#### "Densidade do texto" no lugar de "Texto das tabelas" (dev. Jean, 2026-11-15) (antiga 41.85)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.85)._

Etapa 2 do plano de tipografia (padrão SPO-AA+ v4 §2.2.1). A opção do Personalizar "Texto das tabelas" vira **"Densidade do texto"** (Compacto / Intermediário / Legível), com o mesmo atributo `data-spo-table-text` e os mesmos valores (`compact`/`medium`/`legible`), para não perder a escolha já gravada no navegador (login e ponte do mockup seguem iguais).

Medido na tela (densidade Legível, antes da troca do padrão): FIP 613, PLAN20 e Usuários com células e títulos em 13 px; paginação e filtros em 13 px; 10 primeiras linhas 9% mais altas que com 12 px (FIP 613 1.046 × 958 px; PLAN20 2.138 × 1.966 px), largura da tabela igual (a tabela rola dentro do próprio quadro, como antes); a 390 px nada passa da borda da página. Uma tela por vez, esperando os dados. `pytest` completo duas vezes seguidas; `node --check`.

Sem script de banco. `templates/base.html`: `?v=` do CSS e do `main.js` (`20261115.jean-densidade.2`); `login.html`: `?v=` do CSS e do `login-theme.js` (idem); `index.html` do mockup: `?v=` do `spo-preferencias.css` (`20261115.jean-densidade.1`). Depois da troca do padrão: Personalizar com "Intermediário (padrão SPO)", amostra em 12 px e a sugestão da Atkinson com os cinco ajustes.

#### Densidade do texto, etapa 3 (a): Personalizar e janelas (dev. Jean, 2026-11-15) (antiga 41.86)

O Personalizar (cartões, opções, prévias, declaração) e as janelas de consulta (busca de telas, ajuda dos atalhos, "O padrão SPO-AA+") passam a usar as variáveis da "Densidade do texto" (bloco `[jean]` no fim do `style.css`): sobretítulo = rótulo; título de cartão e de janela = título; descrição, nome de opção, itens e itens da busca = texto; resumo das opções, rótulos de campo, subtítulos e status = controle; ajuda dos atalhos e documento SPO-AA+ = leitura. Títulos de seção dessas janelas: na ajuda dos atalhos, tamanho de título; no documento, tamanho da leitura em negrito — nunca maiores que o título da janela (primeira versão deixava a seção dos atalhos menor que a descrição e a do documento maior que o título da janela; corrigido antes do PR).

Antes: cartões de opção em 12–13 px e janelas em 14–16 px (a diferença notada pelo dev. Jean). Medido (Compacto / Intermediário / Legível): título de cartão e de janela 13/14/16; descrição, nome de opção, texto da prévia e da declaração 12/13/14; resumo e rodapé 11/12/13; itens da busca 12/13/14; ajuda dos atalhos e documento 13/14/16 (seções no mesmo tamanho, negrito); tabela da prévia 10/12/13. A 390 px nada passa da borda (Intermediário e Legível). Botões e campos de formulário em geral ficam para a etapa dos formulários. `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `login.html`: só a linha `?v=` do CSS (`20261115.jean-densidade-personalizar.2`).

#### Densidade do texto, etapa 3 (b): relatórios e cabeçalho de página (dev. Jean, 2026-11-15) (antiga 41.87)

Bloco `[jean]` no fim do `style.css`:
- **Cabeçalho de página** (todas as telas, `.report-header` e `.module-header`): sobretítulo = rótulo (11 px), título = página (24 px), descrição = texto (Compacto 12 / Intermediário 13 / Legível 14). No Intermediário, os valores são os de antes.
- **Relatórios:** "Última atualização (por UO)" e "Totais (filtros aplicados)" = texto em negrito (antes 12 px); nome da UO, linhas da atualização e rótulos dos totais = controle; valores dos totais = dado (a fonte de dados continua). Tabelas, filtros e paginação já seguiam a densidade (#131).

Medido (Compacto / Intermediário / Legível), um relatório por vez: FIP 613 e PLAN20 (com dados), PLAN51 e Receita Anexo 10 (sem dados no banco de dev): descrição 12/13/14, "Última atualização" e título dos totais 12/13/14, UO e linhas 11/12/13, rótulos dos totais 11/12/13, valores 10/12/13, células 10/12/13; nada além da borda. A 390 px (PLAN20, Intermediário e Legível): nada passa da borda e os totais cabem no quadro. Botões ficam para a etapa dos formulários. `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `login.html`: só a linha `?v=` do CSS (`20261115.jean-densidade-relatorios.1`).

#### Densidade do texto, etapa 3 (c): formulários e cadastros (dev. Jean, 2026-11-15) (antiga 41.88)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.88)._

Bloco `[jean]` no fim do `style.css`: título de cartão do conteúdo = título (Intermediário 14, igual a antes); rótulos de campo (`.field > span`, `.field.inline span`) e o texto dentro de `input`/`select`/`textarea` do conteúdo e dos avisos (inclusive nas tabelas) = controle (Intermediário 12, igual a antes; a busca de usuários tinha 14); botões (`.btn` do conteúdo, dos avisos e das janelas de consulta) e legendas de grupo de campos = texto (Intermediário 13: os dos formulários tinham 14, os dos relatórios já tinham 13). O padrão SPO-AA+ §2.2.1 ganhou os botões na linha `--spo-fs-texto`.

Medido (Compacto / Intermediário / Legível) em Usuários, Meta Física e Dotação: título de cartão 13/14/16; rótulos e campos 11/12/13; botões 12/13/14; busca de usuários 11/12/13; busca de telas sem mudança (campo 13, "Fechar" 13). A 390 px (Meta Física, Intermediário e Legível) nada passa da borda. `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `login.html`: só a linha `?v=` do CSS (`20261115.jean-densidade-formularios.3`).

#### Densidade do texto, etapa 3 (d): painéis, cartões dinâmicos e Início (dev. Jean, 2026-11-15) (antiga 41.89)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.89)._

Bloco `[jean]` no fim do `style.css`:
- **Governança e Resultados:** título dos cartões do fluxo e do contexto = título; descrição dos cartões = controle; sobretítulo do contexto = rótulo; texto do contexto e dos cartões = leitura.
- **Cartões dinâmicos** (Institucional, Notas SEE, presets do Contrato): título = título; texto = controle.
- **Teto Orçamentário:** títulos dos quadros e "Filtros" = título; rótulos dos indicadores, legendas, nomes e percentuais = controle; cabeçalhos do quadro comparativo = rótulo; valores, totais do ano, variações e ranking = dado; ano e valor do quadro e "Ver dados do gráfico" = texto; valores em destaque um passo acima do título (indicadores do alto + 2 px; cartões por grupo + 4 px).
- **Início:** texto de boas-vindas = leitura + 2 px (Intermediário 16; era 16,8); dica e sessões = controle; texto do Status = texto.

Medido (Compacto / Intermediário / Legível): Início (boas-vindas 15/16/18, dica 11/12/13, Status 12/13/14); Governança (títulos 13/14/16, descrições 11/12/13, contexto 13/14/16, rótulo 11); Institucional (título 13/14/16, texto 11/12/13); Teto Gráficos (títulos 13/14/16, indicadores 11/12/13 e 15/16/18, "Ver dados" 12/13/14) e Tabelas (legenda, nome e % 11/12/13; valor por grupo 17/18/20; cabeçalho 11; ano e valor 12/13/14; células, variações e ranking 10/12/13). Nada além da borda; a 390 px (Teto, Intermediário e Legível) os valores cabem nos cartões. `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `login.html`: só a linha `?v=` do CSS (`20261115.jean-densidade-paineis.2`).

#### Densidade do texto, etapa 3 (e): gráficos (dev. Jean, 2026-11-15) (antiga 41.90)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.90)._

Medido no Teto Orçamentário (5 gráficos: eixos X e Y, legenda, pizza, árvore): 10/12/13 px nas três densidades; ao voltar ao Intermediário, redesenhados em 12 px. A 390 px, no Legível, os gráficos cabem na tela e nenhum texto sai da área deles. `node --check`; `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html`: só a linha `?v=` do `main.js` (`20261115.jean-densidade-graficos.1`).

#### Densidade do texto, etapa 4: teste contra `font-size` em px (dev. Jean, 2026-11-15) (antiga 41.91)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.91)._

Última etapa do plano de tipografia.

Medido (Compacto / Intermediário / Legível): dica da Início, rótulos de filtro e indicadores do Teto, pílula do Contrato, rótulos dos totais e linhas da atualização 11/12/13; rótulo da paginação 10/12/13; "Fonte: FIPLAN…" 11/12/13; descrição da página 13 no Intermediário. `pytest` completo duas vezes seguidas (204 passam, 4 pulam).

Sem script de banco. `templates/base.html` e `login.html`: só a linha `?v=` do CSS (`20261115.jean-densidade-teste-px.2`).

#### Navegação pelo teclado nas tabelas (dev. Jean, 2026-11-15) (antiga 41.92)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.92)._

Pedido do dev. Jean: navegação otimizada pelo teclado nas tabelas (ele sugeriu Alt + setas). Alt + ← / → no navegador volta/avança a página (a pessoa sairia da tela por engano), por isso foi usado o padrão "grade" do WAI-ARIA, o das planilhas da web — `initNavegacaoTabelas` no fim do `main.js`:

Testado com teclas de verdade: PED (Tab do "Baixar Excel" cai na 1ª célula de dados e o próximo Tab vai ao "itens por página"; ↑ até os filtros, Enter no filtro, ↓ volta aos dados; Enter de novo abre a lista, Tab vai à busca, ↓ fica na lista, Esc fecha e devolve ao filtro; clique numa célula e → ↓ a partir dela; paginação: → até o 2, Enter troca a página e o foco fica no 2, ←, End, ↑ volta à célula da vez, Esc também; o mesmo na FIP 613); FIP 613 (26 controles fora do Tab, nenhum visível sobrando nele; início na 1ª célula de dados; → ↓; End rola até a última coluna; Ctrl+Home; ↓ até os filtros, Enter abre o foco no filtro e Esc volta à célula; PageDown/PageUp 10 linhas) e Usuários (Ctrl+End na última célula visível; Tab sai; Shift+Tab volta à mesma célula). Ajuda dos atalhos com a seção nova; console sem erros. `node --check`; `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html`: `?v=` do CSS e do `main.js` (`20261115.jean-teclado-tabelas.1` e `.8`); `login.html`: `?v=` do CSS.

#### Kit de mockup SPO-AA+ (dev. Jean, 2026-11-15) (antiga 41.93)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.93)._

Pedido do dev. Jean: garantir que as próximas funcionalidades sigam o padrão SPO-AA+ **desde o primeiro mockup**. Novo `static/kit/mockup-spo-aa-plus.html` (+ `kit-mockup.css`, `kit-mockup.js`), descrito no §6.4 do `docs/padrao-visual-spo-aa-plus.md`; item novo no topo do checklist §6.1.
- **Dois achados do kit** (registrados no §7 do padrão, a decidir): título/descrição de `module-card`, texto corrido de cartão e `.alert` não acompanham a densidade do texto; `pos`/`neg` só pintam nos totais e na tabela do FIP 613.

Testado no SPO local: Atkinson → oferta das cinco opções, "Aplicar as cinco" (Atkinson Mono, 112%, 1,75, 72ch, Legível; foco volta à fonte da interface; `localStorage` intacto), "Agora não", sem oferta quando já está tudo no sugerido; aviso a 12 px dos botões; tema escuro + Roxo + Legível; densidades 10/12/13 nas tabelas, filtros e paginação; 150% (zoom 1,5); Atkinson; Tab entra na tabela, End, ↑ ao filtro, Enter abre a lista, Esc fecha e devolve o foco ao filtro, Aplicar filtra (10 linhas) e atualiza o total; paginação → → Enter (foco no 3), Esc volta à célula. Nenhum arquivo do SPO mudou além da documentação: `style.css`, `main.js`, `base.html` e `login.html` intactos (sem `?v=` novo).

##### Kit de mockup: presets do Contrato Visual (dev. Jean, 2026-11-15)

Pedido do dev. Jean no teste do kit: o kit não trazia os presets do Contrato Visual Protegido (Compacto gerencial, Contraste técnico, Pressão operacional), que mudam o visual tanto quanto o Personalizar (`data-visual-contract-preset`, mais de 100 regras no `style.css`).
- Seletor "Preset do Contrato Visual" no painel do kit, com a lista lida do `PROTECTED_CONTRACT_PRESETS` do `main.js`; aplicado no `<html>` e no `<body>`, como o `setProtectedContractVisualPreset`. O preset vigente vem da chave `PROTECTED_CONTRACT_STATE_KEY` (prévia, senão rascunho, senão ativo, como o `getStoredProtectedContractPreset`). Nada é gravado.
- Saiu o botão "As minhas preferências": com o kit abrindo nas preferências vigentes, recarregar a página faz o mesmo.
- O kit continua separado do Contrato (papéis diferentes: o Contrato aplica o visual do SPO, com permissão; o kit é o ponto de partida de tela nova, sem login, e não muda nada). Integração futura a combinar com o dev. Cleber: uma aba "Componentes" no Contrato abrindo o próprio kit (§6.4 do padrão).
- `tests/test_kit_mockup.py`: marcador dos presets, forma da `PROTECTED_CONTRACT_STATE_KEY` e o atributo `data-visual-contract-preset` no `main.js`.

##### Kit de mockup: prévias do Personalizar e gráficos (dev. Jean, 2026-11-15)

Pedido do dev. Jean: com todas as opções do Personalizar no kit, mostrar também as prévias e um gráfico, para ver os temas aplicados.
- **Prévias** "Componentes do SPO" e "Fontes e tabelas" com a mesma marcação do `atualizar_personalizar_spo.html` (sem o resumo e o botão de restaurar, que dependem do Personalizar); `tests/test_kit_mockup.py` compara a marcação das duas amostras com a do template.
- **Gráficos** de pizza ("Teto por grupo de despesa") e de barras agrupadas ("Execução por região") nos painéis do Teto (`.teto-panel`, `.teto-chart`), com o mesmo Plotly do `base.html` (3.0.1). O tema vem como no `getCommonDiagramTheme`: `COMMON_DIAGRAM_THEMES[data-visual-diagram-theme]` passado pelo `spoTemaDiagramaComDestaque` (do `initDiagramasDestaque`); o kit lê e roda também o `initGraficosAcessiveis` (resumo e "Ver dados em tabela") e o `initGraficosEscala` (fonte, densidade, padrões do daltonismo, tela estreita), dando a ele o `spoGetDiagramSemanticTokens` (só o modo). Layout como o `plotLayout` do Teto. Redesenha a cada mudança no painel.
- Testes novos: prévias iguais às do template; blocos dos gráficos completos e o `initGraficosEscala` sem depender de outra função global; mesmo Plotly do `base.html`.

Testado no SPO local: padrão (1ª cor = destaque Clássico `#1f9b7e`, fonte Inter 12 px, resumo `role="img"` e tabela de dados); escuro + Azul + daltonismo + Legível (paleta Okabe-Ito do escuro, padrões de hachura, texto 13 px, amostra das tabelas a 13 px).

##### Kit de mockup: painel com os controles do SPO (dev. Jean, 2026-11-15)

Pedido do dev. Jean: os controles do próprio SPO (cartões do Personalizar) são melhores para escolher as combinações do que as 17 caixas de seleção genéricas do painel do kit.
- O painel passou a ter os cartões do Personalizar — Tema, Cor de destaque, Preferências comuns, Fontes e tamanho do texto, Visão e cores — com a mesma marcação do `atualizar_personalizar_spo.html` (sem os comentários Jinja), e um cartão "Preset do Contrato Visual" com a grade `contract-preset-grid` do template. Ficou de fora só o cartão Teclado (o kit não tem atalhos).
- O painel tem a classe `personalizar-spo` (como a tela do Personalizar), que define as variáveis de raio, sombra e espaço dos cartões.
- O `kit-mockup.js` dá o comportamento do SPO sem gravar nada: escolha com `.active`/`aria-pressed` (`syncThemeControls`, `syncCommonVisualControls`, `sync` do `initFontesPersonalizar`, presets do Contrato), resumo do grupo com o rótulo da opção, um grupo aberto por vez, o grupo fecha ao escolher, status "… aplicada nesta página". A oferta "Leitura confortável" fica logo abaixo do grupo "Fonte da interface", como no SPO, e o foco volta ao botão dele.
- Saíram as caixas de seleção e a lista `GRUPOS_FONTES`. O teste agora compara cada cartão do painel com o do template (e a grade de presets); conferido que reprova quando a cópia diverge.

Testado no SPO local, com cliques de verdade: abrir "Fonte da interface", escolher a Atkinson (grupo fecha, resumo e status atualizados, oferta abaixo do grupo), "Aplicar as cinco" (cinco resumos atualizados, foco de volta, `localStorage` intacto); escuro, Roxo (pizza na cor do destaque), contraste reforçado, preset Compacto gerencial; "Padrão SPO" volta tudo.
- Posição (pedido do dev. Jean no teste): as prévias e os gráficos saíram da tela de exemplo e ficam logo abaixo dos cartões de preferências, dentro do painel (`#kit-previas-bloco`), porque mostram o efeito das escolhas; o "Recolher" esconde os dois.
- Retorno do dev. Jean no teste: o título "Preferências do kit" era um `h2` de 14 px solto no fundo, fora do padrão — virou o cabeçalho de página do SPO (`module-header`: sobretítulo, `h1` 24 px, descrição, ícone `bi-brush`, como o do Personalizar), com os botões "Padrão SPO" e "Recolher" abaixo da descrição. E a grade de presets copiada do Contrato era grande e sem função — virou cartões dinâmicos (`flow-node`, um por preset, com ícone) e o painel de contexto (`flow-context-panel`) logo abaixo, mostrando escopo, estado, regras e detalhes do preset escolhido (dados do `PROTECTED_CONTRACT_PRESETS`, os mesmos do "Catálogo protegido" do Contrato). Os estilos do mockup PTA/OpR (contexto ativo, mapa EAP) ficam para quando ele entrar no SPO; o cartão dinâmico é o padrão do SPO que já cumpre o papel. O cartão ocupa a linha inteira da grade. Nomes das regras em `span` negrito (o `.flow-context-panel strong` é o título do painel).
- Retorno do dev. Jean: o topo da "Tabela de relatório" do kit não seguia o dos relatórios (FIP 613, PLAN20). Virou uma tela de relatório com a marcação deles: `report-page` > `report-header` (sobretítulo, `h1`, descrição, ícone) e o cartão `report-data-card` com `card-title`, o topo `relatorio-top relatorio-top-uo` — à esquerda "Última atualização por UO" com o mesmo HTML que o `renderUltimasAtualizacoes` escreve (UO, enviado por, upload, data do download; e-mail fictício), à direita os totais (`totais-box`, valores `neg`/`pos`) e os botões "Limpar Filtros"/"Baixar Excel" — e o `report-empty-state`. Saíram o título de seção solto e a linha de ajuda (o teclado está no "Como usar" e na ajuda dos atalhos).

##### Kit de mockup: catálogo de padrões (dev. Jean, 2026-11-15)

Pedido do dev. Jean: além dos cartões padrão, o kit deve mostrar os outros padrões de cartão do SPO (os do mockup PTA/OpR entram quando ele for integrado com os bancos novos).
- Seção "Catálogo de padrões" na tela de exemplo do kit, no lugar dos três cartões dinâmicos de antes. Cada exemplo é **recortado do template de origem** (o gerador cortou o bloco inteiro; nada redigitado) e o kit só põe a moldura (nome do padrão e onde é usado): cartões dinâmicos com painel de contexto (`atualizar_governanca_resultados_eap_politicas.html`), fluxo com setas e painel de contexto e cartões de painel (`…_mapa.html`), cartão de documento (`noticia-box` do `institucional_diretrizes.html`, dentro de um `institutional-card`), indicadores (`teto-kpi-grid` do `paineis_teto_orcamentario.html`, com valores de exemplo postos pelo kit, como o JS da tela faz) e opções de envio (`see-upload-options` do `notas_see.html`, dentro de um `see-card`).
- Comportamento dos fluxos de Governança: o kit lê do `main.js` e roda o `initGovernancaSemanticFlows` (seleção, painel de contexto, setas/Home/End, painel embutido no celular), com os dois painéis marcados com `data-governanca-flow-panel` como nas telas.
- Fora do catálogo, já no kit: cabeçalho de página, `module-card`, prévias do Personalizar, gráficos, formulário, tela de relatório com totais. Ficou de fora o cartão de status da Início (`home-status-card`, montado com Jinja e dados da sessão).
- `tests/test_kit_mockup.py` (+2): cada exemplo do catálogo igual ao template de origem (lista `CATALOGO`); o `initGovernancaSemanticFlows` sem chamar outras funções do `main.js`.

Testado no SPO local: os dois fluxos ligados (`data-flow-bound`), o padrão de cada um selecionado e o contexto escrito; clique em "Vínculos oficiais" e → passa a "Histórico anual" com o contexto atualizado; indicadores com os valores de exemplo; console sem erros.

#### Achados do kit de mockup resolvidos: densidade dos cartões e pos/neg (dev. Jean, 2026-11-15) (antiga 41.94)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.94)._

O kit de mockup (§6.4 do padrão) mediu o próprio SPO e achou dois pontos fora do padrão; o dev. Jean pediu para resolver.
1. **Componentes comuns fora da "Densidade do texto"** (medido no kit trocando a densidade: estes não mudavam). Bloco `[jean]` novo no fim do `style.css`, com o mesmo truque de especificidade da densidade (`:not(#spo-dens):not(#spo-dens-b)`): `.module-card-title h2` (15 px fixo) → `--spo-fs-titulo`; `.module-card-title p` (12 fixo) → `--spo-fs-controle`; `.module-card > p` e `> ul/ol > li` (14 herdado do `body`) → `--spo-fs-texto`; `#content-area .alert` (14 herdado) → `--spo-fs-leitura`, menos `.limpeza-regras`/`.limpeza-pendente`, que têm 13 px próprios. **No Intermediário (padrão):** título do cartão 15 → 14 e texto do cartão 14 → 13 nas 11 telas com `module-card-title` (Atualizar EMP/NOB/PED/FIP 613/EST EMP/PLAN20/PLAN51/Teto/Receita, Segurança da sessão, Correção de chave); descrição e avisos ficam iguais. Compacto 13/11/12/13; Legível 16/13/14/16.
2. **`pos`/`neg` só no FIP 613 e nos totais:** o bloco `[jean] Valores em azul e vermelho mais vivos` passou a definir os tokens `--spo-positive`/`--spo-negative` (claro `#0000ff`/`#a00000`, escuro `#94baff`/`#ff9999`, os mesmos valores) e a pintar qualquer `#content-area td.num.pos/.neg` (negrito 600), além dos totais e do FIP 613. Nenhuma tabela além do FIP marca o sinal hoje: muda só para telas novas (e o kit, cujo "Saldo" passou a marcar `pos`/`neg`).

Medido no kit (as mesmas classes das telas): título 13/14/16, descrição 11/12/13, texto e listas 12/13/14, aviso 13/14/16 (Compacto/Intermediário/Legível); célula positiva `rgb(0,0,255)` e negativa `rgb(160,0,0)` no claro, `rgb(148,186,255)` e `rgb(255,153,153)` no escuro; totais sem mudança. Conferido também no SPO local, com o dev. Jean logado: Atualizar EMP com os mesmos valores do kit (título 13/14/16, descrição 11/12/13, texto 12/13/14); Limpeza de Histórico com os avisos próprios em 13 px nas três densidades; FIP 613 sem mudança (53 células positivas `rgb(0,0,255)` e 16 negativas `rgb(160,0,0)`, negrito 600; totais 700; no escuro `rgb(148,186,255)`/`rgb(255,153,153)`). `?v=` do CSS `20261115.jean-densidade-cartoes.1` no `base.html` e no `login.html`. `tests/test_tamanho_fonte.py` sem mudança (nenhum px novo).

##### Kit de mockup: o documento do padrão SPO-AA+ dentro do kit (dev. Jean, 2026-11-15)

Pedido do dev. Jean: o documento técnico do padrão tem de estar no kit, para consulta do usuário e do agente que for verificar as configurações.
- Botões "O padrão SPO-AA+" e "Documento técnico" no cabeçalho do kit (`data-spo-padrao`, `data-spo-padrao="tecnico"`), abrindo **a mesma janela do SPO**: o kit lê e roda o `initPadraoSpoAaPlus` do `main.js` (resumo; documento técnico convertido do `.md` pela rota `/api/documentos/padrao-spo-aa-plus`, com sumário). A rota continua exigindo login (o documento é para quem desenvolve).
- `tests/test_kit_mockup.py` (+1): o kit lê o `initPadraoSpoAaPlus`, que ainda lê a rota; botões e caminho do arquivo-fonte presentes.

Testado: clique de verdade em "Documento técnico" no kit (logado): janela aberta, sumário com 9 seções, foco no título, Esc fecha; pedido sem sessão com `X-Requested-With` responde 401. `?v=` do `main.js`: `20261115.jean-kit-documento.1`.

#### Teste contra cor literal no style.css — Fase B (dev. Jean, 2026-11-15) (antiga 41.95)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.95)._

Avaliação pedida pelo dev. Jean (o kit e o SPO resistem a ampliações?): o tamanho em px já estava travado (`test_tamanho_fonte.py`), a cor não — tela nova podia escrever `#1a5fb4` e nada reprovava. É a Fase B do roteiro do N5 (padrão §7).

#### Teste contra cor e tamanho em style="…" nos templates e no JS (dev. Jean, 2026-11-15) (antiga 41.96)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.96)._

Segunda trava da avaliação "o padrão resiste a ampliações?": os testes do CSS (`test_cor_literal.py`, `test_tamanho_fonte.py`) não veem o que é escrito direto na marcação. `tests/test_style_inline.py`:
- Conferido com cópias de segurança: reprova cor nova e `font-size` novo no template, `.style.color = "#ff0000"` e `style="color:#123456"` em HTML do JS; aceita variável e margem no template e `${cor}`/`var(--text)` no JS. Arquivos devolvidos idênticos.

##### Kit de mockup sem cópias de lógica (dev. Jean, 2026-11-15)

Último item da avaliação "o padrão resiste a ampliações?": o kit tinha pequenas cópias de lógica do `main.js` que podiam divergir sem aviso. Agora usa as funções do SPO (lidas do `main.js` e montadas com os dados que o kit já lê):
- **Destaque:** `applyAccentColor` (rodada com `persist = false`; recebe `ACCENT_COLORS`/`DEFAULT_ACCENT`); saiu o `aplicarDestaque` do kit.
- **Densidade automática:** o bloco `initDensidadeAutomatica` (`window.spoResolverDensidade` e o acompanhamento da largura); saiu o `densidadeEmUso`.
- **Modo dos diagramas:** `getResolvedCommonDiagramMode` com `normalizeCommonVisualPreferences`, `getSystemPrefersMoreContrast`, `getSystemForcedColorsActive` e as duas `matchMedia` (`systemContrastQuery`, `systemForcedColorsQuery`). Com cores forçadas do Windows ela devolve "system", e o kit desenha com a paleta clara (o tema "system" é montado por outra função).
- **"Última atualização por UO":** o trecho do `AMAZON_TZ` até o `renderUltimasAtualizacoes` (fuso de Manaus, datas, escape), chamado com dados de exemplo.
- **Topo do relatório recortado do `relatorios_fip613.html`** (mesmos `id`s: `fip613-relatorio-meta`, `tot-*`, botões), entrando na lista `CATALOGO` do teste; o kit preenche os totais como o JS do FIP (crédito e teto sem cor; bloqueado e saldo com `pos`/`neg`).
- **Ainda reproduzido, com teste de equivalência:** o filtro de lista dos relatórios (o SPO tem uma cópia do `buildFilter` em cada relatório; o teste confere as classes e os textos dos dois lados).
- `tests/test_kit_mockup.py` (+3): as assinaturas lidas continuam no `main.js` e o kit as pede; as funções lidas não passaram a chamar outras do SPO (provado: uma chamada falsa a `syncThemeControls` dentro do `applyAccentColor` reprova); equivalência dos filtros.

Testado no kit: Clássico/Azul, claro/escuro, diagramas automático/alto contraste, densidade automática/ampla, "Padrão SPO" volta tudo; "Última atualização" escrita pela função do SPO (horário no fuso de Manaus); totais do FIP; fluxos, gráficos e teclado intactos; console sem erros.

#### Bateria 3: SPO e kit de mockup (dev. Jean, 2026-11-15) (antiga 41.97)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.97)._

Pedido do dev. Jean para fechar a rodada: bateria geral automatizada das opções de tema/personalização no SPO e no kit, com inventário e correções. Medição de contraste (AA 4,5:1 e AA+ 7:1), rolagem lateral, texto cortado, gráficos e erros de console, em 1366, 1080 e 390 px, uma tela por vez (roteiro local, fora do Git).
- **Kit:** todas as opções do painel, uma a uma, nos dois temas e em três larguras, mais os grupos abertos — 318 medições, duas vezes (antes e depois das correções).
- **SPO:** 18 telas em 5 perfis de preferências (270 medições), 7 telas com "Diagramas" no modo oposto ao tema (42) e a "fase 1" (cada opção do Personalizar clicada pela tela: 35 + 14, todas aplicam e gravam; prévias e oferta da Atkinson).
- Depois das correções: kit 318/318 sem achados; Contrato e Institucional limpos nos perfis de diagramas opostos e no de alto contraste. As opções desabilitadas das Notas SEE (5,15/6,53:1) são o estilo de desabilitado do §3.4, não defeito.
- Fora desta bateria: teclado/foco (testados à mão), leitor de tela, cores forçadas, Firefox/Safari e as telas fora da amostra (os relatórios pesados EMP, NOB e EST EMP ficaram de fora para não carregar o banco).

### 2026-11-14 — antigas 41.40 a 41.71

#### Superfícies que ficaram fora do tema: totais da Meta Física, "Escolher arquivo" e avisos no escuro (dev. Jean, 2026-11-14) (antiga 41.40)

Prints do dev. Jean durante a revisão da tipografia.

| Caso | Causa | Antes (medido) | Depois (medido) |
|---|---|---|---|
| Totais da Meta Física (`.meta-fisica-resumo-card`) | cores fixas de tema claro (fundo `#f6fafc`, borda `#c6d8e2`, texto `#334155`/`#0f172a`) | **escuro:** cartão claro sobre fundo escuro; **claro:** borda 1,47:1 e cartão 1,05:1 contra o fundo (quase some) | fundo `--surface`, borda dos campos `--spo-control-border`, texto `--muted`/`--text`. **Claro:** borda 3,47:1, textos 9,29/16,86:1. **Escuro:** fundo `#242424`, borda 4,40:1, textos 9,96/14,26:1 |
| "Escolher arquivo" (`::file-selector-button`) | o botão nativo nunca teve estilo e ficava claro no escuro | botão claro do navegador nos dois temas | botão em contorno, como os secundários (#60): borda dos campos, texto do tema, 12 px, destaque a 12% no mouse e no foco. Texto 16,86:1 (claro) e 14,26:1 (escuro), borda 3,47 e 4,40:1 |
| Avisos `.alert-info` no escuro (ex.: "Regras de segurança" da Limpeza de Histórico) | texto fixo `#0f4c4c` (verde escuro), sem versão escura | **1,46:1** | texto `--text`, fundo do destaque a 10%, borda dos campos: **11,65:1**, borda 4,40:1. No claro, sem mudança (8,86:1) |

- **Correção:** bloco `[jean]` no fim do `style.css`, com `:not(#spo-sup)` para vencer as regras antigas.
- **Verificação:** SPO local, telas Meta Física (formulário), Atualizar FIP 613 e Limpeza de Histórico, nos dois temas.
  - O "Escolher arquivo" ocupa praticamente o mesmo espaço do botão nativo. No campo estreito do Atualizar FIP 613 (painel a 1036 px), o texto "Nenhum arquivo escolhido" é abreviado pelo navegador, com o botão novo e também com o nativo (conferido desligando a regra).
  - `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS, agora `?v=20261114.jean-superficies.1` nos dois.

#### Login: com sessão já aberta, a segunda tentativa de entrar encerra a outra (dev. Jean, 2026-11-14) (antiga 41.41)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.41)._

Pedido do dev. Jean: o aviso "Usuário já conectado" (#61) não precisa dos botões "Continuar e entrar"/"Cancelar". Basta avisar; se a pessoa tentar entrar de novo, a sessão anterior é encerrada e o login segue.

Sem script de banco. `templates/base.html`: só a linha `?v=` do CSS. `templates/login.html`: o aviso e o script, e o `?v=` do CSS. CSS em `?v=20261114.jean-login-tentativa.1` nos dois.

#### Teto Orçamentário: as tabelas usam as cores do tema, como os gráficos (dev. Jean, 2026-11-14) (antiga 41.42)

Achado do dev. Jean: na aba **Tabelas** do Teto Orçamentário, a barra empilhada "Por grupo de despesa" (e a legenda e os pontos dos cartões) não acompanhava o tema, enquanto a aba **Gráficos** acompanhava.

- **Causa:** a aba Gráficos pega as cores da paleta do tema (`getTetoChartColors()`, que usa `getCommonDiagramTheme()`, #45). Os três quadros da aba Tabelas que colorem por grupo usavam a paleta fixa `groupColors` (`#345feb`, `#048075`, `#f8cb2e`): "Por grupo de despesa" (`renderGroupSummary`), "Teto por grupo e tipificação" e o QOMP. O comentário no código já dizia "o grupo de despesa tem sempre a mesma cor no app inteiro", mas a paleta dos gráficos passou a seguir o tema e a das tabelas ficou fixa.
- **Correção:** três linhas em `initTetoOrcamentarioDashboard` (`main.js`), uma por quadro. O `colorFor` passa a usar `getTetoChartColors().groupColors`, a mesma origem dos gráficos. A troca de tema já redesenhava as tabelas (`spo-common-visual-change` → `render()`), então nada mais precisou mudar.
- **Verificação:** SPO local, logado.
  - Escuro com destaque Clássico: barra, legenda e pontos com `#3dd6b7`, `#93c5fd` e `#fca5a5`, iguais à pizza da aba Gráficos.
  - Trocando para o claro, sem recarregar: `#1f9b7e`, `#1d4ed8` e `#b91c1c`, de novo iguais à pizza.
  - `node --check`; `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html`: só a linha `?v=` do JS, agora `?v=20261114.jean-teto-cores.1`. O CSS e o `login.html` não mudam.

#### Teto Orçamentário, aba Tabelas: fonte de dados e escala tipográfica (dev. Jean, 2026-11-14) (antiga 41.43)

Pergunta do dev. Jean: as fontes da aba **Tabelas** do Teto Orçamentário seguem o padrão? A medição mostrou que não. O passo 1 da escala (#75) mediu o Teto na aba Gráficos, que é a que abre primeiro.

- **Antes (medido):**
  - todos os valores (R$ e %) em **Inter**, quando o padrão de dados (§2.1) é a **Intel One Mono**, já usada no KPI da aba Gráficos;
  - variações ▲/▼ do comparativo com 10,5 px (81 elementos);
  - totais por ano, valores e % do ranking, códigos e "% do teto geral" com 11 px;
  - nomes de tipificação com 11,5 px e nomes de grupo com 12,5 px, ambos com entrelinha 1,3.
- **Correção** (bloco `[jean]` no fim do `style.css`):
  - os valores (`.teto-group-card .value`, `.teto-rank-val/-pct/-code`, `.teto-qomp-value/-delta`, `.yr`, `.yr-total`, `.teto-qomp-fonte-code`) seguem a **fonte de dados do Personalizar** (`data-spo-font-data`), como as tabelas e os totais dos relatórios;
  - os textos vão a **12 px**, e os nomes do comparativo a **entrelinha 1,5**;
  - o cabeçalho de coluna em maiúsculas e negrito ("GRUPO DE DESPESA", 11 px) fica, por ser rótulo;
  - **colunas de ano do comparativo** (QOMP, Fonte/Grupo, Grupo por Teto): o mínimo (`--qomp-w-ano`) era 150 px, ou 128 px até 720 px de janela, e cortava valores em bilhões. A 375 px eram 112 valores cortados já com a Inter, e 166 com a mono. Passa a **184 px**, que cabe "R$ 99.999.999.999,99" em 14 px na mono. Com pouco espaço, o quadro rola para o lado, como antes.
- **Verificação:** SPO local, logado, aba Tabelas.
  - Depois: todos os valores em Intel One Mono, todos os textos com 12 px ou mais (fora o rótulo), entrelinha 1,5.
  - Valores cortados: **0** a 375, 1036 e 1440 px, sem rolagem lateral da página.
  - Os nomes longos de fonte de recurso continuam abreviados com "…", como já eram.
  - `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS, agora `?v=20261114.jean-teto-tipografia.1` nos dois.

#### Cabeçalho padrão (cartão + ícone do menu) em Governança e Resultados, Personalizar e Estrutura do Planejamento (dev. Jean, 2026-11-14) (antiga 41.44)

Achado do dev. Jean: algumas telas não seguiam o cabeçalho da maioria do SPO, que é o título num cartão (`header.module-header`) com o ícone da tela à direita.

- **Inventário** (`templates/partials/`):
  - **sem cartão e sem ícone** (`div.page-header`): as 6 telas de Governança e Resultados (Mapa Conceitual, Estratégia e Cadeia de Valor, EAP e Políticas, Entregas-MT e OKR, Processos/Riscos/Projetos, Matrículas e Metas Físicas) e as 2 do Personalizar (Temas e Contrato Visual Protegido, no mesmo template);
  - **cartão sem ícone**: Programas, Ações/PAOE, Produtos e Componentes da Revista (template compartilhado), Componentes, Modelos de Chave, Catálogo de Chaves e Replicar exercício.
- **Correção, só nos templates:**
  - Governança e Personalizar passam à marcação do cabeçalho padrão (`<header class="page-header module-header">` + `.module-header-copy` + `.module-eyebrow` + `<h1>` + `<p>` + `<i class="bi …">`). Textos e atributos não mudam, e a classe `page-header` fica por compatibilidade;
  - na Estrutura do Planejamento, entra o `<i class="bi …">` no cartão que já existia.
  - **O ícone é o do item do menu** (`MENU_META` em `services/features.py`). No template compartilhado da Estrutura, ele sai de um mapa por `entity`. **Se o ícone do menu mudar, mudar também no template.**
  - Nenhum CSS novo: vale o `.module-header > i` existente, um quadrado na cor de destaque que segue o tema.
- **Fora desta mudança:**
  - **Programação PTA 2027**: cabeçalho próprio, com o painel de contexto à direita (tela imersiva do mockup v63r);
  - as 11 telas de formulário e consulta sem cabeçalho de página (Meta Física, Subação, Etapa, Dotação, Estorno de Dotação, Limpeza de Histórico), que dependem de decisão do dev. Jean.
- **Verificação:** SPO local, logado.
  - As 16 telas comparadas com o cabeçalho do Atualizar Teto Financeiro: mesmo fundo, borda de 1 px, cantos de 18 px, espaçamento 22/24 px, ícone de 54 px igual ao do menu, título de 24 px em negrito.
  - A 375 px: ícone de 46 px dentro do cartão, sem rolagem lateral (Estratégia, Temas, Programas, Catálogo).
  - Os cartões da cadeia de valor da Estratégia continuam funcionando.
  - `pytest` completo duas vezes seguidas.

Sem script de banco, sem mudança de CSS ou JS (o `?v=` não muda). `base.html`/`login.html` não mudam.

#### Personalizar: opção "Espaçamento entre linhas" (dev. Jean, 2026-11-14) (antiga 41.45)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.45)._

Passo 3 do plano de tipografia (padrão spo-aa+ §2.2): o usuário escolhe a entrelinha do texto corrido.

Sem script de banco. `templates/base.html`: só as linhas `?v=` do CSS e do JS, agora `?v=20261114.jean-entrelinhas.1`. `templates/login.html`: só o `?v=` do CSS.

#### Personalizar em tela estreita: escolha atual visível e amostra dentro do cartão (dev. Jean, 2026-11-14) (antiga 41.46)

Achado da bateria do WCAG 1.4.12 (espaçamento de texto), na combinação "Maior (125%)" + "Mais amplo (2,0)" a 375 px.

- **Antes:**
  - o resumo da escolha atual de cada grupo do Personalizar (`.visual-pref-toggle small`, com `nowrap` e reticências) ficava cortado ("Inter (padrão S…", "Legível (12 px, …"), e a pessoa não via o que tinha escolhido. Com o painel a 892 px, isso já acontecia sem nenhuma combinação;
  - a tabela de amostra das fontes ficava mais larga que o cartão (220 px em 182 px).
- **Correção** (bloco `[jean]` no fim do `style.css`):
  - o resumo **quebra linha** em vez de cortar. Vale para todos os grupos, inclusive os de Preferências comuns;
  - na amostra, cada linha **empilha como os totais dos relatórios** (#31/#56): rótulo e valor lado a lado quando cabem; se não, o valor desce inteiro para a linha de baixo, alinhado à direita (sugestão do dev. Jean). A rolagem dentro da amostra (`overflow-x: auto`) fica só como reserva.
- **Verificação:** SPO local, logado.
  - A 375 px, com o padrão e com a combinação: 0 de 9 resumos cortados; na amostra, rótulo e valor empilhados, com o valor inteiro, sem rolagem.
  - A 1440 px, o cartão do Personalizar tem 209 px úteis (grade de três colunas), e a amostra também empilha, com os valores inteiros.
  - Com o painel a 892 px: 0 resumos cortados ("Intel One Mono (padrão SPO)" e "Legível (12 px, padrão SPO)" passam a duas linhas, antes cortados).
  - Versões anteriores foram trocadas: `overflow-wrap: anywhere` quebrava as palavras letra a letra; a rolagem dentro da amostra deixava o valor cortado na borda até rolar. No resumo, `overflow-wrap: break-word` (só quebra uma palavra que não cabe sozinha na linha).
  - `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS, agora `?v=20261114.jean-pref-estreito.1` nos dois.

#### Padrão visual "spo-aa+": versão 2 (dev. Jean, 2026-11-14) (antiga 41.47)

_Atualizou o `docs/padrao-visual-spo-aa-plus.md` (só documentação); o conteúdo em vigor está nesse documento._

Atualização do `docs/padrao-visual-spo-aa-plus.md` com tudo o que entrou do #62 ao #86. **Só documentação.**

- **§1 Metas:** espaçamento de texto (WCAG 1.4.12) e escala tipográfica.
- **§2.2:** títulos de página aplicados (24 px, 700, 1,2); dados fora de `<table>` usam a fonte de dados; o texto dos gráficos (Plotly, 11 px) fica como pendência.
- **§3.3:** a tela de login completa a paleta do destaque num bloco próprio (#67). Se o `ACCENT_COLORS` mudar, mudar lá também.
- **§3.4:** valores `pos`/`neg` com as cores novas e medidas (#70); `.alert-info` no escuro (#79).
- **§3.5:** a cor de um grupo é a mesma em gráficos e tabelas (#81).
- **§4.2:** campo de arquivo (#79).
- **§4.4:**
  - cabeçalho de página com o ícone do item do menu (#84);
  - foco no título sem anel (#69);
  - totais com o valor descendo inteiro e o PLAN 51 por container query (#77);
  - quadros comparativos do Teto (#82);
  - Personalizar (#86);
  - login com a segunda tentativa (#80).
- **§5 Como medir:** foco com clique real; bateria de tipografia; **bateria do WCAG 1.4.12** (estilo forçado, antes × depois, o que descartar, gráficos olhados na captura); conferência do login nos 8 destaques.
- **§6.1 checklist:** cabeçalho padrão, escala, `pos`/`neg`, valor que desce inteiro.
- **§6.3 exceções:** paleta do login e valores `pos`/`neg` até virarem tokens.
- **§7 dívida:** tipografia resolvida (#75, #76, #82); restam o texto e as legendas dos gráficos, as telas de formulário sem cabeçalho, o PTA 2027, as duplicações (ícone do menu e paleta do login) e as áreas não medidas.

Sem mudança de código, sem script de banco. `base.html`/`login.html` não mudam.

#### Teto Orçamentário: indicador "Teto total" sem estourar o cartão (dev. Jean, 2026-11-14) (antiga 41.48)

Achado do dev. Jean (print): na aba Gráficos do Teto Orçamentário, o valor do indicador "Teto total" ("R$ 11.333.931.142,00") saía do cartão.

- **Causa:** os 4 indicadores (`.teto-kpi-grid`) dividiam a largura em partes iguais (`repeat(4, 1fr)`). Com a fonte de dados padrão (Intel One Mono, #63), mais larga, o valor não cabia.
- **Correção** (bloco `[jean]` no fim do `style.css`):
  - a coluna do Teto total passa a ter **mínimo de 15rem**, que cabe "R$ 999.999.999.999,00" (pedido do dev. Jean) em 16 px negrito na mono, cerca de 206 px de texto mais o espaçamento;
  - as outras três colunas dividem o resto, e o valor não quebra (`nowrap`);
  - nas faixas do painel (container `teto-dashboard`), 2 colunas até 820 px, com o mesmo mínimo na primeira, e 1 coluna até 560 px.
- **Verificação:** SPO local, com "R$ 999.999.999.999,00" posto no indicador. **Cabe** a 1440, 1106, 1024, 900, 768, 600, 375 e 320 px, sem rolagem lateral; a folga vai de 227 px (1440) a 1 px (320). `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS, agora `?v=20261114.jean-kpi-total.1` nos dois.

#### Gráficos na escala tipográfica e legíveis em tela estreita (dev. Jean, 2026-11-14) (antiga 41.49)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.49)._

Itens T1/T2 do inventário AA+ (equivalentes aos gates C09/C11.1 do laboratório): os gráficos (Plotly, todos no Teto Orçamentário) estavam fora da escala e quebravam em tela estreita e com espaçamento de texto forçado.

- **Antes (medido):**
  - todo o texto em **Arial 11 px**, fora da escala (mínimo 12 px) e fora da fonte escolhida no Personalizar;
  - rótulos do 2º eixo de Macropolíticas (20%…100%) **saindo do gráfico**, nas duas larguras, porque a margem direita era fixa;
  - legenda horizontal que **se sobrepõe** com espaçamento forçado (WCAG 1.4.12): 2 casos no grupo e 1 em Macropolíticas a 1366 px;
  - a 375 px, o "5,19%" da pizza cortado.
- **Verificação:** SPO local, logado, Teto Orçamentário (5 gráficos).
  - 1366 px: Inter 12 px; nenhum texto fora do gráfico; **0 sobreposições** com espaçamento forçado.
  - 375 px: nenhum problema sem espaçamento forçado. Com ele, resta 1 rótulo inclinado de Macropolíticas a 5 px da borda: o Plotly não remede um estilo imposto depois do desenho, e a tabela "Ver dados do gráfico" traz o mesmo conteúdo.
  - Voltando de 375 para 1366 px sem recarregar: nomes inteiros e margens originais.
  - O clique na pizza continua filtrando o "Grupo de despesa"; os 5 gráficos mantêm `role="img"` e a tabela de dados (#40); nenhum erro no console.
  - `node --check`; `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html`: só a linha `?v=` do JS, agora `?v=20261114.jean-graficos-escala.2`. O CSS e o `login.html` não mudam.

#### Leitura ampliada: largura do texto corrido e espaço entre parágrafos (dev. Jean, 2026-11-14) (antiga 41.50)

_Substituída pela 41.53 (parte em vigor no claude.md) (opção "Leitura" no Personalizar)._

Item "leitura ampliada" do C04 do laboratório (WCAG 1.4.8, AAA): linhas de texto corrido não muito longas e parágrafos bem separados.

- **Antes** (66 telas a 1440 px):
  - 31 blocos de texto corrido em 19 telas passavam de 80 caracteres por linha (até **188**, no texto de boas-vindas da Início);
  - 11 pares de parágrafos seguidos com 14–16 px entre eles, para entrelinha de 18 px (o critério pede ≥ 1,5 entrelinha, 27 px).
- **Correção** (bloco `[jean]` no fim do `style.css`):
  - texto corrido (`p`, `li`, `dd` e avisos `.alert`, fora de tabelas, menus e botões) com **`max-width: 72ch`**; o texto centralizado da Início continua centralizado;
  - parágrafos seguidos (`p + p`) com **1 entrelinha** de espaço (`1lh`, 18 px no texto de 12 px), que acompanha a opção "Espaçamento entre linhas" do Personalizar. **Decisão do dev. Jean:** 1,5 entrelinha (o que o AAA pede, ~33 px em branco) ficava grande; 1 entrelinha separa bem e atende ao AA (1.4.12).
- **Largura: decisão do dev. Jean.**
  - O `ch` é a largura do "0", mais larga que a média das letras. Com 72ch, a média medida (contagem exata dos caracteres de cada linha) fica em **84–110** caracteres por linha.
  - Ficar ≤ 80 em todos os textos exigiria ~56ch, o que deixaria metade dos cartões largos vazia a 1440 px. Foi escolhido o meio-termo, e o critério AAA fica **atendido em parte**.
- **Verificação:**
  - Início, Limpeza, Estratégia, Legislação e Replicar a 1440 px: média de 84–110 caracteres por linha, sem rolagem lateral;
  - parágrafos seguidos com 18 px de margem (antes 14–16 px), ou mais onde já havia margem maior;
  - `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS, agora `?v=20261114.jean-leitura.1` nos dois.

#### "Texto das tabelas" em todas as tabelas (dev. Jean, 2026-11-14) (antiga 41.51)

Pergunta do dev. Jean: as opções de "Fontes e tamanho do texto" valem em todas as telas? A conferência mostrou que o **"Texto das tabelas"** (Compacto 8 px, Intermediário 10 px, Legível 12 px) só alcançava as tabelas dos **relatórios** (`.fip-table`, `.plan20-table`), os filtros e a paginação. As demais tabelas do conteúdo (consultas de cadastro, usuários, Limpeza, API, estrutura do planejamento, Meta Física, tabelas de dados dos gráficos) ficavam com 12 px nas células e 13 px nos títulos, qualquer que fosse a escolha.

- **Correção** (bloco `[jean]` no fim do `style.css`): **Intermediário** e **Compacto** passam a valer para **todas as tabelas** de `#content-area`: células, títulos e o que está dentro deles (selos, links, códigos). Ficam de fora os campos e botões de formulário dentro das tabelas e a amostra do Personalizar. **Legível** (padrão) mantém o tamanho próprio delas, que já é ≥ 12 px.
- **Verificação:** Limpeza, Usuários (editar), Programas, Meta Física, FIP 613 e Personalizar, trocando a opção:
  - Legível: 12/13 px, como antes;
  - Intermediário: 10 px;
  - Compacto: 8 px;
  - os relatórios e a amostra seguem como já eram;
  - `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS, agora `?v=20261114.jean-texto-tabelas.1` nos dois.

#### Login com as fontes e o tamanho escolhidos no Personalizar (dev. Jean, 2026-11-14) (antiga 41.52)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.52)._

Sem script de banco. `templates/base.html`: só a linha `?v=` do CSS. `templates/login.html`: só as linhas `?v=` do CSS e do `login-theme.js`. As duas ficam com `?v=20261114.jean-login-fontes.1`.

#### Opção "Leitura" no Personalizar (dev. Jean, 2026-11-14) (antiga 41.53)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.53)._

O limite fixo de 72ch do texto corrido (seção "Leitura ampliada", #92) deixava muito espaço vazio nos cartões largos. A pedido do dev. Jean, ele virou **opção**: o WCAG 1.4.8 (AAA) aceita que a apresentação seja alcançada por "um mecanismo", e o padrão volta a ser a largura livre.

Sem script de banco. Ficam só nas linhas `?v=` (CSS, JS e `login-theme.js`, todos `?v=20261114.jean-leitura-opcao.1`): `templates/base.html` e `templates/login.html`. O grupo novo está em `templates/partials/atualizar_personalizar_spo.html`.

#### Alto contraste escuro dos diagramas com o fundo do cartão (dev. Jean, 2026-11-14) (antiga 41.54)

Na conferência C07 (comparação com o laboratório), o alto contraste dos diagramas passou nos 70 pares de contraste do `spoValidateDiagramContrast()`. Mas, no tema escuro, o gráfico aparecia numa caixa azul-marinho (`#111827`/`#0f172a`) dentro do cartão cinza: a mesma "paleta azulada" que o #45 tirou dos modos claro e escuro comuns, e que ficou no alto contraste só porque ele estava fora daquele PR.

- **Correção** (função `[jean]` `spoTemaDiagramaComDestaque`, 1 linha): no modo `highDark`, fundo transparente (herda o do cartão do gráfico). As cores das séries, o texto e a moldura continuam as do alto contraste. O alto contraste claro (fundo branco) e os modos comuns não mudam.
- **Contraste sobre o fundo real** (`#1a1a1a` do quadro do gráfico): o texto fica com 16,6:1, o texto secundário com 14,1:1 e as séries com ≥ 6,47:1 (o mínimo exigido é 3:1).
- **Verificação** no Teto Orçamentário, no tema escuro com "Diagramas: alto contraste": os 5 gráficos com fundo transparente e texto `#f8fafc`. O alto contraste claro continua com fundo branco, e os modos claro e escuro comuns continuam transparentes. A captura foi conferida, e as preferências do navegador voltaram ao estado de antes. `pytest` completo duas vezes seguidas e `node --check`.

Sem script de banco. `templates/base.html`: só a linha `?v=` do JS (`?v=20261114.jean-ac-escuro.1`).

#### "Visão e cores": cores dos gráficos seguras para daltonismo (dev. Jean, 2026-11-14) (antiga 41.55)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.55)._

Pedido do dev. Jean: um colega do planejamento tem daltonismo, de tipo não informado, e a Secretaria tem política forte de apoio a pessoas com deficiência. Os gráficos do SPO (Plotly, Teto Orçamentário) distinguem séries **só pela cor**. Na simulação de deuteranopia, o azul `#1d4ed8` e o roxo `#6d28d9` da paleta padrão ficam iguais (diferença CIEDE2000 de 0,4).

Sem script de banco. `templates/base.html` e `templates/login.html`: só as linhas `?v=` do `main.js` e do `login-theme.js` (`?v=20261114.jean-daltonismo.1`).

#### "Visão e cores": foco reforçado e links sublinhados (dev. Jean, 2026-11-14) (antiga 41.56)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.56)._

Segundo item do cartão "Visão e cores" (plano do dev. Jean para baixa visão e navegação por teclado).

Sem script de banco. `templates/base.html` e `templates/login.html`: só as linhas `?v=` (CSS, JS e `login-theme.js`, todos `?v=20261114.jean-foco-links.1`).

#### "Visão e cores": texto 150%, brilho reduzido e reduzir movimento (dev. Jean, 2026-11-14) (antiga 41.57)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.57)._

Terceiro e último item do cartão "Visão e cores" do plano do dev. Jean.

Sem script de banco. `templates/base.html` e `templates/login.html`: só as linhas `?v=` (CSS, JS e `login-theme.js`, todos `?v=20261114.jean-visao-6c.1`).

#### Telas de Usuários no celular (dev. Jean, 2026-11-14) (antiga 41.58)

Achado na medição do texto 150% (seção anterior). A 360 px, no tamanho padrão, as telas de Usuários (Cadastrar, Editar, Perfil, Senha) e a API de Acessos rolavam para o lado: 580 px de conteúdo em 296 px úteis.

- **Causa:** até 900 px, as telas com cabeçalho e duas colunas deviam passar a uma coluna (`.grid:has(> .module-header) { grid-template-columns: 1fr }`). Mas essa regra perdia, por especificidade, para a que define as duas colunas dessas telas (`.grid:has(> .module-header .bi-person-gear)` etc., com coluna mínima de 320 px).
- **Correção** (bloco `[jean]` no fim do `style.css`): a mesma regra de uma coluna até 900 px, com especificidade suficiente.
- **Verificação:** 9 telas (as 5 de Usuários e API, FIP 613 e PLAN 51 de Atualizar, Sessão e Login, Rede de Parceiros) em 360, 390, 768, 900, 1024 e 1366 px. Até 900 px, uma coluna e nenhuma rolagem lateral. Acima disso, duas colunas, como antes; o Perfil (Permissões) segue em uma coluna até 1100 px, pela regra própria. `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS (`?v=20261114.jean-usuarios-celular.1`).

#### Densidade automática (dev. Jean, 2026-11-14) (antiga 41.59)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.59)._

Sem script de banco. `templates/base.html` e `templates/login.html`: só as linhas `?v=` do JS e do `login-theme.js` (`?v=20261114.jean-densidade-auto.1`). Sem CSS novo.

#### Busca de telas (dev. Jean, 2026-11-14) (antiga 41.60)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.60)._

Fase 1 da busca combinada com o dev. Jean. Para achar uma tela, era preciso percorrer o menu, que tem vários níveis e 6 itens chamados só "Consultar".

Sem script de banco. `templates/base.html` e `templates/login.html`: só as linhas `?v=` do CSS e do JS (`?v=20261114.jean-busca-telas.1`).

#### "Seguras para daltonismo": o nome do método no texto da opção (dev. Jean, 2026-11-14) (antiga 41.61)

Pedido do dev. Jean depois de ver o cartão "Visão e cores": citar o método da paleta e dizer o que ela cobre.

- **Opção:** passa a se chamar "Seguras para daltonismo (paleta Okabe-Ito)".
- **Descrição:** Okabe e Ito, Universidade de Tóquio, 2008. Distingue-se nos três tipos de daltonismo (protanopia, deuteranopia e tritanopia) e nas formas mais leves. As listras, os pontos e os marcadores ajudam também quem não distingue cor nenhuma (monocromacia).
- **Não há opção por tipo de daltonismo, por decisão:**
  - quem usa nem sempre sabe o tipo;
  - os padrões e marcadores já separam as séries sem depender da cor;
  - cada paleta a mais teria de ser medida em claro, escuro e alto contraste.

  A decisão será revista se o teste com os colegas na homologação mostrar cores que ainda se confundem.
- **Escopo:** muda só o texto, sem mudança de comportamento. `pytest` completo duas vezes seguidas e `node --check`.

Sem script de banco. `templates/base.html`: só a linha `?v=` do JS (`?v=20261114.jean-rotulo-daltonismo.2`).

#### Correções da bateria completa do Personalizar (dev. Jean, 2026-11-14) (antiga 41.62)

Pedido do dev. Jean: depois dos ajustes do dia, conferir que o SPO reconhece e aplica todas as opções do "Personalizar Temas SPO" em todas as telas.

- **Fase 1:** as 17 opções e 56 combinações de opção e valor, escolhidas pela própria tela, ficaram gravadas, aplicadas e com o efeito esperado. O login aplica o perfil completo, e "Restaurar padrão" volta tudo.
- **Fase 2:** 66 telas × 4 perfis (padrão; escuro com Amarelo e reforçado; acessibilidade máxima; denso com Roxo) × 3 larguras (390, 1080, 1366), num total de 792 medições. Não houve nenhum erro no console, e os gráficos desenharam todos. Relatório completo nas notas do dev. Jean (`BATERIA_PERSONALIZAR_2026-11-14.md`).

**Achados reais e correções** (bloco `[jean]` no fim do `style.css`):

1. **Texto na cor de destaque "forte" ou pura:**
   - **onde:** números das etapas do Contrato Visual, selo e nota das Permissões, aviso de relatório vazio, botões da lista do Teto, submenu do Personalizar, selo de rascunho, "Contexto ativo" da Governança e "Área UENs" das Notas SEE;
   - **contraste antes:** com o Azul, 3,77:1; com o brilho reduzido, 3,24; com o Amarelo no claro, ~1,5;
   - **correção:** passam a `--spo-accent-text`, e os ícones de Permissões e Notas SEE a `--spo-accent-graphic`;
   - **conferido:** 8 destaques × claro, escuro e brilho reduzido, com pior caso de 5,92:1 (Amarelo com brilho reduzido); os demais ficaram ≥ 7,18.
2. **`<code>` com o rosa do Bootstrap (`#d63384`):** 3,45:1 no escuro (API de Acessos). Passa à cor do texto, com fundo leve.
3. **Permissões (Usuários → Perfil):** duas colunas com mínimos fixos de 500 e 520 px. A 1366 px com o menu aberto, rolava para o lado. Acima de 1100 px, as colunas passam a ser proporcionais, com força abaixo das regras de texto ampliado do #103 (que passam a uma coluna antes).
4. **Modelos de Chave:** colunas com mínimos de 420 e 520 px, uma coluna só abaixo de 1280 px. Com o texto ampliado, o limite é multiplicado pelo fator (1440/1600/1920 px).
5. **Formulários de Usuários:** colunas de 210 px no mínimo e campos que não encolhiam. No celular com texto ampliado, a tela rolava 4 px. Agora `minmax(min(100%, 210px), 1fr)`, e `.field` pode encolher.

**Verificação:**
- Perfil a 1366 px sem rolagem nos 4 tamanhos de texto e em uma coluna a 1080 px;
- Modelos de Chave sem rolagem nos 4 tamanhos;
- as 4 telas de Usuários a 390 px sem rolagem, no padrão e com 150%;
- `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS (`?v=20261114.jean-bateria.1`).

#### Declaração de acessibilidade "SPO-AA+" (dev. Jean, 2026-11-14) (antiga 41.63)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.63)._

Pedido do dev. Jean: depois do trabalho de visual e acessibilidade, dizer no próprio sistema quais padrões o SPO atende, inteiros ou em parte.

- **Link "Acessibilidade" no rodapé** (bloco `[jean]` `initLinkAcessibilidade` no fim do `main.js`, inserido pelo JS):
  - abre o Personalizar Temas e leva o foco ao título do cartão; já estando na tela, só rola até ele;
  - aparece só para quem tem a tela Personalizar Temas no menu, porque as permissões escondem o item e quem não a vê não veria o cartão.
- **Verificação** (SPO local):
  - **link pelo rodapé:** a partir da Início e no próprio Personalizar, com o foco no título do cartão;
  - **permissões:** escondendo o item do menu como as permissões fazem, o link some;
  - **celular:** a 390 px, os blocos em uma coluna, sem rolagem lateral;
  - **links:** os 19 externos com nova aba e aviso, contraste 10,53:1; endereços conferidos (todos respondem; o critério cai no texto certo da tradução);
  - `pytest` completo duas vezes seguidas e `node --check`.

Sem script de banco. `templates/base.html` e `templates/login.html`: só as linhas `?v=` (CSS `?v=20261114.jean-acessibilidade.2`, JS `?v=20261114.jean-acessibilidade.1`).

#### Atalhos do teclado (dev. Jean, 2026-11-14) (antiga 41.64)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.64)._

Pedido do dev. Jean: instruções de como usar o SPO só pelo teclado. O caso dele: no Mapa Conceitual as setas trocavam o cartão, mas não havia como saber que o Tab sai dele, nem um jeito curto de voltar ao menu.

- **Declaração de acessibilidade:** o item do teclado ganha o link para a ajuda, e o 2.1.4 entra em "Atendido por opção".
- **Verificação** (SPO local, teclas reais do painel):
  - **Mapa Conceitual:** clique num cartão, → troca e mostra a dica, Alt+M vai a "Administrar SPO" (grupo que contém a tela, menu recolhido), Enter abre o menu já no Mapa Conceitual;
  - **ajuda:** abre com o foco no "Fechar", Tab alterna lista e botão, Esc devolve o foco ao cartão. A tecla **?** foi testada por evento sintético, porque a ferramenta de teclado do painel não gera o "?";
  - **opção:** "Desligados" pela tela grava `keys: "off"`; ? e / não abrem nada, Ctrl+K abre a busca e a ajuda mostra o aviso; voltou a "Ligados";
  - **celular** (375 px): ajuda em tela cheia, sem rolagem lateral; dica visível; Alt+M no menu recolhido;
  - **escuro:** teclas com texto `#f6f5f4` sobre `#242424`;
  - `pytest` completo duas vezes seguidas e `node --check`.

Sem script de banco. `templates/base.html` e `templates/login.html`: só as linhas `?v=` (CSS, JS e `login-theme.js`, `?v=20261114.jean-atalhos.1`).

#### Alt+M: anel visível e Esc para sair do menu (dev. Jean, 2026-11-14) (antiga 41.65)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.65)._

Correção do "Atalhos do teclado" (#110), relatada pelo dev. Jean depois de testar:

- **"O Esc não fecha esse menu":** não havia saída do menu pelo teclado.
  - **Correção:** com o foco no menu, **Esc** devolve o foco para onde a pessoa estava antes do Alt+M (ou ao título da tela) e fecha o menu se ele estiver aberto por cima da tela. Os diálogos (busca, ajuda) continuam tratando o próprio Esc antes.
- A ajuda e a dica do menu dizem "Esc volta para onde você estava".
- **Verificação** (teclas reais, SPO local, menu recolhido): clique de mouse num cartão do Mapa Conceitual → Alt+M → "Administrar SPO" com anel de 3 px e dica → Esc → foco de volta no cartão, com anel. Também Alt+M → Enter (menu aberto por cima) → ↓ → Esc → menu recolhido e foco no cartão. `pytest` completo duas vezes seguidas e `node --check`.

Sem script de banco. `templates/base.html` e `templates/login.html`: só as linhas `?v=` (CSS e JS, `?v=20261114.jean-alt-m.1`).

#### O padrão SPO-AA+ para o usuário: resumo e documento técnico no servidor (dev. Jean, 2026-11-14) (antiga 41.66)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.66)._

Pedido do dev. Jean: a declaração de acessibilidade afirma o padrão "SPO-AA+"; o usuário precisa poder ver o que ele é, e o documento técnico, já consolidado, deve ficar disponível no servidor e se atualizar a cada ajuste.

- **Nome padronizado:** o documento técnico passa a "SPO-AA+" no título (o arquivo continua `padrao-visual-spo-aa-plus.md`).
- **Verificação** (SPO local):
  - API: 200 com 9 seções e 16 tabelas; nome desconhecido → 404;
  - janela: abre pelo link da declaração com o foco no título; vista técnica com sumário (o link leva à seção e dá o foco a ela); Tab preso; Esc fecha e devolve o foco ao link;
  - tamanho (ajuste pedido pelo dev. Jean, que achou a primeira versão grande demais): no computador, até 720 px de largura e 72% da altura (720 × 608 px numa janela de 1139 × 845); no celular, **cartão com margens** (343 × 585 px em 375 × 812), e não tela cheia como a busca, com os dois botões lado a lado; tabelas largas rolam dentro do próprio quadro, sem rolagem lateral da página;
  - contraste: texto 16,9:1 (claro) e 14,3:1 (escuro); links do sumário 10,5 e 12,9;
  - `pytest` completo duas vezes seguidas e `node --check`.

Sem script de banco. `templates/base.html` e `templates/login.html`: só as linhas `?v=` (CSS e JS, `?v=20261114.jean-padrao.1`).

#### Ponte de preferências para o PTA 2027 (dev. Jean, 2026-11-14) (antiga 41.67)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.67)._

Item combinado na bateria do Personalizar: o mockup "Programação PTA 2027" (iframe `static/pta2027_v63r`) recebia só o tema e a cor de destaque (`readThemePayload`/`postTheme`, que **não foram alterados**).

- **Atalhos dentro do mockup:** com o foco no iframe, as teclas não chegavam ao SPO. Agora Ctrl+K, "/", "?" e Alt+M voltam ao SPO pela mensagem `spo-atalho` (os de uma tecla respeitam a opção "Atalhos de uma tecla"); o Esc da busca e do menu devolve o foco ao mockup.
- **Declaração de acessibilidade:** o item do PTA 2027 em "Parcial ou em avaliação" foi atualizado.
- **Verificação** (SPO local, perfis da bateria aplicados nos atributos, sem gravar preferências):
  - padrão: fonte SPO Inter carregada, entrelinha 1,5, tabelas em Intel One Mono 12 px, anel `#1a4f91`;
  - escuro + reforçado + foco reforçado: borda 64% e texto secundário mais claro, anel na cor do destaque;
  - acessibilidade máxima (Atkinson, entrelinha 2,0, leitura estrita, links, brilho reduzido, movimento): parágrafo com 2,0 e no máximo ~48ch, cartões `#eceef1`, transições zeradas;
  - denso (Source Sans, tabelas compactas, IBM Plex, contraste médio): tabelas a 8 px em IBM Plex Sans, borda média;
  - teclas reais com o foco no mockup: Ctrl+K abre a busca e o Esc volta ao mockup; Alt+M vai ao menu e o Esc volta;
  - `pytest` completo duas vezes seguidas e `node --check` nos dois scripts.

Sem script de banco. `templates/base.html`: só a linha `?v=` do JS (`?v=20261114.jean-ponte.1`); o `index.html` do mockup: duas linhas novas.

#### Mockup PTA 2027 no padrão SPO-AA+ (dev. Jean, 2026-11-14) (antiga 41.68)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.68)._

Continuação da ponte (#113): adequar o próprio mockup "Programação PTA 2027" ao padrão. Nada foi mudado nos arquivos originais do mockup; tudo vale só no modo embutido com a ponte (`body.spo-ponte`).

- **Medição antes** (vista inicial, Azul, claro): 65 de 97 textos abaixo de 7:1 (4,5–5,3:1) e 31 abaixo de 12 px. No escuro, o mockup original já tinha textos de 2,2 a 4,2:1.
- **Alvos:** as caixas de marcar das etapas 3 e 4 tinham o rótulo clicável com 19 px de altura; agora 24 px (WCAG 2.5.8).
- **Pedidos do dev. Jean depois de ver o mockup:**
  - **listas suspensas:** a lista aberta era a do Windows (opção marcada no azul do sistema). Agora, como no SPO (listas nativas, #54/#55): superfície do tema, opção sob o mouse com 16% do destaque, ✓ e negrito na escolhida, seta que vira ao abrir. O campo fechado continua o do mockup. Firefox e Safari seguem com a lista nativa;
  - **botão "×" de remover linha:** passa ao nível "Dispensar" do SPO (padrão §4.1): contorno vermelho , sem preenchimento (no escuro ), "×" a 18 px; e ganha o nome "Remover linha" para leitor de tela (antes lia só "×").
- **`index.html` do mockup:** mais um `<link>` (o `spo-escala.css`, antes do `spo-preferencias.css`).
- **Declaração de acessibilidade:** o item do PTA 2027 passa a dizer o que foi medido e o que falta (teclado e leitor de tela dentro do mockup).
- **Verificação** (SPO local; o destaque e o tema foram simulados só na página, sem gravar, e restaurados no fim):
  - **bateria de cores:** 12 vistas (inicial, EAP, visão estratégica, visão processual, política, pilar e as 6 etapas da jornada) × 8 destaques × claro/escuro = 192 medições, 1.039 textos por combinação. Depois das últimas correções, os 4 piores casos (Vermelho e Roxo no claro, Clássico e Amarelo no escuro) foram medidos de novo: aba inativa ≥ 11,07:1 e cartão selecionado ≥ 7,14:1;
  - **tamanho:** nenhum texto abaixo de 12 px, fora os rótulos em maiúsculas de 11 px;
  - **celular** (375 px): nenhuma das 9 vistas com rolagem lateral; rótulos das caixas de marcar com 24 px;
  - `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `login.html`: sem mudança (a versão do CSS do mockup está no `index.html` dele, `?v=20261114.jean-mockup-aa.1`).

#### Tela "Programação PTA/OpR": nome, textos e ajustes da jornada (dev. Jean, 2026-11-14) (antiga 41.69)

Pedidos do dev. Jean depois de ver o mockup:

- **Nome "Programação PTA/OpR"** (PTA = Plano de Trabalho Anual; OpR = Orçamento para Resultado), no lugar de "Programação PTA 2027":
  - menu (`services/features.py`, só o `nome`; o `id` não muda); título da tela com as siglas em `<abbr title>` (o leitor de tela e o mouse mostram o nome por extenso); título do quadro por extenso;
  - no Personalizar, a amostra das fontes e o item da declaração de acessibilidade;
  - no mockup: título da página, "PTA/OpR · NOVA JORNADA DE PROGRAMAÇÃO", "PROGRAMAÇÃO PTA/OpR" e um aviso do rascunho (`pta-2027.js`);
  - busca de telas: "opr", "plano de trabalho anual" e "orçamento para resultado" acham a tela (glossário de `initBuscaTelas`).
- **Texto do cabeçalho** começa direto por "A jornada parte da estratégia…" (saiu "Primeira integração do mockup v63r").
- **Exercício** fora do cartão principal ("Da estratégia à entrega pública"); o campo não era lido pelo código.
- **"Anterior" na 1ª etapa da jornada:** o mockup desativava o botão, e não havia como voltar à tela inicial pela jornada. Agora, na 1ª etapa, ele volta à tela inicial (`returnToStrategicChain`, do próprio mockup), com o nome "Anterior: voltar à tela inicial da programação" para leitor de tela; das outras etapas, volta uma etapa, como antes (`spo-preferencias.js`).
- **O que não muda:** rota e permissão `atualizar/governanca-resultados/programacao-pta2027` (gravadas no banco), pasta e arquivos `pta2027`, o menu antigo desativado do `base.html` (o arquivo só muda no `?v=`) e documentos históricos.
- **Verificação** (SPO local): menu, título, aba do navegador "Programação PTA/OpR — SPO", texto do cabeçalho, cartão sem o exercício; "Anterior" com clique real na 1ª etapa volta à tela inicial, e da 2ª volta à 1ª; busca por "orçamento para resultado" acha a tela; `pytest` completo duas vezes seguidas e `node --check`.

Sem script de banco. `templates/base.html` e `login.html`: só a linha `?v=` do JS (`?v=20261114.jean-nome-pta.1`).

#### Dimensões do mockup PTA/OpR na escala do SPO (dev. Jean, 2026-11-14) (antiga 41.70)

Pedido do dev. Jean: os cartões do mockup estavam maiores que o padrão do SPO. Estudo e proposta aprovados por ele (título do cartão principal a 24 px, como o SPO).

- **Medição antes** (9 vistas, 41 tipos de componente): título principal 40,6 px; cartões com espaço de 12 a 28 px e cantos de 12 a 24 px; espaço entre cartões de 8 a 32 px; campos com 46 px; "Continuar rascunho" com 65 px (quebrava em 2 linhas). **Referência:** a tela Mapa Conceitual (Governança e Resultados, a referência de hierarquia do padrão §2.2) e as regras de densidade do `style.css`.
- **Escala aplicada** (seção "Dimensões" no fim do `static/pta2027_v63r/assets/spo-preferencias.css`, só no modo embutido; os arquivos do mockup não mudam):
  - cartão principal como o cabeçalho de página do SPO: espaço 22/24 px, cantos 18 px, título **24 px**;
  - títulos: página 24 · seção 20 · cartão 16 px;
  - seções e cartões: cantos 14 px; cartões pequenos: cantos e espaço 12 px;
  - espaço dos cartões e entre eles **pela densidade do Personalizar** (compacta 12/10, confortável 16/14, ampla 18/18): a densidade, que ainda não chegava ao mockup (#113), passa a valer nele;
  - campos com 40–42 px (espaço 10/12, cantos 10); botões com espaço 10/14, cantos 12 e mínimo de 40 px (44 px em tela de toque); "Continuar rascunho" numa linha a partir de 600 px.
- **Declaração de acessibilidade** e padrão §4.4 atualizados (a densidade vale no mockup).
- **Verificação** (SPO local):
  - medição depois nas 9 vistas: todos os tipos de componente na escala (títulos 24/20/16, cantos 14/12, campos 42 px, botões 42 px);
  - as três densidades aplicadas na página (sem gravar): 12/10, 16/14 e 18/18 px; de volta à automática no fim;
  - celular (375 px): nenhuma das 9 vistas com rolagem lateral; título principal em 2 linhas (eram 4);
  - capturas de antes e depois (tela inicial e etapa 1 no computador; tela inicial no celular) nas notas do dev. Jean;
  - `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` sem mudança; no `index.html` do mockup, só o `?v=` do `spo-preferencias.css` (`20261114.jean-escala.1`).

#### Amostra das fontes na Prévia do Personalizar (dev. Jean, 2026-11-14 e 15) (antiga 41.71)

Pedido do dev. Jean: a amostra de "Fontes e tamanho do texto" não tinha o mesmo acabamento da prévia "Componentes do SPO" (era uma frase solta e uma tabela sem cabeçalho).

- **Agora** (`templates/partials/atualizar_personalizar_spo.html`; bloco `[jean]` no fim do `style.css`): com a **mesma borda da prévia** (a amostra usa a classe `preview-surface` e o cartão, `personalizar-preview`, e por isso segue o estilo dos cartões, o contraste e o contrato visual; só a grade da prévia é desfeita), e cada passo da escala na fonte escolhida: sobretítulo 11 px, título 16 px, texto 14 px, as letras parecidas "Il1 O0 — ç ã é" numa linha própria e uma tabela de relatório com cabeçalho, linhas alternadas e valores à direita (crédito − empenhado = saldo).
- **A amostra saiu do cartão "Fontes e tamanho do texto" e ganhou cartão próprio de prévia** (decisões do dev. Jean): "Prévia · Fontes e tabelas" (`#spo-previa-fontes`), logo depois de "Prévia · Componentes do SPO" (que não mudou), os dois com uma coluna, lado a lado quando há espaço. Uma primeira versão juntava as duas prévias num cartão de largura toda, mas ele chamava atenção demais. Na tabela, nomes curtos ("Crédito", "Empenhado", "Saldo") para as três linhas ficarem iguais.
- **Daltonismo na prévia dos componentes** (relatado pelo dev. Jean: com "Cores dos gráficos: seguras para daltonismo" a prévia não mudava): os quatro nós do mini diagrama passam às quatro primeiras cores da paleta Okabe-Ito, com contorno, como nos gráficos. A paleta segue o fundo do diagrama (`data-visual-diagram-theme`): diagrama claro = `#0072B2 #E69F00 #009E73 #D55E00` com contorno `#1f2328`; escuro = `#56B4E9`… com contorno `#f5f5f5`.
- **Ajustes pedidos pelo dev. Jean ao ver a primeira versão:** sem a moldura grossa (fundo de página em volta do cartão), sem a linha de metadados, "Compare as letras:" e as letras em linhas separadas, e a borda igual à da prévia.
- As opções que já agiam na amostra continuam: fonte da interface e dos dados, tamanho, texto das tabelas, entrelinha e leitura. Em coluna estreita, o valor desce inteiro (#86).
- **Verificação** (SPO local): tamanhos e fontes conferidos (Inter na interface, Intel One Mono nos dados); contraste do sobretítulo, metadados, legenda e cabeçalho entre 9,3 e 15,4:1 (claro) e 10 e 14,3:1 (escuro); borda igual à da prévia em 6 casos (padrão; cartões contornado e plano; contraste reforçado; presets "mobile" e "contraste" do contrato visual); os dois cartões lado a lado a 1366 px (grade de 4 colunas), 1057 px (3) e 768 px (2), empilhados a 375 px; sem rolagem lateral; `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `login.html`: só a linha `?v=` do CSS (`?v=20261114.jean-amostra.8`).

### 2026-11-13 — antigas 41.12 a 41.39

#### Superfícies do tema: filtros, paginação e caixas de seleção (dev. Jean, 2026-11-13) (antiga 41.12)

**Problema:** peças antigas tinham cores fixas de uma paleta cinza-azulada em vez das variáveis do tema (cinzas neutros `#1e1e1e`/`#242424`/`#3d3d3d`): lista aberta do filtro (`.mf-panel`, `#11171d`), Cancelar/Aplicar (`.mf-btn`, `#1e2833`), paginação e "itens por página" (`#111827`, borda `#1f2937`, a ~1,1:1 sobre o cartão) e rodapé de totais das tabelas (`.table-foot`, `#0f1418`). No escuro a lista do filtro parecia de outro sistema. As caixas de seleção não tinham `accent-color` e ficavam no azul do navegador nos dois temas (só a árvore de permissões tinha cor própria). No claro, as bordas da paginação, de Cancelar e da lista do filtro ficavam em 1,36:1 (mínimo 3:1, WCAG 1.4.11). O selo "editando" dos cadastros tem `color:#111827` no HTML, ilegível no escuro.

**Correção (só CSS, bloco `[jean]` no fim do `style.css`):** essas peças passam a usar `var(--card)`, `var(--border)`, `var(--text)` e `var(--spo-control-border)`; caixas de seleção e opções com `accent-color` — no claro o tom escuro do destaque (`--spo-accent-text`), no escuro `--accent` (a mesma cor do botão Aplicar); selo "editando" com `var(--text)` no escuro. Os diagramas e gráficos do Contrato Visual têm paleta azulada própria e intencional e ficaram como estão.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean. Varredura em 30 telas no escuro procurando fundos/bordas azulados: antes, 8 relatórios com as peças acima; depois, nenhum (fora os diagramas). Nos 8 destaques × 2 temas: caixa marcada sobre o cartão ≥ 4,3:1 (escuro) e ≥ 8,17:1 (claro); bordas da paginação e da lista 4,04:1 (escuro) e 3,69:1 (claro). Capturas antes/depois. `pytest` completo duas vezes seguidas: 192 passam, 4 pulam. Sem script de banco. `templates/base.html`: `?v=20261113.jean-superficies.1` no CSS (JS sem mudança).

#### Zoom, telas estreitas e "Texto das tabelas" no Personalizar (dev. Jean, 2026-11-13) (antiga 41.13)

_Substituída pela 41.85 (parte em vigor no claude.md) ("Densidade do texto" no lugar de "Texto das tabelas")._

**Medição:** 30 telas a 320 px de largura (WCAG 1.4.10), a 640 px (200% de uma tela de 1280, 1.4.4) e com espaçamento de texto ampliado (altura de linha 1,5, letras +0,12 em, palavras +0,16 em; 1.4.12). Nenhuma página rola de lado nem corta texto; as tabelas largas rolam dentro da própria caixa. Dois achados:
1. a ≤ 420 px o cabeçalho cortava com "…" o nome do sistema e a linha do usuário (data e "Logados"), em todas as telas;
2. o texto das tabelas dos relatórios (`.fip-table`, `.plan20-table`) é de **8 px**, com títulos de coluna de 10 px, filtros de 8 px e paginação de 10 px. Com 12 px nas mesmas colunas, as linhas ficam cerca de 2× mais altas (FIP 613: 65 → 143 px).

**Correção:**
- **Cabeçalho:** a ≤ 420 px o nome e a linha do usuário quebram a linha em vez de cortar (o topo passa de 83 para 107 px).
- **Texto das tabelas:** decisão do dev. Jean — virou escolha no Personalizar, cartão "Fontes e tamanho do texto", grupo **"Texto das tabelas"**: **Compacto (padrão SPO)**, que é como hoje (8 px); **Intermediário (10 px)**, do tamanho dos títulos das colunas; ou **Legível (12 px)**, indicado para baixa visão. Nas duas opções maiores, tabelas, filtros e paginação dos relatórios mudam juntos.
  - Mesma mecânica das fontes: `localStorage` `spo-font-preferences`, chave `table`, atributo `data-spo-table-text` no `<body>`.
  - Mudanças: 2 linhas em `initFontesPersonalizar` (`main.js`), o grupo no `atualizar_personalizar_spo.html` e CSS em bloco `[jean]` no fim do `style.css`.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean.
- Menor texto nas tabelas: 8, 10 e 12 px nos três modos, nos 9 relatórios com dados.
- Altura média das linhas (10 primeiras, a 1366 px) nos três modos: FIP 613 48 → 59 → 75 px; PLAN 20 112 → 170 → 204; PED 175 → 215 → 255.
- Voltando para "Compacto", tudo volta a 8 px.
- O ✓ da opção escolhida (A11) ganhou folga para não encostar no texto da descrição.
- A amostra do cartão mostra a tabela no tamanho escolhido em "Texto das tabelas" (antes ficava fixa em 12 px). Os dois ajustes se combinam: o "Tamanho do texto" amplia a página inteira, tabelas incluídas (ex.: Maior + Legível = 12 × 1,25 = 15 px na tela).
- A 320 px, o cabeçalho fica sem "…" e sem rolagem lateral.
- `node --check` ok; `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.
- Sem script de banco. `templates/base.html`: `?v=20261113.jean-zoom.1` no CSS e no JS.

#### Contornos dos controles na cor de destaque, em três níveis (dev. Jean, 2026-11-13) (antiga 41.14)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.14)._

**Problema:** para garantir 3:1 nos contornos (WCAG 1.4.11), um bloco anterior (contornos de controles, AA) usou um **cinza neutro fixo** (`--spo-control-border`: `#86848c` no claro, `#828282` no escuro) em todos os controles, inclusive nas linhas de opção do Personalizar. Isso trouxe dois efeitos:
- a cor de destaque sumiu dos controles; a prévia do Personalizar seguia o destaque, os cartões ao lado não;
- com todas as linhas contornadas com força, a **selecionada** quase não se distinguia das outras.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean, no Personalizar, nos 8 destaques × 2 temas, medido contra o cartão e o fundo da página.

| Elemento | Contraste padrão | Contraste reforçado |
|---|---|---|
| Campos, filtros e botões | 3,02–5,91:1 | 4,51–12,01:1 |
| Linha em repouso | 1,17–2,36:1 (suave, de propósito) | ≥ 4,7:1 |
| Linha selecionada | 4,3–12:1 | 7,5–13,1:1, sempre acima das outras |

Capturas antes/depois. `pytest` completo duas vezes seguidas: 192 passam, 4 pulam. Sem script de banco. `templates/base.html`: `?v=20261113.jean-contornos.3` no CSS (JS sem mudança).

#### Diagramas e gráficos na cor de destaque (dev. Jean, 2026-11-13) (antiga 41.15)

**Problema:** no tema escuro, os diagramas no modo "automático" ou "escuro" usavam uma **paleta azul fixa**, qualquer que fosse o destaque escolhido:
- fundo `#0f172a`/`#111827`;
- nós `#252b36`;
- bordas e conectores `#93c5fd`.

Isso valia para os gráficos do Teto, os cartões do fluxo (Mapa Conceitual e afins) e a prévia do Personalizar. O modo "claro" de diagrama usava um azul fixo, `#2563eb`. Além disso, a paleta dos gráficos no claro tinha cores claras demais para fundo branco: um amarelo `#f8cb2e`, a 1,6:1.

**Correção:**
- **CSS** (bloco `[jean]` no fim do `style.css`), nos diagramas resolvidos como "claro" ou "escuro" (`data-visual-diagram-theme`):
  - **fundo neutro e igual para todos os destaques:** branco puro no claro, `#1a1a1a` no escuro, com nós `#ffffff`/`#262626`. Uma primeira versão tingia o fundo com o destaque; isso apagava as séries (efeito "sépia") e foi descartada;
  - **só bordas, conectores, ícones e seleção seguem o destaque** (`--spo-accent-graphic`): no claro, o passo da paleta com ≥ 3:1; no escuro, o `--accent`;
  - a prévia do Personalizar acompanha;
  - os cartões do fluxo ganham os três níveis dos contornos: suave em repouso, forte no mouse/foco, destaque cheio (2 px) quando selecionado.
- **JS** (bloco `initDiagramasDestaque` no fim do `main.js`): como no AA+ da v4.3.1 (`spo-aa-plus-plotly.js` do laboratório), a paleta dos gráficos passa a ser o **destaque seguido de 7 cores da v4.3.1**.
  - No claro, tons fundos e saturados: `#0f766e`, `#1d4ed8`, `#b91c1c`, `#6d28d9`, `#0369a1`, `#334155`, `#c2410c`.
  - No escuro, tons claros e vivos: `#5eead4`, `#93c5fd`, `#fca5a5`, `#c4b5fd`, `#67e8f9`, `#e2e8f0`, `#fb923c`.
  - A cor mais parecida com o destaque vai para o fim, para as séries continuarem distintas.
  - O fundo do Plotly fica transparente; o fundo vem do `.teto-chart`.
  - Seleção, foco e conector passam para o destaque.
  - **Função existente alterada:** `getCommonDiagramTheme` ganhou 2 linhas, que chamam `window.spoTemaDiagramaComDestaque` se ela existir; sem ela, o comportamento é o anterior.
- **Alto contraste:** continua com as cores fixas, de propósito.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean, nos 8 destaques × 2 temas, todas as séries de todos os gráficos do Teto.
- Menor contraste de qualquer série sobre o fundo: **3,29:1** no claro e **4,82:1** no escuro (mínimo para gráficos: 3:1).
- **Mapa Conceitual:** cartão selecionado com ≥ 3,29:1; cartões em repouso suaves.
- Sem erro no console. Capturas antes, intermediárias (prints do dev. Jean) e depois.
- `node --check` ok; `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

Sem script de banco. `templates/base.html`: `?v=20261113.jean-diagramas.3` no CSS e no JS.

#### Contraste em três níveis: Suave (padrão SPO), Médio e Reforçado (dev. Jean, 2026-11-13) (antiga 41.16)

**Problema:** a preferência "Contraste" (Personalizar → Preferências comuns) tinha só dois níveis:
- **Padrão:** linhas de opção e cartões clicáveis com contorno leve, e só a seleção e o foco fortes;
- **Reforçado:** tudo com ≥ 4,5:1.

Faltava um meio-termo para quem quer opções e cartões clicáveis sempre nítidos, sem o peso do Reforçado.

**Correção:**
- **`static/js/main.js`:** em `COMMON_VISUAL_OPTIONS.contrast` (lista que já existia), o rótulo de `standard` passa a "suave (padrão SPO)" e entra o valor novo `medium` ("médio").
  - O valor gravado do padrão continua `standard`, então quem já tinha a preferência salva não vê mudança.
  - Nada mais no JS foi alterado; `normalizeCommonVisualPreferences` aceita o valor novo porque lê essa lista.
- **`templates/partials/atualizar_personalizar_spo.html`:** botão "Médio" e descrição nova do "Suave (padrão SPO)".
- **CSS** (bloco `[jean]` no fim do `style.css`): no Médio, as linhas de opção e os cartões clicáveis do fluxo (Mapa Conceitual e afins) usam `--spo-control-border` (≥ 3:1 na cor de destaque); os cartões informativos continuam leves.
  - No Reforçado, os cartões do fluxo também ficam fortes; antes ficavam leves.
  - Verde no claro: o contorno passa ao `--color-600`, porque o `--color-500` dava 2,9:1 sobre o fundo `#f6f5f4`.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean.
- Escolha pelo controle real: grava `contrast: "medium"` e o resumo mostra "médio".
- Medido nos 3 níveis × 8 destaques × 2 temas:

| Nível | Linhas de opção em repouso | Cartões do Mapa em repouso | Seleção |
|---|---|---|---|
| Suave | 1,33–2,4:1 (leve, de propósito) | leve | 4,3–12:1 |
| Médio | 3,13–5,84:1 | 3,41–5,91:1 | 4,3–12:1 |
| Reforçado | 4,7–11,9:1 | 5,12–12:1 | sempre a mais forte |

- Campos e filtros: ≥ 3,17:1 no Suave e no Médio.
- Capturas; `node --check` ok; `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

Sem script de banco. `templates/base.html`: `?v=20261113.jean-contraste.2` no CSS e no JS.

#### Plano de fundo da página mais neutro e com mais separação dos cartões (dev. Jean, 2026-11-13) (antiga 41.17)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.17)._

**Problema:** o fundo da página quase não se separava dos cartões, e tudo parecia "chapado":
- **claro:** `#f6f5f4`, um cinza quente, levemente rosado, contra cartões `#fff`: **1,09:1**;
- **escuro:** `#1e1e1e` contra cartões `#242424`: **1,07:1**.

No claro, os botões das Preferências comuns também tinham o fundo tingido pelo destaque (4%) em repouso, o que dava um ar creme.

A ideia constava do C13 ("superfície/plano de fundo") do laboratório do AA+, que não chegou a ser implementado.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean. Medi todos os textos, cada um contra o fundo real atrás dele, nas telas Personalizar, PED e Teto, nos 8 destaques × 2 temas.
- **Claro:** menor contraste de 7,6:1 (Azul, depois do ajuste: 8,03:1 ou mais).
- **Escuro:** menor contraste de 6,19:1, no número da página ativa da paginação com o Amarelo. Esse caso é anterior a esta mudança e fica anotado.
- Capturas antes/depois. `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

Sem script de banco. `templates/base.html` e `templates/login.html`: `?v=20261113.jean-fundo.2` no CSS (JS sem mudança).

#### Abas pelo teclado e página ativa da paginação (dev. Jean, 2026-11-13) (antiga 41.18)

**Problemas:**
- **Abas do Contrato Visual Protegido:** cada seta andava **duas** abas (de Presets, → ia para "Editor local"). As abas já tinham `role="tab"` e tratavam as setas (`initProtectedContractTabs`); o bloco de teclado do AA+ (`[jean] Semântica e teclado`) tratava a mesma tecla de novo.
- **Consulta de Estrutura (Cadastros / Vínculos):** as abas não diziam ao leitor de tela o que controlavam. Usam `data-report-view` e mostram a mesma tabela; o bloco do AA+ só liga `aria-controls` por `data-view`.
- **Paginação no escuro:** o número da página ativa ficava sobre 25% da cor de destaque. Com o Amarelo, **6,19:1** (o AA+ pede 7:1). A página ativa só se distinguia pela cor.

**Correção:**
- `main.js`, bloco `[jean] Semântica e teclado`: o tratamento das setas sai quando a tela já tratou a tecla (`ev.defaultPrevented`). Nova função `marcarPaginaAtual`: `aria-current="page"` no botão `.active` de toda `.pagination`.
- `relatorios_estrutura_planejamento.html`: as abas ganham `id` e `aria-controls="planning-report-panel"`; o cartão da tabela vira `role="tabpanel"`, rotulado pelo título que muda com a consulta (`#planning-report-title`).
- `style.css`, bloco `[jean]` no fim: no escuro, a página ativa fica com fundo a 16% do destaque, texto `var(--text)` e borda `var(--accent)`.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean.
- Contrato Visual: → quatro vezes passa por Regras, Editor, Pacote e Auditoria e volta a Presets; ←, Home, End, ↑ e ↓ andam uma aba.
- Consulta de Estrutura: abas ligadas ao painel; → troca para Vínculos ("Função × Programa") e volta.
- Paginação no escuro, 8 destaques: texto da página ativa de **9,21:1** (Amarelo) a 11,64:1; borda de 4,30:1 (Roxo) a 12,01:1 sobre o fundo da paginação; `aria-current` só na ativa. O claro não muda.
- `node --check static/js/main.js`; `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

Sem script de banco. `templates/base.html`: `?v=20261113.jean-abas-paginacao.1` no CSS e no JS.

#### Listas dos filtros: Esc fecha e opção sob o mouse legível no escuro (dev. Jean, 2026-11-13) (antiga 41.19)

**Problema:** as listas abertas pelos filtros só fechavam clicando fora, em Cancelar ou em Aplicar. Quem usa o teclado ficava sem saída rápida, e o foco continuava dentro da lista.

**Correção (só JS, bloco `[jean] Esc fecha as listas dos filtros` no fim do `main.js`):** com uma lista aberta, Esc fecha a lista e devolve o foco ao botão que a abriu.
- **Filtros dos relatórios (`.mf-panel`):** Esc vale como **Cancelar**. A marcação ainda não aplicada é descartada, como no botão.
- **Listas de marcar do Teto e da Estrutura (`.planning-action-checklist-panel`):** Esc só fecha, porque a marcação já vale na hora.
- A tecla é tratada na captura do `document`, registrada no carregamento. Ela roda antes do Esc dos avisos modais: o primeiro Esc fecha a lista, o segundo, o aviso. Sem lista aberta, o Esc não faz nada aqui.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean, com Esc real do teclado.
- **FIP 613:** com uma opção desmarcada e o foco na busca da lista, Esc fecha a lista, a opção volta, o filtro continua "14101", o foco vai ao botão e `aria-expanded` vira `false`.
- **Teto Orçamentário:** Esc fecha a lista de marcar e devolve o foco ao botão. Um segundo Esc, sem lista aberta, não faz nada.
- Sem erro novo no console; `node --check static/js/main.js`; `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

**Opção sob o mouse no escuro:** nas listas de marcar (Teto, Estrutura), a opção sob o mouse ganhava o fundo `--color-100`, um tom claro do destaque, e o texto claro do tema escuro ficava ilegível (achado do dev. Jean nas telas de teste). Correção no bloco `[jean]` do fim do `style.css`: no escuro, o fundo passa a 16% do destaque (`rgba(var(--accent-rgb), 0.16)`), com texto `var(--text)`. Medido com o mouse real sobre a opção no Teto, nos 8 destaques: de 9,21:1 (Amarelo) a 11,64:1. O claro não muda (de 12,81 a 14,83:1).

Sem script de banco. `templates/base.html`: `?v=20261113.jean-esc-filtros.1` no CSS e no JS.

#### Selo "editando" dos cadastros na cor do tema (dev. Jean, 2026-11-13) (antiga 41.20)

**Problema:** o selo que aparece ao lado do título nos cadastros ("- Edição do registro …", "- Aprovação do registro …") tinha a cor fixa `#111827` no HTML de 5 parciais. No escuro, ficava ilegível; até aqui, isso era compensado com uma regra `!important` no `style.css`.

**Correção:**
- **5 parciais** (`cadastrar_dotacao`, `cadastrar_est_dotacao`, `cadastrar_plan21_nger_etapa`, `cadastrar_plan21_nger_meta_fisica`, `cadastrar_plan21_nger_subacao`): o `style` do selo fica só com `display:none;`, que o JS continua trocando (`none`/`inline`). Cor, peso e margem passam para a classe `spo-editing-badge`.
- **`style.css`:** sai a regra antiga `.theme-dark [id$="-editing-badge"] { … !important }`, e entra um bloco `[jean]` no fim com `.spo-editing-badge` (cor `var(--text)`, peso 500, margem 6 px).
- **`templates/base.html`:** só a linha `?v=` do CSS.
- O JS não muda.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean, nas 5 telas com o selo visível.
- **Escuro:** 14,26:1. **Claro:** 16,86:1.
- Mesmo peso (500) e mesma margem (6 px) de antes. A cor segue o texto do tema, então vale para os 8 destaques.
- `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

Sem script de banco. `templates/base.html`: `?v=20261113.jean-selo-editando.1` no CSS (JS sem mudança).

#### Listas nativas (`<select>`) na cor do tema (dev. Jean, 2026-11-13) (antiga 41.21)

**Problema:** a lista aberta de um `<select>` era desenhada pelo navegador e pelo Windows, com o fundo do sistema e a opção sob o mouse no azul "Highlight", fora do tema e do destaque. Isso acontecia em todos os `<select>` do SPO (achado do dev. Jean na Subação). As listas montadas pelo SPO (filtros `mf-*`, listas de marcar) já seguiam o tema.

**Correção (só CSS, bloco `[jean]` no fim do `style.css`):** usa o *select personalizável* (`appearance: base-select`, Chrome e Edge 135+), dentro de `@supports`.
- **Lista aberta:** superfície do tema, borda dos controles e cantos de 10 px.
- **Opção sob o mouse ou no foco:** 16% do destaque, como nas listas dos filtros.
- **Opção escolhida:** ✓ e negrito, para não depender só da cor.
- **Campo fechado:** mantém as medidas. A seta volta a ser fina, na ponta direita, e gira quando a lista abre (sem animação com movimento reduzido).
- Vale só para `<select>` de uma escolha (sem `multiple`/`size`), que é o caso de todo o SPO.
- Firefox e Safari ignoram o bloco e seguem com a lista nativa.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean.
- **Campo fechado:** em 43 `<select>` de 5 telas (Subação, Dotação, Meta Física, Consulta de Estrutura, FIP 613), largura, altura, espaçamento, borda e fundo ficaram iguais com e sem o recurso.
- **Contraste do texto das opções:**

  | Tema | Em repouso | Sob o mouse (8 destaques) |
  |---|---|---|
  | Escuro | 14,26:1 | 9,21 a 11,64:1 |
  | Claro | 16,86:1 | 12,99 a 14,95:1 |

- **Teclado:** ↓↓ e Enter escolhem a opção, disparam um `change`, fecham a lista e devolvem o foco ao campo.
- Capturas antes/depois. `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

Sem script de banco. `templates/base.html`: `?v=20261113.jean-listas-nativas.2` no CSS (JS sem mudança).

#### Listas nativas: opções longas e valor longo no campo fechado (dev. Jean, 2026-11-13) (antiga 41.22)

Correção da seção anterior, achada no pré-teste do roteiro da homologação, com listas reais longas: a Subação da Dotação tem 471 opções, com até 345 caracteres.

**Problemas:**
- **Lista aberta:** com opções longas, abria com **1.945 px** de largura numa janela de 1.070. O texto passava da borda direita da tela e a lista não ficava alinhada ao campo.
- **Campo fechado:** com um valor longo escolhido, crescia para **94 px**, em 3 linhas. O nativo fica em 40 px e corta o texto.
- **"Itens por página" dos relatórios:** ficava sem a seta e 13 px mais estreito. A seta era apagada por uma regra antiga `.theme-dark .page-size .input { background: … }`, de especificidade (0,3,0), e o nativo mede a largura pela opção mais larga, não pela escolhida.

**Correção (no mesmo bloco `[jean]` do fim do `style.css`):**
- A lista aberta tem no máximo `min(40rem, 100vw − 16px)`, e o texto da opção quebra linha, sem perder informação.
- O campo fechado fica numa linha: `white-space: nowrap`, `overflow: clip` e `overflow-clip-margin: content-box`. O texto é cortado antes da seta, com 34 px reservados à direita.
- A seta passa a ser desenhada no **fundo** do campo, com dois `linear-gradient` em `currentColor`, e vira para cima com a lista aberta. Assim ela não é cortada junto com o texto. A regra da seta leva `:not(#spo-seta)`, que só soma peso de id, para vencer as regras antigas com `background:`. O `::picker-icon` fica oculto.
- "Itens por página": `min-width: 8.3em`.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean.
- **63 `<select>` de 17 telas**, nos dois temas, com o **valor mais longo** escolhido em cada um: todos com seta e com a mesma altura do nativo. A largura só muda nos "Itens por página", e depois do ajuste fica igual nos três tamanhos de "Texto das tabelas" (83, 83 e 100 px).
- **Lista longa:** End vai à 471ª opção, Enter escolhe, a lista fecha e o foco volta ao campo.
- **Celular** (emulado a 375 px): a lista da Etapa, com 439 opções, fica dentro da tela (95 a 370 px) e alinhada ao campo. O toque real fica para a homologação.
- `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

Sem script de banco. `templates/base.html`: `?v=20261113.jean-listas-longas.3` no CSS (JS sem mudança).

#### Totais dos relatórios de execução em duas colunas sempre que cabem (dev. Jean, 2026-11-13) (antiga 41.23)

**Problema** (apontado pelo Cleber, comparando com a produção): no FIP 613, o quadro "Totais (filtros aplicados)" aparecia em **uma coluna de 6 linhas**, e na produção aparece em duas. O A2 (#31) trocou as duas colunas fixas por colunas que se ajustam à largura, com mínimo de **14rem** por coluna. Assim, duas colunas só cabiam com o quadro a partir de ~470 px. A 1.600 px com o menu aberto o quadro tem **467 px**: caía para 1 coluna e a faixa crescia de 112 para **176 px**, tirando espaço da tabela.

**Correção (só CSS, no bloco `[jean] Totais dos relatórios` do `style.css`):**
- mínimo de **12rem** por coluna;
- espaço entre colunas de 12 px (era 20);
- espaço entre rótulo e valor de 6 px (era 8).

O par mais largo ("Bloqueado/Conting.:" com o valor) precisa de ~199 px, e cada coluna passa a ter ~200. Quando duas colunas não cabem, continua em uma, com o valor descendo para a linha de baixo se preciso: o comportamento do A2, sem cortar valores.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean, com dados reais (Créd. Autorizado 5.914.425.031,88).

| Largura | Menu | Colunas | Observação |
|---|---|---|---|
| 1.920 e 1.600 px | aberto ou fechado | **2** | nos 6 relatórios (FIP 613, PED, PLAN 20, EMP, EST EMP, NOB), sem valor cortado nem quebrado. A 1.600 com o menu aberto, a faixa voltou a 112 px. |
| 1.366 px | fechado | **2** | |
| 1.366 px | aberto | 1 | o quadro tem 317 px, e duas colunas cortariam os valores, como antes do A2 |
| 1.024 e 390 px | — | 1 | sem corte e sem rolagem lateral |

- `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

Sem script de banco. `templates/base.html`: `?v=20261113.jean-totais-colunas.1` no CSS (JS sem mudança).

#### Controles dos formulários: campo desabilitado no escuro e botões de decisão mais vivos (dev. Jean, 2026-11-13) (antiga 41.24)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.24)._

**Problemas** (achados do dev. Jean na Etapa):
1. **Campo desabilitado** (ex.: "Exercício"): o fundo era fixo, `#eef2f4`, nos dois temas. No escuro, o campo "acendia" (13,8:1 contra o cartão) em vez de parecer apagado. A borda também vinha na cor de destaque, porque desde o #44 o `--border` é tingido.
2. **Botões de decisão** (Salvar, Excluir, Baixar Excel…): para o texto chegar a 7:1, o AA+ (#36, #37, #39) escureceu o fundo no claro, quase preto (`#0b4c44`), e clareou no escuro, em pastel (`#a9e0d2`, `#ffb3b3`). Os botões ficaram apagados.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean, na Etapa, nos 8 destaques × 2 temas, com o sistema emulado em claro e escuro.

| Tema | Salvar | Excluir | Limpar (texto e contorno) | Campo desabilitado (texto) |
|---|---|---|---|---|
| Claro | 7,36 a 10,19:1 | 7,46:1 | 7,46:1 | 5,15:1 |
| Escuro | 7,53 a 14,61:1 | 7,53:1 | 8,15:1 | 6,53:1 |

- No escuro, o fundo do campo desabilitado fica a 1,16:1 do cartão.
- FIP 613 conferido: o "Baixar Excel" dá 7,36:1.
- A tela de login não muda: as regras dela vencem.
- Capturas antes/depois. `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

Sem script de banco. `templates/base.html`: `?v=20261113.jean-controles.3` no CSS (JS sem mudança).

#### Setas no menu lateral (dev. Jean, 2026-11-13) (antiga 41.25)

**Problema:** no menu lateral, só o Tab percorria os itens, um a um, inclusive os de grupos longos (Relatórios → Execução tem 9 itens). Era o item P5 do inventário do AA+.

**Correção (só JS, bloco `[jean] Setas no menu lateral` no fim do `main.js`):** com o foco num item do menu, as setas seguem o padrão de navegação em árvore. O Tab continua como antes.

| Tecla | O que faz |
|---|---|
| **↓ / ↑** | próximo ou anterior item visível |
| **Home / End** | primeiro e último item |
| **→** | num grupo fechado, abre; num grupo aberto, vai ao 1º item dele |
| **←** | num grupo aberto, fecha; num item ou grupo fechado, volta ao grupo de cima |

Só age com o foco num `.menu-item` dentro de `#menu` e sem Ctrl, Alt ou Meta. Abrir e fechar usa o mesmo clique de sempre, então o `aria-expanded` do #41 continua certo.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean, com o teclado de verdade, com o menu recolhido e expandido.
- Relatórios: → abre, → vai a Planejamento, ↓ Execução, → abre, → FIP 613, ↓ PED, ← Execução, ← fecha, ← Relatórios, ← fecha.
- Administrar SPO: → abre e → vai a Personalizar SPO.
- ↓ ↑ Home End no menu todo (Início … Sair).
- O Tab continua igual (Início → Administrar SPO → Programar PTA/LOA).
- Com um aviso modal aberto, as setas não agem, porque o foco fica no aviso.
- A página não rola com as setas.
- `node --check static/js/main.js`; `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

Sem script de banco. `templates/base.html`: `?v=20261113.jean-setas-menu.1` no JS (CSS sem mudança).

#### Tela de login com o mesmo `?v=` do CSS que o restante do SPO (dev. Jean, 2026-11-13) (antiga 41.26)

_Esta subseção continua inteira no [claude.md](claude.md), seção 41 (41.26), por ser regra em vigor._

#### Botões secundários em contorno: "Cancelar"/"Remover" das consultas e os cinza nos dois temas (dev. Jean, 2026-11-13) (antiga 41.27)

**Problema:** nas telas de Consultar (Dotação, Estorno de Dotação, Etapa, Meta Física, Subação e regra de chave), os botões **"Remover"** (tira um critério da lista) e **"Cancelar"** (fecha a consulta) eram vermelhos cheios, iguais ao **"Excluir"**, que apaga de verdade. Ações que só dispensam ficavam com o mesmo peso visual de uma ação destrutiva.

**Correção (só CSS, no bloco `[jean] Controles dos formulários` do #57):** a regra do contorno vermelho, que valia para o "Limpar Formulário" (`*-clear`), passa a valer também para `*-filtro-cancel` e `*-filtro-remove`, num total de 11 botões. O "Excluir" e os outros vermelhos que apagam ou interrompem continuam cheios. No claro, o escurecer ao passar o mouse não se aplica a esses botões, que usam o fundo translúcido do contorno.

**Botões cinza** (`.btn-secondary`, 31 usos: "Limpar" dos critérios, "Editar"…): eram **cheios no claro** (`#495057`, do #39) e **em contorno no escuro** (regra antiga e #37). Agora o claro faz como o escuro: sem fundo, texto do tema e a borda dos campos (`--spo-control-border`). A borda é posta direto, porque a regra `.btn` do SPO (`border: 1px solid transparent`) vence as variáveis do Bootstrap. O "Excluir" continua vermelho cheio, por decisão do dev. Jean: vermelho cheio fica para o que apaga ou interrompe.

**Hierarquia que resulta:**
- **cheio na cor de destaque:** ação principal (Salvar, Consultar, Aprovar, Imprimir);
- **vermelho cheio:** apaga ou interrompe (Excluir, Desativar, Cancelar processamento);
- **contorno vermelho:** dispensa ou desfaz na tela (Cancelar, Remover, Limpar Formulário);
- **contorno neutro:** secundário (Limpar critérios, Editar).

**Ficaram de fora** (parecidos, mas não são de consulta; avaliar à parte): o "Cancelar" da confirmação de salvar da Meta Física (`data-mf-confirm-action="cancelar"`) e o "Cancelar" de Alterar Senha.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean, na Consultar Etapa.
- "Remover" e "Cancelar" com texto e contorno a 7,46:1 no claro e 8,15:1 no escuro.
- O vermelho é fixo, então vale para os 8 destaques.
- "Adicionar" e "Consultar" continuam cheios, na cor de destaque (10,34:1 no escuro), e o "Excluir" continua vermelho cheio.
- "Limpar" e "Editar" em contorno nos dois temas: texto a 16,86:1 no claro e 12,17:1 no escuro; borda a 3,47:1 no claro e 4,4:1 no escuro.
- Capturas nos dois temas. `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

Sem script de banco. `templates/base.html` e `templates/login.html`: `?v=20261113.jean-botoes-filtros.3` no CSS (JS sem mudança).

#### Login: aviso de sessão já aberta na própria página, no lugar do `window.confirm` (dev. Jean, 2026-11-13) (antiga 41.28)

_Substituída pela 41.41 (parte em vigor no claude.md) (login: a segunda tentativa de entrar encerra a outra sessão)._

**Problema:** quando o usuário já tem uma sessão ativa, o `POST /login` responde **409** e a tela perguntava "Deseja continuar?" com `window.confirm`. Num navegador que bloqueia diálogos (o painel do app Claude, alguns navegadores corporativos ou extensões), a resposta vira "não" e o login **parava sem aviso**: a pessoa digitava a senha e nada acontecia.

**Correção (só no `login.html` e no CSS; o servidor não muda):**
- **Aviso dentro do cartão de login** (`#login-sessao`, `role="alertdialog"`), com título "Usuário já conectado", ícone e uma faixa âmbar. Ele explica que, ao continuar, a outra sessão será encerrada: o servidor troca o token da sessão, então isso é o que acontece de fato.
- **Botões** "Continuar e entrar" e "Cancelar".
- **Foco:** vai para "Continuar e entrar" quando o aviso abre. **Esc** ou "Cancelar" fecham o aviso e devolvem o foco à senha.
- Com o aviso aberto, "Entrar" não manda outro pedido.
- **"Continuar"** manda o mesmo pedido de antes, com `force_login: true`. A rota `/login`, a resposta 409 e o fluxo sem JS (`flash`) não mudam; por isso a automação que usa o login não é afetada.
- Se o aviso não existir na página, o código volta ao `window.confirm`.
- **CSS:** bloco `[jean]` no fim do `style.css`. No claro, a borda do "Cancelar" dentro do aviso fica `#6b7280`, porque a borda dos campos dava 2,96:1 sobre o fundo âmbar.

**Verificação:** SPO local, com a tela de login aberta num quadro isolado e as respostas do servidor **simuladas** (409 e depois "ok"). Nenhuma senha foi usada.
- O 409 abre o aviso, com o foco em "Continuar".
- "Entrar" com o aviso aberto não manda pedido.
- Esc e "Cancelar" fecham o aviso e devolvem o foco à senha.
- "Continuar" manda 1 pedido com `force_login` e segue para o destino.
- **Contraste:**

  | | Escuro | Claro |
  |---|---|---|
  | Título e texto | 10,68:1 | 14,38:1 |
  | Ícone | 6,96:1 | 4,28:1 |
  | Faixa | 8,28:1 | 4,63:1 |
  | "Continuar" | 10,34:1 | 7,36:1 |
  | "Cancelar": texto | 9,09:1 | 14,38:1 |
  | "Cancelar": borda | 3,29:1 | 4,12:1 |

- Sintaxe do script do `login.html` conferida com `node --check`. `pytest` completo duas vezes seguidas: 192 passam, 4 pulam.

Sem script de banco. `templates/base.html` e `templates/login.html`: `?v=20261113.jean-aviso-sessao.2` no CSS.

#### Padrão visual "spo-aa+" documentado (dev. Jean, 2026-11-13) (antiga 41.29)

_Esta subseção continua inteira no [claude.md](claude.md), seção 41 (41.29), por ser regra em vigor._

#### Padrão de fontes do SPO: Inter, Intel One Mono e tabelas a 12 px (dev. Jean, 2026-11-13) (antiga 41.30)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.30)._

**Problema:** a fonte padrão ("Clássica (padrão SPO)", a pilha `Cantarell, Segoe UI, system-ui…`) **variava com o computador**: Segoe UI no Windows, a do sistema no Mac, Cantarell no Linux. O texto padrão das tabelas era o "Compacto", de **8 px**. O SPO não tinha identidade visual estável entre computadores e celulares, e as tabelas ficavam pequenas para ler.

**Mudança:**
- **`main.js`, bloco `initFontesPersonalizar`:** `DEFAULTS` passa a `{ ui: "inter", data: "intel-mono", size: "default", table: "legible" }`, e os rótulos mudam. **Quem já salvou uma escolha no navegador continua com ela.**
- **`style.css`, bloco `[jean] Fontes`:** a Inter vale também quando o `<body>` não tem `data-spo-font-ui`. Isso cobre a tela de login, que não carrega o `main.js`, e o instante antes de o script rodar.
- **Personalizar:**
  - "Clássica (padrão SPO)" vira **"Do sistema"**, com a explicação de que varia com o computador;
  - "(padrão SPO)" passa para Inter, Intel One Mono e Legível (12 px);
  - "Compacto" passa a "Compacto (8 px)".
- **Padrão visual:** nova §2.1, Tipografia.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean, num navegador **sem escolha salva**.
- A tela de login abre em Inter, carregada do SPO.
- Na Início, o `<body>` já vem com `inter`, `intel-mono`, `default` e `legible`.
- O Personalizar mostra as quatro opções do padrão marcadas.
- No FIP 613: título em Inter, células e totais em Intel One Mono 12 px.
- `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: `?v=20261113.jean-padrao-fontes.1` no CSS; `base.html`: o mesmo no JS.

#### Login: "Entrar" na cor de destaque e "×" do Alerta redondo no celular (dev. Jean, 2026-11-13) (antiga 41.31)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.31)._

Dois defeitos achados pelo Cleber na homologação, depois do lote AA+.

**(2) "×" do Alerta da chave de planejamento oval no celular.**
- **Causa:** em tela de toque, a regra dos alvos (#35) põe `min-height: 44px` em todo botão. O "×" (28 px, `border-radius: 50%`) virava 28×44, e o contorno de foco ficava oval.
- **Correção:** no toque, o "×" continua com 28×28 e redondo. A área de toque passa a 44 px por um `::after` invisível (`inset: -8px`), o mesmo recurso do botão do menu.

**Verificação:** SPO local.
- Login nos 8 destaques e nos dois temas:
  - claro: gradiente no tom do destaque, contraste do texto branco entre **9,38:1** (Verde) e **13,78:1** (Roxo);
  - escuro: sem mudança, de 7,53:1 a 14,61:1;
  - "Continuar e entrar" com o mesmo tom.
- "×" com toque emulado (375 px): 28×28, `border-radius: 50%`, foco redondo, toque a 7 px da borda ainda cai no botão.
- `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS, agora `?v=20261113.jean-login-entrar.1` nos dois. O JS continua em `?v=20261113.jean-padrao-fontes.1`.

#### Login com a paleta inteira do destaque: foco dos campos na cor escolhida (dev. Jean, 2026-11-13) (antiga 41.32)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.32)._

Achado do dev. Jean depois do #66: com o Laranja, o "Entrar" já saía laranja, mas o **contorno de foco dos campos Email e Senha continuava verde**.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS, agora `?v=20261113.jean-login-tons.1` nos dois.

#### Título da página sem anel de foco (dev. Jean, 2026-11-13) (antiga 41.33)

Achado do dev. Jean no teste da homologação: logo depois do login, o "Bem-vindo" aparecia dentro de um quadrado.

- **Causa:** o A11 leva o foco ao título da página quando uma tela abre ou quando um aviso fecha (como o Alerta da chave de planejamento logo após o login), para o leitor de tela anunciá-lo. O A12 desenhava nesse foco o mesmo anel dos controles (`body [tabindex="-1"]:focus-visible`).
- **Correção** (bloco `[jean]` no fim do `style.css`): título (`h1`–`h3`, `[role="heading"]`, `.card-title`) e `<main>` com `tabindex="-1"` não mostram mais o anel ao receber foco. **O foco continua indo para lá**, e o leitor de tela continua anunciando. Botões, campos, abas, links e o "Pular para o conteúdo" mantêm o anel. O 2.4.7 (foco visível) vale para o que se opera pelo teclado, e título e `<main>` não são controles.
- **Verificação:** SPO local, logado.
  - Depois de fechar o Alerta, o foco fica no "Bem-vindo", sem anel.
  - "Pular para o conteúdo" pelo Tab: com anel (3 px, cor de destaque). Acionado com Enter, o foco vai ao `<main>` com `:focus-visible` verdadeiro, e sem anel.
  - `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS, agora `?v=20261113.jean-titulo-foco.1` nos dois.

#### Valores em azul e vermelho mais vivos (dev. Jean, 2026-11-13) (antiga 41.34)

Achado do dev. Jean no teste da homologação (FIP 613): o vermelho de "Bloqueado/Conting." e o azul de "Saldo de Dotação" quase se confundiam com o preto.

- **Onde:** as classes `pos` (azul, valor positivo) e `neg` (vermelho, valor negativo), que os relatórios põem nos totais (`.totais-row`: FIP 613, PED, EMP, NOB, EST EMP…) e nas células numéricas do FIP 613 (`.fip-table td.num`). As cores eram `#0b3c91` e `#8b0000` no claro, e `#9cc4ff`/`#ffb3b3` no escuro (A6/A9).
- **Pedido:** as cores clássicas do Excel/VBA, `rgb(0,0,255)` e `rgb(255,0,0)`. Medido: o azul puro passa no AA+ (8,6:1 no branco), mas o vermelho puro dá só 4,0:1, abaixo do AA. **Decisão do dev. Jean:**
  - claro: azul **`#0000ff`** e vermelho **`#a00000`**, o vermelho mais vivo que ainda passa de 7:1;
  - escuro: azul **`#94baff`** e vermelho **`#ff9999`**, mais vivos que os pastéis anteriores e ainda ≥ 7:1 no cartão. O azul puro no escuro daria 1,9:1.
- **Correção:** bloco `[jean]` no fim do `style.css`, com `:not(#spo-valor)` para vencer as regras antigas. O JS não muda. Os chips ▲/▼ do Teto Orçamentário (verde e vermelho de variação) não fazem parte disto.
- **Verificação:** SPO local, FIP 613 com dados reais do banco de desenvolvimento (54 valores azuis, 17 vermelhos, contando totais e tabela).
  - Claro: mínimo **8,37:1** (azul) e **8,20:1** (vermelho), sobre o fundo real de cada célula.
  - Escuro: **7,71:1** e **7,38:1**.
  - Com o mouse sobre a linha, no escuro, o fundo clareia e o contraste fica em torno de 6,4:1 (AA), como já acontecia.
  - `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS, agora `?v=20261113.jean-cores-valores.1` nos dois.

#### "Restaurar padrão" também volta as fontes e os tamanhos (dev. Jean, 2026-11-13) (antiga 41.35)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.35)._

Achado do dev. Jean no teste da homologação: em Personalizar, "Restaurar padrão do laboratório" voltava tema, destaque e preferências visuais, mas **não** o tamanho do texto, o texto das tabelas nem as fontes.

Sem script de banco. `templates/base.html`: só a linha `?v=` do JS, agora `?v=20261113.jean-restaurar-fontes.1`. O CSS continua em `?v=20261113.jean-cores-valores.1`, e o `login.html` não muda.

#### Padrão spo-aa+: escala tipográfica (dev. Jean, 2026-11-13) (antiga 41.36)

Pergunta do dev. Jean no teste da homologação: os textos pareciam pequenos e apertados em várias telas (FIP 613), e adequados nas telas de Governança e Resultados.

- **Medição** (SPO local, padrão de fontes SPO, 100%): **66 telas**, uma por vez, cada uma só depois de estável; os relatórios, com a tabela preenchida.
  - **20 tamanhos de letra** nas telas; no `style.css`, **37 valores de `font-size` e 23 de `line-height`**.
  - Nada abaixo de 10 px dentro das telas. O topo tem o nome e o "Logados" com 9 px.
  - Texto de leitura em 10–11 px: paginação, "Última atualização por UO", selos de situação, rótulos dos totais e dos filtros.
  - Descrições com entrelinha 1,25–1,35.
  - Títulos de página com 22, 23, 24 e 25,6 px, e pesos 500 e 700.
- **Mudança, só documentação** (`docs/padrao-visual-spo-aa-plus.md`):
  - **§2.2 Escala tipográfica**, a partir das telas de Governança e Resultados: 11 px (só rótulo em maiúsculas e negrito), 12, 14, 15–16, 20 e 24 px;
  - entrelinha 1,5 no texto e 1,2–1,25 nos títulos;
  - **mínimo de 12 px** para leitura;
  - título da página em **negrito (700)** em todas as telas (decisão do dev. Jean);
  - **§7:** o que está fora da escala, com o número de telas.
- **Aplicação**, em PRs seguintes, medindo antes e depois:
  1. os casos abaixo de 12 px e as descrições apertadas;
  2. títulos de página iguais;
  3. opção "Entrelinhas" no Personalizar;
  4. tokens `--spo-fs-*`/`--spo-lh-*` na divisão do CSS.

Sem mudança de código e sem script de banco. `base.html`/`login.html` não mudam.

#### Escala tipográfica, passo 1: mínimo de 12 px e entrelinha 1,5 (dev. Jean, 2026-11-13) (antiga 41.37)

Primeira aplicação da escala do padrão spo-aa+ (§2.2): os textos de leitura abaixo de 12 px e as descrições com entrelinha apertada.

- **Levantamento:** nas 66 telas do menu, mais o topo e o rodapé, uma tela por vez, com o padrão de fontes SPO. Foram **50 casos** fora da escala, quase todos de poucas regras:
  - **topo:** nome e "Logados" (`.user-meta`), 9 px;
  - **rodapé:** 11 px, entrelinha 1,35;
  - **paginação:** "Itens por página" e reticências, 10 px;
  - **totais:** rótulos dos totais (`.totais-row > span`), 11 px;
  - **"Última atualização por UO":** metadados, 11 px, entrelinha 1,25;
  - **selos "Ativo"/"Ativa"** (`.planning-status`, `.status-badge`), 11 px;
  - **Teto Orçamentário:** KPIs, 10 px; rótulos dos filtros, 11 px; linha da fonte, 11 px / 1,25;
  - **descrições `p.muted` dos cabeçalhos:** 12 px / 1,25;
  - **cartões de Governança e Resultados** (`.flow-node small`): 12,25 px / 1,35;
  - **casos isolados:** a dica da Início, a pílula de versão do Contrato Visual, `<code>` da API acessos, "Todos" dos filtros múltiplos, opções do upload de Notas SEE, ajuda da Meta Física.
- **Correção** (bloco `[jean]` no fim do `style.css`, com `:not(#spo-tipo)`):
  - esses seletores vão a **12 px**;
  - **exceção, decisão do dev. Jean:** o nome e o "Logados" do topo e o rodapé são informação secundária e ficam com **10 px** (eram 9 e 11 px), também registrados no padrão §2.2;
  - os textos de várias linhas vão a **entrelinha 1,5**;
  - a paginação só sobe com o texto das tabelas em "Legível" (padrão), e respeita "Compacto"/"Intermediário" do Personalizar.
- **Fora deste passo:**
  - os rótulos curtos em maiúsculas e negrito, que ficam em 11 px (passo "Rótulo"), incluindo o cabeçalho "REGIÃO PTA/LOA" da Meta Física;
  - os textos dos gráficos, que são SVG do Plotly configurados no JS;
  - os títulos de página (passo 2).
- **Verificação:** SPO local, logado.
  - Mesmo levantamento depois da mudança: dos 50 casos, sobra 1, o cabeçalho em maiúsculas da Meta Física, no passo "Rótulo".
  - FIP 613: totais em duas colunas a 1440 px, como antes. A 1036 px com o menu aberto, uma coluna, também como antes (conferido voltando a 11 px), e sem corte.
  - A 375 px: o topo com 10 px cabe, sem rolagem lateral.
  - Teto Orçamentário: KPIs e filtros sem corte a 375 e 1440 px.
  - `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS, agora `?v=20261113.jean-tipografia-12.1` nos dois.

#### Escala tipográfica, passo 2: título da página igual em todas as telas (dev. Jean, 2026-11-13) (antiga 41.38)

Segundo passo da escala do padrão spo-aa+ (§2.2): o título da página.

- **Medição** das 66 telas, com o mesmo critério que o A11 usa para achar o título (o primeiro `<h1>` visível, ou o `.card-title` do cabeçalho):
  - **55 telas com cabeçalho de página**, com títulos de 22, 23, 24 e 25,6 px, peso 500 (Governança e Resultados, Personalizar, Notas SEE) ou 700, e entrelinha 1,2 ou 1,5;
  - **11 telas sem cabeçalho de página**: formulários e consultas de Meta Física, Subação, Etapa, Dotação e Estorno de Dotação, e a Limpeza de Histórico. Nelas o "título" é o do cartão do formulário, com 14 px.
- **Correção** (bloco `[jean]` no fim do `style.css`): `#content-area h1`, o `.card-title` dos cabeçalhos de módulo e de relatório e o "Bem-vindo" da Início passam a **24 px, negrito (700) e entrelinha 1,2**. O peso é decisão do dev. Jean. As 11 telas sem cabeçalho não mudam: dar um cabeçalho a elas é mudança de template, que fica para outra decisão.
- **Verificação:** SPO local, logado.
  - Mesma medição depois: as 55 telas com 24 px / 700 / 1,2, um `<h1>` por tela, título numa linha no computador. As 11 telas de formulário sem mudança.
  - A 375 px: o título mais longo ("PLAN 51 - Relação Detalhada de Créditos Adicionais") ocupa 5 linhas, como já ocupava com 23 px, sem corte e sem rolagem lateral.
  - `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS, agora `?v=20261113.jean-titulos.1` nos dois.

#### Totais do PLAN 51 sem corte em telas estreitas (dev. Jean, 2026-11-13) (antiga 41.39)

Achado na medição dos títulos (passo 2 da escala tipográfica): no celular, o quadro "Totais (filtros aplicados)" do PLAN 51 aparecia cortado à direita.

- **Causa:** o quadro do PLAN 51 (`.totais-box--wide`) cresce até caber tudo numa linha (`min-width: max-content`), para os valores em bilhões. Ele fica numa grade de duas colunas fixas ao lado dos botões (`grid-template-columns: max-content auto`). Com o cartão do relatório estreito, o conjunto passava do cartão:
  - a 375 px, quadro de 562 px num cartão de 283 px, com valores e botões cortados;
  - entre ~820 e ~950 px de janela, os botões saíam do cartão.
  - O #31/#56 tinha deixado o PLAN 51 de fora de propósito ("mantém o quadro próprio").
- **Correção** (bloco `[jean]` no fim do `style.css`), medida pela **largura do próprio cartão** (container query `spo-plan51`), não da janela:
  - cartão até 900 px: os botões descem para baixo do quadro, como diz a regra original ("quebra de linha antes dos botões");
  - cartão até 640 px: o quadro usa a mesma regra adaptável dos outros relatórios (#31/#56), com colunas que cabem e o valor descendo de linha quando não cabe ao lado do rótulo;
  - cartão mais largo: o PLAN 51 não muda.
  - O `container` é declarado só no cartão que contém o `.totais-box--wide`, e os outros relatórios não são afetados.
- **Verificação:** SPO local. O PLAN 51 está vazio no banco de desenvolvimento, então os totais foram preenchidos na página com valores de bilhões, para simular o caso real.
  - Janelas de 375, 640, 768, 860, 960, 1100 e 1440 px: em nenhuma o quadro, um valor ou um botão passa do cartão, e não há rolagem lateral.
  - A 1440 e 1100 px, quadro e botões lado a lado, como antes; a 1440 px, nas mesmas posições de antes.
  - `pytest` completo duas vezes seguidas.

Sem script de banco. `templates/base.html` e `templates/login.html`: só a linha `?v=` do CSS, agora `?v=20261113.jean-totais-plan51.1` nos dois.

### 2026-11-12 — antigas 41.1 a 41.11

#### Totais dos relatórios de execução sem corte nem sobreposição (dev. Jean, 2026-11-12) (antiga 41.1)

**Problema (medido no ambiente do dev. Jean):** o quadro "Totais (filtros aplicados)" de PED, FIP 613, PLAN 20, EMP, EST EMP e NOB tinha duas colunas fixas (`.relatorio-top-uo .totais-box`, linhas com `minmax(90px, 130px) minmax(0, 1fr)`) e valores com `white-space: nowrap`. O conteúdo ficava mais largo que o quadro em quase todas as larguras:
- 390 px (celular): o quadro saía do cartão e era cortado (`.card.plan20-card` 342 px de conteúdo em 296; FIP 613 359/296; PLAN 20 366/296; EST EMP 349/296); na tela, "2026Valor PED: 19.23…" encavalado e valores cortados;
- 1024 px: 352–376 px de conteúdo num quadro de 281 px, com os valores invadindo a coluna vizinha;
- 1366 px: até 25 px a mais. NOB e PLAN 21 já cabiam.

**Correção (só CSS, bloco `[jean]` no fim do `static/css/style.css`):** o quadro usa `grid-template-columns: repeat(auto-fit, minmax(min(100%, 14rem), 1fr))` (duas colunas quando cabem, uma quando não) e cada linha vira flex com quebra: rótulo à esquerda, valor à direita, e o valor desce para a linha de baixo se os dois não couberem lado a lado. O PLAN 51 (`.totais-box--wide`) mantém o quadro próprio. `templates/base.html`: `?v=20261112.jean-totais.1` só no link do CSS.

**Efeito visível:** a 1024 e 1366 px o quadro passa a uma coluna (fica mais alto; no FIP 613, os 6 totais em lista), mas nenhum valor é cortado.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean, dados reais já carregados (PED, FIP 613, PLAN 20, PLAN 21, NOB, EST EMP); medição automática do quadro (largura do conteúdo × largura do quadro, valor além da linha, linhas sobrepostas) em 390, 768, 1024 e 1366 px — todos sem corte nem sobreposição depois da correção; capturas antes/depois do FIP 613 em 390 e 1024 px. `pytest` completo duas vezes seguidas: 192 passam, 4 pulam. Sem script de banco.

#### Fontes e tamanho do texto no Personalizar Temas SPO (dev. Jean, 2026-11-12) (antiga 41.2)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.2)._

**Motivo:** o SPO não carregava nenhuma fonte web. A pilha começava por "Cantarell", que o Windows não tem, então cada aparelho mostrava uma fonte diferente (Segoe UI, a do sistema no Mac, Roboto no Android). E os tamanhos mais usados no CSS são pequenos (12 px ×72, 11 px ×59, 10 px ×17; 8 px nas células de tabela), o que pesa contra a meta de acessibilidade AA+.

**O que entra:** um cartão novo **"Fontes e tamanho do texto"** no Personalizar Temas SPO, com três escolhas guardadas no navegador (chave própria `spo-font-preferences`, como as demais preferências locais):
- **Fonte da interface:** Clássica (padrão SPO, a pilha de sempre), Inter, Atkinson Hyperlegible (feita para baixa visão) e Source Sans 3;
- **Fonte de tabelas e dados** (tabelas, valores dos totais e indicadores do Teto): Clássica (padrão SPO), Inter com números de largura fixa (`tabular-nums`), IBM Plex Sans e Intel One Mono (monoespaçada feita com foco em legibilidade);
- **Tamanho do texto:** Padrão (100%), Grande (112%) e Maior (125%), com `zoom` só na área de conteúdo (`.main`); menu e topo não mudam.

**O padrão não muda nada:** "Clássica + 100%" deixa o SPO como era, sem baixar nenhuma fonte.

**Arquivos:** `static/css/style.css` (bloco `[jean]` no fim: `@font-face` com nomes "SPO …", troca pelos atributos `data-spo-font-ui`, `data-spo-font-data` e `data-spo-text-size` no `<body>`); `static/js/main.js` (bloco `initFontesPersonalizar` no fim: lê e aplica a preferência em todas as telas; cliques no cartão por delegação e um `MutationObserver` para o cartão, que é carregado depois); `templates/partials/atualizar_personalizar_spo.html` (o cartão, com amostra de texto e números); `templates/base.html` (`?v=20261112.jean-fontes.2` no CSS e no JS). Nenhuma função ou regra existente foi alterada.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean, com cliques reais no cartão: cada fonte aplicada e só ela baixada; a preferência sobrevive à recarga; "Clássica" nas tabelas continua clássica mesmo com outra fonte na interface; com 125%, nenhuma das 8 telas medidas transborda a 390 px. Ajuste junto: com o texto ampliado, a coluna "Última atualização por UO" (`.meta-uo-col`, `min-width: 190px`) e o e-mail longo passavam 6 px do cartão no celular; só nesse caso, a coluna libera a largura e o texto quebra (totais de PED, FIP 613, PLAN 20, EST EMP e NOB conferidos a 390 px em 112% e 125%). `node --check static/js/main.js` ok; `pytest` completo duas vezes seguidas: 192 passam, 4 pulam. Sem script de banco.

**Depende do PR dos totais** (`fix/totais-relatorios-responsivo`): com fonte monoespaçada e 125%, os totais do FIP 613 encavalam sem aquela correção; com ela, ficam limpos (testado).

#### Contraste AA do botão principal dos relatórios, da prévia do Personalizar e dos números do Teto (dev. Jean, 2026-11-12) (antiga 41.3)

**Problema (medido nas 8 cores de destaque × claro/escuro, no ambiente do dev. Jean):** três elementos usam a cor de destaque e ficavam abaixo do mínimo AA (4,5:1) — 35 das 48 combinações:
- **"Baixar Excel"** (`.report-actions .btn:last-child`, texto branco sobre degradê da cor de destaque): no claro o degradê começa na `--color-400` (a mais clara da paleta; no claro, `--accent-strong` = color-400 e `--accent` = color-500), de 1,47:1 (amarelo) a 5,91 (roxo), Clássico 3,47; no escuro, de 1,19 a 2,51 em **todas** as cores;
- **Números do painel Teto** (`.teto-kpi strong`, cor `--accent-strong`): no claro, de 1,47 a 5,91 (Clássico 3,47); no escuro já passavam;
- **Botão "Ação principal" da prévia do Personalizar** (`.preview-actions button:first-child`, branco sobre `--accent`): no claro falhava em 6 cores (amarelo 3,04); no escuro, de 1,51 a 3,02 em todas.

**Correção (só CSS, bloco `[jean]` no fim do `style.css`, dentro de `@media (forced-colors: none)` para não interferir no modo de alto contraste do sistema):** no claro, texto na cor de destaque usa `--color-700` e o botão com texto branco usa fundo `--color-800`→`--color-700` (prévia: `--color-700`); no escuro, os dois botões na cor de destaque clara passam a ter texto escuro (`#111111`). `templates/base.html`: `?v=` do CSS em `jean-contraste.2`.

**Depois:** as 48 combinações passam no AA; mínimo 4,69:1 (amarelo no claro, color-700), as demais acima de 4,99. Efeito visível: no claro, os números do Teto e o "Baixar Excel" ficam num verde mais escuro (Clássico); no escuro, o "Baixar Excel" passa a texto escuro sobre o verde claro.

**Junto — cores dos totais no tema claro:** os valores positivos (azul) e negativos (vermelho) dos totais (`.totais-row .pos/.neg`, ex.: "Bloqueado/Conting." e "Saldo de Dotação" do FIP 613; totais do PED e do EST EMP) apareciam na cor do texto no claro, porque `.report-data-card .totais-row strong { color: var(--text) }` é mais específica que `.totais-row .pos/.neg`; no escuro as regras `.theme-dark …` venciam. Agora `.report-data-card .totais-row strong.pos/.neg` usam `#0b3c91`/`#8b0000` no claro (10:1) e `#6ea8ff`/`#ff8b8b` no escuro, como antes. `?v=20261112.jean-contraste.2`.

**Fora daqui:** o rótulo "Clássico (padrão SPO)" da paleta, que a auditoria do dev. Jean apontou, é texto só para leitor de tela (recortado em 1 px) — falso positivo, nada a corrigir. Outros botões com texto branco sobre a cor de destaque ainda não foram medidos em todas as cores.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean; contraste calculado no navegador sobre o pior ponto do degradê, com as 8 paletas aplicadas como a `applyAccentColor` faz; capturas antes/depois (números do Teto e "Baixar Excel", claro e escuro). `pytest` completo duas vezes seguidas: 192 passam, 4 pulam. Sem script de banco.

#### Foco visível padrão em todo o SPO (dev. Jean, 2026-11-12) (antiga 41.4)

**Problema (medido com teclado real, no ambiente do dev. Jean):** em 4 telas (FIP 613, Personalizar Temas, painel Teto, Alterar Senha), **nenhum dos 75 elementos focáveis** tinha indicador de foco próprio com 2 px e contraste 3:1 (WCAG 2.2: 2.4.7; meta AA+ 2.4.13). O menu dependia do contorno automático do navegador (fino e diferente em cada navegador); o botão de recolher o menu (`.icon-btn:focus { outline: none }`) e os botões pequenos (Limpar Filtros, Atualizar) não mostravam nada. Há 13 regras no CSS que removem o `outline` e 21 regras de foco, cada uma para um componente.

**Correção (só CSS, bloco `[jean]` no fim do `style.css`, dentro de `@media (forced-colors: none)`):** no foco por teclado (`:focus-visible`), todo controle (links, botões, campos, `summary`, `[tabindex]`, `[role=button]`, `[role=tab]`) ganha um anel de **3 px, afastado 2 px**, na cor de destaque com contraste garantido: `--color-700` no claro e `--accent` no escuro (variável `--spo-focus-ring`). No menu lateral o anel fica por dentro (`outline-offset: -3px`) para não ser cortado. Clique de mouse não mostra o anel. A especificidade (`body :is(...):focus-visible`) vence as regras antigas que tiravam o contorno; a busca do Painel de Permissões, que tem foco no contêiner com `!important`, continua como está. `templates/base.html`: `?v=20261112.jean-foco.1`.

**Depois:** nas mesmas telas, todos os elementos com anel de 3 px — 85/85 no escuro e 65/65 no claro (FIP 613 e Personalizar), contraste mínimo medido 8,5:1; calculado para as 8 cores de destaque: mínimo 4,30:1 (roxo no escuro), acima dos 3:1 exigidos. Clique de mouse no "Limpar Filtros": sem anel (`:focus-visible` falso). Capturas antes/depois com o dev. Jean.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean; Tab real para o modo teclado e foco em cada elemento, esperando as transições terminarem; `pytest` completo duas vezes seguidas: 192 passam, 4 pulam. Sem script de banco.

#### Alvos de toque: mínimo 24 px sempre e 44 px em telas de toque (dev. Jean, 2026-11-12) (antiga 41.5)

**Problema (medido em FIP 613, Teto, Personalizar, Diretrizes, Alterar Senha e Início, no ambiente do dev. Jean):**
- abaixo do mínimo AA (WCAG 2.2, 2.5.8: 24 px) só o **botão de recolher o menu**, `#sidebar-toggle`, com **22×22 px** no desktop (em tela estreita o Contrato Responsivo já o leva a 40 px);
- abaixo da meta AA+ (2.5.5: 44 px), no celular (390 px), **quase todos os controles**: filtros das tabelas (`.mf-display`) 32 px, paginação 29, cores do Personalizar 34, ícones do menu recolhido 38, botões e campos 34–40.

**Correção (só CSS, bloco `[jean]` no fim do `style.css`):**
- em qualquer tela, a **área clicável** do botão de recolher o menu cresce para 34 px por um pseudo-elemento invisível (`::after` com `inset: -6px`), sem mudar o desenho (continua 22×22);
- **só em telas de toque** (`@media (pointer: coarse)`): botões, links-botão, itens do menu, filtros, campos e listas com `min-height: 44px`; ícones, cores do Personalizar, paginação e itens do menu com `min-width: 44px`. Com mouse, a densidade do desktop não muda.

**Depois:** no celular, **91 de 91** controles medidos nas 6 telas com pelo menos 44 px e nenhum abaixo de 24 px, sem transbordo horizontal; no desktop, tamanhos visuais iguais aos de antes e o botão de recolher clicável num raio de 12 px a partir do centro. Efeito visível no celular: botões e filtros mais altos; a paginação do FIP 613 passa de 2 para 3 linhas.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean, emulação de celular (390 px, toque) e desktop (1366 px, mouse); capturas antes/depois (cores do Personalizar e paginação do FIP 613). `pytest` completo duas vezes seguidas: 192 passam, 4 pulam. Sem script de banco. `templates/base.html`: `?v=20261112.jean-alvos.1`.

#### Contraste AA+ (7:1) no tema claro (dev. Jean, 2026-11-12) (antiga 41.6)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.6)._

**Problema (medido em 10 telas no tema claro, no ambiente do dev. Jean):** 256 textos abaixo de 7:1 (meta AA+, WCAG 1.4.6), quase todos por duas cores:
- **cinza secundário `--muted: #5e5c64`** — 210 textos (rótulos, "muted", rodapé), mínimo 5,46:1 no fundo esverdeado do Personalizar;
- **cor de destaque como texto** (`--accent`, `#0f7c68` no Clássico) — 34 textos (subtítulo do topo, `.eyebrow`/`.report-eyebrow`/`.module-eyebrow`, links das Diretrizes, título de modal), mínimo 4,8:1; nas outras cores bem pior (Amarelo 1,9:1).

Casos isolados: `.text-muted` do Bootstrap (6,78), botão cinza "Fechar" do Bootstrap (4,69), "Administrar SPO" com submenu aberto (`--accent-strong` = color-400, 3,45:1) e o **`.btn-primary` genérico** (degradê `--color-600` → `--color-400`, texto branco: 3,47:1 no Clássico, abaixo até do AA).

**Depois:** 0 textos abaixo de 7:1 nas 8 telas medidas, **com as 8 cores de destaque** (1.141 textos por cor; degradês medidos no pior ponto). **Efeito visível (identidade):** no claro, textos secundários um pouco mais escuros; subtítulo, títulos pequenos e links num tom mais fechado da cor de destaque (no Amarelo, um tom ocre escuro); botões principais mais escuros. O tema escuro não muda (exceto o tamanho do rodapé).

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean; contraste de todo texto visível, com as 8 paletas aplicadas como a `applyAccentColor` faz; capturas antes/depois (Diretrizes no Clássico, rodapé, Alterar Senha no Amarelo). `pytest` completo duas vezes seguidas: 192 passam, 4 pulam. Sem script de banco. `templates/base.html`: `?v=20261112.jean-aaplus.1`.

**Pendente (tema escuro, AA+):** azul/vermelho dos números nas tabelas (`td.num.pos/.neg`, 6,3 e 6,7:1) e números do Teto (6,0:1) ficam um pouco abaixo de 7:1 no escuro.

#### Contornos de controles (AA), AA+ no escuro e "Contraste reforçado" de verdade (dev. Jean, 2026-11-12) (antiga 41.7)

**Problema (medido em FIP 613, Alterar Senha, Personalizar e Teto, 8 cores de destaque × claro/escuro × contraste padrão/reforçado, no ambiente do dev. Jean):**
- **contorno dos controles** (campos, listas, filtros `.mf-display`, botões sem preenchimento) abaixo do **AA** (WCAG 1.4.11, 3:1): 1,36:1 no claro e 1,26–1,92:1 no escuro — 232 a 240 de 248 controles; os botões de preferência do Personalizar não tinham borda nenhuma no escuro;
- o **"Contraste reforçado"** (Personalizar → Preferências comuns → Contraste) dava números **idênticos** ao padrão: só mexia em sombras de alguns componentes;
- no **escuro**, texto abaixo de 7:1 (meta AA+): secundário `#c0bfbc` (6,87), cor de destaque como texto (até 3,5:1 no Roxo), azul/vermelho dos números (`#6ea8ff` 5,24; `#ff8b8b` 5,61), números do Teto e "Administrar SPO" aberto (`--accent-strong`), botões com texto escuro sobre a cor de destaque (5,23 no Roxo).

**Correção (só CSS, bloco `[jean]` no fim do `style.css`, dentro de `@media (forced-colors: none)`):**
- variável **`--spo-control-border`** no contorno dos controles, fora de hover/foco: `#86848c` no claro (3,2:1) e `#828282` no escuro (3,3:1); os botões de preferência ganham borda de 1 px;
- escuro: `--muted` `#d0cfcc`; `--spo-accent-text` = `--color-200` (`--color-100` em Azul, Vermelho e Roxo) nos títulos pequenos, subtítulo, links, números do Teto e "Administrar SPO" aberto; números azul `#9cc4ff` e vermelho `#ffb3b3`; botões principais com texto escuro sobre esse tom claro;
- **reforçado**: contorno `#636169` (claro) / `#a6a6a6` (escuro), ≥ 4,5:1, também nos cartões, cabeçalhos e tabelas; texto secundário `#38363d` (claro) / `#e2e1de` (escuro), ≥ 9,6:1.

**Depois:** nas 4 telas × 8 cores × 2 temas × 2 modos (6.248 textos e 344 controles por combinação de tema): **0 textos abaixo de 7:1**; contornos ≥ 3,7:1 no padrão e ≥ 4,9:1 no reforçado. **Efeito visível:** contornos de campos e filtros mais marcados nos dois temas; no escuro, títulos pequenos, links e números um pouco mais claros; o reforçado agora se percebe (contornos fortes nos cartões e texto secundário quase na cor do principal).

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean; contraste de texto (pior ponto dos degradês) e de contorno/preenchimento dos controles contra o fundo de fora, com as 8 paletas aplicadas como a `applyAccentColor` faz; capturas antes/depois (filtros do FIP 613, claro e escuro, e o reforçado). `pytest` completo duas vezes seguidas: 192 passam, 4 pulam. Sem script de banco. `templates/base.html`: `?v=20261112.jean-reforcado.1`.

#### Movimento reduzido (dev. Jean, 2026-11-12) (antiga 41.8)

**Problema:** o SPO não tinha nenhuma regra para quem pede "reduzir movimento" no sistema operacional (WCAG 2.3.3, meta AA+; `prefers-reduced-motion`): 4 animações contínuas (barra, marcador e pontos da tela "Carregando página…", ícone girando de `.planning-component-loading-row`), 32 transições no CSS (18 com deslocamento ou ampliação ao passar o mouse) e 14 rolagens suaves pedidas pelo JS (`scrollIntoView({ behavior: "smooth" })`), que o CSS sozinho não desliga.

**Correção:**
- `static/css/style.css`, bloco `[jean]` no fim, dentro de `@media (prefers-reduced-motion: reduce)`: transições e animações instantâneas (`0.01ms`, uma vez), rolagem `auto`; os indicadores de carregamento ficam parados (`animation: none`; barra em 34%, pontos visíveis) — o texto "Carregando…" continua;
- `static/js/main.js`, bloco `initMovimentoReduzido` no fim: com a preferência ligada, `scrollIntoView` com `behavior: "smooth"` vira `"auto"` (o resto do pedido, como `block`, fica igual); sem a preferência, nada muda. Nenhuma chamada existente foi alterada.
- `templates/base.html`: `?v=20261112.jean-movimento.1` no CSS e no JS.

**Verificação** (o navegador do teste não simula a preferência, então em três partes): regra carregada com a condição `(prefers-reduced-motion: reduce)`; com o corpo da regra aplicado na página, elementos com transição/animação na Início caem de 100 para 0 e a barra/pontos de carregamento param; a lógica do JS testada com a preferência simulada (`smooth` → `auto`; sem a preferência e chamadas sem objeto, intactas) e a rolagem real funcionando; navegação por 4 telas sem erro no console. `node --check` ok; `pytest` completo duas vezes seguidas: 192 passam, 4 pulam. Sem script de banco.

#### AA nas telas que faltavam: botões vermelhos, etapas, títulos de seção, botão "+" e login (dev. Jean, 2026-11-12) (antiga 41.9)

**Problema (medido em uploads, cadastros e login, já com as correções de acessibilidade anteriores, no ambiente do dev. Jean):**
- **botões vermelhos `.btn-danger`** (Cancelar, Remover, Excluir, Limpar Formulário) no claro: texto branco sobre `#ef4444`→`#dc2626`, **3,76:1** (abaixo do AA);
- no **escuro**, a regra `.theme-dark .btn` tirava o preenchimento de `.btn-danger` e `.btn-secondary`: ficavam só com borda translúcida **1,92:1** (abaixo do AA, 1.4.11), e o "Excluir" ficava igual ao "Editar";
- **botão de etapa ativo** do formulário de Subação (`.wizard-step-btn.active`): branco sobre `--accent`, **1,83:1** no escuro e 5,12 no claro;
- **título de seção** (`.form-section-title`) na cor de destaque: 5,12 no claro;
- **botão "+" de adicionar linha** da Meta Física: **18×18 px** (abaixo de 24, 2.5.8) e com opacidade 85%;
- **login** no claro: título e "Esqueci a senha" em `--accent`, 4,70:1; e o "Entrar" (depois do AA+ no claro) ficava verde em qualquer cor de destaque, porque o login só carrega o `--accent`, não a paleta inteira.

**Correção (só CSS, bloco `[jean]` no fim do `style.css`):** `.btn-danger` `#a51d1d`→`#991b1b` no claro (≥ 7,5:1 com branco) e preenchido `#ffb3b3` com texto escuro no escuro; `.btn-secondary` com `--spo-control-border` no escuro; etapa ativa e título de seção no `--spo-accent-text` (fundo ou texto, como os demais) e etapas inativas com contorno de controle; botão "+" com 24×24 px (44 no toque) e opacidade 1; login com `--accent` escurecido por `color-mix` (50%, ou 40% no Amarelo) no título, no link e no "Entrar". `templates/base.html` e `templates/login.html`: `?v=20261112.jean-telas.1` (o login ainda estava em `20261002.sync-main.1`).

**Depois:** as 7 telas (FIP 613 e EMP de Atualizar; Dotação formulário/consulta; Meta Física formulário/consulta; Subação) **sem nenhum texto abaixo de 7:1, contorno abaixo de 3:1 ou alvo abaixo de 24 px, nas 8 cores × claro/escuro**; ícone do "+" ≥ 10:1; login ≥ 9:1 no Clássico e no Amarelo. Efeito visível: vermelho mais escuro no claro; no escuro, botões vermelhos voltam a ser vermelhos (rosa claro).

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean, com o usuário de teste recebendo acesso temporário às telas (só visualização; nada enviado; acesso revogado ao final); capturas antes/depois. `pytest` completo duas vezes seguidas: 192 passam, 4 pulam. Sem script de banco.

#### Gráficos acessíveis: resumo para leitor de tela e tabela de dados (dev. Jean, 2026-11-12) (antiga 41.10)

**Problema:** os 5 gráficos Plotly do painel Teto Orçamentário (grupo de despesa, ADJ, Pareto de macropolíticas, ação/PAOE, exercício × grupo) não tinham **nenhuma alternativa em texto** (WCAG 1.1.1, **nível A**): sem nome, sem descrição, sem tabela; o leitor de tela lia só fragmentos do desenho ("74,4%20,4%5,19%", "417244918040…").

**Correção (bloco `initGraficosAcessiveis` no fim do `static/js/main.js`, CSS em bloco `[jean]` no fim do `style.css`; o código dos gráficos não foi alterado):** para cada gráfico Plotly (`.js-plotly-plot`), lendo os dados do próprio gráfico (`gd.data`):
- `role="img"` + `aria-label` com título (o `h2` do painel), tipo, número de itens, maior item (ou maior valor e série, com várias séries) e total — por exemplo: "Teto por grupo de despesa. Gráfico de pizza com 3 itens. Maior: 1 - Pessoal e Encargos Sociais, R$ 8.430.673.401,00. Total: R$ 11.333.931.142,00…";
- logo abaixo, um `<details>` **"Ver dados do gráfico em tabela"** com os mesmos números (uma coluna por série, `% do total` quando há uma série só, linha de total; `caption`, `th scope`); percentuais do eixo secundário (Pareto) como %;
- atualiza sozinho quando o gráfico é redesenhado (filtros, tema), por `MutationObserver` com assinatura dos dados — sem laço com a própria tabela.

**Verificação:** SPO local contra o banco de desenvolvimento do dev. Jean: na árvore de acessibilidade os 5 gráficos aparecem como imagem com nome; somas conferidas (pizza = indicador "Teto total"; exercício: soma dos grupos = "Total Geral"); filtro "4 - Investimentos" atualiza resumo e tabela; teclado: foco no "Ver dados…" com anel visível e Enter abre; contraste mínimo 9,85:1 (claro) e 10,56:1 (escuro); a 390 px a página não transborda (tabelas largas rolam na própria caixa). `node --check` ok; `pytest` completo duas vezes seguidas: 192 passam, 4 pulam. Sem script de banco. `templates/base.html`: `?v=20261112.jean-graficos.1` no CSS e no JS.

#### Semântica e teclado: menu operável, título da aba, "Pular para o conteúdo", avisos modais, abas e filtros (dev. Jean, 2026-11-12) (antiga 41.11)

_Parte em vigor: ficou no [claude.md](claude.md), seção 41 (41.11)._

**Problema (medido com teclado real, no ambiente do dev. Jean):**
- **o menu não era operável pelo teclado** (WCAG 2.1.1, **nível A**): os grupos (Administrar SPO, Institucional, Relatórios, Painéis, Usuários e os subgrupos) são `<div class="menu-item menu-parent">` sem foco; na Início o Tab só passava por "Expandir menu → Início → Sair → Fechar". Quem usa só o teclado não chegava a nenhum relatório;
- o **título da aba** era sempre "SPO" (2.4.2, A);
- sem **"Pular para o conteúdo"** (2.4.1);
- ao trocar de tela o foco ficava no menu e nada anunciava a página nova; Início e Teto **sem título principal**;
- **avisos modais** (`.modal-overlay`) sem `role="dialog"`/`aria-modal`, sem levar o foco para dentro nem prendê-lo, sem Esc;
- **abas** Gráficos/Tabelas do Teto sem `aria-controls` nem setas;
- **filtros** que abrem lista (`.mf-display`, `.planning-action-checklist-toggle`) sem `aria-expanded` (4.1.2).

**Verificação (teclado real):** Tab: "Pular para o conteúdo → Expandir menu → Início → Administrar SPO → Institucional → Relatórios → Painéis…"; Enter em Relatórios → Execução → FIP 613 abre a tela, foco no `h1`, aba "FIP 613 — SPO", aviso "Página carregada: FIP 613"; modal de pendências da Início: foco dentro (3 de 3 aberturas), Tab preso, Esc fecha e o foco vai ao título; abas do Teto com setas; filtros com `aria-expanded` correto. Dois ajustes no caminho: a troca de tela roubava o foco do modal aberto; e o "devolver foco" de um aviso fechado tirava o foco de outro aviso aberto — corrigidos. `node --check` ok; `pytest` completo duas vezes seguidas: 192 passam, 4 pulam. Sem script de banco.

