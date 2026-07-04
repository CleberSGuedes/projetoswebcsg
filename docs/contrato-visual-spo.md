# Contrato Visual SPO

Este documento registra o contrato atual de layout, temas e responsividade do SPO. Ele existe para evitar que um ajuste de modo móvel, split do Chrome, barra de rolagem ou cor de destaque interfira em outra camada.

## Classes de ambiente

- `spo-shell`: casca global do sistema.
- `spo-env-dev`: ambiente de integração DEV.
- `spo-env-lab`: ambiente de laboratório.
- `lab-spo`: marcador reservado para experiências visuais do laboratório.

## Modos responsivos

- Modo móvel real: controlado por CSS em `@media (max-width: 420px)`. Deve permanecer fluido, sem largura mínima operacional e sem barra horizontal geral.
- Split sob pressão: controlado por JavaScript com a classe `spo-split-pressure`. Este modo é ativado quando a área útil da página fica estreita, mas a janela externa do Chrome continua ampla. Ele restaura a largura mínima operacional interna para preservar a leitura em visualização dividida.
- PTA 2027 embutido: dentro do iframe, o modo móvel real deve remover a largura mínima do mockup e deixar `value-view`, `wizard-view`, árvore de jornada e contexto ativo fluidos. O host só libera a largura do iframe quando `spo-split-pressure` não está ativo, para não misturar o modo celular com o split do Chrome.

## Barras de rolagem

As barras usam o padrão global `scrollbar-visible`: ficam discretas em repouso e assumem a cor de destaque durante interação. Novas áreas roláveis devem entrar na lista de seletores de scrollbar em `static/css/style.css`.

## Cores de destaque

As cores ficam centralizadas em variáveis CSS e no mapa `ACCENT_COLORS` em `static/js/main.js`. Evite cores fixas em novos componentes; prefira `--accent`, `--accent-strong`, `--accent-rgb` e `--accent-soft-rgb`.

## Personalização comum

A personalização comum é a camada gerencial segura para preferências visuais do usuário. No laboratório, ela já pode ser testada em `Personalizar SPO` com armazenamento local no navegador, sem banco de dados.

Padrão visual do laboratório:

- modo de aparência: automático;
- cor de destaque: azul;
- densidade visual: confortável;
- contraste: padrão;
- estilo de cartões: suave;
- diagramas: automático.

O botão `Restaurar padrão do laboratório` deve sempre retornar a esse conjunto. Ele limpa a preferência comum local e reaplica os valores padrão no navegador, sem alterar banco de dados, perfil oficial ou contrato protegido.

Opções atuais:

- tema: claro, escuro ou automático;
- cor de destaque;
- densidade visual: confortável, compacta ou ampla, com efeito em espaçamentos, tabelas, cartões e gráficos;
- contraste: padrão ou reforçado, com bordas, foco e leitura mais fortes;
- estilo de cartões: suave, contornado ou plano, com sombra/borda ajustadas;
- preferência de diagramas: automático, claro, escuro ou alto contraste, com efeito em fluxos conceituais e gráficos Plotly do painel de teto orçamentário.

Na tela, essas preferências aparecem como submenus dinâmicos: o cabeçalho mostra a escolha atual e abre somente as opções daquele grupo. Esse padrão evita um card longo e segue a lógica de contexto expansível usada no piloto PTA 2027.

As preferências comuns usam `localStorage` e atributos no `body`, como `data-visual-density`, `data-visual-contrast`, `data-visual-cards` e `data-visual-diagrams`. Novas regras devem consumir esses atributos sem alterar os contratos de modo móvel e split.

O bloco `Prévia` da tela `Personalizar SPO` deve responder automaticamente a essas preferências. Ele é a miniatura de validação local para densidade, contraste, estilo de cartões e tema de diagramas antes de testar a preferência no restante do sistema.

Quando `data-visual-diagrams` muda, a aplicação emite o evento local `spo-common-visual-change`. Painéis já carregados devem escutar esse evento quando precisarem redesenhar gráficos ou diagramas em tempo real.

Decisão LAB em 04/07/2026 para diagramas:

- `automático`: acompanha o modo de aparência atual. No tema claro, usa preenchimento azul suave com bordas um pouco mais presentes para evitar cards lavados; no tema escuro, aproxima-se do modo escuro. Se o sistema operacional/navegador indicar `prefers-contrast: more`, o modo automático resolve para a variante contextual de alto contraste (`highLight` ou `highDark`) sem alterar a preferência salva.
- `claro`: força cards claros e deve manter bordas legíveis no tema claro. Quando usado sobre tema escuro, cada card precisa carregar contraste próprio, sem depender do fundo da página.
- `escuro`: força cards escuros e deve manter texto, ícones e setas legíveis mesmo quando aplicado sobre tema claro.
- `alto contraste`: é um modo de legibilidade contextual, não uma paleta fixa decorativa. A implementação deve resolver a variante real pelo contexto (`highLight` ou `highDark`) em `data-visual-diagram-theme`, preservando texto, bordas, ícones e setas fortes sem quebrar o tema geral.
- fluxos conceituais, como o `Mapa Conceitual`, não devem receber moldura externa em `.governanca-flow`. O contrato visual atua nos cards e nos elementos funcionais do diagrama. Painéis de gráfico, como `.teto-chart`, podem manter fundo e borda próprios quando isso ajuda a leitura.

Compatibilidade com cores forçadas:

- se o sistema operacional/navegador indicar `forced-colors: active`, a aplicação deve respeitar as cores semânticas do sistema (`Canvas`, `CanvasText`, `Highlight`, `HighlightText` e `LinkText`);
- nesse cenário, o modo resolvido dos diagramas passa para `system` em `data-visual-diagram-theme`, sem alterar a preferência salva em `localStorage`;
- a camada de cores forçadas tem prioridade visual sobre `claro`, `escuro`, `automático` e `alto contraste`, porque o usuário está pedindo ao sistema operacional para controlar a legibilidade;
- sombras, fundos decorativos e gradientes devem ser removidos ou neutralizados, mantendo bordas, foco, menus, prévia, fluxos conceituais e gráficos legíveis;
- gráficos Plotly devem receber cores resolvidas a partir das cores reais do sistema no navegador, para evitar paletas fixas que possam brigar com temas de alto contraste do Windows.

Tokens semânticos de diagramas:

- diagramas devem usar nomes funcionais de cor, não apenas cores decorativas;
- tokens atuais: `connector`, `focus`, `selection`, `warning` e `critical`, além de texto, fundo, borda, eixo e grade;
- `connector` deve orientar setas, conectores e linhas de vínculo;
- `focus` e `selection` devem marcar navegação, foco de teclado, item selecionado ou estado ativo;
- `warning` e `critical` devem ser reservados para atenção operacional, limite, risco ou linha de referência crítica;
- em `forced-colors: active`, esses tokens devem ser mapeados para as cores semânticas do sistema;
- a verificação local `window.spoGetDiagramSemanticTokens()` expõe o modo resolvido e os tokens ativos para teste no navegador.

Validação automática local:

- os temas de diagramas devem ser validados por `window.spoValidateDiagramContrast()`;
- a validação calcula contraste para texto, texto secundário, eixos e séries críticas dos gráficos;
- em LAB/DEV, pares abaixo do contrato visual aparecem como aviso no console, sem bloquear a interface do usuário;
- mínimos iniciais: texto principal e secundário `>= 4.5:1`; eixos, linhas e séries críticas `>= 3:1`.
- linhas de grade dos gráficos são referência auxiliar e devem permanecer discretas; elas não entram no mínimo de contraste crítico quando não carregam informação semântica própria. Se a necessidade permanecer, uma próxima preferência pode controlar grade `discreta`/`oculta`, sem misturar isso com o modo de alto contraste.

Regras de aplicação:

- a personalização comum deve ser reversível pelo botão de restauração do laboratório;
- as mudanças devem ser visíveis imediatamente na tela, especialmente na prévia local;
- as preferências comuns podem alterar conforto visual, leitura e componentes, mas não podem redefinir regras protegidas do contrato visual;
- os modos de diagramas devem ter efeito visual real e testável nos fluxos conceituais e nos gráficos, sem criar molduras decorativas que conectem blocos independentes;
- preferências explícitas de diagramas (`claro`, `escuro` e `alto contraste`) têm prioridade sobre `prefers-contrast`; a preferência do sistema só ajusta o modo `automático`;
- qualquer nova preferência deve ter valor padrão, efeito visual verificável, resumo textual e registro neste documento;
- enquanto estiver no laboratório, a persistência permanece restrita a JavaScript/localStorage.

## Relação entre preferências e presets

As preferências comuns e o contrato protegido pertencem ao mesmo contrato de tema/estilo, mas têm papéis diferentes.

Preferências comuns:

- são ajustes locais de experiência visual;
- podem ser usadas no cotidiano pelo administrador;
- cuidam de tema, cor, densidade, contraste, cartões e diagramas;
- têm prévia automática e restauração rápida para o padrão do laboratório.

Contrato protegido:

- é restrito a Jean/Cleber no piloto;
- organiza presets gerenciais versionados;
- permite testar combinações mais amplas de layout, responsividade e leitura operacional;
- registra rascunho, prévia, ativação local, pacote JSON e auditoria local;
- prepara a futura edição fina de presets com persistência estruturada e governança.

No futuro, o editor local de preset pode evoluir para um editor governado de tema/estilo. Essa evolução deve preservar a divisão atual: preferências comuns continuam sendo ajustes seguros de usuário, enquanto presets protegidos continuam sendo mudanças gerenciais, versionadas e auditáveis.

## SVGs e diagramas temáticos

Diagramas conceituais em SVG devem ter par claro/escuro no mesmo diretório: `arquivo.svg` e `arquivo.dark.svg`. A troca deve ser feita por `data-light-src` e `data-dark-src`, usando a rotina de sincronização do tema após renderização dinâmica e após mudança de tema.

No modo escuro, o SVG deve declarar seu próprio fundo, texto, bordas, preenchimentos e setas. Evite depender de filtros CSS globais para inverter cores, porque isso reduz contraste, prejudica exportação para PNG e torna alguns blocos invisíveis.

## Contrato protegido

O contrato protegido é uma camada de governança visual para ajustes gerenciais sensíveis. No piloto do laboratório, ele aparece apenas para usuários Jean e Cleber na tela `Personalizar SPO`.

A implementação atual usa presets versionados, prévia, rascunho, ativação local, pacote JSON e registro local no navegador. Ela não altera banco de dados, permissões oficiais nem tabelas. Quando for oficializada, a persistência deve ir para o servidor com perfil autorizado, justificativa, trilha de auditoria e restauração do preset padrão.

No LAB, o catálogo de presets protegidos funciona como uma base local em JavaScript/localStorage para permitir testes reais antes da modelagem em banco. O estado fica em `spo-protected-contract-state`, com `active`, `draft`, `preview`, `version`, `source`, `updatedAt` e `updatedBy`; edições experimentais de presets ficam em `spo-protected-contract-custom-presets`; o histórico local fica em `spo-protected-contract-audit`. A chave antiga `spo-protected-contract-preset` é mantida apenas por compatibilidade com versões anteriores.

Subáreas da tela:

- `Presets`: escolha da prévia, promoção para rascunho, ativação do rascunho, ativação direta da prévia e restauração do padrão;
- `Regras protegidas`: leitura do escopo, estado, regras e detalhes de cada preset, com indicação de ativo local, rascunho, prévia e edição local; esta área permanece como catálogo protegido e não edita diretamente os elementos;
- `Editor local`: edição experimental de nome, escopo, estado, resumo, regras e detalhes de uma cópia local do preset escolhido; a edição pode entrar como prévia ou rascunho local, sem criar regra oficial;
- `Pacote local`: geração, cópia e importação de pacote JSON para transporte de testes entre navegadores; a importação entra como rascunho e prévia, inclui edições locais quando existirem e não altera o ativo local;
- `Auditoria`: registro local estruturado das ações do navegador, com ação, preset, usuário, origem, versão, horário e retrato do estado no momento da ação.

Presets atuais:

- `Padrão SPO`: mantém layout, temas e responsividade validados;
- `Compacto gerencial`: reduz espaços, densidade de painéis, padding de cartões e tabelas sem mudar hierarquia;
- `Contraste técnico`: reforça bordas, estados ativos, foco e leitura técnica;
- `Pressão operacional`: aumenta áreas de toque, ajusta grids e realça dimensões usadas no modo móvel e split.

O preset em prévia aplica efeitos visuais reais por atributos `data-visual-contract-preset` no `body` e no `html`. Isso permite testar compactação, contraste e pressão operacional antes de salvar rascunho ou ativar localmente.

Quando migrar para banco, preservar as chaves dos presets, a versão do contrato, o fluxo `preview -> draft -> active`, o esquema do pacote local `spo.protected-contract.local-package`, o campo `customPresets` dos pacotes de laboratório e a trilha de auditoria local como modelo inicial de governança visual.

Os presets protegidos devem alterar somente variáveis e classes de contrato. Evite sobrescrever regras específicas de tela, para não misturar configuração gerencial com correção pontual de layout.

### Validação LAB

Antes de promover a implementação para outro ambiente:

1. confirmar que o bloco protegido aparece apenas para usuários Jean/Cleber;
2. testar `Presets`, `Regras protegidas`, `Editor local`, `Pacote local` e `Auditoria`;
3. validar que prévia, rascunho, ativo local e restauração alteram somente `localStorage`;
4. validar que edições locais aparecem na prévia, nas regras protegidas, no pacote JSON e na auditoria;
5. testar os presets em tema claro, tema escuro, janela ampla, modo móvel real e split sob pressão;
6. confirmar que nenhuma rota ou tabela de banco foi adicionada para o contrato protegido.

## Regra de manutenção

Antes de alterar responsividade, verifique se a mudança afeta:

1. janela normal ampla;
2. modo móvel real;
3. split sob pressão;
4. tema claro;
5. tema escuro.
