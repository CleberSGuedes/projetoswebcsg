# Bateria SPO-AA+ (testes nos navegadores)

Confere, de forma automática, se o SPO e o kit de mockup fazem o que o padrão SPO-AA+
(`docs/padrao-visual-spo-aa-plus.md`) promete, nos três motores de navegador: **Chromium** (Chrome, Edge, Samsung
Internet), **Firefox** e **WebKit** (Safari e todo navegador do iPhone), em desktop, celulares e tablets. Junta numa
ferramenta só as baterias que guiaram o AA+ (contraste, tipografia, Personalizar, modo móvel, equivalência com o kit).

**Só lê e toca:** abre telas pelo menu, clica nas preferências do Personalizar (gravadas no navegador), digita em
filtros e escolhe opções. **Nada é gravado no banco.** Uma tela por vez: o banco de desenvolvimento é remoto e tem
poucas conexões. **Nunca rode contra a produção.**

## Níveis

| Nível | Quando | Tempo | O que roda |
|---|---|---|---|
| `basico` | a cada PR | ~3 min | travas do padrão (pytest); 5 telas (Início, Personalizar, FIP 613, Cadastrar Dotação, Programação PTA/OpR) nos três motores (desktop) e no Galaxy S26; SPO × kit no tema, destaque e tamanho do texto |
| `intermediario` | a cada lote | ~7 min | travas; as 11 telas principais em 6 perfis (3 desktops, S26, iPhone 16, iPad); SPO × kit em todas as opções do Personalizar (58 valores), presets do Contrato e tema "Automático"; contraste em 4 telas, claro/Azul e escuro/Amarelo reforçado; recursos-chave no desktop, no S26 e no iPhone; teclas de um toque |
| `completo` | antes de enviar um lote, ou numa mudança grande | ~30 min | travas; **todas as telas do menu** em 4 perfis, as 11 principais nos 10 aparelhos e 5 telas nos 4 perfis "rápidos" (deitado, tela externa, dobrável, iPad 13); SPO × kit nos três motores; contraste em 7 telas × 5 combinações de preferências, no desktop e no celular; recursos-chave nos 10 aparelhos; teclas de um toque nos três motores |

Ficam fora da varredura: os relatórios pesados (EMP, NOB e EST EMP: consultas de minutos), a Limpeza de Histórico
(mede o banco ao abrir) e "Sair".

## O que cada verificação mede

- **travas** — `tests/test_cor_literal.py`, `test_tamanho_fonte.py`, `test_style_inline.py` e `test_kit_mockup.py`.
- **telas** — rolagem lateral; elemento fora da tela; texto cortado; alvo de toque com menos de 24 px (WCAG 2.5.8;
  links dentro de frases são exceção do critério); aviso que abre sozinho; erro de script.
- **contraste** — todo texto visível contra o fundo real (compõe transparências, opacidade e degradês): **falha**
  abaixo do AA (4,5:1; 3:1 texto grande), **aviso** abaixo da meta AA+ (7:1; 4,5:1 grande).
- **equivalencia** — cada valor de cada opção do Personalizar, clicado no SPO e no kit: os mesmos atributos e
  variáveis, e o mesmo estilo calculado nas duas prévias do Personalizar, elemento por elemento; todo valor fora do
  padrão tem de mudar algo. Presets do Contrato e tema "Automático" com o sistema escuro.
- **recursos** — menu no celular (o "›" abre e o Esc fecha); lupa e "?" no cabeçalho; filtro dos relatórios (abre,
  "Aplicar" à vista, nomes inteiros, letra normal, clicar fora fecha); busca nas listas longas (filtra, conta, Esc
  devolve); barra de título da tela (botões, "›" no centro deles, título inteiro, volta ao sair); rolagem do
  "Automático" (nunca sobe a página); kit (abre, sem rolagem lateral, com o painel); teclas de um toque.

## Preparar (uma vez)

1. Node.js 20 ou mais novo. Nesta pasta: `npm ci`.
2. Navegadores do Playwright: `npx playwright install chromium firefox webkit` (ou aponte
   `PLAYWRIGHT_BROWSERS_PATH` para uma pasta onde já estejam).
3. **Usuário de teste** no banco de **desenvolvimento**, com perfil de administrador (para ver todas as telas), só
   para a bateria — nunca o login de uma pessoa. Credenciais por variável de ambiente (`BATERIA_EMAIL`,
   `BATERIA_SENHA`) ou num arquivo `.env.bateria` na raiz do repositório (ignorado pelo git):

   ```
   BATERIA_EMAIL=bateria@exemplo.test
   BATERIA_SENHA=...
   ```

   Outro caminho: `BATERIA_ENV=caminho/do/arquivo`. A sessão fica em `sessoes/` (ignorada pelo git) e é renovada
   sozinha quando cai; se o usuário estiver logado em outro lugar, a segunda tentativa entra mesmo assim.
4. O SPO ligado (padrão: `http://localhost:5000/`; outro endereço: `BATERIA_BASE`).

## Rodar

```
npm run basico
npm run intermediario
npm run completo
node bateria.mjs intermediario --so=contraste,recursos
```

Relatório em `resultados/<data-hora>/relatorio.md`, com `resultados.json` e as capturas das falhas (`resultados/`
é ignorada pelo git). O comando termina com código 1 se houver falha.

## Comparação antes × depois (mudança que não pode mudar a aparência)

Para provar que uma mudança **não mudou nada na tela** (dividir o `style.css` ou o `main.js`, mover código, trocar a
ordem de carregamento): grava-se um retrato de cada tela **antes** e compara-se **depois**, elemento por elemento.

```
npm run comparar:gravar -- --nome=antes                 # ANTES de mexer (11 telas e o kit × 3 vistas, 2–4 min)
npm run comparar -- --nome=antes                        # DEPOIS (lista cada diferença)
npm run comparar:gravar -- --nome=antes --telas=todas --vistas=ampla   # mais telas e vistas
npm run comparar:gravar -- --nome=antes --telas=inicio,kit             # só as telas que a mudança atinge (~40 s)
```

**Escolha só as telas que a mudança atinge.** Uma mudança no cabeçalho ou no menu aparece em qualquer tela:
`--telas=inicio,kit` basta. Uma mudança num relatório: a rota dele e o kit. A lista completa fica para mudanças que
atingem tudo (dividir o CSS ou o JS).

- **Retrato:** o estilo calculado de cada elemento da tela (cerca de 110 propriedades: cores, fundo, fonte, bordas,
  espaçamentos, grid/flex, sombras...), a caixa (posição e tamanho), os `::before`/`::after`, os tokens (variáveis
  CSS) e uma captura. O elemento é achado pelo caminho de tags desde o `<body>`; `<script>` e `<link>` não contam, então
  acrescentar um arquivo de CSS ou JS não muda o caminho de nada. O retrato é **compacto**: cada combinação de estilo
  é guardada uma vez só e o elemento guarda o número dela (as tabelas repetem o mesmo estilo em milhares de células).
- **Telas:** `basicas` (5), `principais` (11, padrão), `todas`, ou uma lista separada por vírgulas (`inicio`, `kit` e
  rotas do menu); os conjuntos terminam sempre com o **kit de mockup** (ele usa o
  mesmo `style.css` e lê trechos do `main.js`, então uma divisão também pode quebrá-lo); `kit` = só o kit. **Vistas** (aparelho + preferências do
  Personalizar): `padrao` (desktop claro, desktop escuro/Amarelo reforçado, Galaxy S26) ou `ampla` (+ Firefox,
  Safari, leitura com fonte 150% e iPhone escuro).
- **Resultado** por tela: **ok** (igual), **aviso** (estilo igual; só mudou um dado ao vivo ou uma medida de até 1 px)
  ou **falha** (diferença de estilo). A lista inteira vai para `resultados/<data-hora>/diferencas/diferencas.md`
  (tela, elemento, propriedade, antes → depois), com as capturas de antes e depois das telas com falha. A referência
  fica em `resultados/referencias/<nome>/` (fora do git).
- **O que não reprova** (e por quê), sempre listado:
  - **dado ao vivo:** o texto de um elemento mudou (relógio, sessões ativas, dado do banco) ou uma linha de lista ou
    tabela entrou ou saiu; a caixa dele, das que o contêm e as faixas dos grids que o contêm mudam junto;
  - **até 1 px** num comprimento em px ou numa caixa: arredondamento de medida feita pelo script (cor, opacidade e
    peso de fonte não têm tolerância);
  - identificadores sorteados a cada carga (os `url(#clip…)` dos gráficos);
  - o vizinho que só **se deslocou** (mesma largura e altura) dentro de um pai que contém um texto que mudou (a hora
    que encolhe numa linha alinhada à direita empurra o nome que vem antes);
  - uma diferença que **não se repete** quando a tela é aberta de novo (a comparação olha duas vezes antes de
    reprovar).
- **Espera a tela ficar pronta**, sem tempos fixos: carregamento encerrado, nenhuma requisição ao servidor pendente
  (as tabelas chegam por `fetch` depois do carregamento), fontes carregadas e nenhum elemento novo por 0,7 s; depois, o
  fim das animações.
- **Limites:** não entra no conteúdo de `<iframe>` (a Programação PTA/OpR) nem nos estados de passar o mouse e de foco.
  O banco precisa ser o mesmo entre gravar e comparar (com o banco mudando, os dados viram avisos, não falhas).

**Provado** (17/11/2026; 33 casos = 11 telas × 3 vistas, e o kit em 3 vistas; mudanças temporárias, desfeitas depois). Duas provas, uma
para cada lado: a ferramenta não pode dar alarme falso numa mudança segura, e tem de pegar uma mudança de verdade.

| Prova | Mudança feita | O que se esperava | Resultado |
|---|---|---|---|
| A — mudança segura | `style.css` cortado em 3 arquivos, carregados na mesma ordem | "nada mudou" | ✅ nada mudou nos 33 casos |
| B — mudança de propósito | as 3 partes na ordem 3, 1, 2, e a borda do cabeçalho de alfa 0,10 para 0,11 (invisível a olho nu) | "mudou", com a lista | ✅ mudou nos 33 casos: a borda em todas as telas; a ordem trocada em tokens, sombras, cores e larguras |
| Kit — mudança de propósito | +2 px no espaçamento de um cartão só do kit (`.kit-como-usar`) | "mudou" só no kit, onde a regra vale | ✅ mudou no kit no desktop (claro e escuro); no celular, igual: lá uma regra do SPO (densidade espaçosa, 18 px) vence a do kit, então a mudança não aparece; as telas do SPO, iguais |
| Kit — CSS em 3 partes | o kit carregando as 3 partes, na mesma ordem | "nada mudou" | ❌ **mudou: achado real.** O `kit-mockup.js` procura o CSS do SPO pelo nome (`link[href*="css/style.css"]`); sem esse arquivo, o script do kit para e a cor de destaque e várias seções somem. Na divisão do CSS, o kit precisa achar o CSS de outro jeito |

**Tempo** (17/11/2026, banco de desenvolvimento remoto; varia com a carga dele): gravar as 11 telas e o kit em 3
vistas caiu de **358 s para 138–211 s**, e a referência de **329 MB para 45 MB**, com o retrato compacto e a espera
pela tela pronta no lugar dos tempos fixos. A comparação inteira leva 2–3 min; a dirigida (`--telas=inicio,kit`),
~1 min. Conferido que o resultado não mudou: a versão nova, comparando contra uma referência gravada pela antiga, não
achou nenhuma diferença de estilo.

## Tempos-limite (a bateria nunca fica parada em silêncio)

- **Cada passo** (uma tela, um valor do Personalizar, uma checagem de recurso) tem limite de **180 s**
  (`BATERIA_LIMITE_PASSO_S`). Estourou: a falha é registrada com o que estava rodando ("passou de 180 s sem terminar:
  tela relatorios_fip613 (D-firefox-1366)"), o navegador é fechado e a bateria segue (nas telas e no contraste, com
  um navegador novo; na equivalência e nos recursos, a partir do perfil seguinte).
- **Vigia:** se nenhum resultado novo aparecer por **6 min** (`BATERIA_LIMITE_SEM_PROGRESSO_MIN`), a bateria registra
  onde parou, grava o relatório e sai com código 1.
- Também: ações e esperas do Playwright com 30 s, navegação com 60 s, e o pytest das travas com 5 min.
- Para conferir o vigia: `BATERIA_TESTE_TRAVA=1` acrescenta um passo que nunca termina (com
  `BATERIA_LIMITE_SEM_PROGRESSO_MIN=0.25`, a bateria para em ~30 s, com o relatório).

## Manter

- **Tela nova no menu** entra sozinha no nível completo se estiver em `TELAS_TODAS` (`lib/ambiente.mjs`); tela
  importante, também em `TELAS_PRINCIPAIS`.
- **Opção nova no Personalizar:** acrescentar em `GRUPOS` (`verificacoes/equivalencia.mjs`), com o seletor do controle,
  os valores e o padrão.
- **Recurso novo** (componente com comportamento): uma checagem em `verificacoes/recursos.mjs`, dizendo o que se
  espera, como as que estão lá.
- **Aparelho novo da equipe:** em `PERFIS` (`lib/ambiente.mjs`), com as medidas do "Seu aparelho" do kit.
- Um achado da bateria vira PR `fix/` e, se for regra, linha no padrão SPO-AA+.
