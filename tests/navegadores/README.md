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
