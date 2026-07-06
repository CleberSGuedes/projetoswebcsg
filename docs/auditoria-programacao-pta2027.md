# Auditoria inicial - Programacao PTA 2027

Data LAB: 04/07/2026

Esta auditoria registra a leitura inicial do modulo `Programacao PTA 2027` antes de novos ajustes visuais ou funcionais. O objetivo e preservar o que ja funciona, identificar riscos reais e definir uma ordem segura de evolucao dentro do contrato visual do SPO.

## Escopo

Arquivos observados nesta rodada:

- `templates/partials/atualizar_governanca_resultados_programacao_pta2027.html`
- `static/js/main.js`
- `static/css/style.css`
- `static/pta2027_v63r/index.html`
- `static/pta2027_v63r/assets/pta-2027.js`
- `static/pta2027_v63r/assets/pta-2027.css`
- `static/pta2027_v63r/assets/eap-data.js`

Nenhuma alteracao funcional foi feita nesta auditoria.

## Leitura geral

`Programacao PTA 2027` deve ser tratada como um fluxo imersivo especializado. Ela pertence ao contrato visual do SPO, mas nao deve ser encaixada no mesmo molde dos fluxos conceituais simples de `Governanca e Resultados`.

O modulo tem duas camadas:

- host SPO: carrega o iframe, controla o modo imersivo, sincroniza altura, tema, contexto e retorno ao topo;
- mockup PTA 2027: concentra a experiencia real de programacao, com cards explicativos, selecao de macro politica, jornada de programacao, rascunho local, EAP e contexto ativo.

Essa separacao e positiva. O host permanece pequeno e o modulo imersivo preserva sua propria experiencia, mas a comunicacao entre eles precisa continuar explicita e estavel.

## Contrato host e iframe

Mensagens ja usadas entre SPO e iframe:

| Mensagem | Sentido | Funcao |
| --- | --- | --- |
| `spo-theme` | host -> iframe | sincroniza tema, destaque, dimensoes do shell e preferencia visual |
| `pta2027-height` | iframe -> host | ajusta altura do iframe ao conteudo interno |
| `pta2027-immersive` | iframe -> host | ativa ou remove o modo imersivo no host |
| `pta2027-context-focus` | iframe -> host | informa quando a programacao exige foco contextual |
| `pta2027-scroll-top` | iframe -> host | pede retorno ao topo da area integrada |

Essas mensagens devem ser preservadas em qualquer refino. Se um ajuste visual quebrar uma delas, o sintoma pode aparecer como altura errada, menu lateral fora do modo esperado, perda de foco contextual ou comportamento ruim no celular/split.

## Experiencia atual

O `index.html` do mockup organiza a experiencia em duas vistas:

- `value-view`: entrada conceitual, cards explicativos, cadeia de valor, EAP e expansao de macro politicas;
- `wizard-view`: ambiente de programacao com tres areas funcionais.

No modo de programacao, as tres areas esperadas sao:

- arvore de evolucao: orienta as etapas da jornada;
- coluna principal: edita a etapa atual;
- contexto ativo: mostra subsidios, politica publica, metas, chave de planejamento e qualidade.

Esse desenho deve ser preservado. A harmonizacao visual deve melhorar leitura, foco, contraste e responsividade sem transformar a tela em uma lista simples de cards.

## Estrutura hierarquica textual

Esta estrutura registra a leitura funcional atual do ambiente `Programacao PTA 2027`. Ela deve servir como referencia antes de novos ajustes visuais, funcionais ou de persistencia.

```text
SPO
└─ Governanca e Resultados
   └─ Programacao PTA 2027
      ├─ Visao inicial: Cadeia estrategica de programacao
      │  ├─ Macropoliticas
      │  │  ├─ Desenvolvimento Educacional
      │  │  ├─ Curriculo Ampliado
      │  │  ├─ Avaliacao
      │  │  ├─ Equidade e Diversidade
      │  │  ├─ Acesso e Permanencia
      │  │  ├─ Promocao da Cultura de Paz
      │  │  ├─ Gestao e Inovacao
      │  │  ├─ Regime de Colaboracao
      │  │  ├─ Valorizacao Profissional
      │  │  └─ Infraestrutura Escolar
      │  └─ Ao selecionar uma macropolitica
      │     ├─ Lista de eixos de execucao / politicas PAOE
      │     └─ Acao: Programar
      │
      ├─ Ambiente Programar
      │  ├─ Cabecalho imersivo
      │  │  ├─ Governanca e Resultados
      │  │  ├─ Programacao PTA 2027
      │  │  ├─ Usuario / data / logados
      │  │  └─ Voltar a cadeia estrategica
      │  │
      │  ├─ Jornada de programacao
      │  │  ├─ 1. Direcionamento
      │  │  ├─ 2. Regionalizacao
      │  │  ├─ 3. Subacoes
      │  │  ├─ 4. Etapas
      │  │  ├─ 5. Orcamento
      │  │  └─ 6. Revisao
      │  │
      │  ├─ Contexto ativo
      │  │  ├─ Macropolitica
      │  │  ├─ Eixo de execucao
      │  │  ├─ Politica PAOE
      │  │  ├─ Pilar / Metas PEE
      │  │  ├─ Chave de planejamento
      │  │  └─ Qualidade da programacao
      │  │
      │  └─ Etapas detalhadas
      │     ├─ 1. Direcionamento estrategico
      │     │  ├─ Origem estrategica selecionada
      │     │  └─ Responsabilidade e enquadramento
      │     ├─ 2. Regioes e metas fisicas
      │     │  └─ Regiao de referencia
      │     ├─ 3. Subacoes e entregas
      │     │  ├─ Subacao
      │     │  ├─ Componentes vinculados
      │     │  ├─ Regionalizacao da subacao
      │     │  └─ Entrega MT
      │     ├─ 4. Etapas de execucao
      │     │  ├─ Marco de execucao
      │     │  └─ Vinculo com processo / risco / controle
      │     ├─ 5. Recursos necessarios
      │     │  ├─ PAOE
      │     │  ├─ Fonte
      │     │  ├─ Natureza
      │     │  ├─ Valor
      │     │  └─ Justificativa
      │     └─ 6. Revisao integrada
      │        ├─ Sintese da programacao
      │        ├─ Validacoes SPO
      │        └─ Salvar rascunho local
      │
      └─ Persistencia local temporaria
         ├─ Salvar rascunho no navegador
         ├─ Retomar rascunho salvo
         └─ Validar rascunho antes de abrir
```

## Revisao funcional da jornada

Bloco LAB em 04/07/2026: a jornada foi percorrida da cadeia estrategica ate a revisao integrada, com preenchimento de uma programacao de teste e salvamento de rascunho local.

Fluxo testado:

1. abrir a cadeia estrategica;
2. selecionar uma macropolitica;
3. escolher um eixo/politica PAOE e acionar `Programar`;
4. preencher direcionamento, regionalizacao, subacao, entrega, etapa de execucao e orcamento;
5. chegar a `Revisao integrada`;
6. salvar rascunho local;
7. recarregar a pagina;
8. retomar o rascunho salvo.

Resultado esperado confirmado no LAB:

- o rascunho valido retorna para `wizard-view`;
- `programmingFocus` permanece ativo;
- macro, eixo, chave de planejamento, etapa e contexto ativo sao restaurados;
- rascunho local invalido ou incompatível nao deve abrir a jornada sem validacao previa;
- nenhum banco de dados e alterado.

## Persistencia local

O modulo usa armazenamento local do navegador para testes de laboratorio, sem banco de dados. Chaves observadas:

- `spo-strategic-elements`
- `spo-strategic-pending`
- `spo-support-texts`
- `spo-support-texts-pending`
- `spo-eap-rows`
- `spo-pta-2027-draft`

Essas chaves sustentam rascunhos, elementos estrategicos, textos de apoio e EAP local. Ajustes visuais nao devem alterar esse contrato de persistencia.

## Comportamentos protegidos

Antes de qualquer mudanca, estes comportamentos devem continuar funcionando:

- abrir a pagina em experiencia ampla/imersiva;
- manter cards explicativos disponiveis antes da programacao;
- expandir uma macro politica e permitir `Programar` um eixo;
- ao programar, alternar corretamente de `value-view` para `wizard-view`;
- manter arvore de evolucao, area principal e contexto ativo;
- preservar rascunho local, revisao, salvamento e retorno;
- funcionar em desktop, split do Chrome e modo movel real;
- sincronizar tema claro/escuro e cor de destaque com o SPO;
- respeitar o contrato visual sem forcar padronizacao excessiva.

## Pontos de atencao

### Alto risco

- `pta-2027.js` concentra muitas responsabilidades: dados, renderizacao, eventos, rascunho, EAP, textos de apoio, validacao e integracao com host. Uma mudanca pequena pode afetar a jornada toda.
- `pta-2027.css` tem varias camadas historicas, incluindo regras especificas para embedded mode, mobile, modo escuro e foco de programacao. Ajustes visuais devem ser pequenos e testados em mais de um viewport.

### Risco medio

- A comunicacao por `postMessage` e essencial. Novas mensagens devem ser documentadas antes de uso.
- A experiencia mobile usa regras proprias para reorganizar a jornada. Mudancas no layout desktop podem vazar para celular.
- O contexto ativo e parte central do modulo. Ele nao deve ser tratado como painel acessorio descartavel.
- O modulo ainda conserva modo standalone e modo embutido. Sempre verificar se a mudanca pretendida e para um, para outro, ou para ambos.

### Risco baixo, mas relevante

- Existem trechos de HTML gerados por string. Valores vindos de entrada do usuario devem continuar escapados quando voltarem para a tela.
- Tabs e botoes devem manter foco visivel, leitura por teclado e estados claros em tema escuro/alto contraste.

## Ordem segura de evolucao

1. Validar a experiencia atual com checklist antes de mexer.
2. Harmonizar estados de foco, selecao e contexto usando tokens do contrato visual.
3. Ajustar responsividade apenas onde houver evidência visual de problema.
4. Revisar acessibilidade de teclado na arvore, abas e controles principais.
5. So depois extrair pequenas funcoes ou helpers, se houver repeticao real.
6. Evitar refatoracao grande enquanto o comportamento ainda estiver sendo calibrado com testes visuais.

## Checklist de teste antes de novos patches

Na rota `Governanca e Resultados > Programacao PTA 2027`:

1. abrir a tela em desktop normal;
2. abrir em split do Chrome;
3. abrir em viewport movel real;
4. expandir uma macro politica;
5. acionar `Programar`;
6. confirmar arvore, coluna principal e contexto ativo;
7. navegar entre etapas;
8. editar campos e validar rascunho local;
9. alternar tema claro/escuro e cor de destaque;
10. voltar para o mapa/entrada quando aplicavel;
11. observar console sem erros novos.

Consultas uteis no console do host:

```js
document.querySelector('[data-contract-flow="pta2027-immersive"]')?.dataset.contractScope
document.body.classList.contains('pta2027-immersive')
document.body.classList.contains('special-context-focus')
document.getElementById('pta2027-frame')?.dataset.contractEmbeddedFlow
```

Consultas uteis dentro do iframe:

```js
document.body.classList.contains('embedded-mode')
document.body.classList.contains('programming-focus')
document.querySelector('#wizard-view:not(.hidden)') !== null
document.querySelectorAll('.tree-step').length
```

## Baseline de diagnostico local

Bloco LAB em 04/07/2026: foram adicionados marcadores de diagnostico sem impacto visual para facilitar testes antes dos proximos ajustes.

No host SPO:

- `window.spoGetPta2027IntegrationStatus()` retorna o estado da integracao, incluindo contrato, altura do iframe, modo imersivo, foco contextual, ultimo tema sincronizado e ultimas mensagens recebidas;
- o iframe recebe marcadores `data-integration-*`, como `data-integration-state`, `data-integration-height`, `data-integration-immersive`, `data-integration-context`, `data-integration-theme` e `data-integration-last-message`.

Dentro do iframe PTA 2027:

- `window.pta2027GetRuntimeStatus()` retorna o estado do mockup, incluindo modo embutido, foco de programacao, vista atual, macro, eixo, etapa, contagens principais e chaves locais existentes;
- o `body` recebe marcadores `data-pta2027-*`, como `data-pta2027-view`, `data-pta2027-programming`, `data-pta2027-step`, `data-pta2027-macro` e `data-pta2027-axis`.

Comandos recomendados para a linha de base:

```js
spoGetPta2027IntegrationStatus()
```

Dentro do iframe:

```js
pta2027GetRuntimeStatus()
document.body.dataset.pta2027View
document.body.dataset.pta2027Programming
```

## Ajuste responsivo do modo Programar

Bloco LAB em 04/07/2026: o modo `Programar` recebeu uma camada responsiva especifica para split/tablet e celular.

Decisao:

- abaixo de `1180px`, `wizard-view` passa a organizar a experiencia em tres faixas verticais: jornada, contexto ativo e area principal;
- o contexto ativo deixa de disputar largura lateral com o workspace e passa a usar uma coluna propria;
- os cards do contexto usam `auto-fit`, reduzindo corte lateral quando o viewport esta comprimido;
- no celular, o contexto ativo fica em uma coluna, com padding menor e leitura mais direta;
- a logica da jornada, rascunho, dados, EAP e mensagens host/iframe nao foi alterada.

Microajuste posterior:

- no celular real, a arvore de etapas do modo `Programar` passa a ser lista vertical compacta, em vez de grade de duas ou tres colunas;
- os mesmos botoes e estados continuam sendo usados, mas os nomes de etapa ficam mais legiveis para navegacao guiada.
- os blocos secundarios do `Contexto ativo` no celular passam a iniciar recolhidos e alternam aberto/fechado pelo toque no proprio card, preservando de primeira a leitura da politica PAOE, pilar/metas e componentes principais;
- em telas maiores, esses blocos permanecem expandidos para manter a leitura completa do ambiente imersivo.
- na lista de eixos/PAOE, o botao `Programar` passa a ocupar uma linha propria no celular, evitando estouro lateral em larguras proximas de 390-407px.
- o cabecalho imersivo do host passa a concentrar metadados, data/logados e retorno `Voltar a cadeia estrategica`, evitando um botao flutuante competindo com o conteudo;
- no celular estreito, o cabecalho preserva rotulo, titulo e metadados, mas reduz texto auxiliar para manter a leitura compacta;
- em tablets e splits largos, o cabecalho usa composicao em duas areas quando houver largura suficiente, com titulo a esquerda e metadados/retorno a direita;
- em `768x1024` e `912x1368`, o contexto ativo pode usar duas colunas e o indicador de progresso fica a esquerda do titulo da etapa, preservando o padrao movel/tablet aprovado;
- em `1280x800`, o empilhamento deixou de ser aplicado. Essa resolucao passa a ser considerada desktop compacto, evitando arvore vertical com grande vazio lateral;
- `1024x600` permanece dentro da faixa compacta validada, com jornada horizontal, contexto em leitura ampla e sem estouro lateral.

Teste recomendado:

```js
spoGetPta2027IntegrationStatus()
```

Confirmar, apos clicar em `Programar`, que `iframe.view` permanece `wizard`, `iframe.programmingFocus` permanece `true` e o contexto ativo nao gera rolagem horizontal indesejada.

## Registro tecnico da rodada responsiva

Bloco LAB em 04/07/2026: a evolucao responsiva foi consolidada sem alterar banco de dados, rotas oficiais ou persistencia local.

Arquivos ajustados nesta frente:

- `templates/partials/atualizar_governanca_resultados_programacao_pta2027.html`: cabecalho host do modo imersivo e botao de retorno integrado;
- `static/js/main.js`: sincronizacao de metadados, botao de retorno via mensagem para o iframe e diagnostico `spoGetPta2027IntegrationStatus()`;
- `static/css/style.css`: comportamento do cabecalho imersivo, faixas de celular/tablet/desktop compacto e substituicao da topbar global quando ha foco contextual;
- `static/pta2027_v63r/assets/pta-2027.js`: diagnosticos internos, retorno `spo-pta2027-back-to-map`, paineis de contexto recolhiveis no celular e marcadores `data-pta2027-*`;
- `static/pta2027_v63r/assets/pta-2027.css`: regras responsivas do `wizard-view`, arvore de etapas, contexto ativo, botoes `Programar`, progresso, retorno no iframe e breakpoint compacto recalibrado para `1180px`.

Faixas validadas visualmente nesta rodada:

- celular estreito em torno de `393x873`, `407x907` e `430x932`: cabecalho compacto, retorno visivel, arvore vertical, botoes dentro dos cards e contexto secundario recolhivel;
- tablet/split em torno de `768x1024`, `912x1368` e `1024x600`: jornada compacta, contexto ativo em duas colunas quando houver espaco, progresso alinhado a esquerda e leitura sem disputa lateral;
- desktop compacto a partir de `1280x800`: retorno ao comportamento desktop, sem arvore vertical isolada e sem vazio lateral excessivo.

Consultas de validacao usadas durante os testes:

```js
spoGetPta2027IntegrationStatus()
```

Dentro do iframe:

```js
pta2027GetRuntimeStatus()
document.body.dataset.pta2027View
document.body.dataset.pta2027Programming
```

Resultado esperado: `view` igual a `wizard`, `programmingFocus` verdadeiro no modo Programar, `contextFocus` verdadeiro no host e ausencia de rolagem horizontal indevida.

## Decisao de arquitetura

O modulo deve entrar no contrato visual como caso especial. A meta e harmonizar, nao domesticar a tela. A programacao PTA 2027 e um ambiente imersivo de trabalho, com densidade, contexto e progressao proprios; portanto, ajustes futuros devem ser incrementais, testaveis e reversiveis.
