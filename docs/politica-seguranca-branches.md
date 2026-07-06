# Politica de seguranca de branches

Data de referencia: 2026-07-06

Este documento registra o fluxo seguro vigente para promover entregas entre LAB
e DEV no SPO.

## Regra central

O DEV nao recebe merge bruto da arvore LAB. As entregas homologadas no LAB devem
entrar no DEV por promocao seletiva, com origem, destino, arquivos esperados,
validacoes e checkpoint.

Fluxo aprovado:

```text
lab/<frente> -> lab/pta2027-principal -> front/<frente> -> dev/jean
```

## Responsabilidades

- `lab/<frente>`: desenvolvimento e experimentacao por dominio.
- `lab/pta2027-principal`: integrador LAB. Recebe somente mudancas LAB
  autorizadas.
- `front/<frente>`: preparacao DEV da frente correspondente.
- `dev/jean`: integrador DEV. Recebe somente promocoes revisadas.
- `task/*`: sandbox tecnico DEV. Nao e caminho normal de promocao.

## Regras obrigatorias

1. Agentes LAB nao escrevem em `dev/jean`, `front/*` ou `task/*`.
2. Agentes DEV de frente nao escrevem diretamente em `dev/jean`.
3. `dev/jean` nao deve fazer backport para LAB.
4. Merge direto entre `02-laboratorio-pta2027` e `01-integracao-dev-jean` deve
   ser evitado; quando houver conflito, aplicar pacote seletivo orientado pelo
   arquiteto.
5. Alteracoes de identidade de ambiente devem ser preservadas:
   - LAB continua com rotulos e sessoes LAB.
   - DEV continua com rotulos e diagnostico DEV.
6. Antes de promover, registrar:
   - origem e destino;
   - escopo funcional;
   - arquivos alterados;
   - validacoes executadas;
   - riscos pendentes;
   - checkpoint local.
7. Nao usar `--no-verify` para contornar hooks de seguranca.

## Promocao LAB para DEV

A promocao padrao deve ocorrer primeiro na frente DEV correspondente, por
exemplo:

```text
lab/ux-ui -> lab/pta2027-principal -> front/ux-ui -> dev/jean
```

Quando o arquiteto atuar diretamente em `dev/jean`, a promocao deve ser
documentada como integracao controlada e depois propagada aos `front/*` para
manter a base DEV alinhada.

## Checkpoint de referencia

A promocao visual homologada no LAB em 2026-07-06 teve como origem:

```text
lab/pta2027-principal @ 6c7f1ff
checkpoint/lab-principal-ux-ui-ajustes-visuais
```

No DEV, ela deve aparecer como pacote adaptado, preservando a identidade visual
e operacional do ambiente DEV.
