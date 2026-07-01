# Contrato Visual SPO

Este documento registra o contrato atual de layout, temas e responsividade do SPO. Ele existe para evitar que um ajuste de modo movel, split do Chrome, barra de rolagem ou cor de destaque interfira em outra camada.

## Classes de ambiente

- `spo-shell`: casca global do sistema.
- `spo-env-dev`: ambiente de integracao DEV.
- `spo-env-lab`: ambiente de laboratorio.
- `lab-spo`: marcador reservado para experiencias visuais do laboratorio.

## Modos responsivos

- Modo movel real: controlado por CSS em `@media (max-width: 420px)`. Deve permanecer fluido, sem largura minima operacional e sem barra horizontal geral.
- Split sob pressao: controlado por JavaScript com a classe `spo-split-pressure`. Este modo e ativado quando a area util da pagina fica estreita, mas a janela externa do Chrome continua ampla. Ele restaura a largura minima operacional interna para preservar a leitura em visualizacao dividida.

## Barras de rolagem

As barras usam o padrao global `scrollbar-visible`: ficam discretas em repouso e assumem a cor de destaque durante interacao. Novas areas rolaveis devem entrar na lista de seletores de scrollbar em `static/css/style.css`.

## Cores de destaque

As cores ficam centralizadas em variaveis CSS e no mapa `ACCENT_COLORS` em `static/js/main.js`. Evite cores fixas em novos componentes; prefira `--accent`, `--accent-strong`, `--accent-rgb` e `--accent-soft-rgb`.

## Regra de manutencao

Antes de alterar responsividade, verifique se a mudanca afeta:

1. janela normal ampla;
2. modo movel real;
3. split sob pressao;
4. tema claro;
5. tema escuro.
