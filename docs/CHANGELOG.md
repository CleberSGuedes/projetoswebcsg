# Registro de mudanças (CHANGELOG) — SPO

Este arquivo lista o que foi publicado, por data. Formato:

- Uma entrada por data de publicação, a **mais nova em cima**.
- Dentro da entrada, as mudanças ficam agrupadas em **Adicionado**, **Alterado**, **Corrigido** e **Removido** (grupo sem mudança é omitido).
- **Uma linha por mudança**, com o número do PR e, quando houver, a seção do [claude.md](claude.md) ou do [padrao-visual-spo-aa-plus.md](padrao-visual-spo-aa-plus.md) que explica.
- Regras do fluxo de desenvolvimento e de promoção: seção 40 do [claude.md](claude.md).

---

## 2026-11-16 — dev/cleber, ainda não publicado (RELEASE_2_2026_11_16.6)

### Corrigido
- Avisos modais com fonte maior (dev. Cleber, `fix/modal-pendencias-fonte-maior`): o botão "Fechar" do modal de pendências sumia no celular com "Fonte maior" no Personalizar SPO, porque o zoom da fonte multiplicava a altura do aviso. O botão de fechar fica visível em qualquer tamanho de fonte, no celular e no computador; fonte normal sem mudança — seção "Avisos modais com fonte maior" do [claude.md](claude.md) e `tests/test_modal_zoom.py`.

---

## 2026-11-16 — Publicação em produção (RELEASE_2_2026_11_16.4)

Promoção da homologação para a `main` (PR #156): AA+ lotes 1 a 5 (PRs #65, #68, #153 e #154), avisos modais e Teto Orçamentário no celular (#154 e #155). Sincronização da `main` com a `dev/cleber` (#157) e com a `dev/jean` (#158).

### Adicionado
- AA+ (acessibilidade), do dev. Jean, lotes 1 a 5 (#65, #68, #153, #154): fonte e fundo novos (Inter na interface, Intel One Mono nos dados), Personalizar Temas SPO (fontes, densidade do texto, espaçamento entre linhas, "Leitura", cores seguras para daltonismo, foco, brilho e movimento, ✓ na cor de destaque), contraste AA/AA+, alvos de toque de 44 px, gráficos acessíveis (inclusive a linha separadora, #152) — seções 41.1 a 41.99 do `claude.md` e `padrao-visual-spo-aa-plus.md`.
- Atalhos do teclado: busca de telas (Ctrl+K ou "/"), ajuda ("?"), Alt+M para o menu e setas no menu e nas tabelas (#153) — seção 41.
- Declaração de acessibilidade "SPO-AA+" no Personalizar, com link no rodapé (#153) — seção 41.
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
