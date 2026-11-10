# Status: melhorias de formatos FIPLAN em execucao orcamentaria

Data: 2026-08-04.

Funcionalidades ajustadas:

- FIP 613;
- PED;
- EMP;
- EST EMP;
- NOB.

## Objetivo

Padronizar a entrada de arquivos dessas funcionalidades para aceitar relatorios
exportados pelo FIPLAN antigo, mesmo quando a extensao nao corresponde ao
conteudo real do arquivo.

## Formatos aceitos

As telas passam a aceitar:

- `.xlsx` real;
- `.xls` BIFF antigo;
- `.xls` ou `.xlsx` falso com HTML;
- `.xls` ou `.xlsx` falso com XML/SpreadsheetML;
- `.csv` brasileiro separado por ponto e virgula.

PDF permanece fora do escopo dessas cinco funcionalidades nesta etapa.

## Padrao tecnico implementado

Foi criado o leitor compartilhado `services/fiplan_file_reader.py`, responsavel
por:

- detectar o formato real pelo conteudo do arquivo;
- ler previa de linhas para validacao rapida do layout;
- converter arquivos FIPLAN antigos/falsos para `.xlsx` real quando necessario;
- manter o arquivo original salvo para auditoria operacional.

O fluxo final ficou assim:

```text
usuario envia arquivo bruto
-> aplicacao salva o arquivo original
-> aplicacao detecta o formato real pelo conteudo
-> aplicacao valida somente uma amostra inicial do layout
-> worker converte para .xlsx real quando necessario
-> runner atual processa o arquivo normalizado
-> dados seguem para as tabelas ja existentes
```

## Processamento por funcionalidade

FIP 613, PED e EST EMP:

- continuam usando os runners Python atuais;
- recebem `.xlsx` real quando o arquivo original precisa ser normalizado;
- preservam as regras atuais de cabecalho, validacao e gravacao.

EMP e NOB:

- continuam usando o runner Node atual;
- o worker passa caminho absoluto do arquivo para evitar erro de arquivo nao
  encontrado;
- quando o arquivo original nao for `.xlsx` real, o worker gera um `.xlsx`
  normalizado antes de chamar o Node.

## Ajustes importantes feitos durante os testes

1. A conversao completa saiu da rota de upload.

Motivo: arquivos `.xls` BIFF antigos grandes, como EMP, podem demorar muito para
converter. Se isso acontecer dentro da requisicao HTTP, o navegador pode retornar
`NetworkError when attempting to fetch resource`.

2. A conversao completa foi movida para o worker.

Resultado: a tela responde rapidamente e o processamento pesado fica em segundo
plano.

3. O worker agora usa caminhos absolutos.

Motivo: o Node roda dentro de `node_runners`. Com caminho relativo, ele procurava
`upload/...` dentro da pasta errada.

4. Dependencias Node foram restauradas.

EMP e NOB dependem dos pacotes declarados em `node_runners/package.json`. No
ambiente local foi executado:

```powershell
cd C:\workspace\projetoswebcsg_dev_cleber\node_runners
npm ci
```

Esse comando precisa ser executado em ambientes onde `node_runners/node_modules`
nao existir.

## Validacoes realizadas

- Upload de EMP `.xls` BIFF antigo deixou de falhar na tela.
- Worker EMP processou o arquivo normalizado com sucesso.
- Resultado validado no upload EMP `137`:

```text
Processado com sucesso. Registros: 43064.
```

- Checagens tecnicas executadas:

```powershell
.\.venv\Scripts\python.exe -m py_compile services\fiplan_file_reader.py rotas\home_routes.py worker.py services\fip613_runner.py services\ped_runner.py services\est_emp_runner.py
node --check static\js\main.js
node --check node_runners\emp_runner.js
node --check node_runners\nob_runner.js
node --check node_runners\run.js
git diff --check
```

## Observacoes

- Nao foram criadas novas tabelas para essa melhoria.
- As tabelas atuais de cada funcionalidade foram preservadas.
- O `.xlsx` normalizado e arquivo tecnico interno.
- O usuario continua fazendo upload de um arquivo por vez nessas cinco telas.
