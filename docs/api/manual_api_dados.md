# API de dados do SPO — manual de uso

Leitura (somente consulta) dos relatórios do SPO, para atualizar planilhas e painéis sem baixar o Excel à mão.

**Relatórios disponíveis:** `emp`, `nob`, `ped`, `est-emp`, `fip613`, `plan51`. Você só enxerga os que foram liberados para a sua chave.

## 1. O que você recebe do NGER

- `client_id` (ex.: `cli_9304c4c...`) e `client_secret`. O segredo é mostrado **uma única vez**: guarde-o em local seguro e não compartilhe.
- Endereço do sistema (`BASE`, ex.: `https://spo.exemplo.gov.br`).
- A lista de relatórios liberados e, se houver, a data final do acesso.

## 2. Passo a passo

### Passo 1 — pedir o token
`POST https://projetoswebcsg.life/api/auth/token` com o corpo JSON `{"client_id": "...", "client_secret": "..."}`.
A resposta traz `access_token`, válido por **60 minutos**. Em todas as chamadas seguintes envie o cabeçalho:

```
Authorization: Bearer <access_token>
```

### Passo 2 — ver se há dado novo
`GET https://projetoswebcsg.life/api/v1/dados/<relatorio>/meta`

```json
{"ok": true, "relatorio": "nob", "uo": "seduc", "total": 71241,
 "atualizado_em": "2026-11-04T08:12:03", "upload_id": [210], "cargas": [...]}
```

Guarde o `upload_id` (ou `atualizado_em`) da última vez que baixou. **Só baixe de novo se mudou.**

### Passo 3 — baixar os dados
`GET https://projetoswebcsg.life/api/v1/dados/<relatorio>/export` devolve a tabela inteira em CSV (UTF-8, separador vírgula, ponto como decimal, datas `AAAA-MM-DD`). O cabeçalho usa os **mesmos nomes das colunas do Excel** do SPO.

A resposta traz os cabeçalhos HTTP `X-Total` (linhas), `X-Upload-Id` (carga) e `X-Gerado-Em`.

| Parâmetro | Para que serve |
|---|---|
| `uo` | Sem ele, vêm só os dados da SEDUC. `uo=26201` ou `uo=26201,26202` escolhe UOs; `uo=all` traz todas. (Não vale para `plan51`, que é do estado inteiro.) |
| `<coluna>=valor` | Filtra por coluna, pelo nome do Excel ou do banco: `exercicio=2026` ou `exercicio=2026,2025` (no FIP613 o ano é `ano`). |
| `fields` | Escolhe as colunas: `fields=Exercicio,UO,Valor NOB`. Deixa o arquivo menor e mais rápido. |
| `formato` | `csv` (padrão) ou `json`. |
| `colunas` | `rotulo` (padrão, nomes do Excel) ou `banco` (nomes técnicos, mais `id` e `upload_id`). |
| `limit` e `after_id` | **Carga em partes.** `limit=10000` devolve até 10 mil linhas; a resposta traz o cabeçalho HTTP `X-Next-After-Id`. Para a parte seguinte, repita a chamada com `after_id=<esse valor>`, até ele vir vazio. `X-Total` (total do filtro) vem só na primeira parte e `X-Linhas` diz quantas linhas vieram. É o que o script do Google Planilhas usa para EMP e NOB. |

> **Atenção ao tamanho.** A NOB inteira tem ~71 mil linhas (~55 MB) e a EMP ~57 mil. Prefira filtrar por `Exercicio` e escolher só as colunas de que precisa.

### Colunas de um relatório
`GET https://projetoswebcsg.life/api/v1/dados/<relatorio>/schema` lista cada coluna com o nome do Excel, o nome técnico e o tipo (`texto`, `numero`, `data`, `datahora`).

### Consulta paginada (alternativa)
`GET https://projetoswebcsg.life/api/v1/dados/<relatorio>?limit=5000&order_by=id&order_dir=asc` devolve JSON em páginas de até 5.000 linhas. Para a página seguinte, repita a chamada com `after_id=<next_after_id>` da resposta anterior, até `next_after_id` vir vazio. Prefira o `/export`: é uma chamada só.

## 3. Exemplos

### Google Planilhas (Apps Script)
Guarde `CLIENT_ID` e `CLIENT_SECRET` em Configurações do projeto > Propriedades do script (não no código).

```javascript
function atualizarNOB() {
  const BASE = "https://projetoswebcsg.life", REL = "nob";
  const p = PropertiesService.getScriptProperties();
  const tok = JSON.parse(UrlFetchApp.fetch(BASE + "/api/auth/token", {
    method: "post", contentType: "application/json",
    payload: JSON.stringify({client_id: p.getProperty("CLIENT_ID"), client_secret: p.getProperty("CLIENT_SECRET")})
  }).getContentText()).access_token;
  const h = {headers: {Authorization: "Bearer " + tok}};

  // só baixa se a carga mudou
  const meta = JSON.parse(UrlFetchApp.fetch(BASE + "/api/v1/dados/" + REL + "/meta", h).getContentText());
  const atual = meta.upload_id.join(",");
  if (p.getProperty("ULTIMA_CARGA_" + REL) === atual) return;

  const url = BASE + "/api/v1/dados/" + REL + "/export?exercicio=2026";
  const dados = Utilities.parseCsv(UrlFetchApp.fetch(url, h).getContentText());
  const aba = SpreadsheetApp.getActive().getSheetByName("NOB");
  aba.clearContents();
  aba.getRange(1, 1, dados.length, dados[0].length).setValues(dados);
  p.setProperty("ULTIMA_CARGA_" + REL, atual);
}
```
Depois, em **Acionadores**, agende a função (por exemplo, de hora em hora ou todo dia às 8h).

### Excel (Power Query)
Dados > Obter Dados > De Outras Fontes > Consulta em Branco > Editor Avançado:

```
let
  Base = "https://projetoswebcsg.life",
  Tok  = Json.Document(Web.Contents(Base & "/api/auth/token", [
           Headers = [#"Content-Type" = "application/json"],
           Content = Text.ToBinary("{""client_id"":""cli_xxx"",""client_secret"":""xxx""}")])),
  Csv  = Csv.Document(Web.Contents(Base, [
           RelativePath = "api/v1/dados/nob/export",
           Query = [exercicio = "2026"],
           Headers = [Authorization = "Bearer " & Tok[access_token]]]),
           [Delimiter = ",", Encoding = 65001]),
  Tab  = Table.PromoteHeaders(Csv)
in
  Tab
```
Para atualizar: Dados > Atualizar Tudo. As colunas de valor chegam como texto com ponto decimal: converta o tipo para Número usando a localidade **Inglês (Estados Unidos)**.

### Python
```python
import requests, pandas as pd, io
BASE = "https://projetoswebcsg.life"
tok = requests.post(BASE + "/api/auth/token", json={"client_id": "...", "client_secret": "..."}).json()["access_token"]
r = requests.get(BASE + "/api/v1/dados/est-emp/export", headers={"Authorization": "Bearer " + tok})
df = pd.read_csv(io.StringIO(r.text))
```

## 4. Boas práticas
- Peça o token **uma vez por execução** e reaproveite nas chamadas.
- Consulte o `/meta` antes e só baixe quando a carga mudou. O SPO é atualizado quando o NGER envia os relatórios, não a toda hora.
- Filtre por `Exercicio` e escolha só as colunas necessárias.
- Não compartilhe nem publique o `client_secret` (nunca em planilha compartilhada ou repositório).

## 5. Erros comuns

| Código | Significado e o que fazer |
|---|---|
| 400 | Parâmetro inválido (`uo`, `formato`, `colunas`, `after_id`). Confira o texto de `error`. |
| 401 | Token ausente, inválido ou expirado (vale 60 min). Peça outro no Passo 1. |
| 403 | Sem permissão para esse relatório, ou chave revogada/expirada. Fale com o NGER. |
| 500 | Erro no servidor. Tente de novo e, se persistir, avise o NGER com dia, hora e relatório. |

Dúvidas, novos relatórios ou renovação de acesso: NGER/SEDUC-MT.
