"""Teste rápido da API de dados do SPO.

Uso:  python testar_api.py https://SEU-DOMINIO cli_xxxxxxxx SEGREDO [relatorio]
relatorio: emp | nob | ped | est-emp | fip613 | plan51   (padrão: est-emp, o menor)

Confere: token, escopos, última atualização, colunas e carga completa em CSV
(o número de linhas do CSV tem de bater com o `meta` e com o cabeçalho X-Total).
Precisa de:  pip install requests
"""
import csv
import io
import sys
import time

import requests

if len(sys.argv) < 4:
    sys.exit(__doc__)
BASE, CLIENT_ID, CLIENT_SECRET = sys.argv[1].rstrip("/"), sys.argv[2], sys.argv[3]
REL = sys.argv[4] if len(sys.argv) > 4 else "est-emp"


def ok(msg):
    print("  OK  ", msg)


def falha(msg):
    print(" FALHA", msg)
    sys.exit(1)


print(f"[1] Token em {BASE}")
r = requests.post(f"{BASE}/api/auth/token", json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}, timeout=60)
if r.status_code != 200:
    falha(f"{r.status_code} {r.text}")
h = {"Authorization": "Bearer " + r.json()["access_token"]}
ok(f"escopos liberados: {', '.join(r.json()['scopes'])}")

print(f"[2] Última atualização de {REL}")
meta = requests.get(f"{BASE}/api/v1/dados/{REL}/meta", headers=h, timeout=60)
if meta.status_code != 200:
    falha(f"{meta.status_code} {meta.text}")
meta = meta.json()
ok(f"{meta['total']} linhas | carga(s) {meta['upload_id']} | atualizado em {meta['atualizado_em']}")

print("[3] Colunas")
sch = requests.get(f"{BASE}/api/v1/dados/{REL}/schema", headers=h, timeout=60).json()
ok(f"{sch['total_colunas']} colunas, ex.: " + ", ".join(c["rotulo"] for c in sch["colunas"][:4]))

print("[4] Carga completa em CSV")
t = time.time()
resp = requests.get(f"{BASE}/api/v1/dados/{REL}/export", params={"formato": "csv"}, headers=h, timeout=600)
if resp.status_code != 200:
    falha(f"{resp.status_code} {resp.text[:200]}")
linhas = list(csv.reader(io.StringIO(resp.content.decode("utf-8"))))
n = len(linhas) - 1
ok(f"{n} linhas em {time.time() - t:.1f}s ({len(resp.content) // 1024} KB) | X-Total={resp.headers.get('X-Total')} | X-Upload-Id={resp.headers.get('X-Upload-Id')}")
if not (n == int(resp.headers["X-Total"]) == meta["total"]):
    falha("a contagem do CSV não bate com o meta (alguém enviou carga durante o teste? rode de novo)")

print("[5] Paginação por after_id (3 páginas de 5 linhas)")
vistos, after = [], None
for _ in range(3):
    params = {"limit": 5, "order_by": "id", "order_dir": "asc"}
    if after:
        params["after_id"] = after
    j = requests.get(f"{BASE}/api/v1/dados/{REL}", params=params, headers=h, timeout=60).json()
    vistos += [d["id"] for d in j["data"]]
    after = j.get("next_after_id")
    if not after:
        break
if len(vistos) != len(set(vistos)):
    falha("a paginação repetiu linhas")
ok(f"{len(vistos)} linhas distintas")
print("\nTUDO CERTO")
