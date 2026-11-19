// Testes da comparação antes × depois (lib/diferencas.mjs), com retratos montados à mão: sem navegador nem servidor.
//   node --test tests/navegadores/lib/diferencas.test.mjs      (também roda no pytest: tests/test_comparacao_diferencas.py)
import { test } from "node:test";
import assert from "node:assert/strict";
import { alinhar, diferencas } from "./diferencas.mjs";

const PROPS = ["color", "margin-top"];
// retrato no formato completo: lista de [caminho, nome, cor, caixa, texto?]
function retrato(lista) {
  const elementos = {};
  for (const [caminho, n, cor = "red", c = [0, 0, 100, 10], t] of lista) {
    elementos[caminho] = { n, v: [cor, "0px"], c, ...(t !== undefined ? { t } : {}) };
  }
  return { props: PROPS, pseudo: [], elementos, janela: [1366, 768], pagina: [1366, 768], tokens: {} };
}
const reprova = (difs) => difs.filter((d) => !d.dado && !d.pixel && !d.deslocou);

// uma seção com três blocos, um embaixo do outro; cada bloco com a sua cor
const ANTES = [
  ["body", "body"],
  ["body>section[1]", "section.lista"],
  ["body>section[1]>div[1]", "div.a", "red", [0, 0, 100, 10]],
  ["body>section[1]>div[2]", "div.b", "green", [0, 10, 100, 10]],
  ["body>section[1]>div[3]", "div.c", "blue", [0, 20, 100, 10]],
];
const COM_BLOCO_NO_MEIO = [
  ["body", "body"],
  ["body>section[1]", "section.lista"],
  ["body>section[1]>div[1]", "div.a", "red", [0, 0, 100, 10]],
  ["body>section[1]>div[2]", "div.novo", "black", [0, 10, 100, 10]],
  ["body>section[1]>div[3]", "div.b", "green", [0, 20, 100, 10]],
  ["body>section[1]>div[4]", "div.c", "blue", [0, 30, 100, 10]],
];

test("retratos iguais: nenhuma diferença e cada caminho casado consigo mesmo", () => {
  const a = retrato(ANTES), b = retrato(ANTES);
  assert.deepEqual(diferencas(a, b), []);
  for (const [k, v] of alinhar(a, b)) assert.equal(k, v);
});

test("bloco novo no meio: só ele reprova; os seguintes não são comparados com o vizinho", () => {
  const difs = diferencas(retrato(ANTES), retrato(COM_BLOCO_NO_MEIO));
  const r = reprova(difs);
  assert.equal(r.length, 1);
  assert.equal(r[0].prop, "elemento");
  assert.equal(r[0].depois, "apareceu");
  assert.match(r[0].el, /div\.novo \(body>section\[1\]>div\[2\]\)/);
  // na comparação por posição, div.b seria comparado com div.novo (green → black) e div.c com div.b (blue → green)
  assert.equal(difs.filter((d) => d.prop === "color").length, 0);
  // os dois de baixo só desceram 10 px: listados como "deslocou", com o caminho novo depois da seta
  const desl = difs.filter((d) => d.deslocou);
  assert.deepEqual(desl.map((d) => d.el), [
    "div.b (body>section[1]>div[2]) → body>section[1]>div[3]",
    "div.c (body>section[1]>div[3]) → body>section[1]>div[4]",
  ]);
});

test("bloco removido do meio: só ele reprova, como \"sumiu\"", () => {
  const r = reprova(diferencas(retrato(COM_BLOCO_NO_MEIO), retrato(ANTES)));
  assert.equal(r.length, 1);
  assert.equal(r[0].depois, "sumiu");
  assert.match(r[0].el, /^div\.novo/);
});

test("bloco novo e uma mudança de estilo de verdade: as duas aparecem, a de estilo no elemento certo", () => {
  const depois = COM_BLOCO_NO_MEIO.map((l) => (l[1] === "div.c" ? [l[0], l[1], "purple", l[3]] : l));
  const r = reprova(diferencas(retrato(ANTES), retrato(depois)));
  assert.equal(r.length, 2);
  const cor = r.find((d) => d.prop === "color");
  assert.equal(cor.el, "div.c (body>section[1]>div[3]) → body>section[1]>div[4]");
  assert.equal(cor.antes, "blue");
  assert.equal(cor.depois, "purple");
});

test("elemento que trocou de classe no mesmo lugar continua comparado consigo mesmo", () => {
  const depois = ANTES.map((l) => (l[1] === "div.b" ? [l[0], "div.b.ativo", "orange", l[3]] : l));
  const difs = diferencas(retrato(ANTES), retrato(depois));
  assert.deepEqual(difs.map((d) => [d.el, d.prop, d.antes, d.depois]), [["div.b (body>section[1]>div[2])", "color", "green", "orange"]]);
});

test("seção com id que mudou de lugar (o kit): casada pelo id, sem \"sumiu\"/\"apareceu\"", () => {
  const a = retrato([["body", "body"], ["body>section[1]", "section#cores", "red", [0, 0, 100, 10]], ["body>section[2]", "section#fontes", "blue", [0, 10, 100, 10]]]);
  const b = retrato([["body", "body"], ["body>section[1]", "section#fontes", "blue", [0, 0, 100, 10]], ["body>section[2]", "section#cores", "red", [0, 10, 100, 10]]]);
  const difs = diferencas(a, b);
  assert.equal(difs.filter((d) => d.prop === "elemento").length, 0);
  assert.equal(difs.filter((d) => d.prop === "color").length, 0);
  // a ordem mudou de verdade: a caixa de cada uma reprova (sem bloco novo, não há "deslocou")
  assert.equal(reprova(difs).length, 2);
  assert.ok(reprova(difs).every((d) => d.prop.startsWith("caixa")));
});

test("pedaço que sumiu entra uma vez só, com quantos elementos tinha dentro", () => {
  const a = retrato([...ANTES, ["body>section[1]>div[2]>span[1]", "span"], ["body>section[1]>div[2]>span[2]", "span"]]);
  const b = retrato(ANTES.filter((l) => l[1] !== "div.b").map((l) => (l[1] === "div.c" ? ["body>section[1]>div[2]", "div.c", "blue", [0, 10, 100, 10]] : l)));
  const r = reprova(diferencas(a, b));
  assert.equal(r.length, 1);
  assert.equal(r[0].antes, "existe (com 2 elemento(s) dentro)");
});

test("linha de tabela que entrou (dado do banco) não reprova", () => {
  const linhas = (n) => [["body", "body"], ["body>table[1]", "table"], ["body>table[1]>tbody[1]", "tbody"],
    ...Array.from({ length: n }, (_, i) => [`body>table[1]>tbody[1]>tr[${i + 1}]`, "tr", "red", [0, i * 10, 100, 10], i])];
  const difs = diferencas(retrato(linhas(3)), retrato(linhas(4)));
  assert.equal(reprova(difs).length, 0);
  assert.ok(difs.some((d) => d.depois === "apareceu" && d.dado));
});

test("lista longa (5 mil linhas) com uma linha no meio: rápido e só ela muda", () => {
  const linhas = (extra) => {
    const l = [["body", "body"], ["body>ul[1]", "ul"]];
    let n = 0;
    for (let i = 0; i < 5000; i++) {
      if (extra && i === 2500) l.push([`body>ul[1]>li[${++n}]`, "li.nova"]);
      l.push([`body>ul[1]>li[${++n}]`, "li"]);
    }
    return l;
  };
  const t0 = Date.now();
  const difs = diferencas(retrato(linhas(false)), retrato(linhas(true)));
  assert.ok(Date.now() - t0 < 3000, `levou ${Date.now() - t0} ms`);
  assert.deepEqual(difs.filter((d) => d.prop === "elemento").map((d) => d.el), ["li.nova (body>ul[1]>li[2501])"]);
});
