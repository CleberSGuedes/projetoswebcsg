const { criarAmbiente } = require("./harness");
const assert = require("assert");
const ARQ = process.argv[2] || require("path").join(__dirname, "../../docs/api/google_planilhas_varios.gs");

function executar(env, primeira) {
  // simula o Google: chama a função e, enquanto houver gatilho pendente, "dispara" uma nova execução
  let execs = 0;
  env.run(primeira);
  execs++;
  while (env.triggers.length) {
    assert(execs < 60, "ciclo infinito");
    env.advance(60000);
    env.run("continuarAtualizacao()");
    execs++;
  }
  return execs;
}

function dadosAba(env, nome) {
  const s = env.sheets[nome];
  const n = s ? s.getLastRow() : 0;
  const linhas = [];
  for (let r = 1; r <= n; r++) linhas.push([1, 2, 3].map((c) => s.grid[r + "," + c]));
  return linhas;
}

// ---------- 1. cascata completa em várias execuções ----------
{
  const env = criarAmbiente({ arquivo: ARQ });
  const execs = executar(env, "verificarAtualizacoes()");
  console.log("1) execuções necessárias:", execs, "| gatilhos restantes:", env.triggers.length);
  assert(execs > 1, "deveria precisar de várias execuções");
  assert.strictEqual(env.triggers.length, 0);
  assert.strictEqual(env.props.CICLO, undefined, "ciclo deve ser apagado");
  for (const [rel, aba] of [["est-emp", "EST EMP"], ["ped", "PED"], ["fip613", "FIP613"], ["plan51", "PLAN 51"], ["emp", "EMP"], ["nob", "NOB"]]) {
    const d = dadosAba(env, aba);
    assert.strictEqual(d.length - 1, env.dataset[rel].n, rel + " linhas");
    assert.deepStrictEqual(d[0], ["Exercício", "Chave", "Valor"]);
    // sem repetição nem perda, na ordem, e número convertido
    for (let i = 1; i < d.length; i++) {
      assert.strictEqual(d[i][1], rel + "-" + String(i).padStart(6, "0"), rel + " ordem na linha " + i);
      assert.strictEqual(d[i][0], 2026);
      assert.strictEqual(typeof d[i][2], "number");
    }
    assert(!env.sheets[aba + " (novo)"], "temporária de " + aba + " deveria ter sido apagada");
  }
  console.log("   dados conferidos nas 6 abas; temporárias apagadas");
  // cascata: pequenos terminam antes dos grandes
  const ordem = env.log.filter((l) => /: \d+ linhas\.$/.test(l)).map((l) => l.split(":")[0]);
  console.log("   ordem de conclusão:", ordem.join(" > "));
  assert.deepStrictEqual(ordem, ["EST EMP", "PED", "FIP613", "PLAN 51", "EMP", "NOB"]);
  // 2ª verificação: nada mudou -> não baixa nada
  const antes = env.fetchLog.filter((u) => u.includes("/export")).length;
  env.run("verificarAtualizacoes()");
  assert.strictEqual(env.fetchLog.filter((u) => u.includes("/export")).length, antes, "não deve baixar sem carga nova");
  console.log("   2ª verificação sem novidade: nenhum download");
  // só uma mudou
  env.dataset["ped"].upload = 999; env.dataset["ped"].n = 70;
  const antes2 = env.fetchLog.length;
  executar(env, "verificarAtualizacoes()");
  const novos = env.fetchLog.slice(antes2).filter((u) => u.includes("/export"));
  assert(novos.every((u) => u.includes("/ped/")), "só o PED deve ser baixado: " + novos.map(u=>u.slice(30,80)).join(" ; "));
  assert.strictEqual(dadosAba(env, "PED").length - 1, 70);
  console.log("   carga nova só no PED: baixou só o PED (70 linhas)");
  // menu
  env.run("onOpen()");
  assert.deepStrictEqual(env.ctx.__menu.items.map((i) => i[0]), ["Atualizar agora", "Refazer tudo (baixar de novo)", "Ver andamento"]);
  env.run("verAndamento()");
  env.run("refazerTudo()");
  console.log("   menu SPO e 'Refazer tudo' ok");
}

// ---------- 2. carga muda no meio da baixa do EMP ----------
{
  let mudou = false;
  const env = criarAmbiente({
    arquivo: ARQ,
    aoBaixar: (rel, after, ds) => { if (rel === "emp" && after >= 4000 && !mudou) { mudou = true; ds.emp.upload = 777; ds.emp.n = 8000; } },
  });
  executar(env, "verificarAtualizacoes()");
  const d = dadosAba(env, "EMP");
  assert.strictEqual(d.length - 1, 8000, "EMP deve refletir a carga nova");
  assert(env.log.some((l) => /EMP: .*recomeçando/.test(l)));
  assert.strictEqual(env.props["CARGA_EMP"], "777|exercicio=2026");
  console.log("2) carga mudou no meio do EMP: recomeçou e terminou com a carga nova (8000 linhas)");
}

// ---------- 3. um relatório sem permissão não derruba os outros ----------
{
  const env = criarAmbiente({ arquivo: ARQ, falha: { nob: 403 } });
  let erro = null;
  try { env.run("verificarAtualizacoes()"); } catch (e) { erro = e; }
  // a consulta (meta) do NOB falha: erro registrado, mas os demais baixam
  while (env.triggers.length) { env.advance(60000); try { env.run("continuarAtualizacao()"); } catch (e) { erro = e; } }
  assert.strictEqual(dadosAba(env, "EST EMP").length - 1, 120);
  assert.strictEqual(dadosAba(env, "EMP").length - 1, 9500);
  assert(!env.sheets["NOB"] || dadosAba(env, "NOB").length === 0);
  assert.strictEqual(env.props.CICLO, undefined);
  console.log("3) NOB sem permissão: os outros 5 foram baixados; erro:", erro ? "reportado" : "NÃO reportado");
}

// ---------- 4. erro só na baixa (meta ok, export 403) ----------
{
  const env = criarAmbiente({ arquivo: ARQ });
  const orig = env.ctx.UrlFetchApp.fetch;
  env.ctx.UrlFetchApp.fetch = (u, o) => (u.includes("/nob/export") ? { getResponseCode: () => 403, getContentText: () => "negado", getHeaders: () => ({}) } : orig(u, o));
  let erro = null;
  try { executar(env, "verificarAtualizacoes()"); } catch (e) { erro = e; }
  assert(erro && /NOB/.test(erro.message), "erro deve citar a aba NOB");
  assert.strictEqual(dadosAba(env, "EMP").length - 1, 9500);
  assert(!env.sheets["NOB (novo)"], "temporária do NOB deve ser apagada");
  assert.strictEqual(env.props["CARGA_NOB"], undefined, "NOB não pode ser marcado como atualizado");
  console.log("4) export do NOB falhou: erro cita NOB, EMP ok, temporária apagada, NOB será tentado de novo");
  // próxima verificação tenta o NOB de novo
  env.ctx.UrlFetchApp.fetch = orig;
  executar(env, "verificarAtualizacoes()");
  assert.strictEqual(dadosAba(env, "NOB").length - 1, 13000);
  console.log("   na verificação seguinte o NOB foi baixado");
}

// ---------- 5. não roda duas vezes ao mesmo tempo ----------
{
  const env = criarAmbiente({ arquivo: ARQ });
  env.run("verificarAtualizacoes()"); // deixa ciclo em andamento (com gatilho)
  assert(env.props.CICLO);
  const antes = env.fetchLog.length;
  env.run("atualizarAgora()");
  assert.strictEqual(env.fetchLog.length, antes, "não deve iniciar outro ciclo");
  assert(env.toasts.some((t) => /andamento/.test(t)));
  console.log("5) com ciclo em andamento, 'Atualizar agora' não inicia outro");
}
console.log("\nTODOS OS CENÁRIOS PASSARAM");
