// Simulação dos serviços do Apps Script para testar a lógica de google_planilhas_varios.gs
const fs = require("fs");
const vm = require("vm");

function criarAmbiente(opts) {
  const o = Object.assign({ lote: 4000, msFetch: 40000, msGravar: 25000, sizes: { "est-emp": 120, ped: 62, fip613: 300, plan51: 900, emp: 9500, nob: 13000 }, falha: {} }, opts || {});
  let clock = 1_000_000;
  const log = [];
  const props = {};
  const triggers = [];
  const toasts = [];
  let uploadAtual = {}; // relatorio -> upload_id
  const dataset = {}; // relatorio -> {upload, n}
  Object.keys(o.sizes).forEach((r, i) => (dataset[r] = { upload: 100 + i, n: o.sizes[r] }));
  const fetchLog = [];
  let locked = false;

  function sheetFactory(name) {
    const s = { name, rows: 1000, cols: 26, grid: {}, hidden: false, cleared: 0 };
    s.getMaxRows = () => s.rows;
    s.getMaxColumns = () => s.cols;
    s.insertRowsAfter = (a, n) => { s.rows += n; };
    s.insertColumnsAfter = (a, n) => { s.cols += n; };
    s.clearContents = () => { s.grid = {}; s.cleared++; };
    s.hideSheet = () => { s.hidden = true; };
    s.getRange = (r, c, nr, nc) => rangeFactory(s, r, c, nr || 1, nc || 1);
    s.getLastRow = () => Math.max(0, ...Object.keys(s.grid).map((k) => +k.split(",")[0]));
    return s;
  }
  function rangeFactory(s, r, c, nr, nc) {
    return {
      setValues(vals) {
        if (r + nr - 1 > s.rows || c + nc - 1 > s.cols) throw new Error("range fora da grade de " + s.name + ": " + (r + nr - 1) + "x" + (c + nc - 1) + " > " + s.rows + "x" + s.cols);
        if (vals.length !== nr || vals.some((l) => l.length !== nc)) throw new Error("dimensao incorreta");
        vals.forEach((l, i) => l.forEach((v, j) => (s.grid[r + i + "," + (c + j)] = v)));
        clock += o.msGravar * (nr / o.lote);
      },
      copyTo(dest, tipo) {
        if (tipo !== "PASTE_VALUES") throw new Error("tipo inesperado");
        const d = dest._s;
        if (dest._r + nr - 1 > d.rows || dest._c + nc - 1 > d.cols) throw new Error("copyTo fora da grade de " + d.name);
        for (let i = 0; i < nr; i++) for (let j = 0; j < nc; j++) { const v = s.grid[r + i + "," + (c + j)]; if (v !== undefined) d.grid[dest._r + i + "," + (dest._c + j)] = v; }
        clock += 3000 * (nr / 20000);
      },
      _s: s, _r: r, _c: c,
    };
  }
  const sheets = {};
  const ss = {
    getSheetByName: (n) => sheets[n] || null,
    insertSheet: (n) => { if (sheets[n]) throw new Error("ja existe " + n); return (sheets[n] = sheetFactory(n)); },
    deleteSheet: (s) => { delete sheets[s.name]; },
    toast: (m) => toasts.push(m),
  };

  function csvDe(rel, desde, limit) {
    const d = dataset[rel];
    const cab = ["Exercício", "Chave", "Valor"];
    const linhas = [cab.join(",")];
    const ids = [];
    for (let id = desde + 1; id <= d.n && ids.length < limit; id++) {
      ids.push(id);
      linhas.push([2026, rel + "-" + String(id).padStart(6, "0"), (id * 1.25).toFixed(2)].join(","));
    }
    return { texto: linhas.join("\r\n") + "\r\n", ids };
  }

  function resp(code, text, headers) {
    return { getResponseCode: () => code, getContentText: () => text, getHeaders: () => headers || {} };
  }

  const ctx = {
    console,
    Date: Object.assign(function (...a) { return new (Function.prototype.bind.apply(Date, [null, ...a]))(); }, { now: () => clock }),
    Logger: { log: (m) => log.push(m) },
    Utilities: {
      parseCsv: (t) => t.split(/\r?\n/).filter((l) => l.length).map((l) => l.split(",")),
      sleep: () => {},
      formatDate: () => "01/01 00:00",
    },
    Session: { getScriptTimeZone: () => "America/Cuiaba" },
    PropertiesService: { getScriptProperties: () => ({ getProperty: (k) => (k in props ? props[k] : null), setProperty: (k, v) => { props[k] = String(v); }, deleteProperty: (k) => { delete props[k]; } }) },
    LockService: { getScriptLock: () => ({ tryLock: () => { if (locked) return false; locked = true; return true; }, releaseLock: () => { locked = false; } }) },
    ScriptApp: {
      getProjectTriggers: () => triggers.slice().map((t) => ({ getHandlerFunction: () => t.handler, _t: t })),
      deleteTrigger: (g) => { const i = triggers.indexOf(g._t); if (i >= 0) triggers.splice(i, 1); },
      newTrigger: (h) => ({ timeBased: () => ({ after: (ms) => ({ create: () => triggers.push({ handler: h, ms }) }) }) }),
    },
    SpreadsheetApp: {
      getActive: () => ss,
      CopyPasteType: { PASTE_VALUES: "PASTE_VALUES" },
      getUi: () => ({ createMenu: (n) => { const m = { n, items: [], addItem(l, f) { m.items.push([l, f]); return m; }, addToUi() { ctx.__menu = m; } }; return m; }, alert: (t, m) => log.push("ALERT " + m), ButtonSet: { OK: 1 } }),
    },
    UrlFetchApp: {
      fetch: (url, opts) => {
        clock += 200;
        fetchLog.push(url);
        if (url.endsWith("/api/auth/token")) return resp(200, JSON.stringify({ access_token: "tok" }));
        const m = url.match(/\/api\/v1\/dados\/([^/?]+)\/(meta|export)\??(.*)$/);
        if (!m) return resp(404, "x");
        const [, rel, acao, qs] = m;
        if (o.falha[rel]) return resp(o.falha[rel], '{"error":"Escopo insuficiente para este recurso."}');
        const d = dataset[rel];
        if (acao === "meta") return resp(200, JSON.stringify({ ok: true, upload_id: [d.upload], total: d.n }));
        const q = Object.fromEntries(qs.split("&").filter(Boolean).map((x) => x.split("=")));
        clock += o.msFetch;
        const limit = +q.limit, after = +(q.after_id || 0);
        const { texto, ids } = csvDe(rel, after, limit);
        const next = ids.length === limit ? String(ids[ids.length - 1]) : "";
        if (o.aoBaixar) o.aoBaixar(rel, after, dataset);
        return resp(200, texto, { "x-total": after === 0 ? String(d.n) : "", "X-Upload-Id": String(d.upload), "x-next-after-id": next, "X-Linhas": String(ids.length) });
      },
    },
  };
  vm.createContext(ctx);
  let src = fs.readFileSync(o.arquivo, "utf-8").replace(/const LINHAS_POR_LOTE = \d+;/, "const LINHAS_POR_LOTE = " + o.lote + ";");
  vm.runInContext(src, ctx);
  props.CLIENT_ID = "cli_x"; props.CLIENT_SECRET = "sec_x";
  const run = (code) => vm.runInContext(code, ctx);
  return { run, ctx, log, props, triggers, sheets, dataset, fetchLog, toasts, advance: (ms) => { clock += ms; }, now: () => clock };
}
module.exports = { criarAmbiente };
