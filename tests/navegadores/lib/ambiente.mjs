// Bateria SPO-AA+ — ambiente: endereço do SPO, perfis de aparelho, sessão e telas.
import { chromium, firefox, webkit, devices } from "playwright";
import { existsSync, readFileSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const PASTA = resolve(dirname(fileURLToPath(import.meta.url)), "..");
export const RAIZ = resolve(PASTA, "..", "..");
export const BASE = (process.env.BATERIA_BASE || "http://localhost:5000/").replace(/\/?$/, "/");
export const KIT = BASE + "static/kit/mockup-spo-aa-plus.html";
export const MOTORES = { chromium, firefox, webkit };

// ---------- perfis (os aparelhos da equipe; padrão SPO-AA+ §5, item 15) ----------
const movel = (d, extra = {}) => ({ ...d, ...extra });
export const PERFIS = {
  "D-chromium-1366": { tipo: chromium, ctx: { viewport: { width: 1366, height: 768 } } },
  "D-firefox-1366": { tipo: firefox, ctx: { viewport: { width: 1366, height: 768 } } },
  "D-webkit-1366": { tipo: webkit, ctx: { viewport: { width: 1366, height: 768 } } },
  "M-s26-360": { tipo: chromium, ctx: movel(devices["Galaxy S24"]) },
  "M-edge50-407": { tipo: chromium, ctx: movel(devices["Pixel 7"], { viewport: { width: 407, height: 820 }, deviceScaleFactor: 3 }) },
  // o Firefox do Playwright não imita celular: só a largura; "fonte ampliada" = tamanho mínimo de 18 px
  "M-firefox-fonte18-407": { tipo: firefox, ctx: { viewport: { width: 407, height: 820 } }, prefs: { "font.minimum-size.x-western": 18 } },
  "M-iphone16-393": { tipo: webkit, ctx: movel(devices["iPhone 16"]) },
  "M-iphonepromax-440": { tipo: webkit, ctx: movel(devices["iPhone 17 Pro Max"]) },
  "T-ipad11-834": { tipo: webkit, ctx: movel(devices["iPad Pro 11"]) },
  "T-fold-aberto-924": { tipo: chromium, ctx: movel(devices["Galaxy Z Fold 7"], { viewport: { width: 924, height: 1150 } }) },
  "Q-s26-deitado-780": { tipo: chromium, ctx: movel(devices["Galaxy S24"], { viewport: { width: 780, height: 360 } }) },
  "Q-razr-externa-424x360": { tipo: chromium, ctx: movel(devices["Galaxy S24"], { viewport: { width: 424, height: 360 } }) },
  "Q-iphone-duo-aberto-626": { tipo: webkit, ctx: movel(devices["iPhone 16"], { viewport: { width: 626, height: 820 } }) },
  "Q-ipad13-1032": { tipo: webkit, ctx: movel(devices["iPad Pro 11"], { viewport: { width: 1032, height: 1300 } }) },
};
export const DESKTOP = ["D-chromium-1366", "D-firefox-1366", "D-webkit-1366"];
export const APARELHOS = Object.keys(PERFIS).filter((k) => !k.startsWith("Q-"));
export const RAPIDOS = Object.keys(PERFIS).filter((k) => k.startsWith("Q-"));

// ---------- telas ----------
// as 11 principais (com a Início, que é o carregamento da página)
export const TELAS_PRINCIPAIS = [
  "", "atualizar/personalizar-spo/temas", "relatorios/ped", "relatorios/fip613", "relatorios/plan51",
  "cadastrar/dotacao/formulario", "paineis-dashboards/teto-orcamentario", "atualizar/governanca-resultados/mapa",
  "atualizar/governanca-resultados/estrategia", "atualizar/governanca-resultados/programacao-pta2027", "institucional/diretrizes",
];
export const TELAS_BASICAS = ["", "atualizar/personalizar-spo/temas", "relatorios/fip613", "cadastrar/dotacao/formulario",
  "atualizar/governanca-resultados/programacao-pta2027"];
// todas as telas do menu, menos os relatórios pesados (EMP, NOB, EST EMP: consultas de minutos), a Limpeza de
// Histórico (mede o banco ao abrir) e "Sair"; os cadastros abrem por "Formulário" e "Consultar"
export const TELAS_TODAS = `atualizar/personalizar-spo/temas atualizar/personalizar-spo/contrato-visual-protegido
  atualizar/seguranca-sessao atualizar/governanca-resultados/mapa atualizar/governanca-resultados/programacao-pta2027
  atualizar/governanca-resultados/estrategia atualizar/governanca-resultados/eap-politicas atualizar/governanca-resultados/entregas-okr
  atualizar/governanca-resultados/processos-riscos-projetos atualizar/governanca-resultados/matriculas-metas
  atualizar/estrutura-planejamento/programas atualizar/estrutura-planejamento/acoes atualizar/estrutura-planejamento/produtos
  atualizar/estrutura-planejamento/componentes atualizar/estrutura-planejamento/componentes-rev atualizar/estrutura-planejamento/vinculos
  atualizar/estrutura-planejamento/modelos-chave atualizar/estrutura-planejamento/catalogo-chave atualizar/estrutura-planejamento/replicar-exercicio
  atualizar/teto-seduc atualizar/plan20-seduc atualizar/chave_planejamento_regra atualizar/chaves_planejamento_upload atualizar/fip613
  atualizar/ped atualizar/emp atualizar/receita-anexo10 atualizar/est-emp atualizar/nob atualizar/plan51
  institucional/diretrizes institucional/repositorio institucional/legislacao institucional/parceiros relatorios/plan20-seduc
  relatorios/plan21-nger relatorios/estrutura-planejamento relatorios/fip613 relatorios/ped relatorios/plan51 relatorios/receita-anexo10
  relatorios/dotacao relatorios/est-dotacao paineis-dashboards/teto-orcamentario area-uens/sage/notas-see usuarios/cadastrar
  usuarios/editar usuarios/perfil usuarios/senha usuarios/api-acessos painel`.split(/\s+/).filter(Boolean)
  .concat(["dotacao", "est-dotacao", "plan_21-nger/meta_fisica", "plan_21-nger/subacao", "plan_21-nger/etapa"]
    .flatMap((c) => [`cadastrar/${c}/formulario`, `cadastrar/${c}/consultar`]));
export const nomeDaTela = (rota) => rota ? rota.replace(/\//g, "_") : "inicio";

// ---------- sessão (usuário de teste; nunca a senha de uma pessoa) ----------
// E-mail e senha do usuário de teste vêm de BATERIA_EMAIL/BATERIA_SENHA ou do arquivo BATERIA_ENV (padrão: .env.bateria
// na raiz do repositório, ignorado pelo git). A sessão salva fica em sessoes/ (ignorada pelo git).
const ARQ_SESSAO = resolve(PASTA, "sessoes", "sessao.json");
function credenciais() {
  if (process.env.BATERIA_EMAIL && process.env.BATERIA_SENHA) return { email: process.env.BATERIA_EMAIL, senha: process.env.BATERIA_SENHA };
  const arq = process.env.BATERIA_ENV || resolve(RAIZ, ".env.bateria");
  if (!existsSync(arq)) throw new Error(`sem credenciais do usuário de teste: defina BATERIA_EMAIL e BATERIA_SENHA, ou crie ${arq} (veja o README)`);
  const v = Object.fromEntries(readFileSync(arq, "utf-8").split(/\r?\n/).filter((l) => l.includes("=")).map((l) => l.split(/=(.*)/s).slice(0, 2)));
  return { email: v.BATERIA_EMAIL, senha: v.BATERIA_SENHA };
}
export async function garantirSessao() {
  const nav = await chromium.launch();
  try {
    if (existsSync(ARQ_SESSAO)) {
      const ctx = await nav.newContext({ storageState: ARQ_SESSAO });
      const pag = await ctx.newPage();
      await pag.goto(BASE, { waitUntil: "domcontentloaded" });
      const ok = !new URL(pag.url()).pathname.startsWith("/login") && await pag.locator("#sidebar").count();
      await ctx.close();
      if (ok) return ARQ_SESSAO;
    }
    const { email, senha } = credenciais();
    const ctx = await nav.newContext();
    const pag = await ctx.newPage();
    await pag.goto(BASE + "login", { waitUntil: "domcontentloaded" });
    for (let i = 0; i < 2 && new URL(pag.url()).pathname.startsWith("/login"); i++) {
      // a 2ª tentativa vai com force_login (sessão aberta em outro lugar), como no SPO
      await pag.fill("#login-email", email);
      await pag.fill("#password", senha);
      await pag.click('#login-form button[type="submit"]');
      await pag.waitForTimeout(3000);
    }
    if (new URL(pag.url()).pathname.startsWith("/login")) throw new Error("o usuário de teste não entrou no SPO");
    await pag.waitForSelector("#sidebar", { timeout: 20000 });
    mkdirSync(dirname(ARQ_SESSAO), { recursive: true });
    await ctx.storageState({ path: ARQ_SESSAO });
    return ARQ_SESSAO;
  } finally {
    await nav.close();
  }
}

export async function abrirPerfil(nomePerfil, extra = {}) {
  const p = PERFIS[nomePerfil];
  const nav = await p.tipo.launch(p.prefs ? { firefoxUserPrefs: p.prefs } : {});
  const opcoes = { ...p.ctx, storageState: await garantirSessao(), ...extra };
  if (p.tipo === firefox) delete opcoes.isMobile;
  const ctx = await nav.newContext(opcoes);
  const pag = await ctx.newPage();
  const erros = [];
  pag.on("pageerror", (e) => erros.push(String(e.message).slice(0, 160)));
  pag.on("console", (m) => { if (m.type() === "error") erros.push(m.text().slice(0, 160)); });
  return { nav, ctx, pag, erros, motor: p.tipo.name() };
}

// abre uma tela pelo menu (clique no [data-route]) e espera o carregamento e os dados (até 60 s)
export async function abrirTela(pag, rota) {
  if (!pag.url().startsWith(BASE) || !rota) {
    await pag.goto(BASE, { waitUntil: "domcontentloaded", timeout: 60000 });
    await pag.waitForTimeout(1500);
  }
  if (new URL(pag.url()).pathname.startsWith("/login")) throw new Error("sessão expirada");
  if (rota) {
    const achou = await pag.evaluate((r) => { const a = document.querySelector(`[data-route="${r}"]`); if (a) { a.click(); return true; } return false; }, rota);
    if (!achou) throw new Error("tela não encontrada no menu (permissão do usuário de teste?)");
  }
  await pag.waitForFunction(() => ![...document.querySelectorAll(".app-loading-overlay")].some((e) => getComputedStyle(e).display !== "none" && e.getBoundingClientRect().height > 0), null, { timeout: 60000 }).catch(() => {});
  await pag.waitForTimeout(2000);
}

// preferências do Personalizar gravadas no navegador antes de carregar (mesmas chaves do main.js)
export async function gravarPreferencias(ctx, { tema = "light", destaque = "classic", comuns = {}, fontes = {} } = {}) {
  await ctx.addInitScript(({ tema, destaque, comuns, fontes }) => {
    try {
      localStorage.setItem("app-theme", tema);
      localStorage.setItem("app-accent", destaque);
      localStorage.setItem("spo-common-visual-preferences", JSON.stringify({ density: "auto", contrast: "standard", cards: "soft", diagrams: "auto", ...comuns }));
      localStorage.setItem("spo-font-preferences", JSON.stringify({ ui: "inter", data: "intel-mono", size: "default", table: "medium", line: "default", reading: "default", cvd: "default", focus: "default", links: "default", brightness: "default", motion: "auto", keys: "on", ...fontes }));
    } catch (e) { /* armazenamento bloqueado */ }
  }, { tema, destaque, comuns, fontes });
}
