// Recursos-chave do SPO-AA+ (lote 6 em diante), cada um com o comportamento esperado:
// menu no celular, filtro dos relatórios, busca nas listas longas, barra de título da tela, rolagem do "Automático",
// teclas de um toque e o kit. Só abre, toca, digita e escolhe; nada é gravado.
import { abrirPerfil, abrirTela, KIT, PERFIS } from "../lib/ambiente.mjs";
import { medirTela } from "../lib/medidas.mjs";
import { registrar, capturar, etapa } from "../lib/registro.mjs";
import { comTempo, LIMITE_PASSO_S, TempoEsgotado } from "../lib/tempo.mjs";

const celular = (perfil) => PERFIS[perfil].ctx.viewport.width <= 600 || PERFIS[perfil].ctx.viewport.height <= 560;
const quadro = (pag) => pag.evaluate(() => new Promise((ok) => requestAnimationFrame(() => requestAnimationFrame(ok))));

// cada checagem tem o tempo-limite do passo; se estourar, a falha é registrada e o resto deste perfil é pulado
// (o navegador pode ter travado): checar lança TempoEsgotado para o laço do perfil parar
async function checar(perfil, alvo, pag, fn) {
  etapa(`recursos ${perfil}`, alvo);
  try {
    const falhas = await comTempo(fn(), LIMITE_PASSO_S, `${alvo} (${perfil})`);
    registrar({ verificacao: "recursos", perfil, alvo, situacao: falhas.length ? "falha" : "ok", resumo: falhas.join("; "), captura: falhas.length ? await capturar(pag, `recursos-${perfil}-${alvo}`) : null });
  } catch (e) {
    const travou = e instanceof TempoEsgotado;
    registrar({ verificacao: "recursos", perfil, alvo, situacao: "falha", resumo: String(e.message).slice(0, 160), captura: travou ? null : await capturar(pag, `recursos-${perfil}-${alvo}-erro`) });
    if (travou) throw e;
  }
}

export async function recursos(perfis) {
  for (const perfil of perfis) {
    const { nav, ctx, pag } = await abrirPerfil(perfil);
    const movel = celular(perfil);
    try {
      await abrirTela(pag, "");
      if (movel) {
        await checar(perfil, "menu no celular", pag, async () => {
          const f = [];
          const r = await pag.evaluate(() => ({ compacto: document.body.classList.contains("spo-menu-compacto"), seta: !!document.getElementById("sidebar-toggle")?.getClientRects().length }));
          if (!r.compacto) f.push("sem o modo compacto");
          await pag.click("#sidebar-toggle");
          await pag.waitForTimeout(500);
          const aberto = await pag.evaluate(() => { const s = document.getElementById("sidebar"); const b = s.getBoundingClientRect(); return { open: s.classList.contains("open"), altura: Math.round(b.height), janela: innerHeight }; });
          if (!aberto.open) f.push("o \"›\" não abriu o menu");
          else if (aberto.altura < aberto.janela - 2) f.push(`menu aberto com ${aberto.altura} de ${aberto.janela} px`);
          await pag.keyboard.press("Escape");
          await pag.waitForTimeout(400);
          if (await pag.evaluate(() => document.getElementById("sidebar").classList.contains("open"))) f.push("Esc não fechou o menu");
          return f;
        });
      }
      await checar(perfil, "lupa e Ajuda no cabeçalho", pag, async () => {
        const f = [];
        await pag.click(".spo-cabecalho-busca");
        await pag.waitForTimeout(500);
        if (!await pag.evaluate(() => !!document.querySelector(".spo-busca-fundo:not([hidden])"))) f.push("a lupa não abriu a busca");
        await pag.keyboard.press("Escape");
        await pag.waitForTimeout(300);
        await pag.click(".spo-cabecalho-ajuda");
        await pag.waitForTimeout(500);
        if (!await pag.evaluate(() => [...document.querySelectorAll(".spo-busca-fundo:not([hidden]) h2")].some((h) => /Ajuda/.test(h.textContent)))) f.push("o \"?\" não abriu a Ajuda");
        await pag.keyboard.press("Escape");
        return f;
      });
      await checar(perfil, "filtro dos relatórios (FIP 613)", pag, async () => {
        const f = [];
        await abrirTela(pag, "relatorios/fip613");
        const ws = await pag.$$(".mf-wrapper");
        if (!ws.length) return ["sem filtros"];
        for (const w of [ws[1] || ws[0], ws[ws.length - 1]]) {
          await w.scrollIntoViewIfNeeded();
          await (await w.$(".mf-display")).click();
          await quadro(pag);
          const r = await w.evaluate((wrap) => {
            const p = wrap.querySelector(".mf-panel"), pr = p.getBoundingClientRect();
            let dir = document.documentElement.clientWidth;
            // no celular o painel é um cartão fixo no alto da tela: conta a tela, não a área da tabela
            if (getComputedStyle(p).position !== "fixed") for (let e = p.parentElement; e && e !== document.body; e = e.parentElement) if (/(auto|scroll|hidden|clip)/.test(getComputedStyle(e).overflowX)) { dir = Math.min(dir, e.getBoundingClientRect().right); break; }
            const ap = [...p.querySelectorAll("button")].find((b) => /Aplicar/.test(b.textContent)).getBoundingClientRect();
            const cortadas = [...p.querySelectorAll(".mf-option span")].filter((s) => s.scrollWidth > s.clientWidth + 1).length;
            return { aberto: p.classList.contains("open"), aplicar: ap.right <= dir + 1 && ap.bottom <= innerHeight + 1 && ap.top >= 0, cortadas, peso: getComputedStyle(p.querySelector(".mf-option span") || p).fontWeight };
          });
          if (!r.aberto) f.push("o filtro não abriu");
          if (!r.aplicar) f.push("\"Aplicar\" fora da área visível");
          if (r.cortadas) f.push(`${r.cortadas} nome(s) cortado(s)`);
          if (r.peso !== "400") f.push(`opções em peso ${r.peso}`);
          // clicar fora fecha (no celular, o toque no véu)
          if (movel) await pag.evaluate(() => document.querySelector(".spo-filtro-veu")?.click());
          else await pag.mouse.click(5, Math.round((await pag.viewportSize()).height / 2));
          await pag.waitForTimeout(300);
          if (await w.evaluate((wrap) => wrap.querySelector(".mf-panel").classList.contains("open"))) f.push("clicar fora não fechou o filtro");
        }
        return f;
      });
      await checar(perfil, "busca nas listas longas", pag, async () => {
        const f = [];
        await abrirTela(pag, "cadastrar/dotacao/formulario");
        await pag.waitForFunction(() => document.getElementById("dotacao-chave")?.options.length > 40, null, { timeout: 45000 });
        await pag.waitForTimeout(600);
        const campo = pag.locator("#dotacao-chave + .spo-lista-busca input");
        if (!await campo.isVisible()) return ["sem o campo \"Filtrar a lista\" na Chave do Planejamento"];
        const total = await pag.evaluate(() => [...document.getElementById("dotacao-chave").options].filter((o) => o.value).length);
        await campo.scrollIntoViewIfNeeded();
        await campo.click();
        await pag.keyboard.type("saex gestao", { delay: 20 });
        await pag.waitForTimeout(300);
        const r = await pag.evaluate(() => ({ opcoes: [...document.getElementById("dotacao-chave").options].filter((o) => o.value).length, status: document.querySelector("#dotacao-chave + .spo-lista-busca .spo-lista-busca-status").textContent }));
        if (!(r.opcoes > 0 && r.opcoes < total)) f.push(`o filtro deixou ${r.opcoes} de ${total}`);
        if (!/\d+ de \d+/.test(r.status)) f.push(`contagem "${r.status}"`);
        await pag.keyboard.press("Escape");
        await pag.waitForTimeout(200);
        const volta = await pag.evaluate(() => [...document.getElementById("dotacao-chave").options].filter((o) => o.value).length);
        if (volta !== total) f.push(`Esc devolveu ${volta} de ${total}`);
        return f;
      });
      await checar(perfil, "barra de título da tela (PTA/OpR)", pag, async () => {
        const f = [];
        await abrirTela(pag, "atualizar/governanca-resultados/programacao-pta2027");
        let frame = null;
        for (let i = 0; i < 40 && !frame; i++) { await pag.waitForTimeout(500); frame = pag.frames().find((x) => x.url().includes("pta2027")); }
        if (!frame) return ["sem o iframe do mockup"];
        await frame.waitForFunction(() => typeof startWizard === "function", null, { timeout: 20000 });
        await frame.evaluate(() => { document.querySelector(".macro-card")?.click(); });
        await pag.waitForTimeout(800);
        await frame.evaluate(() => startWizard(0));
        await pag.waitForTimeout(1500);
        const r = await pag.evaluate(() => {
          const h = document.querySelector(".pta2027-integration-header"), a = h && h.querySelector(".spo-cabecalho-acoes"), s = document.getElementById("sidebar-toggle").getBoundingClientRect();
          const ar = a && a.getBoundingClientRect(), h1 = h && h.querySelector("h1");
          return { foco: document.body.classList.contains("special-context-focus"), botoes: !!a, dif: ar ? Math.abs((s.top + s.height / 2) - (ar.top + ar.height / 2)) : 999, titulo: h1 ? h1.scrollWidth <= h1.clientWidth + 1 : false };
        });
        if (!r.foco) f.push("a barra da tela não entrou");
        if (!r.botoes) f.push("sem a lupa e o \"?\" na barra");
        if (r.dif > 2) f.push(`"›" a ${Math.round(r.dif)} px do centro dos botões`);
        if (!r.titulo) f.push("título cortado");
        await pag.evaluate(() => document.getElementById("pta2027-sticky-back")?.click());
        await pag.waitForTimeout(1200);
        if (!await pag.evaluate(() => !!document.querySelector(".topbar .spo-cabecalho-acoes"))) f.push("os botões não voltaram ao cabeçalho do SPO");
        return f;
      });
      if (movel) {
        await checar(perfil, "rolagem do Automático (não sobe)", pag, async () => {
          await abrirTela(pag, "atualizar/governanca-resultados/programacao-pta2027");
          let frame = null;
          for (let i = 0; i < 40 && !frame; i++) { await pag.waitForTimeout(500); frame = pag.frames().find((x) => x.url().includes("pta2027")); }
          if (!frame) return ["sem o iframe do mockup"];
          await frame.waitForFunction(() => document.querySelectorAll(".process-lane.finalistic .macro-card").length > 0, null, { timeout: 20000 });
          const y0 = await frame.evaluate(() => { const cs = [...document.querySelectorAll(".process-lane.finalistic .macro-card")]; const c = cs[cs.length - 1]; c.scrollIntoView({ block: "center", behavior: "instant" }); return window.parent.scrollY; });
          await pag.waitForTimeout(200);
          await frame.evaluate(() => { const cs = [...document.querySelectorAll(".process-lane.finalistic .macro-card")]; cs[cs.length - 1].click(); });
          await pag.waitForTimeout(1500);
          const y1 = await pag.evaluate(() => window.scrollY);
          return y1 < y0 - 2 ? [`a página subiu ${Math.round(y0 - y1)} px ao abrir os eixos`] : [];
        });
      }
      await checar(perfil, "kit de mockup", pag, async () => {
        const kit = await ctx.newPage();
        await kit.goto(KIT, { waitUntil: "load" });
        await kit.waitForTimeout(3500);
        const m = await kit.evaluate(medirTela);
        const r = await kit.evaluate(() => ({ opcoes: document.querySelectorAll("#kit-opcoes [data-theme-choice]").length, barra: !!document.getElementById("kit-barra-tela"), aviso: (document.getElementById("kit-aviso")?.textContent || "").trim() }));
        await kit.close();
        const f = [];
        if (m.rolagemLateral) f.push(`rolagem lateral ${m.rolagemLateral}px`);
        if (!r.opcoes) f.push("sem o painel de preferências");
        if (!r.barra) f.push("sem o exemplo da barra de título");
        if (r.aviso) f.push(`aviso: ${r.aviso.slice(0, 80)}`);
        return f;
      });
    } catch (e) {
      if (!(e instanceof TempoEsgotado)) throw e;
      registrar({ verificacao: "recursos", perfil, alvo: "interrompida", situacao: "falha", resumo: "o restante deste perfil não foi medido (um passo estourou o tempo)" });
    } finally {
      await comTempo(nav.close(), 30, "fechar o navegador").catch(() => {});
    }
  }
}

// teclas de um toque: com "Teclado: desligado", "/" não abre a busca; ligado, abre
export async function teclas(perfil) {
  const { nav, pag } = await abrirPerfil(perfil);
  try {
    await abrirTela(pag, "atualizar/personalizar-spo/temas");
    const tentar = async (valor) => {
      await pag.evaluate((v) => document.querySelector(`[data-spo-font-pref="keys"][data-spo-font-value="${v}"]`)?.click(), valor);
      await pag.waitForTimeout(300);
      await pag.evaluate(() => document.activeElement?.blur());
      await pag.keyboard.press("/");
      await pag.waitForTimeout(500);
      const abriu = await pag.evaluate(() => !!document.querySelector(".spo-busca-fundo:not([hidden])"));
      await pag.keyboard.press("Escape");
      return abriu;
    };
    etapa(`recursos ${perfil}`, "teclas de um toque");
    const { off, on } = await comTempo((async () => ({ off: await tentar("off"), on: await tentar("on") }))(), LIMITE_PASSO_S, `teclas de um toque (${perfil})`);
    registrar({ verificacao: "recursos", perfil, alvo: "teclas de um toque", situacao: !off && on ? "ok" : "falha", resumo: !off && on ? "" : `"/" desligado abriu=${off}, ligado abriu=${on}` });
  } catch (e) {
    registrar({ verificacao: "recursos", perfil, alvo: "teclas de um toque", situacao: "falha", resumo: String(e.message).slice(0, 160) });
  } finally {
    await comTempo(nav.close(), 30, "fechar o navegador").catch(() => {});
  }
}
