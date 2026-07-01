const menuTree = [
  { label: "Início", icon: "⌂", page: "inicio" },
  {
    label: "Atualizar", icon: "↻", children: [
      {
        label: "Planejamento", children: [
          { label: "PTA 2027 - Cadeia de Valor", page: "pta2027" },
          { label: "PLAN20 - SEDUC" },
          { label: "Teto - SEDUC" },
          {
            label: "Estrutura do Planejamento", children: [
              { label: "Programas" },
              { label: "Ações/PAOE" },
              { label: "Produtos da Ação" },
              { label: "Componentes" },
              { label: "Modelos de Chave" },
              { label: "Catálogo de Chaves" },
              { label: "Replicar exercício" }
            ]
          },
          { label: "Regras Chave Planejamento" }
        ]
      },
      {
        label: "Execução", children: [
          { label: "FIP 613" }, { label: "PED" }, { label: "EMP" },
          { label: "EST EMP" }, { label: "NOB" }
        ]
      }
    ]
  },
  {
    label: "Cadastrar", icon: "＋", children: [
      {
        label: "Planejamento", children: [
          {
            label: "Plan 21 - NGER", children: [
              { label: "Meta Física", children: [{ label: "Formulário" }, { label: "Consultar" }] },
              { label: "Subação", children: [{ label: "Formulário" }, { label: "Consultar" }] },
              { label: "Etapa", children: [{ label: "Formulário" }, { label: "Consultar" }] }
            ]
          }
        ]
      },
      {
        label: "Execução", children: [
          { label: "Dotação", children: [{ label: "Formulário" }, { label: "Consultar" }] },
          { label: "Estorno de Dotação", children: [{ label: "Formulário" }, { label: "Consultar" }] }
        ]
      }
    ]
  },
  {
    label: "Institucional", icon: "▣", children: [
      { label: "Diretrizes e Procedimentos" },
      { label: "Repositório de Arquivos" },
      { label: "Legislação e Normas" },
      { label: "Rede de Parceiros" }
    ]
  },
  {
    label: "Relatórios", icon: "▥", children: [
      {
        label: "Planejamento", children: [
          { label: "PLAN20 - SEDUC" }, { label: "PLAN21_NGER" },
          { label: "Estrutura do Planejamento", children: [{ label: "Consultar Estrutura" }] }
        ]
      },
      {
        label: "Execução", children: [
          { label: "FIP 613" }, { label: "PED" }, { label: "EMP" },
          { label: "EST EMP" }, { label: "NOB" }, { label: "DOTAÇÃO" },
          { label: "ESTORNO DE DOTAÇÃO" }
        ]
      }
    ]
  },
  {
    label: "Painéis/Dashboards", icon: "◫", children: [
      { label: "Planejamento", children: [{ label: "Teto Orçamentário", page: "dashboard" }] },
      { label: "Execução", children: [{ label: "Em preparação" }] }
    ]
  },
  {
    label: "Usuários", icon: "♙", children: [
      { label: "Cadastrar" }, { label: "Editar" }, { label: "Perfil" },
      { label: "Alterar Senha" }, { label: "API de Acessos" }
    ]
  },
  { label: "Painel - Permissões", icon: "▦" },
  { label: "Sair", icon: "⇥", action: "logout" }
];

const state = {
  currentPath: ["Início"],
  theme: localStorage.getItem("spo-theme") || "light"
};

const loginView = document.querySelector("#login-view");
const appView = document.querySelector("#app-view");
const contentArea = document.querySelector("#content-area");
const menu = document.querySelector("#menu");

function formatLocalDateTime() {
  return new Intl.DateTimeFormat("pt-BR", {
    dateStyle: "short",
    timeStyle: "medium"
  }).format(new Date());
}

function updatePtaIntegratedClock() {
  const clock = document.querySelector("#pta-integrated-clock");
  if (clock) clock.textContent = formatLocalDateTime();
}

function slug(text) {
  return text.toLocaleLowerCase("pt-BR")
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "");
}

function buildMenu(items, parent, path = []) {
  items.forEach(item => {
    const currentPath = [...path, item.label];
    if (item.children) {
      const group = document.createElement("div");
      group.className = "menu-group";
      const parentItem = document.createElement("div");
      parentItem.className = "menu-item menu-parent";
      parentItem.innerHTML = `${item.icon ? `<span class="menu-icon">${item.icon}</span>` : ""}<span>${item.label}</span>`;
      parentItem.addEventListener("click", () => group.classList.toggle("open"));
      const submenu = document.createElement("div");
      submenu.className = "submenu";
      buildMenu(item.children, submenu, currentPath);
      group.append(parentItem, submenu);
      parent.append(group);
      return;
    }

    const link = document.createElement("a");
    link.href = "#";
    link.className = "menu-item";
    link.dataset.path = currentPath.join(" / ");
    link.innerHTML = `${item.icon ? `<span class="menu-icon">${item.icon}</span>` : ""}<span>${item.label}</span>`;
    link.addEventListener("click", event => {
      event.preventDefault();
      if (item.action === "logout") return logout();
      state.currentPath = currentPath;
      document.querySelectorAll(".menu-item.active").forEach(el => el.classList.remove("active"));
      link.classList.add("active");
      renderPage(item);
    });
    parent.append(link);
  });
}

function pageHeader(title, description) {
  return `
    <div class="content-header">
      <div>
        <h1>${title}</h1>
        <p class="muted">${description}</p>
      </div>
      <span class="offline-badge">● OFFLINE</span>
    </div>`;
}

function renderHome() {
  contentArea.innerHTML = `
    ${pageHeader("Bem-vindo", "Use o menu ao lado para navegar pelo laboratório local.")}
    <div class="welcome-grid">
      <article class="card highlight">
        <div class="card-title">Ambiente independente</div>
        <p>Esta cópia não se conecta ao site online nem ao banco de dados institucional.</p>
      </article>
      <article class="card">
        <div class="card-title">Estrutura mapeada</div>
        <div class="metric">7</div>
        <p class="muted">grupos funcionais principais</p>
      </article>
      <article class="card">
        <div class="card-title">Modo de trabalho</div>
        <div class="metric">Local</div>
        <p class="muted">dados de teste ficam apenas neste navegador</p>
      </article>
      <article class="card wide">
        <div class="card-title">Próximos experimentos</div>
        <ul>
          <li>Reorganizar menus e testar novos nomes.</li>
          <li>Prototipar formulários e fluxos de aprovação.</li>
          <li>Criar painéis com dados fictícios antes da implementação oficial.</li>
        </ul>
      </article>
    </div>`;
}

function renderDashboard() {
  contentArea.innerHTML = `
    ${pageHeader("Teto Orçamentário", "Protótipo local de painel gerencial.")}
    <div class="teto-kpi-grid">
      <div class="teto-kpi"><span>Teto total</span><strong>R$ 2,48 bi</strong></div>
      <div class="teto-kpi"><span>Programado</span><strong>R$ 2,12 bi</strong></div>
      <div class="teto-kpi"><span>Saldo</span><strong>R$ 360 mi</strong></div>
      <div class="teto-kpi"><span>Utilização</span><strong>85,5%</strong></div>
    </div>
    <div class="card">
      <div class="card-title">Resumo demonstrativo</div>
      <div class="table-responsive">
        <table class="table demo-table">
          <thead><tr><th>Unidade</th><th>Grupo</th><th>Valor</th></tr></thead>
          <tbody>
            <tr><td>Administração Central</td><td>Pessoal</td><td>R$ 1,18 bi</td></tr>
            <tr><td>Unidades Escolares</td><td>Custeio</td><td>R$ 720 mi</td></tr>
            <tr><td>Infraestrutura</td><td>Investimentos</td><td>R$ 580 mi</td></tr>
          </tbody>
        </table>
      </div>
    </div>`;
}

function exitPtaFocusMode() {
  appView.classList.remove("pta-focus-mode");
}

function renderPtaIntegrated() {
  appView.classList.add("pta-page-view");
  document.querySelector("#sidebar").classList.add("collapsed");
  contentArea.innerHTML = `
    ${pageHeader("PTA 2027 - Cadeia Estratégica", "Tela integrada ao SPO, com menu lateral recolhido automaticamente.")}
    <section class="pta-integrated-shell" aria-label="PTA 2027 integrado ao SPO">
      <div class="pta-integrated-toolbar">
        <div class="pta-integrated-brand">
          <div class="pta-mini-mark">SPO</div>
          <div>
            <strong><span class="standard-title">Programação estratégica 2027</span><span class="focus-title">Plano de Trabalho Anual</span></strong>
            <span class="standard-help">Use o modo tela cheia apenas quando precisar ampliar a área de trabalho.</span>
            <span class="focus-subtitle">Programação estratégica 2027</span>
          </div>
        </div>
        <div class="pta-integrated-actions">
          <span class="draft-state"><i></i> Rascunho local</span>
          <span class="version-state">v63</span>
          <span id="pta-integrated-clock" class="pta-integrated-clock" aria-label="Data e hora local"></span>
          <button id="pta-fullscreen-toggle" class="btn btn-primary" type="button">Ativar modo tela cheia</button>
          <a class="btn" href="pta-2027.html" target="_blank" rel="noopener">Abrir separado</a>
        </div>
      </div>
      <iframe class="pta-integrated-frame" src="pta-2027.html?embedded=1" title="PTA 2027 pela cadeia estratégica"></iframe>
    </section>`;

  const fullscreenButton = document.querySelector("#pta-fullscreen-toggle");
  fullscreenButton.addEventListener("click", () => {
    appView.classList.toggle("pta-focus-mode");
    const active = appView.classList.contains("pta-focus-mode");
    fullscreenButton.textContent = active ? "Sair do modo tela cheia" : "Ativar modo tela cheia";
    showToast(active ? "PTA ampliado em modo tela cheia dentro do SPO." : "PTA retornou ao modo integrado.");
  });
  updatePtaIntegratedClock();
}

function renderPrototype() {
  const title = state.currentPath.at(-1);
  const context = state.currentPath.slice(0, -1).join(" › ");
  const isReport = state.currentPath.includes("Relatórios") || title.startsWith("Consultar");
  const isInstitutional = state.currentPath.includes("Institucional");

  if (isReport) {
    contentArea.innerHTML = `
      ${pageHeader(title, context)}
      <div class="prototype-banner"><span>ⓘ</span><div><strong>Protótipo offline</strong><br><span class="muted">Filtros e resultados abaixo usam dados fictícios.</span></div></div>
      <div class="card">
        <div class="form-grid">
          <label class="field"><span>Exercício</span><select><option>2026</option><option>2025</option></select></label>
          <label class="field"><span>Unidade</span><select><option>Todas</option><option>Administração Central</option></select></label>
          <label class="field"><span>Pesquisar</span><input placeholder="Digite para filtrar"></label>
        </div>
        <div class="actions" style="margin-top:12px"><button class="btn btn-primary demo-action">Consultar</button></div>
      </div>
      <div class="card" style="margin-top:14px">
        <div class="empty-illustration"><div><div class="symbol">▥</div><strong>Nenhuma consulta realizada</strong><p>Use os filtros para simular um relatório.</p></div></div>
      </div>`;
    bindDemoActions();
    return;
  }

  if (isInstitutional) {
    contentArea.innerHTML = `
      ${pageHeader(title, context)}
      <div class="card">
        <div class="empty-illustration"><div><div class="symbol">▣</div><strong>Área institucional de testes</strong><p>Adicione documentos fictícios e experimente uma organização alternativa.</p><button class="btn btn-primary demo-action">Adicionar item de teste</button></div></div>
      </div>`;
    bindDemoActions();
    return;
  }

  contentArea.innerHTML = `
    ${pageHeader(title, context)}
    <div class="prototype-banner"><span>⚗</span><div><strong>Tela experimental</strong><br><span class="muted">O formulário funciona localmente e não grava no sistema online.</span></div></div>
    <form class="card form prototype-form" id="prototype-form">
      <div class="form-grid">
        <label class="field"><span>Exercício</span><select required><option value="">Selecione</option><option>2026</option><option>2027</option></select></label>
        <label class="field"><span>Unidade</span><select required><option value="">Selecione</option><option>Administração Central</option><option>Unidades Escolares</option></select></label>
        <label class="field field-full"><span>Descrição</span><input required placeholder="Descreva o registro de teste"></label>
        <label class="field"><span>Valor</span><input type="number" min="0" step="0.01" placeholder="0,00"></label>
        <label class="field"><span>Situação</span><select><option>Rascunho</option><option>Em análise</option><option>Aprovado</option></select></label>
        <label class="field field-full"><span>Observações</span><textarea placeholder="Observações do protótipo"></textarea></label>
      </div>
      <div class="actions">
        <button class="btn btn-primary" type="submit">Salvar localmente</button>
        <button class="btn" type="reset">Limpar</button>
      </div>
    </form>`;
  document.querySelector("#prototype-form").addEventListener("submit", event => {
    event.preventDefault();
    showToast("Registro de teste salvo apenas neste navegador.");
  });
}

function renderPage(item) {
  exitPtaFocusMode();
  appView.classList.remove("pta-page-view");
  if (item.page === "inicio") return renderHome();
  if (item.page === "dashboard") return renderDashboard();
  if (item.page === "pta2027") {
    renderPtaIntegrated();
    return;
  }
  renderPrototype();
}

function bindDemoActions() {
  document.querySelectorAll(".demo-action").forEach(button => {
    button.addEventListener("click", () => showToast("Ação simulada com sucesso."));
  });
}

function showToast(message) {
  const toast = document.createElement("div");
  toast.className = "toast toast-success";
  toast.textContent = message;
  document.querySelector("#toast-container").append(toast);
  setTimeout(() => toast.remove(), 3200);
}

function applyTheme(theme) {
  state.theme = theme;
  document.body.classList.toggle("theme-dark", theme === "dark");
  document.querySelector("#light-theme").classList.toggle("active", theme === "light");
  document.querySelector("#dark-theme").classList.toggle("active", theme === "dark");
  localStorage.setItem("spo-theme", theme);
}

function login() {
  loginView.classList.add("hidden");
  appView.classList.remove("hidden");
  localStorage.setItem("spo-session", "active");
  renderHome();
}

function logout() {
  localStorage.removeItem("spo-session");
  appView.classList.add("hidden");
  loginView.classList.remove("hidden");
}

document.querySelector("#login-form").addEventListener("submit", event => {
  event.preventDefault();
  const email = document.querySelector("#login-email").value.trim();
  const password = document.querySelector("#login-password").value;
  if (email === "teste@offline.local" && password === "teste123") {
    document.querySelector("#login-error").textContent = "";
    login();
  } else {
    document.querySelector("#login-error").textContent = "Use as credenciais locais exibidas abaixo do formulário.";
  }
});

document.querySelector("#sidebar-toggle").addEventListener("click", () => {
  document.querySelector("#sidebar").classList.toggle("collapsed");
});
document.querySelector("#light-theme").addEventListener("click", () => applyTheme("light"));
document.querySelector("#dark-theme").addEventListener("click", () => applyTheme("dark"));

setInterval(() => {
  document.querySelector("#clock").textContent =
    `Jean Carlos Alves Figueiredo — ${new Intl.DateTimeFormat("pt-BR", { dateStyle: "short", timeStyle: "short" }).format(new Date())}`;
  updatePtaIntegratedClock();
}, 1000);

buildMenu(menuTree, menu);
applyTheme(state.theme);
if (localStorage.getItem("spo-session") === "active") login();
