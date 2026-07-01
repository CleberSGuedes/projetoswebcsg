(function () {
  const content = document.getElementById("content-area");
  const sidebar = document.getElementById("sidebar");
  const toggle = document.getElementById("sidebar-toggle");
  const topbar = document.querySelector(".topbar");
  const logoutBtn = document.getElementById("logout-btn");
  const menu = document.getElementById("menu");
  const userMeta = document.getElementById("user-meta");
  const userPerfilId = userMeta ? userMeta.dataset.perfilId : "";
  const userNivel = userMeta ? userMeta.dataset.nivel : "";
  const themeLightBtn = document.getElementById("theme-light");
  const themeAutoBtn = document.getElementById("theme-auto");
  const themeDarkBtn = document.getElementById("theme-dark");
  const systemThemeQuery = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;
  const ACCENT_COLORS = {
    blue: {
      label: "Azul",
      vars: {
        "--color-50": "#f0f7ff",
        "--color-100": "#d9ecff",
        "--color-200": "#99c1f1",
        "--color-300": "#62a0ea",
        "--color-400": "#3584e4",
        "--color-500": "#1c71d8",
        "--color-600": "#1a5fb4",
        "--color-700": "#1a4f91",
        "--color-800": "#173f73",
        "--color-900": "#12345c",
        "--accent-rgb": "53, 132, 228",
        "--accent-soft-rgb": "153, 193, 241",
      },
      dark: { "--accent": "#78aeed", "--accent-strong": "#99c1f1", "--lab-accent": "#78aeed", "--accent-rgb": "120, 174, 237" },
    },
    teal: {
      label: "Turquesa",
      vars: {
        "--color-50": "#edfafa",
        "--color-100": "#d2f4f4",
        "--color-200": "#93dddf",
        "--color-300": "#47c2c8",
        "--color-400": "#2190a4",
        "--color-500": "#15828e",
        "--color-600": "#0e6f7d",
        "--color-700": "#0b5964",
        "--color-800": "#08444d",
        "--color-900": "#06343c",
        "--accent-rgb": "33, 144, 164",
        "--accent-soft-rgb": "147, 221, 223",
      },
      dark: { "--accent": "#6ed4de", "--accent-strong": "#93dddf", "--lab-accent": "#6ed4de", "--accent-rgb": "110, 212, 222" },
    },
    green: {
      label: "Verde padrão SPO",
      vars: {
        "--color-50": "#e9f8f4",
        "--color-100": "#ccefe6",
        "--color-200": "#96ddcb",
        "--color-300": "#50c1a7",
        "--color-400": "#25a98e",
        "--color-500": "#009879",
        "--color-600": "#007f68",
        "--color-700": "#006756",
        "--color-800": "#075246",
        "--color-900": "#063f38",
        "--accent-rgb": "0, 152, 121",
        "--accent-soft-rgb": "150, 221, 203",
      },
      dark: { "--accent": "#42dfc0", "--accent-strong": "#6ee7cf", "--lab-accent": "#42dfc0", "--accent-rgb": "66, 223, 192" },
    },
    yellow: {
      label: "Amarelo",
      vars: {
        "--color-50": "#fff9e6",
        "--color-100": "#fff0bf",
        "--color-200": "#f9f06b",
        "--color-300": "#f8e45c",
        "--color-400": "#f6d32d",
        "--color-500": "#e5a50a",
        "--color-600": "#c88800",
        "--color-700": "#9c6a00",
        "--color-800": "#704d00",
        "--color-900": "#523800",
        "--accent-rgb": "229, 165, 10",
        "--accent-soft-rgb": "249, 240, 107",
      },
      dark: { "--accent": "#f8e45c", "--accent-strong": "#f9f06b", "--lab-accent": "#f8e45c", "--accent-rgb": "248, 228, 92" },
    },
    orange: {
      label: "Laranja",
      vars: {
        "--color-50": "#fff3e8",
        "--color-100": "#ffe1c8",
        "--color-200": "#ffbe6f",
        "--color-300": "#ffa348",
        "--color-400": "#ff7800",
        "--color-500": "#e66100",
        "--color-600": "#c64600",
        "--color-700": "#a33400",
        "--color-800": "#772600",
        "--color-900": "#571c00",
        "--accent-rgb": "230, 97, 0",
        "--accent-soft-rgb": "255, 190, 111",
      },
      dark: { "--accent": "#ffa348", "--accent-strong": "#ffbe6f", "--lab-accent": "#ffa348", "--accent-rgb": "255, 163, 72" },
    },
    red: {
      label: "Vermelho",
      vars: {
        "--color-50": "#fff0f0",
        "--color-100": "#ffd7d7",
        "--color-200": "#ff7b7b",
        "--color-300": "#f66151",
        "--color-400": "#ed333b",
        "--color-500": "#e01b24",
        "--color-600": "#c01c28",
        "--color-700": "#a51d2d",
        "--color-800": "#7d1824",
        "--color-900": "#5d121b",
        "--accent-rgb": "224, 27, 36",
        "--accent-soft-rgb": "255, 123, 123",
      },
      dark: { "--accent": "#ff7b7b", "--accent-strong": "#f66151", "--lab-accent": "#ff7b7b", "--accent-rgb": "255, 123, 123" },
    },
    purple: {
      label: "Roxo",
      vars: {
        "--color-50": "#f7f0ff",
        "--color-100": "#eadcff",
        "--color-200": "#dc8add",
        "--color-300": "#c061cb",
        "--color-400": "#9141ac",
        "--color-500": "#813d9c",
        "--color-600": "#613583",
        "--color-700": "#4e2a68",
        "--color-800": "#3d2052",
        "--color-900": "#2e183e",
        "--accent-rgb": "129, 61, 156",
        "--accent-soft-rgb": "220, 138, 221",
      },
      dark: { "--accent": "#c061cb", "--accent-strong": "#dc8add", "--lab-accent": "#c061cb", "--accent-rgb": "192, 97, 203" },
    },
  };
  let multiFilterClickBound = false;
  const appLoadingOverlay = document.getElementById("app-loading-overlay");
  const appLoadingTitle = document.getElementById("app-loading-title");
  const appLoadingSubtitle = document.getElementById("app-loading-subtitle");
  let appLoadingCount = 0;
  let tetoDashboardResizeTimer = null;

  function showAppLoading(title = "Carregando...", subtitle = "Aguarde enquanto a aplicação processa a solicitação.") {
    appLoadingCount += 1;
    if (!appLoadingOverlay) return;
    if (appLoadingTitle) appLoadingTitle.textContent = title || "Carregando...";
    if (appLoadingSubtitle) {
      appLoadingSubtitle.textContent = subtitle || "Aguarde enquanto a aplicação processa a solicitação.";
    }
    appLoadingOverlay.hidden = false;
    document.body.classList.add("app-loading-active");
  }

  function hideAppLoading(force = false) {
    if (force) {
      appLoadingCount = 0;
    } else {
      appLoadingCount = Math.max(0, appLoadingCount - 1);
    }
    if (appLoadingCount > 0 || !appLoadingOverlay) return;
    appLoadingOverlay.hidden = true;
    document.body.classList.remove("app-loading-active");
  }

  window.showAppLoading = showAppLoading;
  window.hideAppLoading = hideAppLoading;

  function getResolvedTheme(theme) {
    if (theme === "auto") {
      return systemThemeQuery && systemThemeQuery.matches ? "dark" : "light";
    }
    return theme === "dark" ? "dark" : "light";
  }

  function getStoredAccent() {
    const saved = localStorage.getItem("app-accent") || "blue";
    if (saved === "spo") return "green";
    return ACCENT_COLORS[saved] ? saved : "blue";
  }

  function applyAccentColor(accent, persist = true) {
    const key = ACCENT_COLORS[accent] ? accent : "blue";
    const palette = ACCENT_COLORS[key];
    const root = document.documentElement;
    const targets = [root, document.body].filter(Boolean);
    const setVar = (name, value) => targets.forEach((target) => target.style.setProperty(name, value));
    Object.entries(palette.vars).forEach(([name, value]) => setVar(name, value));
    setVar("--accent", "var(--color-500)");
    setVar("--accent-strong", "var(--color-400)");
    setVar("--lab-accent", "var(--color-400)");
    if (document.body.classList.contains("theme-dark")) {
      Object.entries(palette.dark).forEach(([name, value]) => setVar(name, value));
    }
    document.body.dataset.accentPreference = key;
    document.querySelectorAll("[data-accent-choice]").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.accentChoice === key);
      btn.setAttribute("aria-pressed", String(btn.dataset.accentChoice === key));
    });
    if (persist) localStorage.setItem("app-accent", key);
  }

  function applyTheme(theme, persist = true) {
    const body = document.body;
    const selectedTheme = theme === "auto" ? "auto" : getResolvedTheme(theme);
    const resolvedTheme = getResolvedTheme(selectedTheme);
    const isDark = resolvedTheme === "dark";
    body.classList.toggle("theme-dark", isDark);
    body.dataset.themePreference = selectedTheme;
    if (themeLightBtn && themeDarkBtn) {
      themeLightBtn.classList.toggle("active", selectedTheme === "light");
      if (themeAutoBtn) themeAutoBtn.classList.toggle("active", selectedTheme === "auto");
      themeDarkBtn.classList.toggle("active", selectedTheme === "dark");
    }
    applyAccentColor(getStoredAccent(), false);
    if (persist) localStorage.setItem("app-theme", selectedTheme);
  }

  function initTheme() {
    applyAccentColor(getStoredAccent(), false);
    const saved = localStorage.getItem("app-theme") || "auto";
    applyTheme(saved);
    if (themeLightBtn) {
      themeLightBtn.addEventListener("click", () => applyTheme("light"));
    }
    if (themeAutoBtn) {
      themeAutoBtn.addEventListener("click", () => applyTheme("auto"));
    }
    if (themeDarkBtn) {
      themeDarkBtn.addEventListener("click", () => applyTheme("dark"));
    }
    if (systemThemeQuery) {
      const refreshAutoTheme = () => {
        if ((localStorage.getItem("app-theme") || "auto") === "auto") {
          applyTheme("auto", false);
        }
      };
      if (typeof systemThemeQuery.addEventListener === "function") {
        systemThemeQuery.addEventListener("change", refreshAutoTheme);
      } else if (typeof systemThemeQuery.addListener === "function") {
        systemThemeQuery.addListener(refreshAutoTheme);
      }
    }
  }

  function syncThemeControls(scope = document) {
    const currentTheme = localStorage.getItem("app-theme") || "auto";
    const currentAccent = getStoredAccent();
    scope.querySelectorAll("[data-theme-choice]").forEach((btn) => {
      const active = btn.dataset.themeChoice === currentTheme;
      btn.classList.toggle("active", active);
      btn.setAttribute("aria-pressed", String(active));
    });
    scope.querySelectorAll("[data-accent-choice]").forEach((btn) => {
      const active = btn.dataset.accentChoice === currentAccent;
      btn.classList.toggle("active", active);
      btn.setAttribute("aria-pressed", String(active));
    });
  }

  function initPersonalizarSpo() {
    const panel = document.getElementById("personalizar-spo-panel");
    if (!panel || panel.dataset.bound === "1") return;
    panel.dataset.bound = "1";
    syncThemeControls(panel);
    panel.querySelectorAll("[data-theme-choice]").forEach((btn) => {
      btn.addEventListener("click", () => {
        applyTheme(btn.dataset.themeChoice || "auto");
        syncThemeControls(panel);
      });
    });
    panel.querySelectorAll("[data-accent-choice]").forEach((btn) => {
      btn.addEventListener("click", () => {
        applyAccentColor(btn.dataset.accentChoice || "blue");
        syncThemeControls(panel);
      });
    });
    const reset = document.getElementById("spo-theme-reset");
    if (reset) {
      reset.addEventListener("click", () => {
        applyAccentColor("blue");
        applyTheme("auto");
        syncThemeControls(panel);
        showToast("Personalização restaurada para o padrão do ambiente.");
      });
    }
  }

  function bindToggleVisibility(scope) {
    scope.querySelectorAll(".toggle-visibility").forEach((btn) => {
      const targetId = btn.getAttribute("data-target");
      const target = targetId ? document.getElementById(targetId) : null;
      if (!target) return;
      btn.addEventListener("click", () => {
        const isPwd = target.type === "password";
        target.type = isPwd ? "text" : "password";
        btn.innerHTML = `<i class="bi ${isPwd ? "bi-eye-slash" : "bi-eye"}"></i>`;
      });
    });
  }

  const SCROLLBAR_EDGE_PX = 28;
  const SCROLLBAR_TARGET_SELECTOR = [
    ".sidebar",
    "#content-area",
    ".modal-body",
    ".table-responsive",
    ".planning-structure-layout",
    ".planning-key-model-layout",
    ".teto-table-wrap",
    ".teto-table-scroll",
    ".import-preview",
  ].join(",");

  function isScrollableTarget(el) {
    if (!el) return false;
    if (el === document.documentElement) {
      return document.documentElement.scrollHeight > window.innerHeight + 1;
    }
    return el.scrollHeight > el.clientHeight + 1 || el.scrollWidth > el.clientWidth + 1;
  }

  function setScrollbarVisible(el, visible) {
    if (!el) return;
    el.classList.toggle("scrollbar-visible", !!visible);
  }

  function bindAccentScrollbar(el) {
    if (!el || el.dataset?.scrollbarBound === "1") return;
    if (el.dataset) el.dataset.scrollbarBound = "1";
    let hideTimer = null;

    const showBriefly = () => {
      if (!isScrollableTarget(el)) return;
      setScrollbarVisible(el, true);
      window.clearTimeout(hideTimer);
      hideTimer = window.setTimeout(() => setScrollbarVisible(el, false), 900);
    };

    const updateFromPointer = (ev) => {
      if (!isScrollableTarget(el)) return;
      const rect = el === document.documentElement
        ? { left: 0, top: 0, right: window.innerWidth, bottom: window.innerHeight }
        : el.getBoundingClientRect();
      const nearRight = ev.clientX >= rect.right - SCROLLBAR_EDGE_PX && ev.clientX <= rect.right + 2;
      const nearBottom = ev.clientY >= rect.bottom - SCROLLBAR_EDGE_PX && ev.clientY <= rect.bottom + 2;
      setScrollbarVisible(el, nearRight || nearBottom);
    };

    const hide = () => {
      window.clearTimeout(hideTimer);
      setScrollbarVisible(el, false);
    };

    const scrollTarget = el === document.documentElement ? window : el;
    scrollTarget.addEventListener("scroll", showBriefly, { passive: true });
    el.addEventListener("mousemove", updateFromPointer, { passive: true });
    el.addEventListener("mouseleave", hide, { passive: true });
  }

  function refreshAccentScrollbars(scope = document) {
    bindAccentScrollbar(document.documentElement);
    if (sidebar) bindAccentScrollbar(sidebar);
    if (content) bindAccentScrollbar(content);
    scope.querySelectorAll?.(SCROLLBAR_TARGET_SELECTOR).forEach(bindAccentScrollbar);
  }

  let pta2027IntegrationCleanup = null;

  function initPta2027Integration() {
    const frame = document.getElementById("pta2027-frame");
    const wrap = document.querySelector(".pta2027-frame-wrap");
    if (!frame) return;
    if (typeof pta2027IntegrationCleanup === "function") {
      pta2027IntegrationCleanup();
      pta2027IntegrationCleanup = null;
    }

    const readThemePayload = () => {
      const rootStyles = getComputedStyle(document.documentElement);
      const hostUser = readHostUserMeta();
      return {
        type: "spo-theme",
        theme: document.body.classList.contains("theme-dark") ? "dark" : "light",
        accent: rootStyles.getPropertyValue("--accent").trim(),
        accentStrong: rootStyles.getPropertyValue("--accent-strong").trim(),
        accentRgb: rootStyles.getPropertyValue("--accent-rgb").trim(),
        surface: rootStyles.getPropertyValue("--surface").trim(),
        card: rootStyles.getPropertyValue("--card-bg").trim(),
        bg: rootStyles.getPropertyValue("--bg").trim(),
        text: rootStyles.getPropertyValue("--text").trim(),
        muted: rootStyles.getPropertyValue("--muted").trim(),
        border: rootStyles.getPropertyValue("--border").trim(),
        layoutSidebarCollapsed: rootStyles.getPropertyValue("--spo-layout-sidebar-collapsed-width").trim(),
        layoutPageGutter: rootStyles.getPropertyValue("--spo-layout-page-gutter").trim(),
        layoutPanelGap: rootStyles.getPropertyValue("--spo-layout-panel-gap").trim(),
        userMeta: hostUser.text,
        userName: hostUser.name,
        userInitials: hostUser.initials,
      };
    };

    const syncFrameHeight = (height) => {
      const safeHeight = Math.max(720, Math.ceil(Number(height) || 0));
      frame.style.height = `${safeHeight}px`;
      if (wrap) wrap.style.minHeight = `${safeHeight}px`;
    };

    const postTheme = () => {
      try {
        frame.contentWindow?.postMessage(readThemePayload(), window.location.origin);
      } catch (err) {
        console.debug("Falha ao sincronizar tema do PTA 2027", err);
      }
    };

    const resizeFromDocument = () => {
      try {
        const doc = frame.contentDocument?.documentElement;
        const body = frame.contentDocument?.body;
        if (!doc || !body) return;
        syncFrameHeight(Math.max(doc.scrollHeight, body.scrollHeight, doc.offsetHeight, body.offsetHeight));
      } catch (_) {
        // A leitura direta é apenas um reforço para iframe de mesma origem.
      }
    };

    const setImmersiveMode = (enabled) => {
      document.body.classList.toggle("pta2027-immersive", Boolean(enabled));
      if (sidebar) {
        if (enabled) {
          sidebar.classList.remove("open");
          sidebar.classList.add("collapsed");
        } else {
          sidebar.classList.remove("open");
          if (!isSidebarNarrow()) sidebar.classList.remove("collapsed");
        }
      }
      updateToggleIcon();
      syncTopbarHeight();
      window.setTimeout(() => {
        postTheme();
        resizeFromDocument();
      }, 80);
    };

    const onMessage = (event) => {
      if (event.origin !== window.location.origin || event.source !== frame.contentWindow) return;
      if (event.data?.type === "pta2027-height") syncFrameHeight(event.data.height);
      if (event.data?.type === "pta2027-immersive") setImmersiveMode(Boolean(event.data.enabled));
      if (event.data?.type === "pta2027-context-focus") {
        document.body.classList.toggle("special-context-focus", Boolean(event.data.enabled));
        document.body.classList.toggle("pta2027-context-focus", Boolean(event.data.enabled));
        syncPtaContextMeta();
        syncTopbarHeight();
        window.setTimeout(() => {
          syncTopbarHeight();
          postTheme();
          resizeFromDocument();
        }, 80);
      }
      if (event.data?.type === "pta2027-scroll-top") {
        document.querySelector(".pta2027-integration")?.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    };

    const observer = new MutationObserver(() => {
      postTheme();
      window.setTimeout(resizeFromDocument, 80);
    });

    window.addEventListener("message", onMessage);
    frame.addEventListener("load", () => {
      syncTopbarHeight();
      postTheme();
      resizeFromDocument();
      window.setTimeout(resizeFromDocument, 180);
      window.setTimeout(resizeFromDocument, 700);
    });
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class", "style"] });
    observer.observe(document.body, { attributes: true, attributeFilter: ["class", "style"] });

    postTheme();
    window.setTimeout(resizeFromDocument, 120);

    pta2027IntegrationCleanup = () => {
      window.removeEventListener("message", onMessage);
      observer.disconnect();
      document.body.classList.remove("special-context-focus", "pta2027-context-focus");
      syncTopbarHeight();
      setImmersiveMode(false);
    };
  }

  async function loadPage(route) {
    let url = "/partial/" + route;
    if (route === "logout") {
      await logout();
      return;
    }
    if (route !== "atualizar/governanca-resultados/programacao-pta2027" && typeof pta2027IntegrationCleanup === "function") {
      pta2027IntegrationCleanup();
      pta2027IntegrationCleanup = null;
    }
    showAppLoading("Carregando página...", "Aguarde enquanto a página é carregada.");
    try {
      const res = await fetch(url, { headers: { "X-Requested-With": "fetch" } });
      if (res.status === 401) {
        window.location.href = "/login";
        return;
      }
      if (res.status === 403) {
        content.innerHTML = '<div class="card"><div class="card-title">Acesso negado</div><p>Requer perfil admin.</p></div>';
        return;
      }
      const html = await res.text();
      content.innerHTML = html;
      syncPtaContextMeta();
      syncTopbarHeight();
      initRoute(route);
      refreshAccentScrollbars(content);
    } catch (err) {
      content.innerHTML = '<div class="card"><div class="card-title">Erro</div><p>Falha ao carregar.</p></div>';
      console.error(err);
    } finally {
      hideAppLoading();
    }
  }

  function setActive(route) {
    document.querySelectorAll(".menu-item").forEach((el) => {
      const r = el.getAttribute("data-route");
      if (r === route) {
        el.classList.add("active");
      } else {
        el.classList.remove("active");
      }
    });

    // expand only the ancestors of the active route (avoid opening nested by default)
    document.querySelectorAll(".menu-group").forEach((group) => {
      group.classList.remove("open");
      group.classList.remove("active-ancestor");
    });
    const activeLink = document.querySelector(`.menu-item[data-route="${route}"]`);
    if (activeLink) {
      let parentGroup = activeLink.closest(".menu-group");
      while (parentGroup) {
        parentGroup.classList.add("open");
        parentGroup.classList.add("active-ancestor");
        parentGroup = parentGroup.parentElement?.closest(".menu-group");
      }
    }
  }

  function clearMenuRouteActiveState() {
    document.querySelectorAll(".menu-item.active").forEach((el) => el.classList.remove("active"));
    document.querySelectorAll(".menu-group.active-ancestor").forEach((group) => group.classList.remove("active-ancestor"));
  }

  async function logout() {
    try {
      await fetch("/logout", { method: "POST" });
    } finally {
      window.location.href = "/login";
    }
  }

  const sidebarNarrowQuery = window.matchMedia("(max-width: 1200px)");

  function isSidebarNarrow() {
    return sidebarNarrowQuery.matches;
  }

  function isPta2027Immersive() {
    return document.body.classList.contains("pta2027-immersive");
  }

  function shouldUseSidebarOverlay() {
    return isSidebarNarrow() || isPta2027Immersive();
  }

  function updateToggleIcon() {
    if (!toggle || !sidebar) return;
    const icon = toggle.querySelector("i");
    if (!icon) return;
    const expanded = shouldUseSidebarOverlay()
      ? sidebar.classList.contains("open")
      : !sidebar.classList.contains("collapsed");
    icon.classList.toggle("bi-chevron-right", !expanded);
    icon.classList.toggle("bi-chevron-left", expanded);
    toggle.setAttribute("aria-label", expanded ? "Recolher menu" : "Expandir menu");
    toggle.setAttribute("aria-expanded", String(expanded));
  }

  function syncSidebarForViewport() {
    if (!sidebar) return;
    if (shouldUseSidebarOverlay()) {
      sidebar.classList.toggle("collapsed", !sidebar.classList.contains("open"));
    } else {
      sidebar.classList.remove("open");
    }
    updateToggleIcon();
  }

  function closeNarrowSidebar() {
    if (!sidebar || !shouldUseSidebarOverlay()) return;
    sidebar.classList.remove("open");
    sidebar.classList.add("collapsed");
    updateToggleIcon();
  }

  function resizeTetoDashboardCharts() {
    const dashboard = document.getElementById("teto-dashboard");
    if (!dashboard || typeof Plotly === "undefined") return;
    const resize = () => {
      dashboard.querySelectorAll(".teto-chart").forEach((chart) => {
        if (!chart.hidden && chart.offsetParent !== null) Plotly.Plots.resize(chart);
      });
    };
    requestAnimationFrame(resize);
    window.clearTimeout(tetoDashboardResizeTimer);
    tetoDashboardResizeTimer = window.setTimeout(resize, 340);
  }

  function updateSidebarExpandedWidth() {
    if (!sidebar || !menu) return;
    const measure = sidebar.cloneNode(true);
    measure.classList.remove("collapsed");
    measure.classList.add("open", "sidebar-width-measure");
    measure.querySelectorAll("[id]").forEach((el) => el.removeAttribute("id"));
    measure.querySelectorAll(".menu-group").forEach((group) => group.classList.add("open"));
    document.body.appendChild(measure);
    const measuredWidth = Math.ceil(measure.getBoundingClientRect().width) + 4;
    measure.remove();
    sidebar.style.setProperty("--sidebar-expanded-width", `${Math.max(160, measuredWidth)}px`);
  }

  function syncTopbarHeight() {
    const contextHeader = document.querySelector(".pta2027-integration-header");
    const useContextHeader = document.body.classList.contains("special-context-focus") && contextHeader;
    const height = useContextHeader
      ? Math.ceil(contextHeader.getBoundingClientRect().height)
      : topbar && getComputedStyle(topbar).display !== "none"
        ? Math.ceil(topbar.getBoundingClientRect().height)
        : 0;
    document.documentElement.style.setProperty("--spo-topbar-height", `${height}px`);
    document.documentElement.style.setProperty("--spo-contextbar-height", `${height}px`);
  }

  function syncSplitPressureMode() {
    const viewportWidth = Math.max(
      document.documentElement.clientWidth || 0,
      window.innerWidth || 0
    );
    const outerWidth = window.outerWidth || viewportWidth;
    const isSplitPressure = viewportWidth <= 480 && outerWidth - viewportWidth >= 180;
    document.documentElement.classList.toggle("spo-split-pressure", isSplitPressure);
    document.body.classList.toggle("spo-split-pressure", isSplitPressure);
  }

  function readHostUserMeta() {
    if (!userMeta) return { text: "", name: "", initials: "" };
    const name = userMeta.dataset.name || "";
    const text = userMeta.textContent.trim() || name;
    const initials = name
      .split(/\s+/)
      .filter(Boolean)
      .slice(0, 2)
      .map((part) => part[0])
      .join("")
      .toUpperCase();
    return { text, name, initials };
  }

  function syncPtaContextMeta() {
    const meta = document.getElementById("pta2027-context-meta");
    if (!meta) return;
    meta.textContent = readHostUserMeta().text;
  }

  function setUserMeta() {
    if (!userMeta) return;
    const name = userMeta.dataset.name || "";
    const activeCount = userMeta.dataset.activeCount || "";
    const initialFeats = userMeta.dataset.features
      ? JSON.parse(userMeta.dataset.features || "[]")
      : [];
    if (initialFeats.length) {
      applyMenuPermissions(initialFeats);
    }
    const formatted = new Date().toLocaleString("pt-BR", {
      dateStyle: "short",
      timeStyle: "short",
    });
    const countLabel = activeCount ? ` | Logados: ${activeCount}` : "";
    userMeta.textContent = `${name} - ${formatted}${countLabel}`;
    syncPtaContextMeta();
  }

  if (sidebar && isSidebarNarrow()) {
    sidebar.classList.add("collapsed");
  }

  if (toggle) {
    toggle.addEventListener("click", () => {
      if (shouldUseSidebarOverlay()) {
        const open = !sidebar.classList.contains("open");
        sidebar.classList.toggle("open", open);
        sidebar.classList.toggle("collapsed", !open);
      } else {
        sidebar.classList.toggle("collapsed");
        sidebar.classList.remove("open");
      }
      updateToggleIcon();
      resizeTetoDashboardCharts();
    });
    updateToggleIcon();
  }
  if (sidebarNarrowQuery.addEventListener) {
    sidebarNarrowQuery.addEventListener("change", syncSidebarForViewport);
  } else if (sidebarNarrowQuery.addListener) {
    sidebarNarrowQuery.addListener(syncSidebarForViewport);
  }
  const syncViewportPressure = () => {
    syncSplitPressureMode();
    syncTopbarHeight();
  };

  syncSplitPressureMode();
  window.addEventListener("resize", syncViewportPressure, { passive: true });
  if (window.visualViewport) {
    window.visualViewport.addEventListener("resize", syncViewportPressure, { passive: true });
  }
  if (topbar && "ResizeObserver" in window) {
    new ResizeObserver(syncTopbarHeight).observe(topbar);
  }

  document.addEventListener("click", (ev) => {
    if (!sidebar || !shouldUseSidebarOverlay() || !sidebar.classList.contains("open")) return;
    if (sidebar.contains(ev.target) || toggle?.contains(ev.target)) return;
    closeNarrowSidebar();
  });

  if (logoutBtn) {
    logoutBtn.addEventListener("click", () => logout());
  }

  initTheme();
  refreshAccentScrollbars();
  function initUsuariosForm() {
    const form = document.getElementById("form-criar-usuario");
    const msg = document.getElementById("criar-usuario-msg");
    if (!form || !msg) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    bindToggleVisibility(form);

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      if (!form.checkValidity()) {
        form.reportValidity();
        return;
      }
      msg.textContent = "Salvando...";
      msg.classList.remove("text-error");
      const data = Object.fromEntries(new FormData(form));
      data.ativo = !!data.ativo;
      try {
        const res = await fetch("/api/usuarios", {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(data),
        });
        const payload = await res.json();
        if (!res.ok) {
          msg.textContent = payload.error || "Erro ao salvar.";
          msg.classList.add("text-error");
          return;
        }
        msg.textContent = "Usuário criado.";
        form.reset();
        await loadPage("usuarios");
      } catch (err) {
        console.error(err);
        msg.textContent = "Falha na requisição.";
        msg.classList.add("text-error");
      }
    });
  }

  function initUsuariosEditar() {
    const form = document.getElementById("form-editar-usuario");
    const msg = document.getElementById("editar-usuario-msg");
    const fillFromRow = (row) => {
      const id = row.dataset.id || "";
      const email = row.dataset.email || "";
      document.getElementById("edit-id").value = id;
      document.getElementById("edit-email").value = email;
      document.getElementById("edit-nome").value = row.dataset.nome || "";
      document.getElementById("edit-perfil").value = row.dataset.perfilId || "";
      document.getElementById("edit-senha").value = "";
      document.getElementById("edit-ativo").checked = row.dataset.ativo === "1";
    };
    if (!form || !msg) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    bindToggleVisibility(form);

    document.querySelectorAll(".select-usuario").forEach((btn) => {
      btn.addEventListener("click", () => {
        const row = btn.closest("tr[data-id]");
        if (row) fillFromRow(row);
      });
    });

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      msg.textContent = "Salvando...";
      msg.classList.remove("text-error");
      const id = document.getElementById("edit-id").value;
      const email = document.getElementById("edit-email").value;
      if (!id) {
        msg.textContent = "Selecione um usuário na lista.";
        msg.classList.add("text-error");
        return;
      }
      const payload = {
        email: email,
        nome: document.getElementById("edit-nome").value,
        perfil_id: document.getElementById("edit-perfil").value,
        senha: document.getElementById("edit-senha").value,
        ativo: document.getElementById("edit-ativo").checked,
      };
      const perfilSelect = document.getElementById("edit-perfil");
      const perfilText = perfilSelect?.selectedOptions?.[0]?.textContent?.trim() || "";
      try {
        const res = await fetch(`/api/usuarios/id/${encodeURIComponent(id)}`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payload),
        });
        const raw = await res.text();
        let data = {};
        try {
          data = JSON.parse(raw || "{}");
        } catch {
          // se não for JSON, usa texto bruto na mensagem de erro
        }
        if (!res.ok) throw new Error(data.error || raw || `Falha ao salvar. Status ${res.status}`);
        msg.textContent = data.message || "Usuário atualizado.";
        document.getElementById("edit-senha").value = "";
        const row = document.querySelector(`tr[data-id="${id}"]`);
        if (row) {
          row.dataset.email = email || row.dataset.email || "";
          row.dataset.nome = payload.nome || row.dataset.nome || "";
          row.dataset.perfil = perfilText || row.dataset.perfil || "";
          row.dataset.perfilId = payload.perfil_id || row.dataset.perfilId || "";
          row.dataset.ativo = payload.ativo ? "1" : "0";
          const cells = row.querySelectorAll("td");
          if (cells.length >= 4) {
            cells[0].textContent = email || cells[0].textContent;
            cells[1].textContent = payload.nome || cells[1].textContent;
            cells[2].textContent = perfilText || cells[2].textContent;
            cells[3].textContent = payload.ativo ? "Sim" : "Não";
          }
        }
      } catch (err) {
        console.error(err);
        msg.textContent = err.message;
        msg.classList.add("text-error");
      }
    });
  }

  function initPerfis() {
    const form = document.getElementById("form-perfil");
    const msg = document.getElementById("perfil-msg");
    if (!form || !msg) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const fillForm = (row) => {
      document.getElementById("perfil-id").value = row?.dataset.id || "";
      document.getElementById("perfil-nome").value = row?.dataset.nome || "";
      document.getElementById("perfil-nivel").value = row?.dataset.nivel || "";
      document.getElementById("perfil-ativo").checked = (row?.dataset.ativo || "1") === "1";
    };

    document.querySelectorAll(".select-perfil").forEach((btn) => {
      btn.addEventListener("click", () => {
        const row = btn.closest("tr[data-id]");
        if (row) fillForm(row);
      });
    });
    document.querySelectorAll(".delete-perfil").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const id = btn.dataset.id;
        if (!id) return;
        msg.textContent = "Excluindo...";
        msg.classList.remove("text-error");
        try {
          const res = await fetch(`/api/perfis/${id}`, {
            method: "DELETE",
            headers: { "X-Requested-With": "fetch" },
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao excluir.");
          msg.textContent = data.message || "Perfil excluido.";
          loadPage("usuarios/perfil");
        } catch (err) {
          console.error(err);
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
      });
    });

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      msg.textContent = "Salvando...";
      msg.classList.remove("text-error");
      const id = document.getElementById("perfil-id").value;
      const payload = {
        nome: document.getElementById("perfil-nome").value,
        nivel: document.getElementById("perfil-nivel").value,
        ativo: document.getElementById("perfil-ativo").checked,
      };
      const url = id ? `/api/perfis/${id}` : "/api/perfis";
      const method = id ? "PUT" : "POST";
      try {
        const res = await fetch(url, {
          method,
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao salvar.");
        msg.textContent = data.message || "Perfil salvo.";
        loadPage("usuarios/perfil");
      } catch (err) {
        console.error(err);
        msg.textContent = err.message;
        msg.classList.add("text-error");
      }
    });
  }

  function initApiAcessos() {
    const form = document.getElementById("form-api-client");
    const msg = document.getElementById("api-client-msg");
    const tbody = document.getElementById("api-client-tbody");
    const btnLimpar = document.getElementById("api-client-limpar");
    const boxCred = document.getElementById("api-client-secret-box");
    const credId = document.getElementById("api-client-secret-id");
    const credSecret = document.getElementById("api-client-secret-value");
    if (!form || !msg || !tbody) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const esc = (v) =>
      String(v ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");

    const setMsg = (text, isErr = false) => {
      msg.textContent = text || "";
      msg.classList.toggle("text-error", !!isErr);
    };

    const getScopesFromForm = () =>
      Array.from(form.querySelectorAll(".api-scope:checked"))
        .map((el) => String(el.value || "").trim())
        .filter(Boolean);

    const setScopesToForm = (scopes) => {
      const set = new Set(Array.isArray(scopes) ? scopes : []);
      form.querySelectorAll(".api-scope").forEach((el) => {
        el.checked = set.has(String(el.value || "").trim());
      });
    };

    const toDateTimeLocal = (value) => {
      if (!value) return "";
      const dt = new Date(value);
      if (Number.isNaN(dt.getTime())) return "";
      const pad = (n) => String(n).padStart(2, "0");
      return `${dt.getFullYear()}-${pad(dt.getMonth() + 1)}-${pad(dt.getDate())}T${pad(dt.getHours())}:${pad(dt.getMinutes())}`;
    };

    const resetForm = () => {
      document.getElementById("api-client-id").value = "";
      document.getElementById("api-client-nome").value = "";
      document.getElementById("api-client-status").value = "ativo";
      const now = new Date();
      now.setSeconds(0, 0);
      document.getElementById("api-acesso-inicio").value = toDateTimeLocal(now.toISOString());
      document.getElementById("api-acesso-fim").value = "";
      form.querySelectorAll(".api-scope").forEach((el) => {
        el.checked = true;
      });
      const chkInterno = document.getElementById("api-servidor-cadastrado");
      if (chkInterno) chkInterno.checked = false;
      if (boxCred) boxCred.style.display = "none";
      if (credId) credId.textContent = "";
      if (credSecret) credSecret.textContent = "";
      setMsg("");
    };

    const showCredentials = (clientId, clientSecret) => {
      if (!boxCred || !credId || !credSecret) return;
      credId.textContent = clientId || "";
      credSecret.textContent = clientSecret || "";
      boxCred.style.display = "";
    };

    const formatDateTime = (value) => {
      if (!value) return "";
      const dt = new Date(value);
      if (Number.isNaN(dt.getTime())) return String(value);
      return dt.toLocaleString("pt-BR");
    };

    const renderRows = (rows) => {
      const list = Array.isArray(rows) ? rows : [];
      if (!list.length) {
        tbody.innerHTML = '<tr><td colspan="9" class="muted">Nenhum servidor cadastrado.</td></tr>';
        return;
      }
      tbody.innerHTML = list
        .map(
          (r) => `
          <tr data-id="${esc(r.id)}"
              data-nome="${esc(r.nome)}"
              data-status="${esc(r.status)}"
              data-servidor-cadastrado="${r.servidor_cadastrado ? "1" : "0"}"
              data-acesso-inicio="${esc(r.acesso_inicio_em || "")}"
              data-acesso-fim="${esc(r.acesso_fim_em || "")}"
              data-scopes="${esc((r.scopes || []).join(","))}">
            <td>${esc(r.nome)}</td>
            <td><code>${esc(r.client_id)}</code></td>
            <td>${esc(r.status)}</td>
            <td>${r.servidor_cadastrado ? "Interno (servidor cadastrado)" : "Externo"}</td>
            <td>${esc(formatDateTime(r.acesso_inicio_em))}</td>
            <td>${esc(r.acesso_fim_em ? formatDateTime(r.acesso_fim_em) : "Permanente")}</td>
            <td>${esc((r.scopes_label || []).join(" | "))}</td>
            <td>${esc(formatDateTime(r.last_used_at))}</td>
            <td class="actions" style="display:flex; gap:6px;">
              <button class="icon-btn sm api-select" data-id="${esc(r.id)}" type="button" title="Editar"><i class="bi bi-pencil"></i></button>
              <button class="icon-btn sm api-rotate" data-id="${esc(r.id)}" type="button" title="Rotacionar segredo"><i class="bi bi-arrow-repeat"></i></button>
              <button class="icon-btn sm api-revoke" data-id="${esc(r.id)}" type="button" title="Revogar"><i class="bi bi-slash-circle"></i></button>
              <button class="icon-btn sm api-delete" data-id="${esc(r.id)}" type="button" title="Excluir"><i class="bi bi-trash"></i></button>
            </td>
          </tr>`
        )
        .join("");

      tbody.querySelectorAll(".api-select").forEach((btn) => {
        btn.addEventListener("click", () => {
          const row = btn.closest("tr[data-id]");
          if (!row) return;
          document.getElementById("api-client-id").value = row.dataset.id || "";
          document.getElementById("api-client-nome").value = row.dataset.nome || "";
          document.getElementById("api-client-status").value = row.dataset.status || "ativo";
          document.getElementById("api-acesso-inicio").value = toDateTimeLocal(row.dataset.acessoInicio || "");
          document.getElementById("api-acesso-fim").value = toDateTimeLocal(row.dataset.acessoFim || "");
          const interno = String(row.dataset.servidorCadastrado || "") === "1";
          const chkInterno = document.getElementById("api-servidor-cadastrado");
          if (chkInterno) chkInterno.checked = interno;
          const scopes = String(row.dataset.scopes || "")
            .split(",")
            .map((v) => v.trim())
            .filter(Boolean);
          setScopesToForm(scopes);
          if (boxCred) boxCred.style.display = "none";
          setMsg("Cliente carregado para edicao.");
        });
      });

      tbody.querySelectorAll(".api-rotate").forEach((btn) => {
        btn.addEventListener("click", async () => {
          const id = btn.dataset.id;
          if (!id) return;
          setMsg("Rotacionando segredo...");
          try {
            const res = await fetch(`/api/api-clients/${encodeURIComponent(id)}/rotate-secret`, {
              method: "POST",
              headers: { "X-Requested-With": "fetch" },
            });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || "Falha ao rotacionar segredo.");
            showCredentials(data?.credentials?.client_id || "", data?.credentials?.client_secret || "");
            setMsg(data.message || "Segredo rotacionado.");
            await loadRows();
          } catch (err) {
            console.error(err);
            setMsg(err.message || "Falha ao rotacionar segredo.", true);
          }
        });
      });

      tbody.querySelectorAll(".api-revoke").forEach((btn) => {
        btn.addEventListener("click", async () => {
          const id = btn.dataset.id;
          if (!id) return;
          setMsg("Revogando cliente...");
          try {
            const res = await fetch(`/api/api-clients/${encodeURIComponent(id)}/revoke`, {
              method: "POST",
              headers: { "X-Requested-With": "fetch" },
            });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || "Falha ao revogar cliente.");
            setMsg(data.message || "Cliente revogado.");
            await loadRows();
          } catch (err) {
            console.error(err);
            setMsg(err.message || "Falha ao revogar cliente.", true);
          }
        });
      });

      tbody.querySelectorAll(".api-delete").forEach((btn) => {
        btn.addEventListener("click", async () => {
          const id = btn.dataset.id;
          if (!id) return;
          if (!window.confirm("Deseja realmente excluir este servidor de API?")) return;
          setMsg("Excluindo servidor...");
          try {
            const res = await fetch(`/api/api-clients/${encodeURIComponent(id)}`, {
              method: "DELETE",
              headers: { "X-Requested-With": "fetch" },
            });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || "Falha ao excluir servidor.");
            setMsg(data.message || "Servidor excluido.");
            if (String(document.getElementById("api-client-id")?.value || "") === String(id)) {
              resetForm();
            }
            await loadRows();
          } catch (err) {
            console.error(err);
            setMsg(err.message || "Falha ao excluir servidor.", true);
          }
        });
      });
    };

    const loadRows = async () => {
      try {
        const res = await fetch("/api/api-clients", { headers: { "X-Requested-With": "fetch" } });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar clientes.");
        renderRows(data.rows || []);
      } catch (err) {
        console.error(err);
        tbody.innerHTML = `<tr><td colspan="9" class="text-error">${esc(err.message || "Falha ao carregar.")}</td></tr>`;
      }
    };

    if (btnLimpar) {
      btnLimpar.addEventListener("click", () => resetForm());
    }

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      setMsg("Salvando servidor...");
      const id = document.getElementById("api-client-id").value;
      const payload = {
        nome: document.getElementById("api-client-nome").value,
        status: document.getElementById("api-client-status").value || "ativo",
        acesso_inicio_em: document.getElementById("api-acesso-inicio")?.value || "",
        acesso_fim_em: document.getElementById("api-acesso-fim")?.value || "",
        servidor_cadastrado: !!document.getElementById("api-servidor-cadastrado")?.checked,
        scopes: getScopesFromForm(),
      };
      if (!payload.nome.trim()) {
        setMsg("Informe o nome do servidor.", true);
        return;
      }
      if (!payload.scopes.length) {
        setMsg("Selecione ao menos um escopo.", true);
        return;
      }
      if (!payload.acesso_inicio_em) {
        setMsg("Informe a data/hora de inicio.", true);
        return;
      }
      try {
        const isUpdate = !!id;
        const url = isUpdate
          ? `/api/api-clients/${encodeURIComponent(id)}`
          : "/api/api-clients";
        const res = await fetch(url, {
          method: isUpdate ? "PUT" : "POST",
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao salvar servidor.");
        if (!isUpdate && data?.credentials) {
          showCredentials(data.credentials.client_id || "", data.credentials.client_secret || "");
        } else if (boxCred) {
          boxCred.style.display = "none";
        }
        setMsg(data.message || "Servidor salvo.");
        await loadRows();
        if (!isUpdate) {
          document.getElementById("api-client-id").value = "";
        }
      } catch (err) {
        console.error(err);
        setMsg(err.message || "Falha ao salvar servidor.", true);
      }
    });

    resetForm();
    loadRows();
  }

  function applyMenuPermissions(features = []) {
    if (!menu) return;
    const allowed = new Set(["dashboard", "logout", "atualizar/personalizar-spo", ...features]);
    const isAllowedRoute = (route) => {
      if (!route) return false;
      if (allowed.has(route)) return true;
      if (route.startsWith("cadastrar/dotacao/")) return allowed.has("cadastrar/dotacao");
      if (route.startsWith("cadastrar/est-dotacao/")) return allowed.has("cadastrar/est-dotacao");
      if (route.startsWith("cadastrar/plan_21-nger/meta_fisica/")) return allowed.has("cadastrar/plan_21-nger/meta_fisica");
      if (route.startsWith("cadastrar/plan_21-nger/subacao/")) return allowed.has("cadastrar/plan_21-nger/subacao");
      if (route.startsWith("cadastrar/plan_21-nger/etapa/")) return allowed.has("cadastrar/plan_21-nger/etapa");
      return false;
    };

    // Children: show only allowed
    menu.querySelectorAll(".submenu [data-route]").forEach((link) => {
      const route = link.getAttribute("data-route");
      if (!route) return;
      link.style.display = isAllowedRoute(route) ? "" : "none";
    });

    // Parents: show if any allowed child
    menu.querySelectorAll(".menu-group").forEach((group) => {
      const submenu = group.querySelector(".submenu");
      if (!submenu) return;
      const parentId = group.id?.replace("menu-", "") || "";
      const hasAllowedChild = Array.from(submenu.querySelectorAll("[data-route]")).some((item) =>
        isAllowedRoute(item.getAttribute("data-route"))
      );
      const parentAllowed = parentId && allowed.has(parentId);
      group.style.display = hasAllowedChild || parentAllowed ? "" : "none";
    });

    // Top-level items without submenu
    menu.querySelectorAll(".menu > .menu-item[data-route]").forEach((item) => {
      const route = item.getAttribute("data-route");
      if (!route) return;
      if (route === "logout") return;
      item.style.display = isAllowedRoute(route) ? "" : "none";
    });

    requestAnimationFrame(updateSidebarExpandedWidth);
  }

  function showToast(message, type = "success", timeout = 3500) {
    if (!message) return;
    let container = document.querySelector(".toast-container");
    if (!container) {
      container = document.createElement("div");
      container.className = "toast-container";
      document.body.appendChild(container);
    }
    const toast = document.createElement("div");
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => {
      toast.remove();
      if (container && container.children.length === 0) {
        container.remove();
      }
    }, timeout);
  }

  async function fetchCurrentPermissions() {
    if (userNivel === "1") {
      // admin: libera tudo visível no menu
      const allRoutes = Array.from(menu.querySelectorAll("[data-route]")).map((el) =>
        el.getAttribute("data-route")
      );
      applyMenuPermissions(allRoutes);
      return;
    }
    try {
      const res = await fetch("/api/permissoes/current", {
        headers: { "X-Requested-With": "fetch" },
      });
      if (!res.ok) return;
      const data = await res.json();
      const feats = data.features || [];
      const locked = ["dashboard", "logout"];
      applyMenuPermissions(feats);
    } catch (err) {
      console.error(err);
    }
  }

  function initPainel() {
    const dataScript = document.getElementById("painel-data");
    const treeEl = document.getElementById("painel-tree");
    const ativosEl = document.getElementById("painel-ativos");
    const ativosTitle = document.getElementById("painel-ativos-title");
    const selectTipo = document.getElementById("painel-tipo");
    const selectPerfil = document.getElementById("painel-perfil");
    const selectNivel = document.getElementById("painel-nivel");
    const fieldPerfil = document.getElementById("painel-perfil-field");
    const fieldNivel = document.getElementById("painel-nivel-field");
    const btnSalvar = document.getElementById("painel-salvar");
    const btnCancelar = document.getElementById("painel-cancelar");
    const msg = document.getElementById("painel-msg");
    if (!dataScript || !treeEl || !ativosEl || !selectPerfil || !selectNivel || !selectTipo) return;
    if (treeEl.dataset.bound === "1") return;
    treeEl.dataset.bound = "1";

    const features = JSON.parse(dataScript.dataset.features || "[]");
    const allowedPerfilRaw = JSON.parse(dataScript.dataset.allowedPerfil || "{}");
    const allowedNivelRaw = JSON.parse(dataScript.dataset.allowedNivel || "{}");
    const allowedPerfil = {};
    const allowedNivel = {};
    Object.entries(allowedPerfilRaw).forEach(([k, v]) => {
      allowedPerfil[String(k)] = v;
    });
    Object.entries(allowedNivelRaw).forEach(([k, v]) => {
      allowedNivel[String(k)] = v;
    });
    const lockedBase = new Set(features.filter((f) => f.locked).map((f) => f.id));
    const sortFeatures = (items) =>
      (items || [])
        .map((f) => ({
          ...f,
          children: f.children ? sortFeatures([...f.children]) : [],
        }))
        .sort((a, b) => a.nome.localeCompare(b.nome, "pt-BR", { sensitivity: "base" }));
    const sortedFeatures = sortFeatures(features);
    let originalPerfil = {};
    let originalNivel = {};
    Object.entries(allowedPerfil).forEach(([k, v]) => {
      originalPerfil[k] = [...v];
    });
    Object.entries(allowedNivel).forEach(([k, v]) => {
      originalNivel[k] = [...v];
    });
    let nivelLocked = new Set();
    let profileLocked = new Set();
    let currentMode = selectTipo.value || "perfil";

    const getAllowedMap = () => (currentMode === "nivel" ? allowedNivel : allowedPerfil);
    const getOriginalMap = () => (currentMode === "nivel" ? originalNivel : originalPerfil);
    const getSelectedKey = () => String(currentMode === "nivel" ? selectNivel.value || "" : selectPerfil.value || "");
    const getLockedSet = () => {
      const locked = new Set(lockedBase);
      if (currentMode === "perfil") {
        nivelLocked.forEach((id) => locked.add(id));
      } else {
        profileLocked.forEach((id) => locked.add(id));
      }
      return locked;
    };

    const renderAtivos = (list) => {
      ativosEl.innerHTML = "";
      list.forEach((item) => {
        const li = document.createElement("li");
        li.textContent = item;
        ativosEl.appendChild(li);
      });
    };

    const buildTree = (key) => {
      treeEl.innerHTML = "";
      if (!key) {
        ativosEl.innerHTML = "";
        return;
      }
      const allowedMap = getAllowedMap();
      const currentAllowed = new Set(allowedMap[key] || []);
      lockedBase.forEach((f) => currentAllowed.add(f));
      if (currentMode === "perfil") {
        nivelLocked.forEach((f) => currentAllowed.add(f));
      } else {
        profileLocked.forEach((f) => currentAllowed.add(f));
      }
      const lockedAll = getLockedSet();

      const toggleChildren = (node, checked) => {
        node.querySelectorAll("input[type='checkbox']").forEach((cb) => {
          const id = cb.dataset.id;
          if (lockedAll.has(id)) {
            cb.checked = true;
            return;
          }
          cb.checked = checked;
          if (checked) currentAllowed.add(id);
          else currentAllowed.delete(id);
        });
      };

      const createNode = (feat) => {
        const wrapper = document.createElement("div");
        wrapper.className = "tree-item";
        const controls = document.createElement("div");
        controls.className = "tree-controls";
        if (feat.children && feat.children.length) {
          const toggleBtn = document.createElement("button");
          toggleBtn.type = "button";
          toggleBtn.className = "tree-toggle";
          const startCollapsed = true;
          if (startCollapsed) wrapper.classList.add("collapsed");
          toggleBtn.innerHTML = `<i class="bi bi-caret-${startCollapsed ? "right" : "down"}-fill"></i>`;
          toggleBtn.addEventListener("click", () => {
            const collapsed = wrapper.classList.toggle("collapsed");
            toggleBtn.innerHTML = `<i class="bi bi-caret-${collapsed ? "right" : "down"}-fill"></i>`;
            const childBox = wrapper.querySelector(".tree-children");
            if (childBox) childBox.style.display = collapsed ? "none" : "flex";
          });
          controls.appendChild(toggleBtn);
        } else {
          const spacer = document.createElement("span");
          spacer.style.display = "inline-block";
          spacer.style.width = "14px";
          controls.appendChild(spacer);
        }
        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.checked = currentAllowed.has(feat.id);
        cb.dataset.id = feat.id;
        cb.disabled = lockedAll.has(feat.id);
        controls.appendChild(cb);
        const label = document.createElement("span");
        label.textContent = feat.nome;
        wrapper.appendChild(controls);
        wrapper.appendChild(label);

        cb.addEventListener("change", () => {
          if (cb.checked) {
            currentAllowed.add(feat.id);
            if (feat.parentId) {
              const parentCb = treeEl.querySelector(`input[data-id='${feat.parentId}']`);
              if (parentCb) {
                parentCb.checked = true;
                currentAllowed.add(feat.parentId);
              }
            }
          } else {
            if (!lockedAll.has(feat.id)) currentAllowed.delete(feat.id);
            if (feat.children && feat.children.length) {
              const subtree = wrapper.querySelector(".tree-children");
              if (subtree) toggleChildren(subtree, false);
            }
          }
          const updated = Array.from(currentAllowed).filter((id) => !lockedAll.has(id));
          allowedMap[key] = updated;
          renderAtivos(Array.from(currentAllowed));
        });

        if (feat.children && feat.children.length) {
          const childrenBox = document.createElement("div");
          childrenBox.className = "tree-children";
          if (wrapper.classList.contains("collapsed")) {
            childrenBox.style.display = "none";
          }
          feat.children.forEach((ch) => {
            ch.parentId = feat.id;
            const childNode = createNode(ch);
            childrenBox.appendChild(childNode);
          });
          wrapper.appendChild(childrenBox);
        }
        return wrapper;
      };

      sortedFeatures.forEach((f) => {
        const node = createNode(f);
        treeEl.appendChild(node);
      });
      renderAtivos(Array.from(currentAllowed));
    };

    const loadPerfilPermissions = async (perfil) => {
      if (!perfil) return { features: [], nivelFeatures: [], nivel: "" };
      try {
        const res = await fetch(`/api/permissoes/${perfil}`, { headers: { "X-Requested-With": "fetch" } });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar permissoes.");
        return {
          features: Array.isArray(data.features) ? data.features : [],
          nivelFeatures: Array.isArray(data.nivel_features) ? data.nivel_features : [],
          nivel: data.nivel,
        };
      } catch (err) {
        console.error(err);
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
        return { features: [], nivelFeatures: [], nivel: "" };
      }
    };

    const loadNivelPermissions = async (nivel) => {
      if (!nivel) return { features: [], perfilFeatures: [] };
      try {
        const res = await fetch(`/api/permissoes/nivel/${nivel}`, { headers: { "X-Requested-With": "fetch" } });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar permissoes.");
        return {
          features: Array.isArray(data.features) ? data.features : [],
          perfilFeatures: Array.isArray(data.perfil_features) ? data.perfil_features : [],
        };
      } catch (err) {
        console.error(err);
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
        return { features: [], perfilFeatures: [] };
      }
    };

    const updateMode = async () => {
      currentMode = selectTipo.value || "perfil";
      if (fieldPerfil) fieldPerfil.style.display = currentMode === "perfil" ? "" : "none";
      if (fieldNivel) fieldNivel.style.display = currentMode === "nivel" ? "" : "none";
      if (ativosTitle) {
        ativosTitle.textContent = currentMode === "nivel" ? "Ativos para o nivel" : "Ativos para o perfil";
      }
      treeEl.innerHTML = "";
      ativosEl.innerHTML = "";
      if (msg) {
        msg.textContent = "";
        msg.classList.remove("text-error");
      }
      const key = getSelectedKey();
      if (!key) {
        nivelLocked = new Set();
        profileLocked = new Set();
        return;
      }
      if (currentMode === "perfil") {
        const result = await loadPerfilPermissions(key);
        allowedPerfil[key] = result.features.filter((f) => typeof f === "string");
        originalPerfil[key] = [...allowedPerfil[key]];
        nivelLocked = new Set(result.nivelFeatures.filter((f) => typeof f === "string"));
        profileLocked = new Set();
      } else {
        const result = await loadNivelPermissions(key);
        allowedNivel[key] = result.features.filter((f) => typeof f === "string");
        originalNivel[key] = [...allowedNivel[key]];
        nivelLocked = new Set();
        profileLocked = new Set(result.perfilFeatures.filter((f) => typeof f === "string"));
      }
      buildTree(key);
    };

    selectTipo.addEventListener("change", updateMode);

    selectPerfil.addEventListener("change", async () => {
      if (currentMode !== "perfil") return;
      const perfil = String(selectPerfil.value || "");
      if (!perfil) {
        treeEl.innerHTML = "";
        ativosEl.innerHTML = "";
        return;
      }
      const result = await loadPerfilPermissions(perfil);
      allowedPerfil[perfil] = result.features.filter((f) => typeof f === "string");
      originalPerfil[perfil] = [...allowedPerfil[perfil]];
      nivelLocked = new Set(result.nivelFeatures.filter((f) => typeof f === "string"));
      buildTree(perfil);
    });

    selectNivel.addEventListener("change", async () => {
      if (currentMode !== "nivel") return;
      const nivel = String(selectNivel.value || "");
      if (!nivel) {
        treeEl.innerHTML = "";
        ativosEl.innerHTML = "";
        return;
      }
      const result = await loadNivelPermissions(nivel);
      allowedNivel[nivel] = result.features.filter((f) => typeof f === "string");
      originalNivel[nivel] = [...allowedNivel[nivel]];
      nivelLocked = new Set();
      profileLocked = new Set(result.perfilFeatures.filter((f) => typeof f === "string"));
      buildTree(nivel);
    });

    const handleSalvar = async () => {
      const key = getSelectedKey();
      if (!key) {
        if (msg) msg.textContent = currentMode === "nivel" ? "Selecione um nivel." : "Selecione um perfil.";
        return;
      }
      const allowedMap = getAllowedMap();
      const feats = allowedMap[key] || [];
      if (msg) msg.textContent = "Salvando...";
      try {
        const url = currentMode === "nivel" ? `/api/permissoes/nivel/${key}` : `/api/permissoes/${key}`;
        const res = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify({ features: feats }),
        });
        if (!res.ok) {
          const text = await res.text();
          throw new Error(text || `Erro ${res.status}`);
        }
        const data = await res.json();
        const originalMap = getOriginalMap();
        originalMap[key] = [...feats];
        if (msg) msg.textContent = data.message || "Permissoes salvas.";
      } catch (err) {
        console.error(err);
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
      }
    };

    const handleCancelar = () => {
      const key = getSelectedKey();
      if (!key) return;
      const allowedMap = getAllowedMap();
      const originalMap = getOriginalMap();
      allowedMap[key] = [...(originalMap[key] || [])];
      buildTree(key);
      if (msg) {
        msg.textContent = "";
        msg.classList.remove("text-error");
      }
    };

    if (btnSalvar) btnSalvar.addEventListener("click", handleSalvar);
    if (btnCancelar) btnCancelar.addEventListener("click", handleCancelar);
    updateMode();
  }

  function initUsuariosSenha() {
    const formBuscar = document.getElementById("form-buscar-usuario");
    const formAlterar = document.getElementById("form-alterar-senha");
    const areaSenha = document.getElementById("senha-area");
    const msgBuscar = document.getElementById("buscar-usuario-msg");
    const msgSenha = document.getElementById("senha-msg");
    const btnCancelar = document.getElementById("senha-cancelar");
    if (!formBuscar || !formAlterar || !areaSenha) return;
    if (formBuscar.dataset.bound === "1") return;
    formBuscar.dataset.bound = "1";
    bindToggleVisibility(formAlterar);

    const fillUser = (data) => {
      document.getElementById("senha-email").value = data.email || "";
      document.getElementById("senha-nome").value = data.nome || "";
      document.getElementById("senha-perfil").value = data.perfil || "";
      document.getElementById("senha-atual").value = "";
      document.getElementById("senha-nova").value = "";
      document.getElementById("senha-confirmar").value = "";
      areaSenha.style.display = "block";
      if (msgBuscar) msgBuscar.textContent = "";
    };

    formBuscar.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const email = (document.getElementById("buscar-email").value || "").trim();
      if (!email) return;
      if (msgBuscar) msgBuscar.textContent = "Consultando...";
      try {
        const res = await fetch(`/api/usuarios/${encodeURIComponent(email)}`, {
          headers: { "X-Requested-With": "fetch" },
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao consultar.");
        fillUser(data);
      } catch (err) {
        console.error(err);
        if (msgBuscar) {
          msgBuscar.textContent = err.message;
          msgBuscar.classList.add("text-error");
        }
        areaSenha.style.display = "none";
      }
    });

    formAlterar.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const email = document.getElementById("senha-email").value;
      if (!email) return;
      if (msgSenha) {
        msgSenha.textContent = "Salvando...";
        msgSenha.classList.remove("text-error");
      }
      const payload = {
        senha_atual: document.getElementById("senha-atual").value,
        senha_nova: document.getElementById("senha-nova").value,
        senha_confirmar: document.getElementById("senha-confirmar").value,
      };
      try {
        const res = await fetch(`/api/usuarios/${encodeURIComponent(email)}/senha`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao salvar.");
        if (msgSenha) msgSenha.textContent = data.message || "Senha atualizada.";
        formAlterar.reset();
      } catch (err) {
        console.error(err);
        if (msgSenha) {
          msgSenha.textContent = err.message;
          msgSenha.classList.add("text-error");
        }
      }
    });

    if (btnCancelar) {
      btnCancelar.addEventListener("click", () => {
        formAlterar.reset();
        areaSenha.style.display = "none";
        if (msgSenha) {
          msgSenha.textContent = "";
          msgSenha.classList.remove("text-error");
        }
      });
    }
  }

  const AMAZON_TZ = "America/Manaus";

  const parseUtc = (value) => {
    if (!value) return null;
    const text = String(value);
    if (/[zZ]|[+-]\d{2}:\d{2}$/.test(text)) return new Date(text);
    return new Date(`${text}Z`);
  };

  const parseManausLocal = (value) => {
    if (!value) return null;
    const text = String(value);
    if (/[zZ]|[+-]\d{2}:\d{2}$/.test(text)) return new Date(text);
    return new Date(`${text}-04:00`);
  };

  const formatAmazonTime = (value) => {
    const date = parseUtc(value);
    return date ? date.toLocaleString("pt-BR", { timeZone: AMAZON_TZ }) : "-";
  };

  const formatAmazonLocalTime = (value) => {
    const date = parseManausLocal(value);
    return date ? date.toLocaleString("pt-BR", { timeZone: AMAZON_TZ }) : "-";
  };

  async function loadFipStatus(target) {
    if (!target) return;
    target.textContent = "Carregando...";
    try {
      const res = await fetch("/api/fip613/status");
      if (!res.ok) throw new Error("Erro ao consultar status");
      const data = await res.json();
      if (!data.last) {
        target.textContent = "Nenhuma atualização encontrada.";
        return;
      }
      const last = data.last;
      const uploaded = formatAmazonTime(last.uploaded_at);
      const dataArquivo = formatAmazonLocalTime(last.data_arquivo);
      target.innerHTML = `
        <div><strong>Enviado por:</strong> ${last.user_email || "-"}</div>
        <div><strong>Upload em:</strong> ${uploaded}</div>
        <div><strong>Data do download:</strong> ${dataArquivo}</div>
        <div><strong>Arquivo original:</strong> ${last.original_filename || "-"}</div>
        <div><strong>Saída gerada:</strong> ${last.output_filename || "-"}</div>
      `;
    } catch (err) {
      target.textContent = "Falha ao carregar status.";
      console.error(err);
    }
  }

  async function loadPedStatus(target, submitBtn, viewLabel) {
    if (!target) return;
    target.textContent = "Carregando...";
    try {
      const res = await fetch("/api/ped/status");
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Erro ao consultar status");
      if (!data.last) {
        target.textContent = "Nenhuma atualização encontrada.";
        if (submitBtn) {
          submitBtn.dataset.mode = "upload";
          submitBtn.textContent = "Upload e processar";
          submitBtn.dataset.output = "";
        }
        return;
      }
      const last = data.last;
      const uploaded = formatAmazonTime(last.uploaded_at);
      const dataArquivo = formatAmazonLocalTime(last.data_arquivo);
      target.innerHTML = `
        <div><strong>Enviado por:</strong> ${last.user_email || "-"}</div>
        <div><strong>Upload em:</strong> ${uploaded}</div>
        <div><strong>Data do download:</strong> ${dataArquivo}</div>
        <div><strong>Arquivo original:</strong> ${last.original_filename || "-"}</div>
        <div><strong>Saída gerada:</strong> ${last.output_filename || "-"}</div>
      `;
      if (submitBtn && last.output_filename) {
        submitBtn.dataset.mode = "view";
        submitBtn.dataset.output = last.output_filename;
        submitBtn.textContent = viewLabel || "Ver relatório";
      }
    } catch (err) {
      target.textContent = "Falha ao carregar status.";
      console.error(err);
    }
  }

  async function loadEmpStatus(target, submitBtn, viewLabel) {
    if (!target) return;
    target.textContent = "Carregando...";
    try {
      const res = await fetch("/api/emp/status");
      const raw = await res.text();
      let data = {};
      try {
        data = JSON.parse(raw || "{}");
      } catch {
        throw new Error(raw || "Resposta invalida do servidor.");
      }
      if (!res.ok) throw new Error(data.error || "Erro ao consultar status");
      if (!data.last) {
        target.textContent = "Nenhuma atualização encontrada.";
        if (submitBtn) {
          submitBtn.dataset.mode = "upload";
          submitBtn.textContent = "Upload e processar";
          submitBtn.dataset.output = "";
        }
        return null;
      }
      const last = data.last;
      const uploaded = formatAmazonTime(last.uploaded_at);
      const dataArquivo = formatAmazonLocalTime(last.data_arquivo);
      const statusText = last.status || "-";
      const statusMsg = last.status_message || "";
        const statusUpdated = formatAmazonTime(last.status_updated_at);
      const statusProgress =
        typeof last.status_progress === "number" ? `${last.status_progress}%` : "-";
      const statusPid = last.status_pid ? String(last.status_pid) : "-";
      target.innerHTML = `
        <div><strong>Enviado por:</strong> ${last.user_email || "-"}</div>
        <div><strong>Upload em:</strong> ${uploaded}</div>
        <div><strong>Data do download:</strong> ${dataArquivo}</div>
        <div><strong>Arquivo original:</strong> ${last.original_filename || "-"}</div>
        <div><strong>Status:</strong> ${statusText}</div>
        <div><strong>Progresso:</strong> ${statusProgress}</div>
        <div><strong>PID:</strong> ${statusPid}</div>
        <div><strong>Atualizado em:</strong> ${statusUpdated}</div>
        <div><strong>Mensagem:</strong> ${statusMsg || "-"}</div>
        <div><strong>Saída gerada:</strong> ${last.output_filename || "-"}</div>
      `;
      if (submitBtn && last.output_filename) {
        submitBtn.dataset.mode = "view";
        submitBtn.dataset.output = last.output_filename;
        submitBtn.textContent = viewLabel || "Ver relatório";
      }
      if (last.output_filename) return "done";
      return last.status || null;
    } catch (err) {
      target.textContent = "Falha ao carregar status.";
      console.error(err);
      return null;
    }
  }

  async function loadEstEmpStatus(target, submitBtn, viewLabel) {
    if (!target) return;
    target.textContent = "Carregando...";
    try {
      const res = await fetch("/api/est-emp/status");
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Erro ao consultar status");
      if (!data.last) {
        target.textContent = "Nenhuma atualização encontrada.";
        if (submitBtn) {
          submitBtn.dataset.mode = "upload";
          submitBtn.textContent = "Upload e processar";
          submitBtn.dataset.output = "";
        }
        return;
      }
      const last = data.last;
      const uploaded = formatAmazonTime(last.uploaded_at);
      const dataArquivo = formatAmazonLocalTime(last.data_arquivo);
      target.innerHTML = `
        <div><strong>Enviado por:</strong> ${last.user_email || "-"}</div>
        <div><strong>Upload em:</strong> ${uploaded}</div>
        <div><strong>Data do download:</strong> ${dataArquivo}</div>
        <div><strong>Arquivo original:</strong> ${last.original_filename || "-"}</div>
        <div><strong>Saída gerada:</strong> ${last.output_filename || "-"}</div>
      `;
      if (submitBtn && last.output_filename) {
        submitBtn.dataset.mode = "view";
        submitBtn.dataset.output = last.output_filename;
        submitBtn.textContent = viewLabel || "Ver relatório";
      }
    } catch (err) {
      target.textContent = "Falha ao carregar status.";
      console.error(err);
    }
  }

  async function loadNobStatus(target, submitBtn, viewLabel) {
    if (!target) return;
    target.textContent = "Carregando...";
    try {
      const res = await fetch("/api/nob/status");
      const raw = await res.text();
      let data = {};
      try {
        data = JSON.parse(raw || "{}");
      } catch {
        throw new Error(raw || "Resposta invalida do servidor.");
      }
      if (!res.ok) throw new Error(data.error || "Erro ao consultar status");
      if (!data.last) {
        target.textContent = "Nenhuma atualização encontrada.";
        return null;
      }
      const last = data.last;
      const uploaded = formatAmazonTime(last.uploaded_at);
      const dataArquivo = formatAmazonLocalTime(last.data_arquivo);
      const statusText = last.status || "-";
      const statusMsg = last.status_message || "";
        const statusUpdated = formatAmazonTime(last.status_updated_at);
      const statusProgress =
        typeof last.status_progress === "number" ? `${last.status_progress}%` : "-";
      const statusPid = last.status_pid ? String(last.status_pid) : "-";
      target.innerHTML = `
        <div><strong>Enviado por:</strong> ${last.user_email || "-"}</div>
        <div><strong>Upload em:</strong> ${uploaded}</div>
        <div><strong>Data do download:</strong> ${dataArquivo}</div>
        <div><strong>Arquivo original:</strong> ${last.original_filename || "-"}</div>
        <div><strong>Status:</strong> ${statusText}</div>
        <div><strong>Progresso:</strong> ${statusProgress}</div>
        <div><strong>PID:</strong> ${statusPid}</div>
        <div><strong>Atualizado em:</strong> ${statusUpdated}</div>
        <div><strong>Mensagem:</strong> ${statusMsg || "-"}</div>
        <div><strong>Saída gerada:</strong> ${last.output_filename || "-"}</div>
      `;
      if (submitBtn && last.output_filename) {
        submitBtn.dataset.mode = "view";
        submitBtn.dataset.output = last.output_filename;
        submitBtn.textContent = viewLabel || "Ver relatório";
      }
      if (last.output_filename) return "done";
      return last.status || null;
    } catch (err) {
      target.textContent = "Falha ao carregar status.";
      console.error(err);
      return null;
    }
  }

  function startStatusPolling(loader, attempts = 20, intervalMs = 30000) {
    const tick = async (left) => {
      if (left <= 0) return;
      const state = await loader();
      if (state === "done" || state === "error") return;
      setTimeout(() => tick(left - 1), intervalMs);
    };
    setTimeout(() => tick(attempts), intervalMs);
  }

  function setDefaultAmazonTime(input) {
    if (!input) return;
    const now = new Date();
    const parts = new Intl.DateTimeFormat("sv-SE", {
      timeZone: "America/Manaus",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    })
      .formatToParts(now)
      .reduce((acc, p) => ({ ...acc, [p.type]: p.value }), {});
    input.value = `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}`;
  }

  function initFip613() {
    const form = document.getElementById("form-fip613");
    if (!form) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";
    const msg = document.getElementById("fip613-msg");
    const statusBox = document.getElementById("fip613-status");
    const inputData = document.getElementById("fip613-data");
    const fileInput = document.getElementById("fip613-file");
    const loading = document.getElementById("fip613-loading");
  const submitBtn = document.getElementById("fip613-submit");
  const defaultLabel = "Upload e processar";
  const viewLabel = "Ver Relatório";

  if (inputData) {
    setDefaultAmazonTime(inputData);
  }

    loadFipStatus(statusBox);

    if (submitBtn) {
      submitBtn.dataset.mode = "upload";
      submitBtn.textContent = defaultLabel;
      submitBtn.addEventListener("click", (ev) => {
        if (submitBtn.dataset.mode === "view") {
          ev.preventDefault();
          ev.stopPropagation();
          setActive("relatorios/fip613");
          loadPage("relatorios/fip613");
        }
      });
    }

    if (fileInput && submitBtn) {
      fileInput.addEventListener("change", () => {
        submitBtn.dataset.mode = "upload";
        submitBtn.textContent = defaultLabel;
      });
    }

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      if (submitBtn?.dataset.mode === "view") {
        setActive("relatorios/fip613");
        loadPage("relatorios/fip613");
        return;
      }
      if (!fileInput?.files?.length) {
        if (msg) msg.textContent = "Selecione um arquivo .xlsx.";
        return;
      }
      if (loading) loading.style.display = "inline";
      if (submitBtn) submitBtn.disabled = true;
      const fd = new FormData(form);
      try {
        const res = await fetch("/api/fip613/upload", {
          method: "POST",
          body: fd,
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao enviar.");
        if (msg) {
          msg.textContent = data.message || "Upload concluído.";
          msg.classList.remove("text-error");
        }
        form.reset();
        if (inputData) inputData.value = "";
        loadFipStatus(statusBox);
        if (submitBtn) {
          submitBtn.textContent = viewLabel;
          submitBtn.dataset.mode = "view";
        }
      } catch (err) {
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
        console.error(err);
        if (submitBtn) {
          submitBtn.textContent = defaultLabel;
          submitBtn.dataset.mode = "upload";
        }
      } finally {
        if (loading) loading.style.display = "none";
        if (submitBtn) submitBtn.disabled = false;
      }
    });
  }

  function initPed() {
    const form = document.getElementById("form-ped");
    if (!form) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";
    const msg = document.getElementById("ped-msg");
    const statusBox = document.getElementById("ped-status");
    const inputData = document.getElementById("ped-data");
    const fileInput = document.getElementById("ped-file");
    const loading = document.getElementById("ped-loading");
    const submitBtn = document.getElementById("ped-submit");
    const reprocessBtn = document.getElementById("ped-reprocess");
    const cancelBtn = document.getElementById("ped-cancel");
    const defaultLabel = "Upload e processar";
    const viewLabel = "Ver relatório";
    const goToReport = () => {
      setActive("relatorios/ped");
      loadPage("relatorios/ped");
    };

    if (inputData) {
      setDefaultAmazonTime(inputData);
    }

    loadPedStatus(statusBox, submitBtn, viewLabel);

    if (submitBtn) {
      submitBtn.dataset.mode = "upload";
      submitBtn.textContent = defaultLabel;
      submitBtn.addEventListener("click", (ev) => {
        if (submitBtn.dataset.mode === "view") {
          ev.preventDefault();
          ev.stopPropagation();
          goToReport();
        }
      });
    }

    if (reprocessBtn) {
      reprocessBtn.addEventListener("click", async () => {
        if (msg) {
          msg.textContent = "Reprocessando...";
          msg.classList.remove("text-error");
        }
        try {
          const res = await fetch("/api/ped/reprocess", {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify({}),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao reprocessar.");
          if (msg) msg.textContent = data.message || "Reprocessamento iniciado.";
          await loadPedStatus(statusBox, submitBtn, viewLabel);
          startStatusPolling(() => loadPedStatus(statusBox, submitBtn, viewLabel));
        } catch (err) {
          if (msg) {
            msg.textContent = err.message;
            msg.classList.add("text-error");
          }
          console.error(err);
        }
      });
    }

    if (cancelBtn) {
      cancelBtn.addEventListener("click", async () => {
        if (msg) {
          msg.textContent = "Solicitando cancelamento...";
          msg.classList.remove("text-error");
        }
        try {
          const res = await fetch("/api/ped/cancel", {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify({}),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao cancelar.");
          if (msg) msg.textContent = data.message || "Cancelamento solicitado.";
          await loadPedStatus(statusBox, submitBtn, viewLabel);
        } catch (err) {
          if (msg) {
            msg.textContent = err.message;
            msg.classList.add("text-error");
          }
          console.error(err);
        }
      });
    }

    if (fileInput && submitBtn) {
      fileInput.addEventListener("change", () => {
        submitBtn.dataset.mode = "upload";
        submitBtn.textContent = defaultLabel;
      });
    }

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      if (submitBtn?.dataset.mode === "view") {
        goToReport();
        return;
      }
      if (!fileInput?.files?.length) {
        if (msg) msg.textContent = "Selecione um arquivo .xlsx.";
        return;
      }
      if (loading) loading.style.display = "inline";
      if (submitBtn) submitBtn.disabled = true;
      const fd = new FormData(form);
      try {
        const res = await fetch("/api/ped/upload", {
          method: "POST",
          body: fd,
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao enviar.");
        if (msg) {
          msg.textContent = data.message || "Upload concluído.";
          msg.classList.remove("text-error");
        }
        form.reset();
        if (inputData) inputData.value = "";
        await loadPedStatus(statusBox, submitBtn, viewLabel);
        if (submitBtn && data.output) {
          submitBtn.textContent = viewLabel;
          submitBtn.dataset.mode = "view";
          submitBtn.dataset.output = data.output;
        }
      } catch (err) {
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
        console.error(err);
        if (submitBtn) {
          submitBtn.textContent = defaultLabel;
          submitBtn.dataset.mode = "upload";
        }
      } finally {
        if (loading) loading.style.display = "none";
        if (submitBtn) submitBtn.disabled = false;
      }
    });
  }

  function initEmp() {
    const form = document.getElementById("form-emp");
    if (!form) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";
    const msg = document.getElementById("emp-msg");
    const statusBox = document.getElementById("emp-status");
    const inputData = document.getElementById("emp-data");
    const fileInput = document.getElementById("emp-file");
    const loading = document.getElementById("emp-loading");
    const submitBtn = document.getElementById("emp-submit");
    const reprocessBtn = document.getElementById("emp-reprocess");
    const cancelBtn = document.getElementById("emp-cancel");
    const defaultLabel = "Upload e processar";
    const viewLabel = "Ver relatório";
    const goToReport = () => {
      setActive("relatorios/emp");
      loadPage("relatorios/emp");
    };

    if (inputData) {
      setDefaultAmazonTime(inputData);
    }

    loadEmpStatus(statusBox, submitBtn, viewLabel);

    if (submitBtn) {
      submitBtn.dataset.mode = "upload";
      submitBtn.textContent = defaultLabel;
      submitBtn.addEventListener("click", (ev) => {
        if (submitBtn.dataset.mode === "view") {
          ev.preventDefault();
          ev.stopPropagation();
          goToReport();
        }
      });
    }

    if (reprocessBtn) {
      reprocessBtn.addEventListener("click", async () => {
        if (msg) {
          msg.textContent = "Reprocessando...";
          msg.classList.remove("text-error");
        }
        try {
          const res = await fetch("/api/emp/reprocess", {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify({}),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao reprocessar.");
          if (msg) msg.textContent = data.message || "Reprocessamento iniciado.";
          await loadEmpStatus(statusBox, submitBtn, viewLabel);
          startStatusPolling(() => loadEmpStatus(statusBox, submitBtn, viewLabel));
        } catch (err) {
          if (msg) {
            msg.textContent = err.message;
            msg.classList.add("text-error");
          }
          console.error(err);
        }
      });
    }

    if (cancelBtn) {
      cancelBtn.addEventListener("click", async () => {
        if (msg) {
          msg.textContent = "Solicitando cancelamento...";
          msg.classList.remove("text-error");
        }
        try {
          const res = await fetch("/api/emp/cancel", {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify({}),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao cancelar.");
          if (msg) msg.textContent = data.message || "Cancelamento solicitado.";
          await loadEmpStatus(statusBox, submitBtn, viewLabel);
        } catch (err) {
          if (msg) {
            msg.textContent = err.message;
            msg.classList.add("text-error");
          }
          console.error(err);
        }
      });
    }

    if (fileInput && submitBtn) {
      fileInput.addEventListener("change", () => {
        submitBtn.dataset.mode = "upload";
        submitBtn.textContent = defaultLabel;
      });
    }

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      if (submitBtn?.dataset.mode === "view") {
        goToReport();
        return;
      }
      if (!fileInput?.files?.length) {
        if (msg) msg.textContent = "Selecione um arquivo .xlsx.";
        return;
      }
      if (loading) loading.style.display = "inline";
      if (submitBtn) submitBtn.disabled = true;
      const fd = new FormData(form);
      try {
        const res = await fetch("/api/emp/upload", {
          method: "POST",
          body: fd,
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao enviar.");
        if (msg) {
          msg.textContent = data.message || "Upload concluido.";
          msg.classList.remove("text-error");
        }
        form.reset();
        if (inputData) inputData.value = "";
        await loadEmpStatus(statusBox, submitBtn, viewLabel);
        startStatusPolling(() => loadEmpStatus(statusBox, submitBtn, viewLabel));
        if (submitBtn && data.output) {
          submitBtn.textContent = viewLabel;
          submitBtn.dataset.mode = "view";
          submitBtn.dataset.output = data.output;
        }
      } catch (err) {
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
        console.error(err);
        if (submitBtn) {
          submitBtn.textContent = defaultLabel;
          submitBtn.dataset.mode = "upload";
        }
      } finally {
        if (loading) loading.style.display = "none";
        if (submitBtn) submitBtn.disabled = false;
      }
    });
  }

  function initEstEmp() {
    const form = document.getElementById("form-est-emp");
    if (!form) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";
    const msg = document.getElementById("est-emp-msg");
    const statusBox = document.getElementById("est-emp-status");
    const inputData = document.getElementById("est-emp-data");
    const fileInput = document.getElementById("est-emp-file");
    const loading = document.getElementById("est-emp-loading");
    const submitBtn = document.getElementById("est-emp-submit");
    const defaultLabel = "Upload e processar";
    const viewLabel = "Ver relatório";
    const goToReport = () => {
      setActive("relatorios/est-emp");
      loadPage("relatorios/est-emp");
    };

    if (inputData) {
      setDefaultAmazonTime(inputData);
    }

    loadEstEmpStatus(statusBox, submitBtn, viewLabel);

    if (submitBtn) {
      submitBtn.dataset.mode = "upload";
      submitBtn.textContent = defaultLabel;
      submitBtn.addEventListener("click", (ev) => {
        if (submitBtn.dataset.mode === "view") {
          ev.preventDefault();
          ev.stopPropagation();
          goToReport();
        }
      });
    }

    if (fileInput && submitBtn) {
      fileInput.addEventListener("change", () => {
        submitBtn.dataset.mode = "upload";
        submitBtn.textContent = defaultLabel;
      });
    }

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      if (submitBtn?.dataset.mode === "view") {
        goToReport();
        return;
      }
      if (!fileInput?.files?.length) {
        if (msg) msg.textContent = "Selecione um arquivo .xlsx.";
        return;
      }
      if (loading) loading.style.display = "inline";
      if (submitBtn) submitBtn.disabled = true;
      const fd = new FormData(form);
      try {
        const res = await fetch("/api/est-emp/upload", {
          method: "POST",
          body: fd,
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao enviar.");
        if (msg) {
          msg.textContent = data.message || "Upload concluido.";
          msg.classList.remove("text-error");
        }
        form.reset();
        if (inputData) inputData.value = "";
        await loadEstEmpStatus(statusBox, submitBtn, viewLabel);
        if (submitBtn && data.output) {
          submitBtn.textContent = viewLabel;
          submitBtn.dataset.mode = "view";
          submitBtn.dataset.output = data.output;
        }
      } catch (err) {
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
        console.error(err);
        if (submitBtn) {
          submitBtn.textContent = defaultLabel;
          submitBtn.dataset.mode = "upload";
        }
      } finally {
        if (loading) loading.style.display = "none";
        if (submitBtn) submitBtn.disabled = false;
      }
    });
  }

  function initNob() {
    const form = document.getElementById("form-nob");
    if (!form) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";
    const msg = document.getElementById("nob-msg");
    const statusBox = document.getElementById("nob-status");
    const inputData = document.getElementById("nob-data");
    const fileInput = document.getElementById("nob-file");
    const loading = document.getElementById("nob-loading");
    const submitBtn = document.getElementById("nob-submit");
    const reprocessBtn = document.getElementById("nob-reprocess");
    const cancelBtn = document.getElementById("nob-cancel");
    const defaultLabel = "Upload e processar";
    const viewLabel = "Ver relatório";
    const goToReport = () => {
      setActive("relatorios/nob");
      loadPage("relatorios/nob");
    };

    if (inputData) {
      setDefaultAmazonTime(inputData);
    }

    loadNobStatus(statusBox, submitBtn, viewLabel);

    if (submitBtn) {
      submitBtn.dataset.mode = "upload";
      submitBtn.textContent = defaultLabel;
      submitBtn.addEventListener("click", (ev) => {
        if (submitBtn.dataset.mode === "view") {
          ev.preventDefault();
          ev.stopPropagation();
          goToReport();
        }
      });
    }

    if (reprocessBtn) {
      reprocessBtn.addEventListener("click", async () => {
        if (msg) {
          msg.textContent = "Reprocessando...";
          msg.classList.remove("text-error");
        }
        try {
          const res = await fetch("/api/nob/reprocess", {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify({}),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao reprocessar.");
          if (msg) msg.textContent = data.message || "Reprocessamento iniciado.";
          await loadNobStatus(statusBox, submitBtn, viewLabel);
          startStatusPolling(() => loadNobStatus(statusBox, submitBtn, viewLabel));
        } catch (err) {
          if (msg) {
            msg.textContent = err.message;
            msg.classList.add("text-error");
          }
          console.error(err);
        }
      });
    }

    if (cancelBtn) {
      cancelBtn.addEventListener("click", async () => {
        if (msg) {
          msg.textContent = "Solicitando cancelamento...";
          msg.classList.remove("text-error");
        }
        try {
          const res = await fetch("/api/nob/cancel", {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify({}),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao cancelar.");
          if (msg) msg.textContent = data.message || "Cancelamento solicitado.";
          await loadNobStatus(statusBox, submitBtn, viewLabel);
        } catch (err) {
          if (msg) {
            msg.textContent = err.message;
            msg.classList.add("text-error");
          }
          console.error(err);
        }
      });
    }
    if (fileInput && submitBtn) {
      fileInput.addEventListener("change", () => {
        submitBtn.dataset.mode = "upload";
        submitBtn.textContent = defaultLabel;
      });
    }

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      if (submitBtn?.dataset.mode === "view") {
        goToReport();
        return;
      }
      if (!fileInput?.files?.length) {
        if (msg) msg.textContent = "Selecione um arquivo .xlsx.";
        return;
      }
      if (loading) loading.style.display = "inline";
      if (submitBtn) submitBtn.disabled = true;
      const fd = new FormData(form);
      try {
        const res = await fetch("/api/nob/upload", {
          method: "POST",
          body: fd,
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao enviar.");
        if (msg) {
          msg.textContent = data.message || "Upload concluido.";
          msg.classList.remove("text-error");
        }
        form.reset();
        if (inputData) inputData.value = "";
        await loadNobStatus(statusBox, submitBtn, viewLabel);
        startStatusPolling(() => loadNobStatus(statusBox, submitBtn, viewLabel));
        if (submitBtn && data.output) {
          submitBtn.textContent = viewLabel;
          submitBtn.dataset.mode = "view";
          submitBtn.dataset.output = data.output;
        }
      } catch (err) {
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
        console.error(err);
      } finally {
        if (loading) loading.style.display = "none";
        if (submitBtn) submitBtn.disabled = false;
      }
    });
  }

  function initPlan20() {
    const form = document.getElementById("form-plan20");
    if (!form) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";
    const msg = document.getElementById("plan20-msg");
    const statusBox = document.getElementById("plan20-status");
    const inputData = document.getElementById("plan20-data");
    const fileInput = document.getElementById("plan20-file");
    const loading = document.getElementById("plan20-loading");
  const submitBtn = document.getElementById("plan20-submit");
  const defaultLabel = "Upload e processar";
  const viewLabel = "Ver Relatório";
  const goToRelatorio = () => {
    setActive("relatorios/plan20-seduc");
    loadPage("relatorios/plan20-seduc");
  };

  if (inputData) {
    setDefaultAmazonTime(inputData);
  }

    const loadStatus = async () => {
      if (!statusBox) return;
      statusBox.textContent = "Carregando...";
      try {
        const res = await fetch("/api/plan20/status");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Erro ao consultar status");
        if (!data.last) {
          statusBox.textContent = "Nenhuma atualização encontrada.";
          return;
        }
        const last = data.last;
        const uploaded = formatAmazonTime(last.uploaded_at);
        const dataArquivo = formatAmazonLocalTime(last.data_arquivo);
        statusBox.innerHTML = `
          <div><strong>Enviado por:</strong> ${last.user_email || "-"}</div>
          <div><strong>Upload em:</strong> ${uploaded}</div>
          <div><strong>Data do download:</strong> ${dataArquivo}</div>
          <div><strong>Arquivo original:</strong> ${last.original_filename || "-"}</div>
          <div><strong>Saída gerada:</strong> ${last.output_filename || "-"}</div>
        `;
        if (submitBtn && data.last && data.last.output_filename) {
          submitBtn.dataset.mode = "view";
          submitBtn.textContent = viewLabel;
          submitBtn.dataset.output = data.last.output_filename;
        }
      } catch (err) {
        statusBox.textContent = "Falha ao carregar status.";
        console.error(err);
      }
    };

    if (submitBtn) {
      submitBtn.dataset.mode = "upload";
      submitBtn.textContent = defaultLabel;
      submitBtn.addEventListener("click", (ev) => {
        if (submitBtn.dataset.mode === "view") {
          ev.preventDefault();
          goToRelatorio();
        }
      });
    }

    if (fileInput && submitBtn) {
      fileInput.addEventListener("change", () => {
        submitBtn.dataset.mode = "upload";
        submitBtn.textContent = defaultLabel;
      });
    }

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      if (submitBtn?.dataset.mode === "view") {
        goToRelatorio();
        return;
      }
      if (!fileInput?.files?.length) {
        if (msg) msg.textContent = "Selecione um arquivo .xlsx.";
        return;
      }
      if (loading) loading.style.display = "inline";
      if (submitBtn) submitBtn.disabled = true;
      const fd = new FormData(form);
      try {
        const res = await fetch("/api/plan20/upload", {
          method: "POST",
          body: fd,
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao enviar.");
        if (msg) {
          msg.textContent = data.message || "Upload concluído.";
          msg.classList.remove("text-error");
        }
        form.reset();
        if (inputData) inputData.value = "";
        await loadStatus();
        if (submitBtn && data.output) {
          submitBtn.textContent = viewLabel;
          submitBtn.dataset.mode = "view";
          submitBtn.dataset.output = data.output;
        }
      } catch (err) {
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
        console.error(err);
        if (submitBtn) {
          submitBtn.textContent = defaultLabel;
          submitBtn.dataset.mode = "upload";
        }
      } finally {
        if (loading) loading.style.display = "none";
        if (submitBtn) submitBtn.disabled = false;
      }
    });

    loadStatus();
  }

  function initDotacao() {
    const form = document.getElementById("form-dotacao");
    const msg = document.getElementById("dotacao-msg");
    const idInput = document.getElementById("dotacao-id");
    if (!form || !msg) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const selects = {
      exercicio: document.getElementById("dotacao-exercicio"),
      chave_planejamento: document.getElementById("dotacao-chave"),
      uo: document.getElementById("dotacao-uo"),
      programa: document.getElementById("dotacao-programa"),
      acao_paoe: document.getElementById("dotacao-acao"),
      produto: document.getElementById("dotacao-produto"),
      ug: document.getElementById("dotacao-ug"),
      regiao: document.getElementById("dotacao-regiao"),
      subacao_entrega: document.getElementById("dotacao-subacao"),
      etapa: document.getElementById("dotacao-etapa"),
      natureza_despesa: document.getElementById("dotacao-natureza"),
      elemento: document.getElementById("dotacao-elemento"),
      subelemento: document.getElementById("dotacao-subelemento"),
      fonte: document.getElementById("dotacao-fonte"),
      iduso: document.getElementById("dotacao-iduso"),
    };
    const adjSelect = document.getElementById("dotacao-adj");
    const emprestadaRadios = document.querySelectorAll("input[name='dotacao-emprestada']");
    const adjConcedenteWrap = document.getElementById("dotacao-adj-concedente-wrap");
    const adjConcedenteSelect = document.getElementById("dotacao-adj-concedente");
    const elementoInput = selects.elemento;
    const valorInput = document.getElementById("dotacao-valor");
    const saldoInput = document.getElementById("dotacao-saldo");
    const saldoInfo = document.getElementById("dotacao-saldo-info");
    const saldoDebug = document.getElementById("dotacao-saldo-debug");
    const prefixInput = document.getElementById("dotacao-chave-prefixo");
    const justificativaInput = document.getElementById("dotacao-justificativa");
    const clearBtn = document.getElementById("dotacao-clear");
    const filterForm = document.getElementById("dotacao-filtro-form");
    const filterField = document.getElementById("dotacao-filtro-campo");
    const filterOp = document.getElementById("dotacao-filtro-operador");
    const filterValue = document.getElementById("dotacao-filtro-valor");
    const filterAdd = document.getElementById("dotacao-filtro-add");
    const filterList = document.getElementById("dotacao-filtro-list");
    const filterRemove = document.getElementById("dotacao-filtro-remove");
    const filterClear = document.getElementById("dotacao-filtro-clear");
    const filterCancel = document.getElementById("dotacao-filtro-cancel");
    const filterApply = document.getElementById("dotacao-filtro-apply");
    const filterMsg = document.getElementById("dotacao-filtro-msg");
    const dotacaoSummary = document.getElementById("dotacao-summary");
    const summaryBody = document.querySelector("#dotacao-summary-table tbody");
    const pageSizeSelect = document.getElementById("dotacao-page-size");
    const paginationEl = document.getElementById("dotacao-pagination");
    const approveBtn = document.getElementById("dotacao-approve");
    const editBtn = document.getElementById("dotacao-edit");
    const deleteBtn = document.getElementById("dotacao-delete");
    const printBtn = document.getElementById("dotacao-print");
    const approvalFields = document.getElementById("dotacao-aprovacao-fields");
    const approvalJustificativa = document.getElementById("dotacao-justificativa-aprovacao");
    const approvalRadios = document.querySelectorAll("input[name='dotacao-aprovada']");
    const dotacaoPage = document.getElementById("dotacao-page");
    const pageMode = String(dotacaoPage?.dataset?.viewMode || "formulario").trim();
    const editBadge = document.getElementById("dotacao-editing-badge");
    const currentUserPerfilId = String(dotacaoPage?.dataset?.userPerfilId || userPerfilId || "").trim();
    const currentUserId = dotacaoPage?.dataset?.userId || "";
    const currentUserNome = dotacaoPage?.dataset?.userNome || "";

    const hasAllSelects = Object.values(selects).every((el) => el);
    if (!hasAllSelects || !adjSelect) return;

    let updating = false;
    const baseSaldoKeys = new Set(["exercicio", "chave_planejamento"]);
    let approvalMode = false;
    const pendingStorageKey = "spo.dotacao.pendingAction";
    let dotacaoOptionsRequestSeq = 0;

    const currentOptionFilters = () => {
      const params = {};
      Object.entries(selects).forEach(([key, el]) => {
        const val = el.value;
        if (!val) return;
        if (baseSaldoKeys.has(key) || el.dataset.touched === "1") {
          params[key] = val;
        }
      });
      return params;
    };

    const currentSaldoFilters = () => {
      const params = {};
      Object.entries(selects).forEach(([key, el]) => {
        const val = el.value;
        if (!val) return;
        if (baseSaldoKeys.has(key) || el.dataset.touched === "1") {
          params[key] = val;
        }
      });
      return params;
    };

    const getAdjLabel = () => {
      const opt = adjSelect.options[adjSelect.selectedIndex];
      return opt ? String(opt.textContent || "").trim() : "";
    };

    const getEmprestadaValue = () => {
      const found = Array.from(emprestadaRadios).find((r) => r.checked);
      return found ? found.value : "nao";
    };

    const isEmprestada = () => getEmprestadaValue() === "sim";

    const toggleAdjConcedente = () => {
      if (!adjConcedenteSelect || !adjConcedenteWrap) return;
      const show = isEmprestada();
      adjConcedenteWrap.style.display = show ? "" : "none";
      adjConcedenteSelect.required = show;
      if (!show) {
        adjConcedenteSelect.value = "";
      }
    };

    const buildDotacaoPrefix = () => {
      const exercicio = selects.exercicio.value || "";
      const adjLabel = getAdjLabel();
      return `DOT.${exercicio}.${adjLabel}.`;
    };

    const setFormDisabled = (disabled) => {
      Object.values(selects).forEach((el) => {
        el.disabled = disabled;
      });
      if (adjSelect) adjSelect.disabled = disabled;
      if (valorInput) valorInput.disabled = disabled;
      if (justificativaInput) justificativaInput.disabled = disabled;
      if (adjConcedenteSelect) adjConcedenteSelect.disabled = disabled;
      if (emprestadaRadios.length) {
        emprestadaRadios.forEach((r) => {
          r.disabled = disabled;
        });
      }
    };

    const setApprovalMode = (enabled) => {
      approvalMode = enabled;
      if (approvalFields) approvalFields.style.display = enabled ? "" : "none";
      if (approvalJustificativa) approvalJustificativa.required = enabled;
      if (enabled) {
        setFormDisabled(true);
      } else {
        setFormDisabled(false);
        if (approvalJustificativa) approvalJustificativa.value = "";
        approvalRadios.forEach((r) => {
          r.checked = r.value === "sim";
        });
      }
    };

    const updateJustificativaPrefix = () => {
      if (prefixInput) prefixInput.value = `${buildDotacaoPrefix()}*`;
    };

    const criteria = [];
    let criteriaSelected = -1;
    const fieldLabels = {
      exercicio: "Exercício",
      statusAprovacao: "Status da Dotação",
      chaveDotacao: "Controle de Dotação",
      adjunta: "Adjunta Solicitante",
      programa: "Programa",
      paoe: "Ação/PAOE",
    };
    const opLabels = {
      eq: "Igual a",
      contains: "Contém",
      gt: "Maior que",
      lt: "Menor que",
      gte: "Maior igual a",
      lte: "Menor igual a",
    };

    const setFilterMsg = (text, isError = false) => {
      if (!filterMsg) return;
      filterMsg.textContent = text || "";
      if (isError) filterMsg.classList.add("text-error");
      else filterMsg.classList.remove("text-error");
    };

    const flashSummaryWarning = () => {
      if (!dotacaoSummary) return;
      dotacaoSummary.classList.add("dotacao-summary-warn");
      setTimeout(() => {
        dotacaoSummary.classList.remove("dotacao-summary-warn");
      }, 1200);
    };

    const renderCriteria = () => {
      if (!filterList) return;
      filterList.innerHTML = "";
      criteria.forEach((c, idx) => {
        const li = document.createElement("li");
        const label = fieldLabels[c.field] || c.field;
        const op = opLabels[c.op] || c.op;
        li.textContent = `${label} ${op} ${c.value}`;
        li.dataset.index = String(idx);
        if (idx === criteriaSelected) {
          li.style.borderColor = "var(--primary)";
        }
        li.addEventListener("click", () => {
          criteriaSelected = idx;
          renderCriteria();
        });
        filterList.appendChild(li);
      });
    };

    const formatPtBr = (value) => {
      const n = Number(value || 0);
      if (Number.isNaN(n)) return "";
      return new Intl.NumberFormat("pt-BR", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }).format(n);
    };

    const normalizeMetaDisplay = (value) => {
      let raw = String(value || "").trim();
      if (!raw) return "";
      if (raw.startsWith("[") && raw.endsWith("]")) {
        try {
          const parsed = JSON.parse(raw);
          if (Array.isArray(parsed) && parsed.length) {
            raw = String(parsed[0] ?? "").trim();
          }
        } catch (err) {
          const inner = raw.slice(1, -1);
          const first = inner.split(",")[0] || "";
          raw = first.replace(/^["']|["']$/g, "").trim();
        }
      } else if (raw.includes("*")) {
        raw = raw.split("*")[0].trim();
      }
      if (!raw) return "";
      let cleaned = raw;
      if (cleaned.includes(".") && !cleaned.includes(",")) {
        if (/^\d+\.\d+$/.test(cleaned)) {
          cleaned = cleaned.replace(",", ".");
        } else {
          cleaned = cleaned.replace(/\./g, "").replace(",", ".");
        }
      } else {
        cleaned = cleaned.replace(/\./g, "").replace(",", ".");
      }
      let num = Number(cleaned);
      if (Number.isNaN(num)) {
        const digits = raw.replace(/\D/g, "");
        if (!digits) return raw;
        num = Number(digits) / 100;
      }
      return formatPtBr(num);
    };

    const parsePtBr = (value) => {
      if (value === null || value === undefined) return null;
      const raw = String(value).trim();
      if (!raw) return null;
      if (raw.includes(",")) {
        const cleaned = raw.replace(/\./g, "").replace(",", ".");
        const num = Number(cleaned);
        return Number.isNaN(num) ? null : num;
      }
      const num = Number(raw);
      return Number.isNaN(num) ? null : num;
    };

    const formatValorDotacaoInput = () => {
      if (!valorInput) return;
      const digits = String(valorInput.value || "").replace(/\D/g, "");
      if (!digits) {
        valorInput.value = "";
        return;
      }
      const num = Number(digits) / 100;
      valorInput.value = formatPtBr(num);
    };

    const parseMaybeNumber = (value) => {
      if (value === null || value === undefined) return { raw: "", num: null };
      const raw = String(value).trim();
      if (!raw) return { raw, num: null };
      const num = Number(raw.replace(",", "."));
      return Number.isNaN(num) ? { raw, num: null } : { raw, num };
    };

    const compareValues = (left, right, op) => {
      const l = parseMaybeNumber(left);
      const r = parseMaybeNumber(right);
      if (l.num !== null && r.num !== null) {
        if (op === "eq") return l.num === r.num;
        if (op === "gt") return l.num > r.num;
        if (op === "lt") return l.num < r.num;
        if (op === "gte") return l.num >= r.num;
        if (op === "lte") return l.num <= r.num;
      }
      const lraw = l.raw.toLowerCase();
      const rraw = r.raw.toLowerCase();
      const cmp = lraw.localeCompare(rraw, "pt-BR", { sensitivity: "base" });
      if (op === "eq") return cmp === 0;
      if (op === "contains") return lraw.includes(rraw);
      if (op === "gt") return cmp > 0;
      if (op === "lt") return cmp < 0;
      if (op === "gte") return cmp >= 0;
      if (op === "lte") return cmp <= 0;
      return false;
    };

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;

    const getRows = () => {
      if (!summaryBody) return [];
      return Array.from(summaryBody.querySelectorAll(".dotacao-summary-row"));
    };
    const rowSnapshot = (row) => (row ? Object.fromEntries(Object.entries(row.dataset || {})) : {});
    const rowFromSnapshot = (dataset) => ({ dataset: dataset || {} });
    const openDotacaoFormulario = (action, row) => {
      try {
        sessionStorage.setItem(
          pendingStorageKey,
          JSON.stringify({
            action,
            dataset: rowSnapshot(row),
          })
        );
      } catch (err) {
        // noop
      }
      loadPage("cadastrar/dotacao/formulario");
    };
    const updateEditBadge = (action, row) => {
      if (!editBadge) return;
      const controle = String(row?.dataset?.chaveDotacao || row?.dataset?.id || "").trim();
      if (!controle || !action) {
        editBadge.textContent = "";
        editBadge.style.display = "none";
        return;
      }
      editBadge.textContent = action === "approve"
        ? `- Aprovação do registro ${controle}`
        : `- Edição do registro ${controle}`;
      editBadge.style.display = "inline";
    };

    const clearPagination = () => {
      if (paginationEl) paginationEl.innerHTML = "";
    };

    const setResultsVisible = (show) => {
      if (!dotacaoSummary) return;
      dotacaoSummary.classList.toggle("dotacao-summary-hidden", !show);
      dotacaoSummary.classList.toggle("consulta-summary-hidden", !show);
      if (!show) {
        getRows().forEach((row) => row.classList.remove("selected"));
        clearPagination();
      }
    };

    const getFilteredRows = () => {
      const rows = getRows();
      if (!criteria.length) return rows;
      return rows.filter((row) =>
        criteria.every((c) => {
          const field = c.field;
          const rowVal = row.dataset[field] || "";
          return compareValues(rowVal, c.value, c.op);
        })
      );
    };

    const renderPagination = (totalPages) => {
      if (!paginationEl) return;
      paginationEl.innerHTML = "";
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
        b.type = "button";
        b.className = "page-btn";
        b.textContent = label;
        if (disabled) b.disabled = true;
        if (active) b.classList.add("active");
        b.addEventListener("click", () => {
          if (disabled || page === currentPage) return;
          currentPage = page;
          renderSummaryPage();
          setFilterMsg("");
        });
        paginationEl.appendChild(b);
      };
      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);
      const maxButtons = 5;
      let start = Math.max(1, currentPage - Math.floor(maxButtons / 2));
      let end = Math.min(totalPages, start + maxButtons - 1);
      if (end - start + 1 < maxButtons) {
        start = Math.max(1, end - maxButtons + 1);
      }
      if (start > 1) {
        addBtn("1", 1, false, currentPage === 1);
        if (start > 2) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          paginationEl.appendChild(ellipsis);
        }
      }
      for (let p = start; p <= end; p += 1) {
        addBtn(String(p), p, false, p === currentPage);
      }
      if (end < totalPages) {
        const ellipsis = document.createElement("span");
        ellipsis.textContent = "...";
        paginationEl.appendChild(ellipsis);
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }
      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };

    const renderSummaryPage = () => {
      const allRows = getRows();
      const filtered = getFilteredRows();
      const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const pageRows = filtered.slice(startIdx, startIdx + pageSize);
      allRows.forEach((row) => {
        row.style.display = "none";
        row.classList.remove("selected");
      });
      pageRows.forEach((row) => {
        row.style.display = "";
      });
      renderPagination(totalPages);
    };

    const applyCriteriaToResults = (resetPage = true) => {
      if (resetPage) currentPage = 1;
      renderSummaryPage();
    };

    const normalizeOptionValue = (value) => String(value || "").replace(/\s+/g, " ").trim();

    const setSelectValueFallback = (select, value) => {
      if (!select) return;
      select.value = value;
      if (select.value === value) return;
      const target = normalizeOptionValue(value);
      if (!target) return;
      const option = Array.from(select.options).find((opt) => {
        const optVal = normalizeOptionValue(opt.value);
        const optText = normalizeOptionValue(opt.textContent || "");
        return optVal === target || optText === target;
      });
      if (option) select.value = option.value;
    };

    const extractJustificativaOnly = (value) => {
      const text = String(value || "").trim();
      const match = text.match(/^DOT\.[^.]*\.[^.]*\.\d+(?:\s+(.*))?$/);
      if (match) return match[1] || "";
      return text;
    };

    const selectRow = (row) => {
      getRows().forEach((el) => el.classList.remove("selected"));
      if (row) row.classList.add("selected");
    };

    const bindRowSelection = () => {
      getRows().forEach((row) => {
        row.addEventListener("click", () => {
          selectRow(row);
          setFilterMsg("");
        });
      });
    };

    const fillFormFromRow = async (row) => {
      if (!row) return;
      const adjConcedente = row.dataset.adjConcedente || "";
      const adjSolicitante = row.dataset.adjunta || "";
      const emprestada = adjConcedente && adjSolicitante && adjConcedente !== adjSolicitante;
      emprestadaRadios.forEach((radio) => {
        radio.checked = radio.value === (emprestada ? "sim" : "nao");
      });
      toggleAdjConcedente();
      if (adjConcedenteSelect && emprestada) {
        adjConcedenteSelect.value = adjConcedente;
      }
      if (idInput) idInput.value = row.dataset.id || "";
      selects.exercicio.value = row.dataset.exercicio || "";
      adjSelect.value = row.dataset.perfilId || "";
      selects.chave_planejamento.value = row.dataset.chave || "";
      selects.uo.value = row.dataset.uo || "";
      selects.programa.value = row.dataset.programaRaw || "";
      selects.acao_paoe.value = row.dataset.acaoPaoe || "";
      selects.produto.value = row.dataset.produto || "";
      selects.ug.value = row.dataset.ug || "";
      selects.regiao.value = row.dataset.regiao || "";
      setSelectValueFallback(selects.subacao_entrega, row.dataset.subacao || "");
      selects.etapa.value = row.dataset.etapa || "";
      selects.natureza_despesa.value = row.dataset.natureza || "";
      setSelectValueFallback(selects.elemento, row.dataset.elemento || "");
      setSelectValueFallback(selects.subelemento, row.dataset.subelemento || "");
      selects.fonte.value = row.dataset.fonte || "";
      selects.iduso.value = row.dataset.iduso || "";
      setSelectValueFallback(selects.elemento, row.dataset.elemento || "");
      setSelectValueFallback(selects.subelemento, row.dataset.subelemento || "");
      Object.values(selects).forEach((el) => {
        if (el && el.value) el.dataset.touched = "1";
      });
      if (valorInput) valorInput.value = formatPtBr(parsePtBr(row.dataset.valor) || 0);
      if (justificativaInput) {
        justificativaInput.value = extractJustificativaOnly(row.dataset.justificativa || "");
      }
      updateJustificativaPrefix();
      await loadOptions();
      toggleAdjConcedente();
      if (adjConcedenteSelect && emprestada) {
        setSelectValueFallback(adjConcedenteSelect, adjConcedente);
      }
      selects.exercicio.value = row.dataset.exercicio || "";
      adjSelect.value = row.dataset.perfilId || "";
      selects.chave_planejamento.value = row.dataset.chave || "";
      selects.uo.value = row.dataset.uo || "";
      selects.programa.value = row.dataset.programaRaw || "";
      selects.acao_paoe.value = row.dataset.acaoPaoe || "";
      selects.produto.value = row.dataset.produto || "";
      selects.ug.value = row.dataset.ug || "";
      selects.regiao.value = row.dataset.regiao || "";
      setSelectValueFallback(selects.subacao_entrega, row.dataset.subacao || "");
      selects.etapa.value = row.dataset.etapa || "";
      selects.natureza_despesa.value = row.dataset.natureza || "";
      setSelectValueFallback(selects.elemento, row.dataset.elemento || "");
      setSelectValueFallback(selects.subelemento, row.dataset.subelemento || "");
      selects.fonte.value = row.dataset.fonte || "";
      selects.iduso.value = row.dataset.iduso || "";
      Object.values(selects).forEach((el) => {
        if (el && el.value) el.dataset.touched = "1";
      });
      updateJustificativaPrefix();
      loadSaldo();
    };
    const restorePendingFormulario = async () => {
      if (pageMode !== "formulario") return;
      let pending = null;
      try {
        pending = JSON.parse(sessionStorage.getItem(pendingStorageKey) || "null");
        sessionStorage.removeItem(pendingStorageKey);
      } catch (err) {
        pending = null;
      }
      if (!pending || !pending.dataset) return;
      const row = rowFromSnapshot(pending.dataset);
      if (pending.action === "edit") {
        setApprovalMode(false);
        await fillFormFromRow(row);
        updateEditBadge("edit", row);
        return;
      }
      if (pending.action === "approve") {
        setApprovalMode(true);
        await fillFormFromRow(row);
        updateEditBadge("approve", row);
        if (approvalJustificativa) approvalJustificativa.value = "";
      }
    };

    const escapeHtml = (value) => {
      return String(value || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    };

    const buildRowFromPayload = (data) => {
      return {
        dataset: {
          exercicio: data.exercicio || "",
          adjunta: data.adjunta || "",
          chave: data.chave_planejamento || "",
          uo: data.uo || "",
          programaRaw: data.programa || "",
          acaoPaoe: data.acao_paoe || "",
          produto: data.produto || "",
          ug: data.ug || "",
          regiao: data.regiao || "",
          subacao: data.subacao_entrega || "",
          etapa: data.etapa || "",
          natureza: data.natureza_despesa || "",
          elemento: data.elemento || "",
          subelemento: data.subelemento || "",
          fonte: data.fonte || "",
          iduso: data.iduso || "",
          justificativa: data.justificativa_historico || "",
          valor: data.valor_dotacao || "",
          perfilId: data.perfil_id || "",
          chaveDotacao: data.chave_dotacao || "",
          adjConcedente: data.adj_concedente || "",
          adjConcedenteId: data.adj_concedente_id || "",
          statusAprovacao: data.status_aprovacao || "",
          aprovadoPor: data.aprovado_por || "",
          aprovadoPorNome: data.aprovado_por_nome || "",
          aprovadoPorPerfil: data.aprovado_por_perfil || "",
          dataAprovacao: data.data_aprovacao || "",
          motivoRejeicao: data.motivo_rejeicao || "",
          usuarioNome: data.usuario_nome || "",
          usuarioPerfil: data.usuario_perfil || "",
          criadoEm: data.criado_em || "",
          alteradoEm: data.alterado_em || "",
        },
      };
    };

    const buildPrintTable = (row) => {
      const adjSolic = row.dataset.adjunta || "";
      const adjConc = row.dataset.adjConcedente || "";
      const isEmp = adjConc && adjConc !== adjSolic;
      const fields = [
        ["Exercício", row.dataset.exercicio],
        ["Adjunta Solicitante", row.dataset.adjunta],
        ...(isEmp ? [["Adjunta Concedente", adjConc]] : []),
        ["Chave do Planejamento", row.dataset.chave],
        ["UO", row.dataset.uo],
        ["Programa", row.dataset.programaRaw],
        ["Ação/PAOE", row.dataset.acaoPaoe],
        ["Produto", row.dataset.produto],
        ["UG", row.dataset.ug],
        ["Região", row.dataset.regiao],
        ["Subação/Entrega", row.dataset.subacao],
        ["Etapa", row.dataset.etapa],
        ["Natureza de Despesa", row.dataset.natureza],
        ["Elemento de Despesa", row.dataset.elemento],
        ["Subelemento", row.dataset.subelemento],
        ["Fonte", row.dataset.fonte],
        ["Iduso", row.dataset.iduso],
        ["Justificativa/Histórico", row.dataset.justificativa],
        ["Valor da Dotação", formatPtBr(parsePtBr(row.dataset.valor) || 0)],
      ];
      const rowsHtml = fields
        .map(([label, value]) => `<tr><th>${label}</th><td>${escapeHtml(value)}</td></tr>`)
        .join("");
      return `
        <table class="print-table">
          <tbody>${rowsHtml}</tbody>
        </table>
      `;
    };

    const formatPrintDate = (value) => {
      if (!value) return "";
      const d = new Date(value);
      if (Number.isNaN(d.getTime())) return value;
      return d.toLocaleString("pt-BR");
    };

    const buildFooterText = (row) => {
      const nome = row?.dataset?.usuarioNome || "";
      const perfil = row?.dataset?.usuarioPerfil || "";
      const criado = row?.dataset?.criadoEm || "";
      const alterado = row?.dataset?.alteradoEm || "";
      const chave = row?.dataset?.chaveDotacao || "";
      let label = "criado em";
      let dataRef = criado;
      if (alterado && criado && alterado !== criado) {
        label = "alterado em";
        dataRef = alterado;
      } else if (alterado && !criado) {
        label = "alterado em";
        dataRef = alterado;
      }
      const dataFmt = formatPrintDate(dataRef);
      const parts = [];
      if (nome) {
        if (perfil) parts.push(`${nome} - ${perfil.toUpperCase()}`);
        else parts.push(nome);
      }
      if (dataFmt) parts.push(`${label} ${dataFmt}`);
      if (chave) parts.push(chave);
      return parts.join(" - ");
    };

    const buildApprovalText = (row) => {
      const status = String(row?.dataset?.statusAprovacao || "").trim().toLowerCase();
      const aprovadorNome = row?.dataset?.aprovadoPorNome || "";
      const aprovadorPerfil = row?.dataset?.aprovadoPorPerfil || "";
      const aprovador = aprovadorNome || row?.dataset?.aprovadoPor || "";
      const data = row?.dataset?.dataAprovacao || "";
      const dataFmt = formatPrintDate(data);
      if (status !== "aprovado" && status !== "rejeitado") return "";
      if (!aprovador && !dataFmt) return "";
      const parts = [];
      const aprovadorFull = aprovadorPerfil ? `${aprovador} - ${aprovadorPerfil.toUpperCase()}` : aprovador;
      if (status === "aprovado") {
        if (aprovador) parts.push(`Aprovado por ${aprovadorFull}`);
      } else {
        if (aprovador) parts.push(`Rejeitado por ${aprovadorFull}`);
      }
      if (dataFmt) parts.push(`- em ${dataFmt}`);
      return parts.join(" ");
    };

    const openPrintPopup = (rows) => {
      const content = rows.map((row) => buildPrintTable(row)).join('<div class="print-gap"></div>');
      const footerText = buildFooterText(rows[0]);
      const approvalText = buildApprovalText(rows[0]);
      const status = String(rows[0]?.dataset?.statusAprovacao || "").trim().toLowerCase();
      const adjSolic = rows[0]?.dataset?.adjunta || "";
      const adjConc = rows[0]?.dataset?.adjConcedente || "";
      const isEmprestada = adjConc && adjConc !== adjSolic;
      let watermarkText = "";
      if (status === "aguardando") watermarkText = "AGUARDANDO";
      if (status === "rejeitado") watermarkText = "Sem Validade";
      const showRegularizacao = isEmprestada && status !== "rejeitado";
      const html = `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Dotação Cadastrada</title>
  <style>
    body { font-family: Arial, sans-serif; color: #000; margin: 12px 20px 24px; padding-bottom: 70px; }
    .print-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; padding-bottom: 6px; border-bottom: 1px dashed #000; }
    .print-brand { display: flex; align-items: center; gap: 12px; }
    .print-brand img { height: 48px; }
    .print-brand-title { font-weight: 700; font-size: 16px; }
    .print-brand-subtitle { font-size: 12px; color: #333; }
    .print-title-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin: 0 0 12px; }
    .print-title { text-align: center; font-weight: 700; flex: 1; text-transform: uppercase; }
    .print-title-key { min-width: 200px; font-size: 12px; }
    .print-title-date { min-width: 200px; text-align: right; font-size: 12px; }
    .print-footer { position: fixed; left: 20px; right: 20px; bottom: 12px; border-top: 1px dashed #000; font-size: 12px; padding-top: 6px; display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .print-footer img { height: 36px; }
    .print-footer-text { flex: 1; text-align: center; line-height: 1.3; }
    .print-footer-approval { margin-top: 4px; font-weight: 400; }
    .print-footer-note { margin-top: 4px; font-size: 10px; font-weight: 400; }
    .print-table { width: 100%; border-collapse: collapse; margin-bottom: 12px; table-layout: auto; }
    .print-table th, .print-table td { border: 1px solid #000; padding: 6px 8px; text-align: left; font-size: 8px; vertical-align: top; word-break: break-word; }
    .print-table th { width: auto; white-space: nowrap; background: #dddddd; box-shadow: inset 0 0 0 9999px #dddddd; text-transform: uppercase; -webkit-print-color-adjust: exact; print-color-adjust: exact; color-adjust: exact; }
    .print-gap { height: 10px; }
    .print-watermark { position: fixed; top: 45%; left: 50%; transform: translate(-50%, -50%) rotate(-30deg); font-size: 65px; color: rgba(0,0,0,0.12); font-family: "Arial Black", Arial, sans-serif; text-transform: uppercase; white-space: pre-line; text-align: center; pointer-events: none; }
  </style>
</head>
<body>
  ${watermarkText ? `<div class="print-watermark">${watermarkText}</div>` : ""}
  <div class="print-header">
    <div class="print-brand">
      <img src="/static/img/logo.jpg" alt="Logo" />
      <div class="print-brand-text">
        <div class="print-brand-title">Sistema de Planejamento e Orçamento</div>
        <div class="print-brand-subtitle">SPO-NGER-SEDUCMT</div>
      </div>
    </div>
  </div>
  <div class="print-title-row">
    <div class="print-title-key">${escapeHtml(rows[0]?.dataset?.chaveDotacao || "")}</div>
    <div class="print-title">DOTAÇÃO CADASTRADA</div>
    <div class="print-title-date">${formatPrintDate((rows[0]?.dataset?.alteradoEm && rows[0]?.dataset?.alteradoEm !== rows[0]?.dataset?.criadoEm) ? rows[0]?.dataset?.alteradoEm : rows[0]?.dataset?.criadoEm)}</div>
  </div>
  <div style="height: 36px;"></div>
  ${content}
  <div class="print-footer">
    <img src="/static/img/logo.jpg" alt="Logo" />
    <div class="print-footer-text">
      <div>${footerText}</div>
      ${approvalText ? `<div class="print-footer-approval">${approvalText}</div>` : ""}
      ${showRegularizacao ? `<div class="print-footer-note">Dotação estará sujeita a regularização</div>` : ""}
    </div>
    <img src="/static/img/logoseduc.jpg" alt="Logo Seduc" />
  </div>
</body>
</html>`;
      const win = window.open("", "_blank");
      if (!win) {
        setFilterMsg("Popup bloqueado. Libere o navegador para imprimir.", true);
        return;
      }
      win.document.open();
      win.document.write(html);
      win.document.close();
      win.focus();
      setTimeout(() => {
        win.print();
      }, 300);
    };

    const setSelectOptions = (select, options, current) => {
      const currentValue = String(current || "");
      const optionValues = (options || []).map((opt) => String(opt || ""));
      const keep = currentValue && optionValues.includes(currentValue) ? currentValue : "";
      select.innerHTML = '<option value="">Selecione...</option>';
      optionValues.forEach((opt) => {
        const o = document.createElement("option");
        o.value = opt;
        o.textContent = opt;
        select.appendChild(o);
      });
      if (keep) {
        select.value = keep;
      } else if (currentValue) {
        const o = document.createElement("option");
        o.value = currentValue;
        o.textContent = currentValue;
        select.appendChild(o);
        select.value = currentValue;
      }
    };

    const setAdjOptions = (options, current) => {
      const currentValue = String(current || "");
      const keep = options.some((o) => String(o.id) === currentValue) ? currentValue : "";
      adjSelect.innerHTML = '<option value="">Selecione...</option>';
      options.forEach((opt) => {
        const o = document.createElement("option");
        o.value = String(opt.id);
        o.textContent = opt.label || "";
        adjSelect.appendChild(o);
      });
      if (keep) {
        adjSelect.value = keep;
      } else if (currentValue) {
        const o = document.createElement("option");
        o.value = currentValue;
        o.textContent = currentValue;
        adjSelect.appendChild(o);
        adjSelect.value = currentValue;
      }
    };

    const setPerfilOptions = (select, options, current) => {
      if (!select) return;
      const currentValue = String(current || "");
      const optionValues = (options || []).map((opt) => String(opt || ""));
      const keep = optionValues.includes(currentValue) ? currentValue : "";
      select.innerHTML = '<option value="">Selecione...</option>';
      optionValues.forEach((opt) => {
        const o = document.createElement("option");
        o.value = opt;
        o.textContent = opt;
        select.appendChild(o);
      });
      if (keep) {
        select.value = keep;
      } else if (currentValue) {
        const o = document.createElement("option");
        o.value = currentValue;
        o.textContent = currentValue;
        select.appendChild(o);
        select.value = currentValue;
      }
    };

    const getCurrentYear = () => {
      return new Intl.DateTimeFormat("pt-BR", { timeZone: "America/Manaus", year: "numeric" }).format(
        new Date()
      );
    };

    const loadOptions = async () => {
      const params = currentSaldoFilters();
      const requestSeq = ++dotacaoOptionsRequestSeq;
      const url = new URL("/api/dotacao/options", window.location.origin);
      Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
      try {
        const res = await fetch(url, { headers: { "X-Requested-With": "fetch" } });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar opcoes.");
        if (requestSeq !== dotacaoOptionsRequestSeq) return;
        const currentValues = Object.fromEntries(
          Object.entries(selects).map(([key, el]) => [key, el?.value || ""])
        );
        const currentAdj = adjSelect.value;
        const currentAdjConcedente = adjConcedenteSelect?.value || "";
        updating = true;
        Object.entries(selects).forEach(([key, el]) => {
          let opts = (data.options && data.options[key]) || [];
          if (key === "exercicio") {
            opts = [getCurrentYear()];
          }
          setSelectOptions(el, opts, currentValues[key]);
        });
        if (Array.isArray(data.adj)) {
          setAdjOptions(data.adj, currentAdj);
        }
        if (Array.isArray(data.perfis) && adjConcedenteSelect) {
          setPerfilOptions(adjConcedenteSelect, data.perfis, currentAdjConcedente);
        }
        updateJustificativaPrefix();
      } catch (err) {
        console.error(err);
      } finally {
        updating = false;
      }
    };

    const loadSaldo = async () => {
      if (!saldoInput) return;
      const params = currentSaldoFilters();
      const requiredKeys = ["exercicio", "chave_planejamento"];
      const missing = requiredKeys.some((k) => !params[k]);
      if (missing) {
        saldoInput.value = "";
        if (saldoInfo) saldoInfo.textContent = "";
        if (saldoDebug) {
          saldoDebug.textContent = "";
          saldoDebug.classList.remove("dotacao-saldo-debug-active");
        }
        return;
      }
      const url = new URL("/api/dotacao/saldo", window.location.origin);
      Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
      try {
        const res = await fetch(url, { headers: { "X-Requested-With": "fetch" } });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao calcular saldo.");
        saldoInput.value = formatPtBr(data.saldo);
        if (saldoInfo) {
          saldoInfo.textContent = "";
        }
        if (saldoDebug) {
          const pedCount = data.ped_count ?? 0;
          const empCount = data.emp_count ?? 0;
          const dotCount = data.dotacao_count ?? 0;
          const pedSum = formatPtBr(data.valor_ped ?? 0);
          const empSum = formatPtBr(data.valor_emp_liquido ?? 0);
          const dotSum = formatPtBr(data.valor_dotacao ?? 0);
          const planSum = formatPtBr(data.valor_atual ?? 0);
          saldoDebug.textContent =
            `Plan21: ${planSum} | Dotacao: ${dotSum} (${dotCount}) | PED: ${pedSum} (${pedCount}) | EMP: ${empSum} (${empCount})`;
          if (saldoDebug.textContent.trim()) {
            saldoDebug.classList.add("dotacao-saldo-debug-active");
          } else {
            saldoDebug.classList.remove("dotacao-saldo-debug-active");
          }
        }
      } catch (err) {
        console.error(err);
        saldoInput.value = "";
        if (saldoInfo) saldoInfo.textContent = "";
        if (saldoDebug) {
          saldoDebug.textContent = "";
          saldoDebug.classList.remove("dotacao-saldo-debug-active");
        }
      }
    };

    Object.entries(selects).forEach(([key, el]) => {
      el.addEventListener("change", () => {
        if (updating) return;
        if (!baseSaldoKeys.has(key)) {
          el.dataset.touched = "1";
        }
        loadOptions();
        loadSaldo();
        updateJustificativaPrefix();
      });
    });
    adjSelect.addEventListener("change", updateJustificativaPrefix);
    emprestadaRadios.forEach((radio) => {
      radio.addEventListener("change", () => {
        toggleAdjConcedente();
      });
    });
    if (valorInput) {
      valorInput.addEventListener("input", formatValorDotacaoInput);
      valorInput.addEventListener("blur", formatValorDotacaoInput);
    }

    if (filterForm) {
      renderCriteria();
      if (filterAdd) {
        filterAdd.addEventListener("click", () => {
          const field = String(filterField?.value || "");
          const op = String(filterOp?.value || "eq");
          const value = String(filterValue?.value || "").trim();
          if (!field) {
            setFilterMsg("Selecione um campo.", true);
            return;
          }
          if (!value) {
            setFilterMsg("Informe um valor.", true);
            return;
          }
          if (field !== "exercicio" && !criteria.some((c) => c.field === "exercicio")) {
            setFilterMsg("Informe um critério de Exercício antes dos demais.", true);
            return;
          }
          criteria.push({ field, op, value });
          criteriaSelected = criteria.length - 1;
          renderCriteria();
          setFilterMsg("");
          if (filterValue) filterValue.value = "";
        });
      }
      if (filterRemove) {
        filterRemove.addEventListener("click", () => {
          if (criteriaSelected < 0 || criteriaSelected >= criteria.length) {
            setFilterMsg("Selecione um criterio para remover.", true);
            return;
          }
          criteria.splice(criteriaSelected, 1);
          criteriaSelected = -1;
          renderCriteria();
          setResultsVisible(false);
          setFilterMsg("");
        });
      }
      if (filterClear) {
        filterClear.addEventListener("click", () => {
          criteria.length = 0;
          criteriaSelected = -1;
          renderCriteria();
          setResultsVisible(false);
          setFilterMsg("");
        });
      }
      if (filterCancel) {
        filterCancel.addEventListener("click", () => {
          criteria.length = 0;
          criteriaSelected = -1;
          renderCriteria();
          setResultsVisible(false);
          if (filterField) filterField.value = "";
          if (filterOp) filterOp.value = "eq";
          if (filterValue) filterValue.value = "";
          setFilterMsg("");
        });
      }
      if (filterApply) {
        filterApply.addEventListener("click", () => {
          if (!criteria.some((c) => c.field == "exercicio")) {
            setFilterMsg("Informe o critério de Exercício antes de consultar.", true);
            return;
          }
          setResultsVisible(true);
          applyCriteriaToResults(true);
          setFilterMsg("");
        });
      }
    }

    const formatSummaryValues = () => {
      getRows().forEach((row) => {
        const cell = row.querySelector(".dotacao-summary-valor");
        if (!cell) return;
        const raw = row.dataset.valor || cell.textContent || "";
        cell.textContent = formatPtBr(parsePtBr(raw) || 0);
      });
    };

    bindRowSelection();
    formatSummaryValues();

    if (editBtn) {
      editBtn.addEventListener("click", async () => {
        if (dotacaoSummary && dotacaoSummary.style.display === "none") {
          setFilterMsg("Consulte antes de editar.", true);
          return;
        }
        const selected = summaryBody?.querySelector(".dotacao-summary-row.selected");
        if (!selected) {
          setFilterMsg("Selecione um registro para editar.", true);
          return;
        }
        const criadorPerfilId = String(selected.dataset.criadorPerfilId || "").trim();
        if (!criadorPerfilId || !currentUserPerfilId || currentUserPerfilId !== criadorPerfilId) {
          setFilterMsg("Usuário sem permissão para editar a dotação atual.", true);
          return;
        }
        const status = String(selected.dataset.statusAprovacao || "").trim().toLowerCase();
        if (status && status !== "aguardando") {
          setFilterMsg("Somente dotações com status Aguardando podem ser editadas.", true);
          return;
        }
        if (pageMode === "consultar") {
          openDotacaoFormulario("edit", selected);
          return;
        }
        setApprovalMode(false);
        await fillFormFromRow(selected);
        updateEditBadge("edit", selected);
      });
    }

    if (deleteBtn) {
      deleteBtn.addEventListener("click", async () => {
        if (dotacaoSummary && dotacaoSummary.style.display === "none") {
          setFilterMsg("Consulte antes de excluir.", true);
          return;
        }
        const selected = summaryBody?.querySelector(".dotacao-summary-row.selected");
        if (!selected) {
          setFilterMsg("Selecione um registro para excluir.", true);
          return;
        }
        const criadorPerfilId = String(selected.dataset.criadorPerfilId || "").trim();
        if (!criadorPerfilId || !currentUserPerfilId || currentUserPerfilId !== criadorPerfilId) {
          setFilterMsg("Usuário sem permissão para excluir a dotação atual.", true);
          return;
        }
        const status = String(selected.dataset.statusAprovacao || "").trim().toLowerCase();
        if (status && status !== "aguardando") {
          setFilterMsg("Somente dotações com status Aguardando podem ser excluídas.", true);
          return;
        }
        const dotacaoId = selected.dataset.id;
        if (!dotacaoId) {
          setFilterMsg("Registro inválido para exclusão.", true);
          return;
        }
        try {
          const res = await fetch(`/api/dotacao/${encodeURIComponent(dotacaoId)}`, {
            method: "DELETE",
            headers: { "X-Requested-With": "fetch" },
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao excluir.");
          selected.remove();
          renderSummaryPage();
          setFilterMsg(data.message || "Dotação excluída.", false);
        } catch (err) {
          console.error(err);
          setFilterMsg(err.message || "Falha ao excluir.", true);
        }
      });
    }

    if (printBtn) {
      printBtn.addEventListener("click", () => {
        if (dotacaoSummary && dotacaoSummary.style.display === "none") {
          setFilterMsg("Consulte antes de imprimir.", true);
          return;
        }
        const selected = summaryBody?.querySelector(".dotacao-summary-row.selected");
        if (!selected) {
          setFilterMsg("Selecione um registro para imprimir.", true);
          flashSummaryWarning();
          return;
        }
        openPrintPopup([selected]);
      });
    }

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const dotacaoId = idInput ? String(idInput.value || "") : "";
      msg.textContent = approvalMode ? "Aprovando..." : dotacaoId ? "Atualizando..." : "Salvando...";
      msg.classList.remove("text-error");
      if (approvalMode) {
        if (!dotacaoId) {
          msg.textContent = "Registro inválido para aprovação.";
          msg.classList.add("text-error");
          return;
        }
        const aprovado = Array.from(approvalRadios).find((r) => r.checked)?.value || "sim";
        const justificativa = String(approvalJustificativa?.value || "").trim();
        if (!justificativa) {
          msg.textContent = "Justificativa obrigatória.";
          msg.classList.add("text-error");
          return;
        }
        try {
          const res = await fetch(`/api/dotacao/${encodeURIComponent(dotacaoId)}/aprovar`, {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify({
              dotacao_aprovada: aprovado,
              motivo_rejeicao: justificativa,
            }),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao aprovar.");
          msg.textContent = data.message || "Dotação atualizada.";
          if (data.dotacao) {
            openPrintPopup([buildRowFromPayload(data.dotacao)]);
          }
          setApprovalMode(false);
          form.reset();
          if (idInput) idInput.value = "";
          if (saldoInput) saldoInput.value = "";
          Object.values(selects).forEach((el) => {
            delete el.dataset.touched;
          });
          emprestadaRadios.forEach((radio) => {
            radio.checked = radio.value === "nao";
          });
          toggleAdjConcedente();
          await loadPage("cadastrar/dotacao/consultar");
        } catch (err) {
          console.error(err);
          msg.textContent = err.message || "Falha ao aprovar.";
          msg.classList.add("text-error");
        }
        return;
      }
      const payload = {
        exercicio: selects.exercicio.value,
        perfil_id: adjSelect.value,
        dotacao_emprestada: isEmprestada() ? "sim" : "nao",
        adj_concedente: isEmprestada() ? adjConcedenteSelect?.value || "" : getAdjLabel(),
        chave_planejamento: selects.chave_planejamento.value,
        uo: selects.uo.value,
        programa: selects.programa.value,
        acao_paoe: selects.acao_paoe.value,
        produto: selects.produto.value,
        ug: selects.ug.value,
        regiao: selects.regiao.value,
        subacao_entrega: selects.subacao_entrega.value,
        etapa: selects.etapa.value,
        natureza_despesa: selects.natureza_despesa.value,
        elemento: elementoInput.value,
        subelemento: selects.subelemento.value,
        fonte: selects.fonte.value,
        iduso: selects.iduso.value,
        valor_dotacao: valorInput.value,
        justificativa_historico: justificativaInput.value,
      };
      try {
        const url = dotacaoId ? `/api/dotacao/${encodeURIComponent(dotacaoId)}` : "/api/dotacao";
        const method = dotacaoId ? "PUT" : "POST";
        const res = await fetch(url, {
          method,
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao salvar.");
        msg.textContent = data.message || (dotacaoId ? "Dotacao atualizada." : "Dotacao cadastrada.");
        if (data.dotacao) {
          openPrintPopup([buildRowFromPayload(data.dotacao)]);
        }
        form.reset();
        if (idInput) idInput.value = "";
        if (saldoInput) saldoInput.value = "";
        Object.values(selects).forEach((el) => {
          delete el.dataset.touched;
        });
        emprestadaRadios.forEach((radio) => {
          radio.checked = radio.value === "nao";
        });
        setApprovalMode(false);
        toggleAdjConcedente();
        await loadPage("cadastrar/dotacao/consultar");
      } catch (err) {
        console.error(err);
        msg.textContent = err.message || "Falha ao salvar.";
        msg.classList.add("text-error");
      }
    });

    if (clearBtn) {
      clearBtn.addEventListener("click", () => {
        setApprovalMode(false);
        form.reset();
        if (idInput) idInput.value = "";
        msg.textContent = "";
        msg.classList.remove("text-error");
        if (saldoInput) saldoInput.value = "";
        if (saldoInfo) saldoInfo.textContent = "";
        if (saldoDebug) saldoDebug.textContent = "";
        Object.values(selects).forEach((el) => {
          delete el.dataset.touched;
        });
        emprestadaRadios.forEach((radio) => {
          radio.checked = radio.value === "nao";
        });
        setApprovalMode(false);
        toggleAdjConcedente();
        loadOptions();
        updateJustificativaPrefix();
      });
    }

    if (approveBtn) {
      approveBtn.addEventListener("click", async () => {
        if (dotacaoSummary && dotacaoSummary.style.display === "none") {
          setFilterMsg("Consulte antes de aprovar.", true);
          return;
        }
        const selected = summaryBody?.querySelector(".dotacao-summary-row.selected");
        if (!selected) {
          setFilterMsg("Selecione um registro para aprovar.", true);
          return;
        }
        const status = String(selected.dataset.statusAprovacao || "").trim().toLowerCase();
        if (status && status !== "aguardando") {
          setFilterMsg("Somente dotações com status Aguardando podem ser aprovadas.", true);
          return;
        }
        const adjConcedenteId = String(selected.dataset.adjConcedenteId || "").trim();
        if (!adjConcedenteId) {
          setFilterMsg("Adjunta Concedente não definida.", true);
          return;
        }
        if (!currentUserPerfilId || currentUserPerfilId !== String(selected.dataset.adjConcedenteId || "").trim()) {
          setFilterMsg("Usuário sem permissão para aprovar a dotação atual.", true);
          return;
        }
        if (pageMode === "consultar") {
          openDotacaoFormulario("approve", selected);
          return;
        }
        setApprovalMode(true);
        await fillFormFromRow(selected);
        updateEditBadge("approve", selected);
        if (approvalJustificativa) approvalJustificativa.value = "";
      });
    }

    loadOptions();
    loadSaldo();
    updateJustificativaPrefix();
    toggleAdjConcedente();
    setApprovalMode(false);
    restorePendingFormulario();
    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        pageSize = parseInt(pageSizeSelect.value || "20", 10) || 20;
        if (dotacaoSummary && dotacaoSummary.style.display !== "none") {
          renderSummaryPage();
        }
      });
    }

    setResultsVisible(false);
  }

  function toggleReportEmptyState({ tableEl, emptyEl, btnDownloadEl, pagerEl, hasRows }) {
    const tableWrapEl = tableEl ? tableEl.closest(".table-responsive") : null;
    const tableFootEl = tableEl ? tableEl.closest(".card")?.querySelector(".table-foot") : null;
    if (emptyEl) emptyEl.hidden = hasRows;
    if (tableWrapEl) tableWrapEl.style.display = hasRows ? "" : "none";
    if (tableFootEl) tableFootEl.style.display = hasRows ? "" : "none";
    if (btnDownloadEl) btnDownloadEl.disabled = !hasRows;
    if (!hasRows && pagerEl) pagerEl.innerHTML = "";
  }

  function initTetoSeduc() {
    const form = document.getElementById("form-teto-seduc");
    if (!form || form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const exercicio = document.getElementById("teto-seduc-exercicio");
    const submitBtn = document.getElementById("teto-seduc-submit");
    const loading = document.getElementById("teto-seduc-loading");
    const msg = document.getElementById("teto-seduc-msg");

    const clearMessage = () => {
      if (!msg) return;
      msg.textContent = "";
      msg.classList.remove("text-error");
    };

    form.addEventListener("input", clearMessage);
    form.addEventListener("change", clearMessage);

    if (exercicio) {
      exercicio.addEventListener("input", () => {
        exercicio.value = exercicio.value.replace(/\D/g, "").slice(0, 4);
      });
    }

    const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

    const waitForJob = async (jobId) => {
      let networkFailures = 0;
      for (let attempt = 0; attempt < 600; attempt += 1) {
        await wait(1000);
        try {
          const response = await fetch(
            `/api/teto-seduc/status/${encodeURIComponent(jobId)}`,
            { headers: { "X-Requested-With": "fetch" } }
          );
          const data = await response.json();
          if (!response.ok) {
            throw new Error(data.error || "Falha ao consultar o processamento.");
          }
          networkFailures = 0;
          if (msg) msg.textContent = data.message || "Processando...";
          if (data.state === "processamento finalizado") return data;
          if (data.state === "falha no processamento") {
            throw new Error(data.message || "Falha ao processar o arquivo.");
          }
        } catch (err) {
          networkFailures += 1;
          if (networkFailures >= 5 || !(err instanceof TypeError)) throw err;
        }
      }
      throw new Error("O processamento excedeu o tempo máximo de acompanhamento.");
    };

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      if (!form.reportValidity()) return;

      if (msg) {
        msg.textContent = "";
        msg.classList.remove("text-error");
      }
      if (loading) loading.style.display = "inline";
      if (submitBtn) submitBtn.disabled = true;
      showAppLoading(
        "Processando Teto - SEDUC...",
        "Aguarde enquanto o arquivo é tratado e gravado no banco de dados."
      );

      try {
        const response = await fetch("/api/teto-seduc/upload", {
          method: "POST",
          headers: { "X-Requested-With": "fetch" },
          body: new FormData(form),
        });
        const data = await response.json();
        if (!response.ok) {
          throw new Error(data.error || "Falha ao processar o arquivo.");
        }
        if (!data.job_id) {
          throw new Error("O servidor não retornou o identificador do processamento.");
        }
        if (msg) msg.textContent = data.message || "Processamento iniciado.";
        const result = await waitForJob(data.job_id);
        if (msg) msg.textContent = result.message || "Processamento concluído.";
        showToast(result.message || "Teto - SEDUC atualizado.", "success", 6000);
        form.reset();
      } catch (err) {
        console.error(err);
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
        showToast(err.message, "error", 6000);
      } finally {
        hideAppLoading();
        if (loading) loading.style.display = "none";
        if (submitBtn) submitBtn.disabled = false;
      }
    });
  }

  function initChavePlanejamentoRegra() {
    const form = document.getElementById("form-chave-regra");
    const msg = document.getElementById("chave-regra-msg");
    if (!form || !msg) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const idEl = document.getElementById("chave-regra-id");
    const tipoEl = document.getElementById("chave-regra-tipo");
    const origemEl = document.getElementById("chave-regra-origem");
    const origemWrapEl = document.getElementById("chave-regra-origem-wrap");
    const origemLabelEl = document.getElementById("chave-regra-origem-label");
    const destinoEl = document.getElementById("chave-regra-destino");
    const destinoWrapEl = document.getElementById("chave-regra-destino-wrap");
    const arquivoWrapEl = document.getElementById("chave-regra-arquivo-wrap");
    const arquivoEl = document.getElementById("chave-regra-arquivo");
    const observacaoEl = document.getElementById("chave-regra-observacao");
    const ativoEl = document.getElementById("chave-regra-ativo");
    const cancelBtn = document.getElementById("chave-regra-cancel");
    const tableBody = document.getElementById("chave-regra-table-body");
    const pageSizeEl = document.getElementById("chave-regra-page-size");
    const paginationEl = document.getElementById("chave-regra-pagination");
    const filtroMsg = document.getElementById("chave-regra-filtro-msg");
    const filtroCampoEl = document.getElementById("chave-regra-filtro-campo");
    const filtroTipoEl = document.getElementById("chave-regra-filtro-tipo");
    const filtroValorEl = document.getElementById("chave-regra-filtro-valor");
    const filtroListEl = document.getElementById("chave-regra-filtro-list");
    const filtroAddBtn = document.getElementById("chave-regra-filtro-add");
    const filtroRemoveBtn = document.getElementById("chave-regra-filtro-remove");
    const filtroAplicarBtn = document.getElementById("chave-regra-filtro-aplicar");
    const filtroLimparBtn = document.getElementById("chave-regra-filtro-limpar");
    const rawFeatures = userMeta?.dataset?.features ? JSON.parse(userMeta.dataset.features || "[]") : [];
    const hasPlanejamentoUpload =
      Number(userNivel || 0) === 1 ||
      rawFeatures.includes("atualizar/chaves_planejamento_upload") ||
      rawFeatures.includes("painel/chaves_planejamento_upload");
    const tipoPlanejamentoOption = tipoEl
      ? tipoEl.querySelector("option[value='chaves_planejamento']")
      : null;

    const allRows = tableBody
      ? Array.from(tableBody.querySelectorAll("tr[data-id]"))
      : [];
    let filteredRows = [...allRows];
    let currentPage = 1;
    let pageSize = parseInt(pageSizeEl?.value || "5", 10) || 5;
    let criteriaApplied = false;
    const criteria = [];
    let criteriaSelected = -1;

    const syncTipoFields = () => {
      const tipo = String(tipoEl?.value || "").trim();
      const isPlanejamento = tipo === "chaves_planejamento";
      const isForcar = tipo === "forcar_chave";
      if (origemLabelEl) origemLabelEl.textContent = isForcar ? "Nº EMP" : "Chave origem";
      if (origemWrapEl) origemWrapEl.style.display = isPlanejamento ? "none" : "";
      if (destinoWrapEl) destinoWrapEl.style.display = isPlanejamento ? "none" : "";
      if (arquivoWrapEl) arquivoWrapEl.style.display = isPlanejamento ? "" : "none";
      if (destinoEl) destinoEl.required = !isPlanejamento;
      if (origemEl) origemEl.required = !isPlanejamento;
      if (!isPlanejamento && arquivoEl) arquivoEl.value = "";
      if (isPlanejamento && !hasPlanejamentoUpload) {
        if (tipoEl) tipoEl.value = "";
        if (arquivoEl) arquivoEl.value = "";
        if (arquivoWrapEl) arquivoWrapEl.style.display = "none";
        if (origemWrapEl) origemWrapEl.style.display = "";
        if (destinoWrapEl) destinoWrapEl.style.display = "";
        if (msg) {
          msg.textContent = "Usuario sem permissao para upload anual de chaves_planejamento.";
          msg.classList.add("text-error");
        }
      }
    };

    const limparForm = () => {
      if (idEl) idEl.value = "";
      if (tipoEl) tipoEl.value = "";
      if (origemEl) origemEl.value = "";
      if (destinoEl) destinoEl.value = "";
      if (arquivoEl) arquivoEl.value = "";
      if (observacaoEl) observacaoEl.value = "";
      if (ativoEl) ativoEl.checked = true;
      syncTipoFields();
    };

    const fillForm = (row) => {
      if (!row) return;
      if (idEl) idEl.value = row.dataset.id || "";
      if (tipoEl) tipoEl.value = row.dataset.tipo || "";
      if (origemEl) origemEl.value = row.dataset.origem || "";
      if (destinoEl) destinoEl.value = row.dataset.destino || "";
      if (observacaoEl) observacaoEl.value = row.dataset.observacao || "";
      if (ativoEl) ativoEl.checked = (row.dataset.ativo || "1") === "1";
      syncTipoFields();
    };

    const setFiltroMsg = (text, isError = false) => {
      if (!filtroMsg) return;
      filtroMsg.textContent = text || "";
      filtroMsg.classList.toggle("text-error", !!isError);
    };

    const criteriaFieldLabel = (field) => {
      if (field === "id") return "ID";
      if (field === "chave_origem") return "Chave de Origem";
      if (field === "chave_destino") return "Chave de Destino";
      if (field === "num_emp") return "Nº EMP";
      return field || "";
    };

    const getRowFieldValue = (row, field) => {
      if (!row) return "";
      if (field === "id") return String(row.dataset.id || "");
      if (field === "chave_origem") return String(row.dataset.origem || "");
      if (field === "chave_destino") return String(row.dataset.destino || "");
      if (field === "num_emp") {
        return String(row.dataset.tipo || "") === "forcar_chave"
          ? String(row.dataset.origem || "")
          : "";
      }
      return "";
    };

    const normalizeText = (v) =>
      String(v || "")
        .trim()
        .toLowerCase()
        .replace(/\s+/g, " ");

    const compareField = (field, rowValue, filterValue) => {
      const rv = normalizeText(rowValue);
      const fv = normalizeText(filterValue);
      if (!fv) return false;
      if (field === "id") return rv === fv;
      if (field === "num_emp") {
        const onlyDigits = (s) => String(s || "").replace(/\D+/g, "");
        return onlyDigits(rv) === onlyDigits(fv) && onlyDigits(fv) !== "";
      }
      return rv.includes(fv);
    };

    const renderCriteriaList = () => {
      if (!filtroListEl) return;
      filtroListEl.innerHTML = "";
      criteria.forEach((c, idx) => {
        const li = document.createElement("li");
        li.className = "pill";
        li.textContent = `${criteriaFieldLabel(c.field)} | ${c.tipo} | ${c.value}`;
        if (idx === criteriaSelected) li.classList.add("active");
        li.addEventListener("click", () => {
          criteriaSelected = idx;
          renderCriteriaList();
        });
        filtroListEl.appendChild(li);
      });
    };

    const renderPagination = (totalPages) => {
      if (!paginationEl) return;
      paginationEl.innerHTML = "";
      if (totalPages <= 1) return;
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
        b.type = "button";
        b.textContent = label;
        if (disabled) b.disabled = true;
        if (active) b.classList.add("active");
        b.addEventListener("click", () => {
          if (disabled || page === currentPage) return;
          currentPage = page;
          renderTablePage();
        });
        paginationEl.appendChild(b);
      };

      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);

      const maxButtons = 5;
      const start = Math.max(1, Math.min(currentPage - 2, totalPages - maxButtons + 1));
      const end = Math.min(totalPages, start + maxButtons - 1);
      for (let p = start; p <= end; p += 1) {
        addBtn(String(p), p, false, p === currentPage);
      }

      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };

    const renderTablePage = () => {
      if (!criteriaApplied) {
        allRows.forEach((row) => {
          row.style.display = "none";
        });
        if (paginationEl) paginationEl.innerHTML = "";
        return;
      }
      allRows.forEach((row) => {
        row.style.display = "none";
      });
      const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const start = (currentPage - 1) * pageSize;
      const pageRows = filteredRows.slice(start, start + pageSize);
      pageRows.forEach((row) => {
        row.style.display = "";
      });
      renderPagination(totalPages);
    };

    const applyFilters = () => {
      if (!criteria.length) {
        criteriaApplied = false;
        filteredRows = [];
        currentPage = 1;
        renderTablePage();
        setFiltroMsg("Adicione ao menos um critério para consultar.", true);
        return;
      }

      filteredRows = allRows.filter((row) =>
        criteria.every((c) => {
          const rowTipo = String(row.dataset.tipo || "");
          if (rowTipo !== c.tipo) return false;
          return compareField(c.field, getRowFieldValue(row, c.field), c.value);
        })
      );

      criteriaApplied = true;
      currentPage = 1;
      renderTablePage();
      if (!filteredRows.length) {
        setFiltroMsg("Nenhuma regra encontrada para os filtros informados.", true);
      } else {
        setFiltroMsg(`Total encontrado: ${filteredRows.length}`);
      }
    };

    if (filtroAddBtn) {
      filtroAddBtn.addEventListener("click", () => {
        const field = String(filtroCampoEl?.value || "").trim();
        const tipo = String(filtroTipoEl?.value || "").trim();
        const value = String(filtroValorEl?.value || "").trim();
        if (!field || !tipo || !value) {
          setFiltroMsg("Preencha Campo, Operador e Valor para adicionar o critério.", true);
          return;
        }
        criteria.push({ field, tipo, value });
        criteriaSelected = criteria.length - 1;
        setFiltroMsg("");
        renderCriteriaList();
      });
    }

    if (filtroRemoveBtn) {
      filtroRemoveBtn.addEventListener("click", () => {
        if (criteriaSelected < 0 || criteriaSelected >= criteria.length) {
          setFiltroMsg("Selecione um critério para remover.", true);
          return;
        }
        criteria.splice(criteriaSelected, 1);
        criteriaSelected = -1;
        renderCriteriaList();
        setFiltroMsg("");
      });
    }

    if (filtroAplicarBtn) {
      filtroAplicarBtn.addEventListener("click", () => applyFilters());
    }
    if (filtroLimparBtn) {
      filtroLimparBtn.addEventListener("click", () => {
        if (filtroCampoEl) filtroCampoEl.value = "";
        if (filtroTipoEl) filtroTipoEl.value = "";
        if (filtroValorEl) filtroValorEl.value = "";
        criteria.length = 0;
        criteriaSelected = -1;
        renderCriteriaList();
        setFiltroMsg("");
        criteriaApplied = false;
        filteredRows = [];
        currentPage = 1;
        renderTablePage();
      });
    }
    [filtroValorEl].forEach((el) => {
      if (!el) return;
      el.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter") {
          ev.preventDefault();
          if (filtroAddBtn) filtroAddBtn.click();
        }
      });
    });
    if (pageSizeEl) {
      pageSizeEl.addEventListener("change", () => {
        pageSize = parseInt(pageSizeEl.value || "5", 10) || 5;
        currentPage = 1;
        renderTablePage();
      });
    }

    if (tableBody) {
      tableBody.addEventListener("click", async (ev) => {
        const selectBtn = ev.target.closest(".select-chave-regra");
        if (selectBtn) {
          const row = selectBtn.closest("tr[data-id]");
          if (row) {
            const rowTipo = String(row.dataset.tipo || "");
            if (rowTipo === "chaves_planejamento" && !hasPlanejamentoUpload) {
              msg.textContent = "Usuario sem permissao para editar chaves_planejamento.";
              msg.classList.add("text-error");
              return;
            }
            fillForm(row);
          }
          return;
        }
        const deleteBtn = ev.target.closest(".delete-chave-regra");
        if (deleteBtn) {
          const id = deleteBtn.dataset.id;
          if (!id) return;
          msg.textContent = "Desativando...";
          msg.classList.remove("text-error");
          try {
            const res = await fetch(`/api/chave-planejamento-regra/${encodeURIComponent(id)}`, {
              method: "DELETE",
              headers: { "X-Requested-With": "fetch" },
            });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || "Falha ao desativar regra.");
            msg.textContent = data.message || "Regra desativada.";
            loadPage("atualizar/chave_planejamento_regra");
          } catch (err) {
            console.error(err);
            msg.textContent = err.message;
            msg.classList.add("text-error");
          }
        }
      });
    }

    if (cancelBtn) {
      cancelBtn.addEventListener("click", () => {
        msg.textContent = "";
        msg.classList.remove("text-error");
        limparForm();
      });
    }
    if (tipoEl) {
      tipoEl.addEventListener("change", () => {
        syncTipoFields();
      });
    }

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      msg.textContent = "Salvando...";
      msg.classList.remove("text-error");
      const id = String(idEl?.value || "").trim();
      const tipo = String(tipoEl?.value || "").trim();
      if (tipo === "chaves_planejamento" && !hasPlanejamentoUpload) {
        msg.textContent = "Usuario sem permissao para upload anual de chaves_planejamento.";
        msg.classList.add("text-error");
        return;
      }
      if (tipo === "chaves_planejamento") {
        const arquivo = arquivoEl?.files?.[0] || null;
        if (!arquivo) {
          msg.textContent = "Selecione um arquivo .xlsx para importar as chaves.";
          msg.classList.add("text-error");
          return;
        }
        const fd = new FormData();
        fd.append("arquivo", arquivo);
        fd.append("observacao", observacaoEl?.value || "");
        fd.append("ativo", ativoEl?.checked ? "1" : "0");
        try {
          const res = await fetch("/api/chave-planejamento-regra/import", {
            method: "POST",
            headers: { "X-Requested-With": "fetch" },
            body: fd,
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao importar chaves.");
          const resumo = [
            data.message || "Importação concluída.",
            `Inseridas: ${data.inseridas || 0}`,
            `Atualizadas: ${data.atualizadas || 0}`,
            `Ignoradas (duplicadas no arquivo): ${data.ignoradas_duplicadas_arquivo || 0}`,
            `Ignoradas (vazias): ${data.ignoradas_vazias || 0}`,
          ].join(" | ");
          msg.textContent = resumo;
          loadPage("atualizar/chave_planejamento_regra");
        } catch (err) {
          console.error(err);
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
        return;
      }
      const payload = {
        tipo_regra: tipo,
        chave_origem: origemEl?.value || "",
        chave_destino: destinoEl?.value || "",
        observacao: observacaoEl?.value || "",
        ativo: !!ativoEl?.checked,
      };
      const url = id
        ? `/api/chave-planejamento-regra/${encodeURIComponent(id)}`
        : "/api/chave-planejamento-regra";
      const method = id ? "PUT" : "POST";
      try {
        const res = await fetch(url, {
          method,
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao salvar regra.");
        msg.textContent = data.message || "Regra salva.";
        loadPage("atualizar/chave_planejamento_regra");
      } catch (err) {
        console.error(err);
        msg.textContent = err.message;
        msg.classList.add("text-error");
      }
    });

    syncTipoFields();
    if (tipoPlanejamentoOption && !hasPlanejamentoUpload) {
      tipoPlanejamentoOption.disabled = true;
      tipoPlanejamentoOption.hidden = true;
      if (tipoEl && tipoEl.value === "chaves_planejamento") {
        tipoEl.value = "";
      }
    }
    renderTablePage();
  }

  function initEstruturaPlanejamento() {
    const page = document.getElementById("estrutura-planejamento-page");
    const form = document.getElementById("estrutura-planejamento-form");
    if (!page || !form || form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const entity = String(page.dataset.entity || "").trim();
    const singular = String(page.dataset.singular || "Registro").trim();
    const idEl = document.getElementById("estrutura-planejamento-id");
    const exercicioEl = document.getElementById("estrutura-planejamento-exercicio");
    const programaEl = document.getElementById("estrutura-planejamento-programa");
    const acaoEl = document.getElementById("estrutura-planejamento-acao");
    const acaoChecklistEl = document.getElementById("estrutura-planejamento-acao-checklist");
    const acaoChecklistToggle = document.getElementById("estrutura-planejamento-acao-checklist-toggle");
    const acaoChecklistPanel = document.getElementById("estrutura-planejamento-acao-checklist-panel");
    const acaoChecklistOptions = document.getElementById("estrutura-planejamento-acao-checklist-options");
    const codigoEl = document.getElementById("estrutura-planejamento-codigo");
    const nomeEl = document.getElementById("estrutura-planejamento-nome");
    const responsavelEl = document.getElementById("estrutura-planejamento-responsavel");
    const cpfEl = document.getElementById("estrutura-planejamento-cpf");
    const emailEl = document.getElementById("estrutura-planejamento-email");
    const ativoEl = document.getElementById("estrutura-planejamento-ativo");
    const limparBtn = document.getElementById("estrutura-planejamento-limpar");
    const atualizarBtn = document.getElementById("estrutura-planejamento-atualizar");
    const msg = document.getElementById("estrutura-planejamento-msg");
    const tableBody = document.getElementById("estrutura-planejamento-table-body");
    const filtroExercicioEl = document.getElementById("estrutura-planejamento-filtro-exercicio");
    const filtroStatusEl = document.getElementById("estrutura-planejamento-filtro-status");
    const filtroTextoEl = document.getElementById("estrutura-planejamento-filtro-texto");
    const pageSizeEl = document.getElementById("estrutura-planejamento-page-size");
    const paginationEl = document.getElementById("estrutura-planejamento-pagination");
    const productLinksCard = document.getElementById("estrutura-planejamento-produto-links-card");
    const productLinksTitle = document.getElementById("estrutura-planejamento-produto-links-title");
    const productLinksClose = document.getElementById("estrutura-planejamento-produto-links-close");
    const productLinkSubfuncaoEl = document.getElementById("estrutura-planejamento-produto-link-subfuncao");
    const productLinkUgEl = document.getElementById("estrutura-planejamento-produto-link-ug");
    const productLinkSaveBtn = document.getElementById("estrutura-planejamento-produto-link-save");
    const productLinksMsg = document.getElementById("estrutura-planejamento-produto-links-msg");
    const productLinksBody = document.getElementById("estrutura-planejamento-produto-links-body");

    let rows = [];
    let programas = [];
    let acoes = [];
    let currentActionOptions = [];
    let currentPage = 1;
    let pageSize = Number(pageSizeEl?.value || 10) || 10;
    const isProductEntity = entity === "produtos";
    const productLinkState = {
      productId: "",
      subfuncoes: [],
      ugs: [],
      ugBySubfuncao: {},
      rows: [],
    };

    const esc = (value) =>
      String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");

    const normalize = (value) =>
      String(value || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();

    const setMsg = (text, isError = false) => {
      if (!msg) return;
      msg.textContent = text || "";
      msg.classList.toggle("text-error", !!isError);
    };

    const setProductLinksMsg = (text, isError = false) => {
      if (!productLinksMsg) return;
      productLinksMsg.textContent = text || "";
      productLinksMsg.classList.toggle("text-error", !!isError);
    };

    const optionLabel = (row) => {
      const base = [row.codigo, row.nome].filter(Boolean).join(" - ");
      return row.ativo ? base : `${base} (inativo)`;
    };

    const setOptions = (select, options, placeholder, selectedValue = "") => {
      if (!select) return;
      select.innerHTML = `<option value="">${esc(placeholder)}</option>`;
      const selectedValues = Array.isArray(selectedValue)
        ? selectedValue.map((value) => String(value))
        : [String(selectedValue || "")];
      options.forEach((item) => {
        const option = document.createElement("option");
        option.value = String(item.id);
        option.textContent = optionLabel(item);
        option.selected = selectedValues.includes(String(item.id));
        select.appendChild(option);
      });
      if (!select.multiple) {
        select.value = selectedValue ? String(selectedValue) : "";
      }
    };

    const setLabeledOptions = (select, options, placeholder, selectedValue = "") => {
      if (!select) return;
      select.innerHTML = `<option value="">${esc(placeholder)}</option>`;
      options.forEach((item) => {
        const option = document.createElement("option");
        option.value = String(item.id);
        option.textContent = item.label || "";
        option.selected = String(item.id) === String(selectedValue || "");
        select.appendChild(option);
      });
      select.value = selectedValue ? String(selectedValue) : "";
    };

    const populateProductLinkUgs = (selectedValue = "") => {
      if (!productLinkUgEl) return;
      const subfuncaoId = String(productLinkSubfuncaoEl?.value || "");
      if (!subfuncaoId) {
        setLabeledOptions(productLinkUgEl, [], "Selecione primeiro uma subfunção...");
        productLinkUgEl.disabled = true;
        return;
      }
      const allowedIds = new Set(
        (productLinkState.ugBySubfuncao[subfuncaoId] || []).map((value) => String(value))
      );
      const options = productLinkState.ugs.filter(
        (item) => !allowedIds.size || allowedIds.has(String(item.id))
      );
      setLabeledOptions(
        productLinkUgEl,
        options,
        options.length ? "Selecione..." : "Nenhuma UG vinculada à subfunção",
        selectedValue
      );
      productLinkUgEl.disabled = !options.length;
    };

    const selectedChecklistActionIds = () => {
      if (!acaoChecklistOptions) return [];
      return Array.from(
        acaoChecklistOptions.querySelectorAll("input[type='checkbox']:checked")
      ).map((input) => input.value);
    };

    const updateActionChecklistLabel = () => {
      if (!acaoChecklistToggle) return;
      const selected = selectedChecklistActionIds();
      if (!selected.length) {
        acaoChecklistToggle.textContent = "Selecione...";
      } else if (selected.length === 1) {
        const option = currentActionOptions.find(
          (item) => String(item.id) === String(selected[0])
        );
        acaoChecklistToggle.textContent = option ? optionLabel(option) : "1 Ação/PAOE selecionada";
      } else {
        acaoChecklistToggle.textContent = `${selected.length} Ações/PAOE selecionadas`;
      }
    };

    const renderActionChecklist = (options = [], selectedValues = []) => {
      if (!acaoChecklistOptions) return;
      const selected = new Set((selectedValues || []).map((value) => String(value)));
      acaoChecklistOptions.innerHTML = options.length
        ? options
            .map(
              (option) => `
                <label class="planning-action-checklist-option">
                  <input type="checkbox" value="${esc(option.id)}" ${selected.has(String(option.id)) ? "checked" : ""} />
                  <span>${esc(optionLabel(option))}</span>
                </label>
              `
            )
            .join("")
        : '<div class="muted">Nenhuma Ação/PAOE cadastrada para este programa.</div>';
      updateActionChecklistLabel();
    };

    const setProductActionMode = () => {
      if (!acaoEl || !isProductEntity) return;
      acaoEl.hidden = true;
      acaoEl.disabled = true;
      if (acaoChecklistEl) acaoChecklistEl.hidden = false;
      if (acaoChecklistPanel) acaoChecklistPanel.hidden = true;
    };

    const populatePrograms = (selectedValue = "") => {
      if (!programaEl) return;
      const exercicio = String(exercicioEl?.value || "").trim();
      const options = programas.filter(
        (row) => !exercicio || String(row.exercicio) === exercicio
      );
      setOptions(programaEl, options, "Selecione...", selectedValue);
    };

    const populateActions = (selectedValue = "") => {
      if (!acaoEl) return;
      const exercicio = String(exercicioEl?.value || "").trim();
      const programaId = String(programaEl?.value || "").trim();
      const options = acoes.filter(
        (row) =>
          (!exercicio || String(row.exercicio) === exercicio) &&
          (!programaId || String(row.programa_id) === programaId)
      );
      currentActionOptions = options;
      const placeholder = !programaId
        ? "Selecione primeiro um programa..."
        : options.length
          ? "Selecione..."
          : "Nenhuma Ação/PAOE cadastrada para este programa";
      setOptions(acaoEl, options, placeholder, selectedValue);
      acaoEl.disabled = !programaId || isProductEntity;
      if (isProductEntity) {
        const selectedValues = Array.isArray(selectedValue)
          ? selectedValue
          : selectedValue
            ? [selectedValue]
            : [];
        renderActionChecklist(options, selectedValues);
        if (acaoChecklistToggle) {
          acaoChecklistToggle.disabled = !programaId || !options.length;
        }
      }
    };

    const resetForm = () => {
      form.reset();
      if (idEl) idEl.value = "";
      if (ativoEl) ativoEl.checked = true;
      setProductActionMode(false);
      populatePrograms();
      populateActions();
      setMsg("");
      exercicioEl?.focus();
    };

    const formatCpf = (value) => {
      const digits = String(value || "").replace(/\D/g, "").slice(0, 11);
      if (digits.length <= 3) return digits;
      if (digits.length <= 6) return `${digits.slice(0, 3)}.${digits.slice(3)}`;
      if (digits.length <= 9) {
        return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6)}`;
      }
      return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6, 9)}-${digits.slice(9)}`;
    };

    const fillForm = (row) => {
      if (!row) return;
      if (idEl) idEl.value = row.id || "";
      if (exercicioEl) exercicioEl.value = row.exercicio || "";
      setProductActionMode();
      populatePrograms(row.programa_id || "");
      if (programaEl && row.programa_id) programaEl.value = String(row.programa_id);
      populateActions(row.acao_id || "");
      if (acaoEl && row.acao_id) acaoEl.value = String(row.acao_id);
      if (codigoEl) codigoEl.value = row.codigo || "";
      if (nomeEl) nomeEl.value = row.nome || "";
      if (responsavelEl) responsavelEl.value = row.responsavel || "";
      if (cpfEl) cpfEl.value = row.cpf || "";
      if (emailEl) emailEl.value = row.email || "";
      if (ativoEl) ativoEl.checked = !!row.ativo;
      setMsg("");
      page.scrollIntoView({ behavior: "smooth", block: "start" });
    };

    const getFilteredRows = () => {
      const exercicio = String(filtroExercicioEl?.value || "");
      const status = String(filtroStatusEl?.value || "");
      const text = normalize(filtroTextoEl?.value || "");
      return rows.filter((row) => {
        if (exercicio && String(row.exercicio) !== exercicio) return false;
        if (status && String(row.ativo ? 1 : 0) !== status) return false;
        if (!text) return true;
        const searchable = normalize(
          [
            row.codigo,
            row.nome,
            row.responsavel,
            row.programa_codigo,
            row.programa_nome,
            row.acao_codigo,
            row.acao_nome,
          ]
            .filter(Boolean)
            .join(" ")
        );
        return searchable.includes(text);
      });
    };

    const renderPagination = (totalPages) => {
      if (!paginationEl) return;
      paginationEl.innerHTML = "";
      if (totalPages <= 1) return;
      const addButton = (label, pageNumber, disabled = false, active = false) => {
        const button = document.createElement("button");
        button.type = "button";
        button.textContent = label;
        button.disabled = disabled;
        button.classList.toggle("active", active);
        button.addEventListener("click", () => {
          if (disabled || pageNumber === currentPage) return;
          currentPage = pageNumber;
          renderTable();
        });
        paginationEl.appendChild(button);
      };
      addButton("<<", 1, currentPage === 1);
      addButton("<", Math.max(1, currentPage - 1), currentPage === 1);
      const start = Math.max(1, currentPage - 2);
      const end = Math.min(totalPages, start + 4);
      for (let pageNumber = start; pageNumber <= end; pageNumber += 1) {
        addButton(String(pageNumber), pageNumber, false, pageNumber === currentPage);
      }
      addButton(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addButton(">>", totalPages, currentPage === totalPages);
    };

    const rowContextCells = (row) => {
      if (entity === "acoes") {
        return `<td>${esc([row.programa_codigo, row.programa_nome].filter(Boolean).join(" - "))}</td>`;
      }
      if (entity === "produtos") {
        return `
          <td>${esc([row.programa_codigo, row.programa_nome].filter(Boolean).join(" - "))}</td>
          <td>${esc([row.acao_codigo, row.acao_nome].filter(Boolean).join(" - "))}</td>
        `;
      }
      return "";
    };

    const renderTable = () => {
      if (!tableBody) return;
      const filtered = getFilteredRows();
      const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const pageRows = filtered.slice(
        (currentPage - 1) * pageSize,
        currentPage * pageSize
      );
      if (!pageRows.length) {
        const colspan = entity === "produtos" ? 9 : entity === "acoes" ? 8 : 7;
        tableBody.innerHTML = `<tr><td colspan="${colspan}" class="muted">Nenhum registro encontrado.</td></tr>`;
        renderPagination(0);
        return;
      }
      tableBody.innerHTML = pageRows
        .map(
          (row) => `
            <tr data-id="${esc(row.id)}">
              <td>${esc(row.exercicio)}</td>
              ${rowContextCells(row)}
              <td>${esc(row.codigo || "")}</td>
              <td>${esc(row.nome || "")}</td>
              <td>${esc(row.responsavel || "")}</td>
              <td><span class="planning-status ${row.ativo ? "is-active" : "is-inactive"}">${row.ativo ? "Ativo" : "Inativo"}</span></td>
              <td class="planning-structure-row-actions">
                ${isProductEntity ? `
                  <button class="icon-btn sm planning-product-links" type="button" data-id="${esc(row.id)}" title="Vínculos com Subfunção e UG" aria-label="Vínculos com Subfunção e UG">
                    <i class="bi bi-link-45deg"></i>
                  </button>
                ` : ""}
                <button class="icon-btn sm planning-edit" type="button" data-id="${esc(row.id)}" title="Editar" aria-label="Editar">
                  <i class="bi bi-pencil"></i>
                </button>
                <button class="icon-btn sm planning-disable" type="button" data-id="${esc(row.id)}" title="Desativar" aria-label="Desativar" ${row.ativo ? "" : "disabled"}>
                  <i class="bi bi-slash-circle"></i>
                </button>
              </td>
            </tr>
          `
        )
        .join("");
      renderPagination(totalPages);
    };

    const renderProductLinks = () => {
      if (!productLinksBody) return;
      if (!productLinkState.productId) {
        productLinksBody.innerHTML =
          '<tr><td colspan="3" class="muted">Selecione um produto para visualizar os vínculos.</td></tr>';
        return;
      }
      if (!productLinkState.rows.length) {
        productLinksBody.innerHTML =
          '<tr><td colspan="3" class="muted">Nenhum vínculo cadastrado.</td></tr>';
        return;
      }
      productLinksBody.innerHTML = productLinkState.rows
        .map(
          (row) => `
            <tr>
              <td>${esc(row.subfuncao || "")}</td>
              <td>${esc(row.ug || "")}</td>
              <td class="planning-structure-row-actions">
                <button
                  class="icon-btn sm planning-product-link-delete"
                  type="button"
                  data-subfuncao-id="${esc(row.subfuncao_id)}"
                  data-ug-id="${esc(row.ug_id)}"
                  title="Remover vínculo"
                  aria-label="Remover vínculo"
                >
                  <i class="bi bi-trash"></i>
                </button>
              </td>
            </tr>
          `
        )
        .join("");
    };

    const closeProductLinks = () => {
      if (productLinksCard) productLinksCard.hidden = true;
      productLinkState.productId = "";
      productLinkState.subfuncoes = [];
      productLinkState.ugs = [];
      productLinkState.ugBySubfuncao = {};
      productLinkState.rows = [];
      if (productLinkSubfuncaoEl) productLinkSubfuncaoEl.value = "";
      populateProductLinkUgs();
      setProductLinksMsg("");
      renderProductLinks();
    };

    const loadProductLinks = async (productId) => {
      if (!isProductEntity || !productLinksCard) return;
      productLinksCard.hidden = false;
      productLinkState.productId = String(productId || "");
      if (productLinksTitle) productLinksTitle.textContent = "Carregando produto...";
      if (productLinksBody) {
        productLinksBody.innerHTML =
          '<tr><td colspan="3" class="muted">Carregando vínculos...</td></tr>';
      }
      setProductLinksMsg("");
      try {
        const response = await fetch(
          `/api/estrutura-planejamento/produtos/${encodeURIComponent(productId)}/subfuncao-ug`,
          { headers: { "X-Requested-With": "fetch" } }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao carregar vínculos.");
        productLinkState.subfuncoes = Array.isArray(data.subfuncoes) ? data.subfuncoes : [];
        productLinkState.ugs = Array.isArray(data.ugs) ? data.ugs : [];
        productLinkState.ugBySubfuncao = data.ug_by_subfuncao || {};
        productLinkState.rows = Array.isArray(data.rows) ? data.rows : [];
        const product = data.product || {};
        if (productLinksTitle) {
          productLinksTitle.textContent = [
            product.exercicio,
            product.programa_codigo,
            product.acao_codigo,
            product.codigo,
            product.nome,
          ]
            .filter(Boolean)
            .join(" | ");
        }
        setLabeledOptions(
          productLinkSubfuncaoEl,
          productLinkState.subfuncoes,
          productLinkState.subfuncoes.length ? "Selecione..." : "Nenhuma subfunção vinculada à Ação/PAOE"
        );
        populateProductLinkUgs();
        renderProductLinks();
        productLinksCard.scrollIntoView({ behavior: "smooth", block: "start" });
      } catch (error) {
        console.error(error);
        setProductLinksMsg(error.message || "Falha ao carregar vínculos.", true);
        renderProductLinks();
      }
    };

    const populateFilterYears = () => {
      if (!filtroExercicioEl) return;
      const selected = filtroExercicioEl.value;
      const years = [...new Set(rows.map((row) => String(row.exercicio)).filter(Boolean))]
        .sort((a, b) => Number(b) - Number(a));
      filtroExercicioEl.innerHTML = '<option value="">Todos</option>';
      years.forEach((year) => {
        const option = document.createElement("option");
        option.value = year;
        option.textContent = year;
        filtroExercicioEl.appendChild(option);
      });
      filtroExercicioEl.value = years.includes(selected) ? selected : "";
    };

    const loadRows = async () => {
      setMsg("Carregando registros...");
      if (programaEl) {
        programaEl.disabled = true;
        programaEl.innerHTML = '<option value="">Carregando programas...</option>';
      }
      if (acaoEl) {
        acaoEl.disabled = true;
        acaoEl.innerHTML = '<option value="">Aguardando programas...</option>';
      }
      try {
        const response = await fetch(`/api/estrutura-planejamento/${encodeURIComponent(entity)}`, {
          headers: { "X-Requested-With": "fetch" },
        });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao carregar os registros.");
        rows = Array.isArray(data.rows) ? data.rows : [];
        programas = Array.isArray(data.programas) ? data.programas : [];
        acoes = Array.isArray(data.acoes) ? data.acoes : [];
        if (programaEl) programaEl.disabled = false;
        populatePrograms(programaEl?.value || "");
        populateActions(acaoEl?.value || "");
        populateFilterYears();
        currentPage = 1;
        renderTable();
        if (productLinkState.productId) {
          await loadProductLinks(productLinkState.productId);
        }
        setMsg("");
      } catch (error) {
        console.error(error);
        setMsg(error.message || "Falha ao carregar os registros.", true);
      }
    };

    exercicioEl?.addEventListener("input", () => {
      exercicioEl.value = exercicioEl.value.replace(/\D/g, "").slice(0, 4);
      populatePrograms();
      populateActions();
      setMsg("");
    });
    programaEl?.addEventListener("change", () => {
      const selectedProgram = programas.find(
        (row) => String(row.id) === String(programaEl.value || "")
      );
      if (selectedProgram && exercicioEl) {
        exercicioEl.value = String(selectedProgram.exercicio || "");
      }
      populateActions();
      setMsg("");
    });
    acaoEl?.addEventListener("change", () => setMsg(""));
    acaoChecklistToggle?.addEventListener("click", () => {
      if (acaoChecklistToggle.disabled || !acaoChecklistPanel) return;
      acaoChecklistPanel.hidden = !acaoChecklistPanel.hidden;
    });
    acaoChecklistOptions?.addEventListener("change", () => {
      updateActionChecklistLabel();
      setMsg("");
    });
    document.addEventListener("click", (event) => {
      if (
        !acaoChecklistPanel ||
        acaoChecklistPanel.hidden ||
        acaoChecklistEl?.contains(event.target)
      ) {
        return;
      }
      acaoChecklistPanel.hidden = true;
    });
    cpfEl?.addEventListener("input", () => {
      cpfEl.value = formatCpf(cpfEl.value);
      setMsg("");
    });
    form.querySelectorAll("input, select").forEach((control) => {
      control.addEventListener("change", () => setMsg(""));
    });

    limparBtn?.addEventListener("click", resetForm);
    atualizarBtn?.addEventListener("click", loadRows);
    [filtroExercicioEl, filtroStatusEl].forEach((control) => {
      control?.addEventListener("change", () => {
        currentPage = 1;
        renderTable();
      });
    });
    filtroTextoEl?.addEventListener("input", () => {
      currentPage = 1;
      renderTable();
    });
    pageSizeEl?.addEventListener("change", () => {
      pageSize = Number(pageSizeEl.value || 10) || 10;
      currentPage = 1;
      renderTable();
    });
    productLinksClose?.addEventListener("click", closeProductLinks);
    productLinkSubfuncaoEl?.addEventListener("change", () => {
      populateProductLinkUgs();
      setProductLinksMsg("");
    });
    productLinkUgEl?.addEventListener("change", () => setProductLinksMsg(""));
    productLinkSaveBtn?.addEventListener("click", async () => {
      if (!productLinkState.productId) return;
      const payload = {
        subfuncao_id: productLinkSubfuncaoEl?.value || "",
        ug_id: productLinkUgEl?.value || "",
      };
      if (!payload.subfuncao_id || !payload.ug_id) {
        setProductLinksMsg("Selecione subfunção e unidade gestora.", true);
        return;
      }
      setProductLinksMsg("Salvando vínculo...");
      try {
        const response = await fetch(
          `/api/estrutura-planejamento/produtos/${encodeURIComponent(productLinkState.productId)}/subfuncao-ug`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "X-Requested-With": "fetch",
            },
            body: JSON.stringify(payload),
          }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao salvar vínculo.");
        showToast(data.message || "Vínculo cadastrado.", "success");
        if (productLinkSubfuncaoEl) productLinkSubfuncaoEl.value = "";
        populateProductLinkUgs();
        await loadProductLinks(productLinkState.productId);
      } catch (error) {
        console.error(error);
        setProductLinksMsg(error.message || "Falha ao salvar vínculo.", true);
      }
    });
    productLinksBody?.addEventListener("click", async (event) => {
      const deleteButton = event.target.closest(".planning-product-link-delete");
      if (!deleteButton || !productLinkState.productId) return;
      if (!window.confirm("Remover este vínculo?")) return;
      setProductLinksMsg("Removendo vínculo...");
      try {
        const response = await fetch(
          `/api/estrutura-planejamento/produtos/${encodeURIComponent(productLinkState.productId)}/subfuncao-ug`,
          {
            method: "DELETE",
            headers: {
              "Content-Type": "application/json",
              "X-Requested-With": "fetch",
            },
            body: JSON.stringify({
              subfuncao_id: deleteButton.dataset.subfuncaoId || "",
              ug_id: deleteButton.dataset.ugId || "",
            }),
          }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao remover vínculo.");
        showToast(data.message || "Vínculo removido.", "success");
        await loadProductLinks(productLinkState.productId);
      } catch (error) {
        console.error(error);
        setProductLinksMsg(error.message || "Falha ao remover vínculo.", true);
      }
    });

    setProductActionMode(false);

    tableBody?.addEventListener("click", async (event) => {
      const linkButton = event.target.closest(".planning-product-links");
      if (linkButton) {
        await loadProductLinks(linkButton.dataset.id);
        return;
      }
      const editButton = event.target.closest(".planning-edit");
      if (editButton) {
        const row = rows.find((item) => String(item.id) === String(editButton.dataset.id));
        fillForm(row);
        return;
      }
      const disableButton = event.target.closest(".planning-disable");
      if (!disableButton || disableButton.disabled) return;
      const row = rows.find((item) => String(item.id) === String(disableButton.dataset.id));
      if (!row || !window.confirm(`Desativar ${singular.toLowerCase()} "${row.nome}"?`)) return;
      setMsg("Desativando registro...");
      try {
        const response = await fetch(
          `/api/estrutura-planejamento/${encodeURIComponent(entity)}/${encodeURIComponent(row.id)}`,
          {
            method: "DELETE",
            headers: { "X-Requested-With": "fetch" },
          }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao desativar o registro.");
        showToast(data.message || "Registro desativado.", "success");
        resetForm();
        await loadRows();
      } catch (error) {
        console.error(error);
        setMsg(error.message || "Falha ao desativar o registro.", true);
      }
    });

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!form.checkValidity()) {
        form.reportValidity();
        return;
      }
      const id = String(idEl?.value || "").trim();
      const payload = {
        exercicio: exercicioEl?.value || "",
        programa_id: programaEl?.value || null,
        acao_id: acaoEl?.value || null,
        codigo: codigoEl?.value || "",
        nome: nomeEl?.value || "",
        responsavel: responsavelEl?.value || "",
        cpf: cpfEl?.value || "",
        email: emailEl?.value || "",
        ativo: !!ativoEl?.checked,
      };
      if (isProductEntity && acaoEl) {
        payload.acao_ids = selectedChecklistActionIds();
        payload.acao_id = payload.acao_ids[0] || null;
      }
      setMsg("Salvando registro...");
      try {
        const response = await fetch(
          id
            ? `/api/estrutura-planejamento/${encodeURIComponent(entity)}/${encodeURIComponent(id)}`
            : `/api/estrutura-planejamento/${encodeURIComponent(entity)}`,
          {
            method: id ? "PUT" : "POST",
            headers: {
              "Content-Type": "application/json",
              "X-Requested-With": "fetch",
            },
            body: JSON.stringify(payload),
          }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao salvar o registro.");
        showToast(data.message || "Registro salvo.", "success");
        resetForm();
        await loadRows();
      } catch (error) {
        console.error(error);
        setMsg(error.message || "Falha ao salvar o registro.", true);
      }
    });

    loadRows();
  }

  function initEstruturaComponentes() {
    const page = document.getElementById("estrutura-componentes-page");
    const form = document.getElementById("estrutura-componente-form");
    if (!page || !form || form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const configs = JSON.parse(page.dataset.configs || "[]");
    const tipoEl = document.getElementById("estrutura-componente-tipo");
    const idEl = document.getElementById("estrutura-componente-id");
    const fieldsEl = document.getElementById("estrutura-componente-fields");
    const formTitle = document.getElementById("estrutura-componente-form-title");
    const listTitle = document.getElementById("estrutura-componente-list-title");
    const tableHead = document.getElementById("estrutura-componente-table-head");
    const tableBody = document.getElementById("estrutura-componente-table-body");
    const msg = document.getElementById("estrutura-componente-msg");
    const limparBtn = document.getElementById("estrutura-componente-limpar");
    const atualizarBtn = document.getElementById("estrutura-componente-atualizar");
    const statusEl = document.getElementById("estrutura-componente-filtro-status");
    const searchEl = document.getElementById("estrutura-componente-filtro-texto");
    const pageSizeEl = document.getElementById("estrutura-componente-page-size");
    const paginationEl = document.getElementById("estrutura-componente-pagination");
    const linksCard = document.getElementById("estrutura-componente-links-card");
    const linksTitle = document.getElementById("estrutura-componente-links-title");
    const linksSelected = document.getElementById("estrutura-componente-links-selected");
    const linksClose = document.getElementById("estrutura-componente-links-close");
    const linksTipo = document.getElementById("estrutura-componente-links-tipo");
    const linksDestino = document.getElementById("estrutura-componente-links-destino");
    const linksDestinoLabel = document.getElementById("estrutura-componente-links-destino-label");
    const linksAdd = document.getElementById("estrutura-componente-links-add");
    const linksMsg = document.getElementById("estrutura-componente-links-msg");
    const linksTableLabel = document.getElementById("estrutura-componente-links-table-label");
    const linksBody = document.getElementById("estrutura-componente-links-body");

    let currentConfig = configs[0] || null;
    let rows = [];
    let sources = {};
    let currentPage = 1;
    let pageSize = Number(pageSizeEl?.value || 10) || 10;
    let requestToken = 0;
    let selectedLinkRow = null;
    let currentMapping = null;
    let currentMappingRows = [];

    const esc = (value) =>
      String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");

    const normalize = (value) =>
      String(value || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();

    const setMsg = (text, isError = false) => {
      if (!msg) return;
      msg.textContent = text || "";
      msg.classList.toggle("text-error", !!isError);
    };

    const setLinksMsg = (text, isError = false) => {
      if (!linksMsg) return;
      linksMsg.textContent = text || "";
      linksMsg.classList.toggle("text-error", !!isError);
    };

    const currentKey = () => String(tipoEl?.value || currentConfig?.key || "");

    const fieldValue = (field, row) => {
      if (field.type === "select") return row[`${field.name}_label`] || "";
      return row[field.name] ?? "";
    };

    const renderFields = () => {
      if (!fieldsEl || !currentConfig) return;
      fieldsEl.innerHTML = currentConfig.fields
        .map((field) => {
          const required = field.required ? "required" : "";
          const requiredLabel = field.required ? "*" : "";
          if (field.type === "select") {
            const options = Array.isArray(sources[field.source]) ? sources[field.source] : [];
            return `
              <label class="field">
                <span>${requiredLabel}${esc(field.label)}</span>
                <select data-component-field="${esc(field.name)}" ${required}>
                  <option value="">Selecione...</option>
                  ${options.map((option) => `<option value="${esc(option.id)}">${esc(option.label)}${option.ativo ? "" : " (inativo)"}</option>`).join("")}
                </select>
              </label>
            `;
          }
          if (field.type === "textarea") {
            return `
              <label class="field planning-admin-field-wide">
                <span>${requiredLabel}${esc(field.label)}</span>
                <textarea data-component-field="${esc(field.name)}" ${required}></textarea>
              </label>
            `;
          }
          return `
            <label class="field">
              <span>${requiredLabel}${esc(field.label)}</span>
              <input
                type="${field.type === "integer" ? "number" : "text"}"
                data-component-field="${esc(field.name)}"
                ${field.max ? `maxlength="${esc(field.max)}"` : ""}
                ${required}
              />
            </label>
          `;
        })
        .join("");
      if (currentConfig.has_active) {
        fieldsEl.insertAdjacentHTML(
          "beforeend",
          '<label class="field inline planning-structure-active"><input type="checkbox" id="estrutura-componente-ativo" checked /><span>Ativo</span></label>'
        );
      }
    };

    const resetForm = () => {
      form.reset();
      if (idEl) idEl.value = "";
      const active = document.getElementById("estrutura-componente-ativo");
      if (active) active.checked = true;
      setMsg("");
    };

    const fillForm = (row) => {
      if (!row || !currentConfig) return;
      if (idEl) idEl.value = row.id;
      currentConfig.fields.forEach((field) => {
        const input = fieldsEl.querySelector(`[data-component-field="${field.name}"]`);
        if (input) input.value = row[field.name] ?? "";
      });
      const active = document.getElementById("estrutura-componente-ativo");
      if (active) active.checked = !!row.ativo;
      setMsg("");
      page.scrollIntoView({ behavior: "smooth", block: "start" });
    };

    const filteredRows = () => {
      const status = String(statusEl?.value || "");
      const search = normalize(searchEl?.value || "");
      return rows.filter((row) => {
        if (currentConfig?.has_active && status && String(row.ativo ? 1 : 0) !== status) {
          return false;
        }
        if (!search) return true;
        return normalize(
          currentConfig.fields.map((field) => fieldValue(field, row)).join(" ")
        ).includes(search);
      });
    };

    const renderPager = (totalPages) => {
      if (!paginationEl) return;
      paginationEl.innerHTML = "";
      if (totalPages <= 1) return;
      const button = (label, target, disabled = false, active = false) => {
        const el = document.createElement("button");
        el.type = "button";
        el.textContent = label;
        el.disabled = disabled;
        el.classList.toggle("active", active);
        el.addEventListener("click", () => {
          currentPage = target;
          renderTable();
        });
        paginationEl.appendChild(el);
      };
      button("<", Math.max(1, currentPage - 1), currentPage === 1);
      for (let number = Math.max(1, currentPage - 2); number <= Math.min(totalPages, currentPage + 2); number += 1) {
        button(String(number), number, false, number === currentPage);
      }
      button(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
    };

    const closeLinks = () => {
      selectedLinkRow = null;
      currentMapping = null;
      currentMappingRows = [];
      if (linksCard) linksCard.hidden = true;
      if (linksTipo) linksTipo.innerHTML = "";
      if (linksDestino) linksDestino.innerHTML = '<option value="">Selecione...</option>';
      if (linksBody) linksBody.innerHTML = "";
      setLinksMsg("");
    };

    const mappingDefinition = () =>
      (currentConfig?.mappings || []).find(
        (mapping) => mapping.key === String(linksTipo?.value || "")
      ) || null;

    const loadLinks = async () => {
      const definition = mappingDefinition();
      if (!selectedLinkRow || !definition || !linksDestino || !linksBody) return;
      currentMapping = definition;
      setLinksMsg("Carregando vínculos...");
      try {
        const response = await fetch(
          `/api/estrutura-planejamento/mapeamentos/${encodeURIComponent(definition.key)}`,
          { headers: { "X-Requested-With": "fetch" } }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao carregar vínculos.");

        const fixedId = String(selectedLinkRow.id);
        const fixedField = definition.fixed_side === "left" ? "left_id" : "right_id";
        const targetField = definition.fixed_side === "left" ? "right_id" : "left_id";
        const targetLabelField =
          definition.fixed_side === "left" ? "right_label" : "left_label";
        const options =
          definition.fixed_side === "left" ? data.right_options : data.left_options;

        currentMappingRows = (data.rows || []).filter(
          (row) => String(row[fixedField]) === fixedId
        );
        const linkedIds = new Set(
          currentMappingRows.map((row) => String(row[targetField]))
        );

        if (linksDestinoLabel) {
          linksDestinoLabel.textContent = `*${definition.target_label}`;
        }
        if (linksTableLabel) linksTableLabel.textContent = definition.target_label;
        linksDestino.innerHTML = `
          <option value="">Selecione...</option>
          ${(options || [])
            .filter((option) => !linkedIds.has(String(option.id)))
            .map(
              (option) =>
                `<option value="${esc(option.id)}">${esc(option.label)}${
                  option.ativo ? "" : " (inativo)"
                }</option>`
            )
            .join("")}
        `;
        linksBody.innerHTML = currentMappingRows.length
          ? currentMappingRows
              .map(
                (row) => `
                  <tr>
                    <td>${esc(row[targetLabelField])}</td>
                    <td class="planning-structure-row-actions">
                      <button
                        class="icon-btn sm component-link-remove"
                        type="button"
                        data-left-id="${esc(row.left_id)}"
                        data-right-id="${esc(row.right_id)}"
                        title="Remover vínculo"
                      ><i class="bi bi-link-45deg"></i></button>
                    </td>
                  </tr>
                `
              )
              .join("")
          : '<tr><td colspan="2" class="muted">Nenhum vínculo cadastrado.</td></tr>';
        setLinksMsg("");
      } catch (error) {
        console.error(error);
        setLinksMsg(error.message || "Falha ao carregar vínculos.", true);
      }
    };

    const openLinks = async (row) => {
      const mappings = currentConfig?.mappings || [];
      if (!row || !mappings.length || !linksCard || !linksTipo) return;
      selectedLinkRow = row;
      linksCard.hidden = false;
      if (linksTitle) linksTitle.textContent = `Vínculos de ${currentConfig.singular}`;
      if (linksSelected) linksSelected.textContent = row.label;
      linksTipo.innerHTML = mappings
        .map(
          (mapping) =>
            `<option value="${esc(mapping.key)}">${esc(mapping.title)}</option>`
        )
        .join("");
      await loadLinks();
      linksCard.scrollIntoView({ behavior: "smooth", block: "start" });
    };

    const renderTable = () => {
      if (!currentConfig || !tableHead || !tableBody) return;
      tableHead.innerHTML = `
        <tr>
          ${currentConfig.fields.map((field) => `<th>${esc(field.label)}</th>`).join("")}
          ${currentConfig.has_active ? "<th>Situação</th>" : ""}
          <th class="planning-structure-actions-column">Ações</th>
        </tr>
      `;
      const filtered = filteredRows();
      const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
      currentPage = Math.min(currentPage, totalPages);
      const visible = filtered.slice((currentPage - 1) * pageSize, currentPage * pageSize);
      if (!visible.length) {
        tableBody.innerHTML = `<tr><td colspan="${currentConfig.fields.length + (currentConfig.has_active ? 2 : 1)}" class="muted">Nenhum registro encontrado.</td></tr>`;
        renderPager(0);
        return;
      }
      tableBody.innerHTML = visible
        .map(
          (row) => `
            <tr>
              ${currentConfig.fields.map((field) => `<td>${esc(fieldValue(field, row))}</td>`).join("")}
              ${currentConfig.has_active ? `<td><span class="planning-status ${row.ativo ? "is-active" : "is-inactive"}">${row.ativo ? "Ativo" : "Inativo"}</span></td>` : ""}
              <td class="planning-structure-row-actions">
                <button class="icon-btn sm component-edit" type="button" data-id="${esc(row.id)}" title="Editar"><i class="bi bi-pencil"></i></button>
                ${(currentConfig.mappings || []).length ? `<button class="icon-btn sm component-links" type="button" data-id="${esc(row.id)}" title="Gerenciar vínculos"><i class="bi bi-link-45deg"></i></button>` : ""}
                ${currentConfig.has_active ? `<button class="icon-btn sm component-disable" type="button" data-id="${esc(row.id)}" title="Desativar" ${row.ativo ? "" : "disabled"}><i class="bi bi-slash-circle"></i></button>` : ""}
              </td>
            </tr>
          `
        )
        .join("");
      renderPager(totalPages);
    };

    const load = async () => {
      const token = ++requestToken;
      const key = currentKey();
      currentConfig = configs.find((config) => config.key === key) || configs[0];
      if (!currentConfig) return;
      let loadingOverlayVisible = false;
      const loadingTimer = window.setTimeout(() => {
        if (token !== requestToken) return;
        loadingOverlayVisible = true;
        showAppLoading(
          `Carregando ${currentConfig.title}...`,
          "Aguarde enquanto os campos e registros são atualizados."
        );
      }, 250);
      if (formTitle) formTitle.textContent = currentConfig.singular;
      if (listTitle) listTitle.textContent = currentConfig.title;
      page.setAttribute("aria-busy", "true");
      if (tipoEl) tipoEl.disabled = true;
      rows = [];
      sources = {};
      currentPage = 1;
      setMsg("Carregando registros...");
      closeLinks();
      fieldsEl.innerHTML = '<div class="muted">Carregando campos...</div>';
      if (tableHead) tableHead.innerHTML = "";
      if (tableBody) {
        tableBody.innerHTML =
          '<tr><td class="muted planning-component-loading-row"><i class="bi bi-arrow-repeat"></i> Carregando registros...</td></tr>';
      }
      if (paginationEl) paginationEl.innerHTML = "";
      try {
        const response = await fetch(
          `/api/estrutura-planejamento/componentes/${encodeURIComponent(currentConfig.key)}`,
          { headers: { "X-Requested-With": "fetch" } }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao carregar componentes.");
        if (token !== requestToken) return;
        currentConfig = data.config;
        rows = Array.isArray(data.rows) ? data.rows : [];
        sources = data.sources || {};
        if (statusEl) {
          statusEl.disabled = !currentConfig.has_active;
          statusEl.value = "";
        }
        renderFields();
        currentPage = 1;
        renderTable();
        setMsg("");
      } catch (error) {
        console.error(error);
        if (token !== requestToken) return;
        if (tableBody) {
          tableBody.innerHTML =
            '<tr><td class="text-error">Não foi possível carregar os registros.</td></tr>';
        }
        setMsg(error.message || "Falha ao carregar componentes.", true);
      } finally {
        window.clearTimeout(loadingTimer);
        if (loadingOverlayVisible) hideAppLoading();
        if (token === requestToken) {
          page.setAttribute("aria-busy", "false");
          if (tipoEl) tipoEl.disabled = false;
        }
      }
    };

    tipoEl?.addEventListener("change", () => {
      resetForm();
      if (statusEl) statusEl.value = "";
      if (searchEl) searchEl.value = "";
      if (pageSizeEl) pageSizeEl.value = "10";
      pageSize = 10;
      currentPage = 1;
      load();
    });
    limparBtn?.addEventListener("click", resetForm);
    atualizarBtn?.addEventListener("click", load);
    linksClose?.addEventListener("click", closeLinks);
    linksTipo?.addEventListener("change", loadLinks);
    linksAdd?.addEventListener("click", async () => {
      const definition = currentMapping || mappingDefinition();
      const targetId = Number(linksDestino?.value || 0);
      if (!selectedLinkRow || !definition || !targetId) {
        setLinksMsg("Selecione o registro que será vinculado.", true);
        return;
      }
      const payload =
        definition.fixed_side === "left"
          ? { left_id: selectedLinkRow.id, right_id: targetId }
          : { left_id: targetId, right_id: selectedLinkRow.id };
      setLinksMsg("Salvando vínculo...");
      try {
        const response = await fetch(
          `/api/estrutura-planejamento/mapeamentos/${encodeURIComponent(definition.key)}`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "X-Requested-With": "fetch",
            },
            body: JSON.stringify(payload),
          }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao salvar vínculo.");
        showToast(data.message, "success");
        await loadLinks();
      } catch (error) {
        setLinksMsg(error.message || "Falha ao salvar vínculo.", true);
      }
    });
    linksBody?.addEventListener("click", async (event) => {
      const button = event.target.closest(".component-link-remove");
      if (!button || !currentMapping) return;
      if (!window.confirm("Remover este vínculo?")) return;
      setLinksMsg("Removendo vínculo...");
      try {
        const response = await fetch(
          `/api/estrutura-planejamento/mapeamentos/${encodeURIComponent(currentMapping.key)}`,
          {
            method: "DELETE",
            headers: {
              "Content-Type": "application/json",
              "X-Requested-With": "fetch",
            },
            body: JSON.stringify({
              left_id: Number(button.dataset.leftId),
              right_id: Number(button.dataset.rightId),
            }),
          }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao remover vínculo.");
        showToast(data.message, "success");
        await loadLinks();
      } catch (error) {
        setLinksMsg(error.message || "Falha ao remover vínculo.", true);
      }
    });
    statusEl?.addEventListener("change", () => {
      currentPage = 1;
      renderTable();
    });
    searchEl?.addEventListener("input", () => {
      currentPage = 1;
      renderTable();
    });
    pageSizeEl?.addEventListener("change", () => {
      pageSize = Number(pageSizeEl.value || 10) || 10;
      currentPage = 1;
      renderTable();
    });

    tableBody?.addEventListener("click", async (event) => {
      const edit = event.target.closest(".component-edit");
      if (edit) {
        fillForm(rows.find((row) => String(row.id) === String(edit.dataset.id)));
        return;
      }
      const links = event.target.closest(".component-links");
      if (links) {
        await openLinks(
          rows.find((row) => String(row.id) === String(links.dataset.id))
        );
        return;
      }
      const disable = event.target.closest(".component-disable");
      if (!disable || disable.disabled) return;
      const row = rows.find((item) => String(item.id) === String(disable.dataset.id));
      if (!row || !window.confirm(`Desativar "${row.label}"?`)) return;
      try {
        const response = await fetch(
          `/api/estrutura-planejamento/componentes/${encodeURIComponent(currentConfig.key)}/${encodeURIComponent(row.id)}`,
          { method: "DELETE", headers: { "X-Requested-With": "fetch" } }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao desativar componente.");
        showToast(data.message, "success");
        resetForm();
        await load();
      } catch (error) {
        setMsg(error.message, true);
      }
    });

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!form.checkValidity()) {
        form.reportValidity();
        return;
      }
      const id = String(idEl?.value || "");
      const payload = {};
      currentConfig.fields.forEach((field) => {
        payload[field.name] =
          fieldsEl.querySelector(`[data-component-field="${field.name}"]`)?.value || "";
      });
      const active = document.getElementById("estrutura-componente-ativo");
      if (active) payload.ativo = active.checked;
      setMsg("Salvando componente...");
      try {
        const response = await fetch(
          id
            ? `/api/estrutura-planejamento/componentes/${encodeURIComponent(currentConfig.key)}/${encodeURIComponent(id)}`
            : `/api/estrutura-planejamento/componentes/${encodeURIComponent(currentConfig.key)}`,
          {
            method: id ? "PUT" : "POST",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify(payload),
          }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao salvar componente.");
        showToast(data.message, "success");
        resetForm();
        await load();
      } catch (error) {
        setMsg(error.message, true);
      }
    });

    load();
  }

  function initEstruturaMapeamentos() {
    const page = document.getElementById("estrutura-mapeamentos-page");
    const form = document.getElementById("estrutura-mapeamento-form");
    if (!page || !form || form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const configs = JSON.parse(page.dataset.configs || "[]");
    const tipoEl = document.getElementById("estrutura-mapeamento-tipo");
    const leftEl = document.getElementById("estrutura-mapeamento-left");
    const rightEl = document.getElementById("estrutura-mapeamento-right");
    const leftLabelEl = document.getElementById("estrutura-mapeamento-left-label");
    const rightLabelEl = document.getElementById("estrutura-mapeamento-right-label");
    const tableLeftEl = document.getElementById("estrutura-mapeamento-table-left");
    const tableRightEl = document.getElementById("estrutura-mapeamento-table-right");
    const listTitle = document.getElementById("estrutura-mapeamento-list-title");
    const tableBody = document.getElementById("estrutura-mapeamento-table-body");
    const msg = document.getElementById("estrutura-mapeamento-msg");
    const limparBtn = document.getElementById("estrutura-mapeamento-limpar");
    const atualizarBtn = document.getElementById("estrutura-mapeamento-atualizar");
    const searchEl = document.getElementById("estrutura-mapeamento-filtro");
    const pageSizeEl = document.getElementById("estrutura-mapeamento-page-size");
    const paginationEl = document.getElementById("estrutura-mapeamento-pagination");

    let currentConfig = configs[0] || null;
    let rows = [];
    let currentPage = 1;
    let pageSize = Number(pageSizeEl?.value || 10) || 10;
    let requestToken = 0;

    const esc = (value) =>
      String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");

    const normalize = (value) =>
      String(value || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase();

    const setMsg = (text, isError = false) => {
      msg.textContent = text || "";
      msg.classList.toggle("text-error", !!isError);
    };

    const fillSelect = (select, options) => {
      select.innerHTML = '<option value="">Selecione...</option>';
      options.forEach((option) => {
        const el = document.createElement("option");
        el.value = String(option.id);
        el.textContent = `${option.label}${option.ativo ? "" : " (inativo)"}`;
        el.disabled = !option.ativo;
        select.appendChild(el);
      });
      select.disabled = false;
    };

    const filteredRows = () => {
      const search = normalize(searchEl?.value || "");
      return search
        ? rows.filter((row) => normalize(`${row.left_label} ${row.right_label}`).includes(search))
        : rows;
    };

    const renderPager = (totalPages) => {
      paginationEl.innerHTML = "";
      if (totalPages <= 1) return;
      const add = (label, target, disabled = false, active = false) => {
        const button = document.createElement("button");
        button.type = "button";
        button.textContent = label;
        button.disabled = disabled;
        button.classList.toggle("active", active);
        button.addEventListener("click", () => {
          currentPage = target;
          renderTable();
        });
        paginationEl.appendChild(button);
      };
      add("<", Math.max(1, currentPage - 1), currentPage === 1);
      for (let number = Math.max(1, currentPage - 2); number <= Math.min(totalPages, currentPage + 2); number += 1) {
        add(String(number), number, false, number === currentPage);
      }
      add(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
    };

    const renderTable = () => {
      const filtered = filteredRows();
      const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
      currentPage = Math.min(currentPage, totalPages);
      const visible = filtered.slice((currentPage - 1) * pageSize, currentPage * pageSize);
      if (!visible.length) {
        tableBody.innerHTML = '<tr><td colspan="3" class="muted">Nenhum mapeamento encontrado.</td></tr>';
        renderPager(0);
        return;
      }
      tableBody.innerHTML = visible
        .map(
          (row) => `
            <tr>
              <td>${esc(row.left_label)}</td>
              <td>${esc(row.right_label)}</td>
              <td class="planning-structure-row-actions">
                <button class="icon-btn sm mapping-delete" type="button" data-left="${esc(row.left_id)}" data-right="${esc(row.right_id)}" title="Remover vínculo">
                  <i class="bi bi-trash"></i>
                </button>
              </td>
            </tr>
          `
        )
        .join("");
      renderPager(totalPages);
    };

    const load = async () => {
      const token = ++requestToken;
      const key = String(tipoEl?.value || configs[0]?.key || "");
      currentConfig = configs.find((config) => config.key === key) || configs[0];
      if (!currentConfig) return;
      leftEl.disabled = true;
      rightEl.disabled = true;
      leftEl.innerHTML = '<option value="">Carregando...</option>';
      rightEl.innerHTML = '<option value="">Carregando...</option>';
      setMsg("Carregando mapeamentos...");
      try {
        const response = await fetch(
          `/api/estrutura-planejamento/mapeamentos/${encodeURIComponent(currentConfig.key)}`,
          { headers: { "X-Requested-With": "fetch" } }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao carregar mapeamentos.");
        if (token !== requestToken) return;
        currentConfig = data.config;
        rows = Array.isArray(data.rows) ? data.rows : [];
        leftLabelEl.textContent = `*${currentConfig.left_label}`;
        rightLabelEl.textContent = `*${currentConfig.right_label}`;
        tableLeftEl.textContent = currentConfig.left_label;
        tableRightEl.textContent = currentConfig.right_label;
        listTitle.textContent = currentConfig.title;
        fillSelect(leftEl, data.left_options || []);
        fillSelect(rightEl, data.right_options || []);
        currentPage = 1;
        renderTable();
        setMsg("");
      } catch (error) {
        console.error(error);
        setMsg(error.message || "Falha ao carregar mapeamentos.", true);
      }
    };

    tipoEl?.addEventListener("change", load);
    limparBtn?.addEventListener("click", () => {
      leftEl.value = "";
      rightEl.value = "";
      setMsg("");
    });
    atualizarBtn?.addEventListener("click", load);
    searchEl?.addEventListener("input", () => {
      currentPage = 1;
      renderTable();
    });
    pageSizeEl?.addEventListener("change", () => {
      pageSize = Number(pageSizeEl.value || 10) || 10;
      currentPage = 1;
      renderTable();
    });

    tableBody?.addEventListener("click", async (event) => {
      const button = event.target.closest(".mapping-delete");
      if (!button || !window.confirm("Remover este vínculo?")) return;
      try {
        const response = await fetch(
          `/api/estrutura-planejamento/mapeamentos/${encodeURIComponent(currentConfig.key)}`,
          {
            method: "DELETE",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify({ left_id: button.dataset.left, right_id: button.dataset.right }),
          }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao remover vínculo.");
        showToast(data.message, "success");
        await load();
      } catch (error) {
        setMsg(error.message, true);
      }
    });

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!form.checkValidity()) {
        form.reportValidity();
        return;
      }
      setMsg("Salvando mapeamento...");
      try {
        const response = await fetch(
          `/api/estrutura-planejamento/mapeamentos/${encodeURIComponent(currentConfig.key)}`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify({ left_id: leftEl.value, right_id: rightEl.value }),
          }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao salvar vínculo.");
        showToast(data.message, "success");
        leftEl.value = "";
        rightEl.value = "";
        await load();
      } catch (error) {
        setMsg(error.message, true);
      }
    });

    load();
  }

  function initModelosChave() {
    const page = document.getElementById("modelos-chave-page");
    if (!page || page.dataset.bound === "1") return;
    page.dataset.bound = "1";

    const sources = JSON.parse(page.dataset.sources || "[]");
    const modelForm = document.getElementById("key-model-form");
    const modelList = document.getElementById("key-model-list");
    const componentCard = document.getElementById("key-model-components-card");
    const componentForm = document.getElementById("key-component-form");
    const componentList = document.getElementById("key-component-list");
    const sourceEl = document.getElementById("key-component-source");
    const idFieldEl = document.getElementById("key-component-id-field");
    const codeFieldEl = document.getElementById("key-component-code-field");
    const descriptionFieldEl = document.getElementById("key-component-description-field");
    const groupEnabledEl = document.getElementById("key-component-group-enabled");
    const groupFieldsEl = document.getElementById("key-component-group-fields");
    const modelMessage = document.getElementById("key-model-message");
    const componentMessage = document.getElementById("key-component-message");

    let rows = [];
    let selectedModelId = null;

    const esc = (value) =>
      String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    const setMessage = (element, message = "", error = false) => {
      element.textContent = message;
      element.classList.toggle("text-error", error);
    };
    const request = async (url, options = {}) => {
      const response = await fetch(url, {
        ...options,
        headers: {
          "Content-Type": "application/json",
          "X-Requested-With": "fetch",
          ...(options.headers || {}),
        },
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || "Falha ao processar a solicitação.");
      return data;
    };
    const selectedModel = () =>
      rows.find((row) => Number(row.id) === Number(selectedModelId));
    const updateGroupFields = () => {
      const enabled = groupEnabledEl.value === "1";
      groupFieldsEl.hidden = !enabled;
      [
        document.getElementById("key-component-group"),
        document.getElementById("key-component-group-order"),
        document.getElementById("key-component-group-separator"),
      ].forEach((element) => {
        element.disabled = !enabled;
        element.required = enabled;
        if (!enabled) element.value = "";
      });
    };

    const clearModel = () => {
      document.getElementById("key-model-id").value = "";
      document.getElementById("key-model-name").value = "";
      document.getElementById("key-model-start").value = "";
      document.getElementById("key-model-end").value = "";
      document.getElementById("key-model-prefix").value = "* ";
      document.getElementById("key-model-separator").value = " * ";
      document.getElementById("key-model-suffix").value = " *";
      document.getElementById("key-model-active").checked = true;
      setMessage(modelMessage);
    };
    const clearComponent = () => {
      document.getElementById("key-component-id").value = "";
      document.getElementById("key-component-code").value = "";
      document.getElementById("key-component-name").value = "";
      document.getElementById("key-component-order").value = "";
      groupEnabledEl.value = "0";
      document.getElementById("key-component-group").value = "";
      document.getElementById("key-component-group-order").value = "";
      document.getElementById("key-component-group-separator").value = "";
      document.getElementById("key-component-required").checked = true;
      document.getElementById("key-component-active").checked = true;
      sourceEl.value = "";
      fillSourceFields();
      updateGroupFields();
      setMessage(componentMessage);
    };
    const fillSourceFields = () => {
      const source = sources.find((item) => item.table === sourceEl.value);
      const fields = source?.fields || [];
      const enabled = Boolean(source);
      const fill = (element, emptyLabel = "Selecione...") => {
        element.innerHTML = [
          `<option value="">${esc(emptyLabel)}</option>`,
          ...fields.map((field) => `<option value="${esc(field)}">${esc(field)}</option>`),
        ].join("");
        element.disabled = !enabled;
      };
      fill(idFieldEl);
      fill(codeFieldEl);
      fill(descriptionFieldEl);
    };
    const renderComponents = () => {
      const model = selectedModel();
      componentCard.hidden = !model;
      if (!model) return;
      document.getElementById("key-model-selected").textContent =
        `${model.nome} | Vigência: ${model.exercicio_inicio} a ${model.exercicio_fim || "sem limite"}`;
      componentList.innerHTML = model.componentes.length
        ? model.componentes
            .map(
              (item) => `
                <tr>
                  <td>${esc(item.ordem)}</td>
                  <td><strong>${esc(item.nome)}</strong><br><span class="muted">${esc(item.codigo)}</span></td>
                  <td>${esc(item.tabela_origem)}</td>
                  <td>${esc(item.campo_codigo)}${item.campo_descricao ? ` / ${esc(item.campo_descricao)}` : ""}</td>
                  <td>${item.obrigatorio ? "Sim" : "Não"}</td>
                  <td><span class="status-badge ${item.ativo ? "active" : "inactive"}">${item.ativo ? "Ativo" : "Inativo"}</span></td>
                  <td class="planning-structure-actions">
                    <button class="icon-btn" type="button" data-component-edit="${item.id}" title="Editar componente"><i class="bi bi-pencil"></i></button>
                    <button class="icon-btn" type="button" data-component-delete="${item.id}" title="Remover componente"><i class="bi bi-trash"></i></button>
                  </td>
                </tr>`
            )
            .join("")
        : '<tr><td colspan="7" class="muted">Nenhum componente cadastrado.</td></tr>';
    };
    const renderModels = () => {
      modelList.innerHTML = rows.length
        ? rows
            .map(
              (item) => `
                <tr class="${Number(item.id) === Number(selectedModelId) ? "selected-row" : ""}">
                  <td>${esc(item.nome)}</td>
                  <td>${esc(item.exercicio_inicio)} a ${esc(item.exercicio_fim || "sem limite")}</td>
                  <td>${item.componentes.length}</td>
                  <td><span class="status-badge ${item.ativo ? "active" : "inactive"}">${item.ativo ? "Ativo" : "Inativo"}</span></td>
                  <td class="planning-structure-actions">
                    <button class="icon-btn" type="button" data-model-select="${item.id}" title="Configurar componentes"><i class="bi bi-list-ol"></i></button>
                    <button class="icon-btn" type="button" data-model-edit="${item.id}" title="Editar modelo"><i class="bi bi-pencil"></i></button>
                  </td>
                </tr>`
            )
            .join("")
        : '<tr><td colspan="5" class="muted">Nenhum modelo cadastrado.</td></tr>';
      renderComponents();
    };
    const load = async () => {
      showAppLoading("Carregando modelos...", "Aguarde enquanto a estrutura é atualizada.");
      try {
        const data = await request("/api/estrutura-planejamento/modelos-chave");
        rows = data.rows || [];
        if (selectedModelId && !selectedModel()) selectedModelId = null;
        renderModels();
      } catch (error) {
        setMessage(modelMessage, error.message, true);
      } finally {
        hideAppLoading();
      }
    };

    sourceEl.innerHTML = [
      '<option value="">Selecione...</option>',
      ...sources.map(
        (item) => `<option value="${esc(item.table)}">${esc(item.title)}</option>`
      ),
    ].join("");
    fillSourceFields();
    sourceEl.addEventListener("change", fillSourceFields);
    groupEnabledEl.addEventListener("change", updateGroupFields);
    document.getElementById("key-model-clear").addEventListener("click", clearModel);
    document.getElementById("key-component-clear").addEventListener("click", clearComponent);
    document.getElementById("key-model-refresh").addEventListener("click", load);

    modelForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const id = document.getElementById("key-model-id").value;
      const payload = {
        nome: document.getElementById("key-model-name").value,
        exercicio_inicio: document.getElementById("key-model-start").value,
        exercicio_fim: document.getElementById("key-model-end").value,
        prefixo: document.getElementById("key-model-prefix").value,
        separador: document.getElementById("key-model-separator").value,
        sufixo: document.getElementById("key-model-suffix").value,
        ativo: document.getElementById("key-model-active").checked,
      };
      try {
        const data = await request(
          `/api/estrutura-planejamento/modelos-chave${id ? `/${id}` : ""}`,
          { method: id ? "PUT" : "POST", body: JSON.stringify(payload) }
        );
        setMessage(modelMessage, data.message);
        clearModel();
        await load();
      } catch (error) {
        setMessage(modelMessage, error.message, true);
      }
    });

    modelList.addEventListener("click", (event) => {
      const selectButton = event.target.closest("[data-model-select]");
      const editButton = event.target.closest("[data-model-edit]");
      const id = Number(selectButton?.dataset.modelSelect || editButton?.dataset.modelEdit);
      const model = rows.find((item) => Number(item.id) === id);
      if (!model) return;
      if (selectButton) {
        selectedModelId = id;
        clearComponent();
        renderModels();
        componentCard.scrollIntoView({ behavior: "smooth", block: "start" });
      } else {
        document.getElementById("key-model-id").value = model.id;
        document.getElementById("key-model-name").value = model.nome;
        document.getElementById("key-model-start").value = model.exercicio_inicio;
        document.getElementById("key-model-end").value = model.exercicio_fim || "";
        document.getElementById("key-model-prefix").value = model.prefixo;
        document.getElementById("key-model-separator").value = model.separador;
        document.getElementById("key-model-suffix").value = model.sufixo;
        document.getElementById("key-model-active").checked = model.ativo;
        modelForm.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    });

    componentForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!selectedModelId) return;
      const id = document.getElementById("key-component-id").value;
      const payload = {
        codigo: document.getElementById("key-component-code").value,
        nome: document.getElementById("key-component-name").value,
        ordem: document.getElementById("key-component-order").value,
        tabela_origem: sourceEl.value,
        campo_id: idFieldEl.value,
        campo_codigo: codeFieldEl.value,
        campo_descricao: descriptionFieldEl.value,
        agrupador:
          groupEnabledEl.value === "1"
            ? document.getElementById("key-component-group").value
            : "",
        ordem_agrupador:
          groupEnabledEl.value === "1"
            ? document.getElementById("key-component-group-order").value
            : "",
        separador_agrupador:
          groupEnabledEl.value === "1"
            ? document.getElementById("key-component-group-separator").value
            : "",
        obrigatorio: document.getElementById("key-component-required").checked,
        ativo: document.getElementById("key-component-active").checked,
      };
      try {
        const url = id
          ? `/api/estrutura-planejamento/modelos-chave/componentes/${id}`
          : `/api/estrutura-planejamento/modelos-chave/${selectedModelId}/componentes`;
        const data = await request(url, {
          method: id ? "PUT" : "POST",
          body: JSON.stringify(payload),
        });
        setMessage(componentMessage, data.message);
        clearComponent();
        await load();
      } catch (error) {
        setMessage(componentMessage, error.message, true);
      }
    });

    componentList.addEventListener("click", async (event) => {
      const editButton = event.target.closest("[data-component-edit]");
      const deleteButton = event.target.closest("[data-component-delete]");
      const id = Number(editButton?.dataset.componentEdit || deleteButton?.dataset.componentDelete);
      const component = selectedModel()?.componentes.find((item) => Number(item.id) === id);
      if (!component) return;
      if (deleteButton) {
        if (!window.confirm(`Remover o componente "${component.nome}" deste modelo?`)) return;
        try {
          await request(`/api/estrutura-planejamento/modelos-chave/componentes/${id}`, {
            method: "DELETE",
          });
          await load();
        } catch (error) {
          setMessage(componentMessage, error.message, true);
        }
        return;
      }
      document.getElementById("key-component-id").value = component.id;
      document.getElementById("key-component-code").value = component.codigo;
      document.getElementById("key-component-name").value = component.nome;
      document.getElementById("key-component-order").value = component.ordem;
      sourceEl.value = component.tabela_origem;
      fillSourceFields();
      idFieldEl.value = component.campo_id;
      codeFieldEl.value = component.campo_codigo;
      descriptionFieldEl.value = component.campo_descricao;
      groupEnabledEl.value = component.agrupador ? "1" : "0";
      updateGroupFields();
      document.getElementById("key-component-group").value = component.agrupador;
      document.getElementById("key-component-group-order").value = component.ordem_agrupador || "";
      document.getElementById("key-component-group-separator").value = component.separador_agrupador;
      document.getElementById("key-component-required").checked = component.obrigatorio;
      document.getElementById("key-component-active").checked = component.ativo;
      componentForm.scrollIntoView({ behavior: "smooth", block: "start" });
    });

    clearModel();
    clearComponent();
    load();
  }

  function initCatalogoChave() {
    const page = document.getElementById("catalogo-chave-page");
    if (!page || page.dataset.bound === "1") return;
    page.dataset.bound = "1";

    const form = document.getElementById("key-catalog-form");
    const modelEl = document.getElementById("key-catalog-model");
    const componentsEl = document.getElementById("key-catalog-components");
    const programEl = document.getElementById("key-catalog-program");
    const actionEl = document.getElementById("key-catalog-action");
    const productEl = document.getElementById("key-catalog-product");
    const previewEl = document.getElementById("key-catalog-preview");
    const listEl = document.getElementById("key-catalog-list");
    const statusEl = document.getElementById("key-catalog-status");
    const searchEl = document.getElementById("key-catalog-search");
    const messageEl = document.getElementById("key-catalog-message");

    let data = { models: [], rows: [], programs: [], actions: [], products: [] };
    let builder = { model: null, components: [], mappings: [], facts: [] };
    let builderLoadSeq = 0;

    const esc = (value) =>
      String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    const normalize = (value) =>
      String(value || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase();
    const setMessage = (message = "", error = false) => {
      messageEl.textContent = message;
      messageEl.classList.toggle("text-error", error);
    };
    const request = async (url, options = {}) => {
      const response = await fetch(url, {
        ...options,
        headers: {
          "Content-Type": "application/json",
          "X-Requested-With": "fetch",
          ...(options.headers || {}),
        },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Falha ao processar a solicitação.");
      return payload;
    };
    const selectedValues = () => {
      const values = Object.fromEntries(
        [...componentsEl.querySelectorAll("select[data-key-component]")].map((select) => [
          select.dataset.keyComponent,
          select.value,
        ])
      );
      componentsEl.querySelectorAll("[data-key-component-multi]").forEach((wrap) => {
        const componentId = wrap.dataset.keyComponentMulti;
        values[componentId] = [
          ...wrap.querySelectorAll("input[data-key-component-option]:checked"),
        ].map((input) => input.value);
      });
      return values;
    };
    const fillSelect = (element, rows, placeholder) => {
      element.innerHTML = [
        `<option value="">${esc(placeholder)}</option>`,
        ...rows.map(
          (row) =>
            `<option value="${row.id}">${esc(
              [row.codigo, row.nome].filter(Boolean).join(" - ")
            )}</option>`
        ),
      ].join("");
    };
    const contextSelections = () =>
      [
        { source: "programa_planejamento", id: programEl.value },
        { source: "acao_planejamento", id: actionEl.value },
        { source: "produto_acao_planejamento", id: productEl.value },
      ].filter((item) => item.id);
    const factSources = () => {
      const sources = new Set();
      builder.facts.forEach((row) =>
        Object.keys(row || {}).forEach((source) => sources.add(source))
      );
      return sources;
    };
    const factRowsFor = (selected) => {
      const sources = factSources();
      const active = selected.filter((item) => sources.has(item.source));
      if (!builder.facts.length || !active.length) return builder.facts || [];
      return builder.facts.filter((row) =>
        active.every((item) => String(row[item.source]) === String(item.id))
      );
    };
    const isCompatibleWithSelections = (source, id, selected) => {
      const sources = factSources();
      if (!builder.facts.length || !sources.has(source)) return true;
      const rows = factRowsFor(selected);
      if (!rows.length) return false;
      return rows.some((row) => String(row[source]) === String(id));
    };
    const updateContext = () => {
      const programId = Number(programEl.value || 0);
      const selected = componentSelections();
      const actions = data.actions.filter(
        (item) =>
          (!programId || Number(item.programa_id) === programId) &&
          isCompatibleWithSelections("acao_planejamento", item.id, selected)
      );
      const currentAction = actionEl.value;
      fillSelect(
        actionEl,
        actions,
        programId ? "Selecione..." : "Selecione primeiro um programa..."
      );
      actionEl.disabled = !programId;
      if (actions.some((item) => String(item.id) === currentAction)) {
        actionEl.value = currentAction;
      }
      const selectedActionId = Number(actionEl.value || 0);
      const products = data.products.filter(
        (item) =>
          selectedActionId &&
          Number(item.acao_id) === selectedActionId &&
          isCompatibleWithSelections("produto_acao_planejamento", item.id, selected)
      );
      const currentProduct = productEl.value;
      fillSelect(
        productEl,
        products,
        selectedActionId ? "Selecione..." : "Selecione primeiro uma ação..."
      );
      productEl.disabled = !selectedActionId;
      if (products.some((item) => String(item.id) === currentProduct)) {
        productEl.value = currentProduct;
      }
    };
    const isRegionComponent = (component) => component.tabela_origem === "regiao";
    const updateMultiToggleLabel = (wrap) => {
      const toggle = wrap?.querySelector("[data-key-multi-toggle]");
      if (!toggle) return;
      const checked = [...wrap.querySelectorAll("input[data-key-component-option]:checked")];
      if (!checked.length) {
        toggle.textContent = "Selecione...";
      } else if (checked.length === 1) {
        toggle.textContent = checked[0].dataset.label || "1 região selecionada";
      } else {
        toggle.textContent = `${checked.length} regiões selecionadas`;
      }
    };
    const updatePreview = () => {
      if (!builder.model) {
        previewEl.textContent = "Selecione um modelo e seus componentes.";
        return;
      }
      const codesFor = (component) => {
        if (isRegionComponent(component)) {
          return [
            ...componentsEl.querySelectorAll(
              `[data-key-component-multi="${component.id}"] input[data-key-component-option]:checked`
            ),
          ]
            .map((input) => input.dataset.code || "")
            .filter(Boolean);
        }
        const select = componentsEl.querySelector(
          `[data-key-component="${component.id}"]`
        );
        const code = select?.selectedOptions[0]?.dataset.code || "";
        if (component.tabela_origem === "ug") {
          return code ? [code.replace(/^0+/, "") || "0"] : [];
        }
        return code ? [code] : [];
      };
      const tokenSlots = [];
      builder.components.forEach((component) => {
        if (component.agrupador) {
          if (tokenSlots.some((slot) => slot.group === component.agrupador)) return;
          const group = builder.components
            .filter((item) => item.agrupador === component.agrupador)
            .map((item) => ({
              order: Number(item.ordem_agrupador || item.ordem || 0),
              codes: codesFor(item),
            }))
            .filter((item) => item.codes.length)
            .sort((a, b) => a.order - b.order);
          if (group.length) {
            tokenSlots.push({
              group: component.agrupador,
              values: [
                group
                  .map((item) => item.codes[0])
                  .join(component.separador_agrupador || " + "),
              ],
            });
          }
          return;
        }
        const codes = codesFor(component);
        if (codes.length) tokenSlots.push({ values: codes });
      });
      const multiSlot = tokenSlots.find((slot) => slot.values.length > 1);
      if (!multiSlot) {
        const tokens = tokenSlots.map((slot) => slot.values[0]);
        previewEl.textContent = `${builder.model.prefixo || ""}${tokens.join(
          builder.model.separador || ""
        )}${builder.model.sufixo || ""}`;
        return;
      }
      const previews = multiSlot.values.map((value) => {
        const tokens = tokenSlots.map((slot) =>
          slot === multiSlot ? value : slot.values[0]
        );
        return `${builder.model.prefixo || ""}${tokens.join(
          builder.model.separador || ""
        )}${builder.model.sufixo || ""}`;
      });
      previewEl.textContent = previews.join("\n");
    };
    const selectedSourceId = (component, value) => {
      const option = (component.options || []).find(
        (item) => String(item.id) === String(value)
      );
      return option?.source_id || value;
    };
    const selectedSourceIds = (component, value) => {
      if (Array.isArray(value)) {
        return value.map((item) => selectedSourceId(component, item)).filter(Boolean);
      }
      const id = selectedSourceId(component, value);
      return id ? [id] : [];
    };
    const sourceOf = (component) => {
      const byTable = {
        regiao: "regiao",
        municipio: "municipio",
        funcao: "funcao",
        subfuncao: "subfuncao",
        ug: "ug",
        adj: "adj",
        macropolitica: "macropolitica",
        pilar: "pilar",
        metas_pee: "meta_pee",
        indicadores_pee: "indicador_pee",
        eixo: "eixo",
        politica_decr: "politica_decreto",
        publico_transversal: "publico_transversal",
        programa_planejamento: "programa_planejamento",
        acao_planejamento: "acao_planejamento",
        produto_acao_planejamento: "produto_acao_planejamento",
      };
      return byTable[component.tabela_origem] || component.source || component.tabela_origem;
    };
    const componentSelections = (values = selectedValues(), excludedComponentId = "") =>
      builder.components
        .filter((item) => String(item.id) !== String(excludedComponentId))
        .flatMap((item) =>
          selectedSourceIds(item, values[item.id]).map((id) => ({
            source: sourceOf(item),
            id,
          }))
        )
        .filter((item) => item.id);
    const filteredOptions = (component, values) => {
      const selected = [...contextSelections(), ...componentSelections(values, component.id)];
      const componentSource = sourceOf(component);
      return (component.options || []).filter((option) =>
        isCompatibleWithSelections(
          componentSource,
          option.source_id || option.id,
          selected
        )
      );
    };
    const renderBuilder = (values = {}) => {
      componentsEl.innerHTML = builder.components
        .map(
          (component) => {
            const options = filteredOptions(component, values);
            if (isRegionComponent(component)) {
              const selected = new Set(
                (Array.isArray(values[component.id])
                  ? values[component.id]
                  : [values[component.id]].filter(Boolean)
                ).map((value) => String(value))
              );
              return `
            <label class="field">
              <span>${component.obrigatorio ? "*" : ""}${esc(component.nome)}</span>
              <div class="planning-action-checklist planning-key-multi" data-key-component-multi="${component.id}">
                <button class="planning-action-checklist-toggle" data-key-multi-toggle type="button">Selecione...</button>
                <div class="planning-action-checklist-panel" data-key-multi-panel hidden>
                  <div class="planning-action-checklist-options">
                    ${options
                      .map(
                        (option) => {
                          const label = option.label || [option.codigo, option.descricao].filter(Boolean).join(" - ");
                          return `<label class="planning-action-checklist-option">
                            <input type="checkbox" data-key-component-option value="${esc(option.id)}" data-source-id="${esc(
                              option.source_id || option.id
                            )}" data-code="${esc(option.codigo)}" data-label="${esc(label)}" ${
                              selected.has(String(option.id)) ? "checked" : ""
                            } />
                            <span>${esc(label)}</span>
                          </label>`;
                        }
                      )
                      .join("")}
                  </div>
                </div>
              </div>
            </label>`;
            }
            return `
            <label class="field">
              <span>${component.obrigatorio ? "*" : ""}${esc(component.nome)}</span>
              <select data-key-component="${component.id}" ${component.obrigatorio ? "required" : ""}>
                <option value="">Selecione...</option>
                ${options
                  .map(
                    (option) =>
                      `<option value="${option.id}" data-source-id="${esc(
                        option.source_id || option.id
                      )}" data-code="${esc(option.codigo)}" ${
                        String(values[component.id] || "") === String(option.id)
                          ? "selected"
                          : ""
                      }>${esc(option.label || [option.codigo, option.descricao].filter(Boolean).join(" - "))}</option>`
                  )
                  .join("")}
              </select>
            </label>`;
          }
        )
        .join("");
      componentsEl.querySelectorAll("[data-key-component-multi]").forEach(updateMultiToggleLabel);
      updatePreview();
    };
    const loadBuilder = async (values = selectedValues()) => {
      const sequence = ++builderLoadSeq;
      if (!modelEl.value) {
        builder = { model: null, components: [], mappings: [], facts: [] };
        renderBuilder({});
        return;
      }
      const result = await request(
        "/api/estrutura-planejamento/catalogo-chave/opcoes",
        {
          method: "POST",
          body: JSON.stringify({
            modelo_chave_id: modelEl.value,
            selecionados: values,
          }),
        }
      );
      if (sequence !== builderLoadSeq) return;
      builder = {
        model: result.model,
        components: result.components || [],
        mappings: result.mappings || [],
        facts: result.facts || [],
      };
      renderBuilder(values);
    };
    const renderList = () => {
      const status = statusEl.value;
      const search = normalize(searchEl.value);
      const rows = data.rows.filter((row) => {
        if (status && String(Number(row.ativo)) !== status) return false;
        return (
          !search ||
          normalize(`${row.exercicio} ${row.modelo} ${row.chave_formatada}`).includes(search)
        );
      });
      listEl.innerHTML = rows.length
        ? rows
            .map(
              (row) => `
                <tr>
                  <td>${esc(row.exercicio)}</td>
                  <td>${esc(row.modelo)}</td>
                  <td class="planning-key-value">${esc(row.chave_formatada)}</td>
                  <td>${row.contextos.length}</td>
                  <td><span class="status-badge ${row.ativo ? "active" : "inactive"}">${row.ativo ? "Ativa" : "Inativa"}</span></td>
                  <td class="planning-structure-actions">
                    <button class="icon-btn" type="button" data-key-status="${row.id}" title="${row.ativo ? "Desativar" : "Ativar"}"><i class="bi ${row.ativo ? "bi-slash-circle" : "bi-check-circle"}"></i></button>
                  </td>
                </tr>`
            )
            .join("")
        : '<tr><td colspan="6" class="muted">Nenhuma chave encontrada.</td></tr>';
    };
    const clear = () => {
      form.reset();
      document.getElementById("key-catalog-origin").value = "";
      document.getElementById("key-catalog-form-title").textContent = "Nova chave";
      modelEl.value = "";
      builder = { model: null, components: [], mappings: [], facts: [] };
      componentsEl.innerHTML = "";
      previewEl.textContent = "Selecione um modelo e seus componentes.";
      fillSelect(programEl, data.programs, "Selecione...");
      actionEl.innerHTML = '<option value="">Selecione primeiro um programa...</option>';
      actionEl.disabled = true;
      productEl.innerHTML = '<option value="">Selecione primeiro uma ação...</option>';
      productEl.disabled = true;
      setMessage();
    };
    const load = async () => {
      showAppLoading("Carregando catálogo...", "Aguarde enquanto as chaves são atualizadas.");
      try {
        data = await request("/api/estrutura-planejamento/catalogo-chave");
        modelEl.innerHTML = [
          '<option value="">Selecione...</option>',
          ...data.models.map(
            (model) =>
              `<option value="${model.id}">${esc(model.nome)} (${model.exercicio_inicio}${model.exercicio_fim ? `-${model.exercicio_fim}` : "+"})</option>`
          ),
        ].join("");
        fillSelect(programEl, data.programs, "Selecione...");
        renderList();
      } catch (error) {
        setMessage(error.message, true);
      } finally {
        hideAppLoading();
      }
    };

    modelEl.addEventListener("change", () => loadBuilder({}));
    componentsEl.addEventListener("change", async (event) => {
      const field = event.target.closest("[data-key-component], [data-key-component-option]");
      if (!field) return;
      if (field.matches("[data-key-component-option]")) {
        updateMultiToggleLabel(field.closest("[data-key-component-multi]"));
        updatePreview();
        updateContext();
        return;
      }
      const values = selectedValues();
      renderBuilder(values);
      updateContext();
    });
    componentsEl.addEventListener("click", (event) => {
      const toggle = event.target.closest("[data-key-multi-toggle]");
      if (!toggle) return;
      const wrap = toggle.closest("[data-key-component-multi]");
      const panel = wrap?.querySelector("[data-key-multi-panel]");
      if (!panel) return;
      componentsEl.querySelectorAll("[data-key-multi-panel]").forEach((current) => {
        if (current !== panel) current.hidden = true;
      });
      panel.hidden = !panel.hidden;
    });
    document.addEventListener("click", (event) => {
      if (!page.contains(event.target)) return;
      if (event.target.closest("[data-key-component-multi]")) return;
      componentsEl.querySelectorAll("[data-key-multi-panel]").forEach((panel) => {
        panel.hidden = true;
      });
    });
    programEl.addEventListener("change", () => {
      actionEl.value = "";
      productEl.value = "";
      updateContext();
      renderBuilder(selectedValues());
    });
    actionEl.addEventListener("change", () => {
      productEl.value = "";
      updateContext();
      renderBuilder(selectedValues());
    });
    productEl.addEventListener("change", () => {
      renderBuilder(selectedValues());
    });
    statusEl.addEventListener("change", renderList);
    searchEl.addEventListener("input", renderList);
    document.getElementById("key-catalog-clear").addEventListener("click", clear);
    document.getElementById("key-catalog-refresh").addEventListener("click", load);

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const values = selectedValues();
      const payload = {
        modelo_chave_id: modelEl.value,
        exercicio: document.getElementById("key-catalog-exercise").value,
        observacao: document.getElementById("key-catalog-note").value,
        chave_origem_id: document.getElementById("key-catalog-origin").value,
        produto_acao_id: productEl.value,
        valores: values,
        ativo: true,
      };
      try {
        const result = await request("/api/estrutura-planejamento/catalogo-chave", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        const details = result.chaves?.length
          ? result.chaves.map((item) => item.chave_formatada).join("; ")
          : result.chave_formatada;
        setMessage(`${result.message} ${details || ""}`.trim());
        clear();
        await load();
      } catch (error) {
        setMessage(error.message, true);
      }
    });

    listEl.addEventListener("click", async (event) => {
      const statusButton = event.target.closest("[data-key-status]");
      if (!statusButton) return;
      const id = Number(statusButton.dataset.keyStatus);
      const row = data.rows.find((item) => Number(item.id) === id);
      if (!row) return;
      try {
        await request(
          `/api/estrutura-planejamento/catalogo-chave/${id}/situacao`,
          {
            method: "PUT",
            body: JSON.stringify({ ativo: !row.ativo }),
          }
        );
        await load();
      } catch (error) {
        setMessage(error.message, true);
      }
    });

    load();
  }

  function initReplicarExercicio() {
    const page = document.getElementById("replicar-exercicio-page");
    if (!page || page.dataset.bound === "1") return;
    page.dataset.bound = "1";

    const options = JSON.parse(page.dataset.options || "[]");
    const optionMap = Object.fromEntries(options.map((item) => [item.key, item]));
    const sourceEl = document.getElementById("replication-source-year");
    const targetEl = document.getElementById("replication-target-year");
    const dependencyMessage = document.getElementById(
      "replication-dependency-message"
    );
    const preview = document.getElementById("replication-preview");
    const previewBody = document.getElementById("replication-preview-body");
    const summaryEl = document.getElementById("replication-summary");
    const warningsEl = document.getElementById("replication-warnings");
    const confirmEl = document.getElementById("replication-confirm");
    const executeBtn = document.getElementById("replication-execute");
    const resultEl = document.getElementById("replication-result");
    const explicitSelection = new Set();
    let effectiveSelection = new Set();
    let analyzedPayload = null;

    const esc = (value) =>
      String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    const setMessage = (message = "", error = false) => {
      dependencyMessage.textContent = message;
      dependencyMessage.classList.toggle("text-error", error);
    };
    const request = async (url, payload) => {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Requested-With": "fetch",
        },
        body: JSON.stringify(payload),
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || "Falha ao processar a solicitação.");
      return data;
    };
    const invalidatePreview = () => {
      analyzedPayload = null;
      preview.hidden = true;
      confirmEl.checked = false;
      executeBtn.disabled = true;
      resultEl.textContent = "";
    };
    const dependencyClosure = () => {
      const result = new Set(explicitSelection);
      let changed = true;
      while (changed) {
        changed = false;
        [...result].forEach((key) => {
          (optionMap[key]?.requires || []).forEach((dependency) => {
            if (!result.has(dependency)) {
              result.add(dependency);
              changed = true;
            }
          });
        });
      }
      return result;
    };
    const requiredBy = (key) =>
      [...effectiveSelection]
        .filter((selected) => optionMap[selected]?.requires?.includes(key))
        .map((selected) => optionMap[selected].label);
    const syncOptions = (message = "") => {
      effectiveSelection = dependencyClosure();
      page.querySelectorAll("[data-replication-option]").forEach((input) => {
        const key = input.dataset.replicationOption;
        const automatic =
          effectiveSelection.has(key) && !explicitSelection.has(key);
        input.checked = effectiveSelection.has(key);
        input.closest(".planning-replication-option")?.classList.toggle(
          "required",
          automatic
        );
        const badge = input
          .closest(".planning-replication-option")
          ?.querySelector("[data-dependency-badge]");
        if (badge) {
          badge.hidden = !automatic;
          badge.title = automatic
            ? `Obrigatório para: ${requiredBy(key).join(", ")}`
            : "";
        }
      });
      setMessage(message);
      invalidatePreview();
    };
    const renderOptions = () => {
      ["estrutura", "vinculos", "chaves"].forEach((group) => {
        const container = document.getElementById(
          `replication-options-${group}`
        );
        container.innerHTML = options
          .filter((item) => item.group === group)
          .map(
            (item) => `
              <label class="planning-replication-option">
                <input type="checkbox" data-replication-option="${esc(item.key)}" />
                <span>${esc(item.label)}</span>
                <small data-dependency-badge hidden>Obrigatório</small>
              </label>`
          )
          .join("");
      });
    };
    const payload = () => ({
      exercicio_origem: sourceEl.value,
      exercicio_destino: targetEl.value,
      selecionados: [...effectiveSelection],
    });

    renderOptions();
    page.addEventListener("change", (event) => {
      const input = event.target.closest("[data-replication-option]");
      if (!input) return;
      const key = input.dataset.replicationOption;
      if (input.checked) {
        explicitSelection.add(key);
        syncOptions();
        return;
      }
      explicitSelection.delete(key);
      const nextSelection = dependencyClosure();
      const stillRequired = nextSelection.has(key);
      if (stillRequired) {
        effectiveSelection = nextSelection;
        const parents = [...nextSelection]
          .filter((selected) => optionMap[selected]?.requires?.includes(key))
          .map((selected) => optionMap[selected].label);
        syncOptions(
          `${optionMap[key].label} é obrigatório para: ${parents.join(", ")}.`
        );
      } else {
        syncOptions();
      }
    });
    sourceEl.addEventListener("change", () => {
      const source = Number(sourceEl.value || 0);
      if (source && !targetEl.value) targetEl.value = String(source + 1);
      invalidatePreview();
    });
    targetEl.addEventListener("input", invalidatePreview);
    confirmEl.addEventListener("change", () => {
      executeBtn.disabled = !confirmEl.checked;
    });
    document.getElementById("replication-clear").addEventListener("click", () => {
      explicitSelection.clear();
      sourceEl.value = "";
      targetEl.value = "";
      syncOptions();
    });
    document
      .getElementById("replication-analyze")
      .addEventListener("click", async () => {
        if (!effectiveSelection.size) {
          setMessage("Selecione ao menos uma estrutura para replicar.", true);
          return;
        }
        showAppLoading(
          "Analisando replicação...",
          "Aguarde enquanto os registros e vínculos são conferidos."
        );
        try {
          const data = await request(
            "/api/estrutura-planejamento/replicar-exercicio/analisar",
            payload()
          );
          analyzedPayload = payload();
          summaryEl.innerHTML = `
            <strong>${esc(data.source)} → ${esc(data.target)}</strong>
            <span>${data.items.length} grupos selecionados</span>`;
          previewBody.innerHTML = data.items
            .map(
              (item) => `
                <tr>
                  <td>${esc(item.label)}</td>
                  <td>${item.source}</td>
                  <td>${item.destination}</td>
                </tr>`
            )
            .join("");
          warningsEl.innerHTML = data.warnings.length
            ? `<strong>Atenção:</strong><ul>${data.warnings
                .map((warning) => `<li>${esc(warning)}</li>`)
                .join("")}</ul>`
            : "";
          preview.hidden = false;
          confirmEl.checked = false;
          executeBtn.disabled = true;
          resultEl.textContent = "";
          setMessage();
          preview.scrollIntoView({ behavior: "smooth", block: "start" });
        } catch (error) {
          setMessage(error.message, true);
        } finally {
          hideAppLoading();
        }
      });
    executeBtn.addEventListener("click", async () => {
      if (!analyzedPayload || !confirmEl.checked) return;
      const source = analyzedPayload.exercicio_origem;
      const target = analyzedPayload.exercicio_destino;
      if (
        !window.confirm(
          `Confirma a replicação da estrutura de ${source} para ${target}?`
        )
      ) {
        return;
      }
      showAppLoading(
        "Replicando exercício...",
        "A operação é transacional. Aguarde a conclusão."
      );
      executeBtn.disabled = true;
      try {
        const data = await request(
          "/api/estrutura-planejamento/replicar-exercicio/executar",
          analyzedPayload
        );
        const details = Object.entries(data.stats)
          .map(([key, stat]) => {
            const label = optionMap[key]?.label || key;
            return `${label}: ${stat.criados} criados, ${stat.reutilizados} reutilizados`;
          })
          .join(" | ");
        resultEl.textContent = `${data.message} ${details}`;
        resultEl.classList.remove("text-error");
        confirmEl.checked = false;
      } catch (error) {
        resultEl.textContent = error.message;
        resultEl.classList.add("text-error");
        executeBtn.disabled = false;
      } finally {
        hideAppLoading();
      }
    });
  }

  function initRelatorioEstruturaPlanejamento() {
    const page = document.getElementById("relatorio-estrutura-planejamento");
    const table = document.getElementById("planning-report-table");
    if (!page || !table || page.dataset.bound === "1") return;
    page.dataset.bound = "1";

    const configs = JSON.parse(page.dataset.configs || "{}");
    const typeEl = document.getElementById("planning-report-type");
    const typeLabel = document.getElementById("planning-report-type-label");
    const statusEl = document.getElementById("planning-report-status");
    const searchEl = document.getElementById("planning-report-search");
    const resetBtn = document.getElementById("planning-report-reset");
    const downloadBtn = document.getElementById("planning-report-download");
    const refreshBtn = document.getElementById("planning-report-refresh");
    const titleEl = document.getElementById("planning-report-title");
    const metaEl = document.getElementById("planning-report-meta");
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");
    const pageSizeEl = document.getElementById("planning-report-page-size");
    const paginationEl = document.getElementById("planning-report-pagination");

    let activeView = "cadastros";
    let columns = [];
    let rows = [];
    let currentPage = 1;
    let pageSize = Number(pageSizeEl?.value || 20) || 20;
    let requestToken = 0;

    const esc = (value) =>
      String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");

    const normalize = (value) =>
      String(value || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();

    const currentConfigs = () =>
      Array.isArray(configs[activeView]) ? configs[activeView] : [];

    const fillTypes = () => {
      const items = currentConfigs();
      typeEl.innerHTML = items
        .map((item) => `<option value="${esc(item.key)}">${esc(item.title)}</option>`)
        .join("");
      typeLabel.textContent =
        activeView === "cadastros" ? "*Tipo de cadastro" : "*Tipo de vínculo";
      statusEl.disabled = activeView !== "cadastros";
      statusEl.value = "";
    };

    const filteredRows = () => {
      const status = String(statusEl?.value || "");
      const search = normalize(searchEl?.value || "");
      return rows.filter((row) => {
        if (activeView === "cadastros" && status && row.situacao !== status) {
          return false;
        }
        if (!search) return true;
        return columns.some((column) =>
          normalize(row[column.key]).includes(search)
        );
      });
    };

    const renderPager = (totalPages) => {
      paginationEl.innerHTML = "";
      if (totalPages <= 1) return;
      const add = (label, target, disabled = false, active = false) => {
        const button = document.createElement("button");
        button.type = "button";
        button.textContent = label;
        button.disabled = disabled;
        button.classList.toggle("active", active);
        button.addEventListener("click", () => {
          currentPage = target;
          render();
        });
        paginationEl.appendChild(button);
      };
      add("<", Math.max(1, currentPage - 1), currentPage === 1);
      for (
        let number = Math.max(1, currentPage - 2);
        number <= Math.min(totalPages, currentPage + 2);
        number += 1
      ) {
        add(String(number), number, false, number === currentPage);
      }
      add(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
    };

    const render = () => {
      thead.innerHTML = `<tr>${columns
        .map((column) => `<th>${esc(column.label)}</th>`)
        .join("")}</tr>`;
      const filtered = filteredRows();
      const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
      currentPage = Math.min(currentPage, totalPages);
      const visible = filtered.slice(
        (currentPage - 1) * pageSize,
        currentPage * pageSize
      );
      tbody.innerHTML = visible.length
        ? visible
            .map(
              (row) =>
                `<tr>${columns
                  .map((column) => `<td>${esc(row[column.key])}</td>`)
                  .join("")}</tr>`
            )
            .join("")
        : `<tr><td colspan="${Math.max(1, columns.length)}" class="muted">Nenhum registro encontrado.</td></tr>`;
      metaEl.textContent = `${filtered.length} de ${rows.length} registros`;
      renderPager(visible.length ? totalPages : 0);
    };

    const load = async () => {
      const token = ++requestToken;
      const key = String(typeEl.value || currentConfigs()[0]?.key || "");
      if (!key) return;
      showAppLoading(
        "Carregando estrutura...",
        "Aguarde enquanto a consulta é atualizada."
      );
      thead.innerHTML = "";
      tbody.innerHTML =
        '<tr><td class="muted planning-component-loading-row"><i class="bi bi-arrow-repeat"></i> Carregando registros...</td></tr>';
      metaEl.textContent = "Carregando...";
      try {
        const response = await fetch(
          `/api/relatorios/estrutura-planejamento/${activeView}/${encodeURIComponent(key)}`,
          { headers: { "X-Requested-With": "fetch" } }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Falha ao carregar relatório.");
        if (token !== requestToken) return;
        columns = Array.isArray(data.columns) ? data.columns : [];
        rows = Array.isArray(data.rows) ? data.rows : [];
        titleEl.textContent = data.title || "Estrutura do Planejamento";
        currentPage = 1;
        render();
      } catch (error) {
        console.error(error);
        if (token !== requestToken) return;
        columns = [];
        rows = [];
        metaEl.textContent = error.message || "Falha ao carregar relatório.";
        tbody.innerHTML =
          '<tr><td class="text-error">Não foi possível carregar a consulta.</td></tr>';
      } finally {
        hideAppLoading();
      }
    };

    page.querySelectorAll("[data-report-view]").forEach((button) => {
      button.addEventListener("click", () => {
        activeView = button.dataset.reportView;
        page.dataset.activeView = activeView;
        page.querySelectorAll("[data-report-view]").forEach((item) => {
          const active = item === button;
          item.classList.toggle("active", active);
          item.setAttribute("aria-selected", String(active));
        });
        searchEl.value = "";
        statusEl.value = "";
        fillTypes();
        load();
      });
    });

    typeEl.addEventListener("change", () => {
      searchEl.value = "";
      statusEl.value = "";
      currentPage = 1;
      load();
    });
    statusEl.addEventListener("change", () => {
      currentPage = 1;
      render();
    });
    searchEl.addEventListener("input", () => {
      currentPage = 1;
      render();
    });
    pageSizeEl.addEventListener("change", () => {
      pageSize = Number(pageSizeEl.value || 20) || 20;
      currentPage = 1;
      render();
    });
    resetBtn.addEventListener("click", () => {
      searchEl.value = "";
      statusEl.value = "";
      currentPage = 1;
      render();
    });
    refreshBtn.addEventListener("click", load);
    downloadBtn.addEventListener("click", () => {
      const key = String(typeEl.value || "");
      if (!key) return;
      window.open(
        `/api/relatorios/estrutura-planejamento/download?view=${encodeURIComponent(
          activeView
        )}&key=${encodeURIComponent(key)}&status=${encodeURIComponent(
          activeView === "cadastros" ? statusEl.value : ""
        )}&search=${encodeURIComponent(searchEl.value)}`,
        "_blank"
      );
    });

    fillTypes();
    load();
  }

  function initRelatorioFip() {
    const table = document.getElementById("fip613-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
    const emptyState = document.getElementById("fip613-empty");
    const meta = document.getElementById("fip613-relatorio-meta");
    const pager = document.getElementById("fip613-pagination");
    const pageSizeSelect = document.getElementById("fip613-page-size");
    const btnDownload = document.getElementById("fip613-download");
    const btnReset = document.getElementById("fip613-reset");
    if (!table || !tbody) return;
    if (table.dataset.bound === "1") return;
    table.dataset.bound = "1";

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;
    let filteredRows = [];
    const sumCols = [
      "dotacao_inicial",
      "cred_suplementar",
      "cred_especial",
      "cred_extraordinario",
      "reducao",
      "cred_autorizado",
      "bloqueado_conting",
      "reserva_empenho",
      "saldo_destaque",
      "saldo_dotacao",
      "empenhado",
      "liquidado",
      "a_liquidar",
      "valor_pago",
      "valor_a_pagar",
    ];

    const numFmt = new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const fmt = (v) => {
      const n = Number(v || 0);
      if (Object.is(n, -0)) return "-";
      return n === 0 ? "-" : numFmt.format(n);
    };
    const numCls = (v) => {
      const n = Number(v || 0);
      const classes = ["num"];
      if (n > 0) classes.push("pos");
      else if (n < 0) classes.push("neg");
      return classes.join(" ");
    };

    const computeTotals = (rows) => {
      const totals = Object.fromEntries(sumCols.map((c) => [c, 0]));
      const paoeSet = new Set();
      const grupoSet = new Set();
      rows.forEach((r) => {
        const paoeParts = (r.projeto_atividade || "")
          .split(/\s+/)
          .filter((p) => /^\d+$/.test(p));
        if (paoeParts.length) paoeSet.add(paoeParts.join("*"));
        const natStr = String(r.natureza_despesa || "");
        if (natStr.length >= 2) grupoSet.add(natStr[1]);
        sumCols.forEach((c) => {
          const v = Number(r[c] || 0);
          if (!Number.isNaN(v)) totals[c] += v;
        });
      });
      return { totals, paoeSet, grupoSet };
    };

    const renderPagination = (totalPages) => {
      if (!pager) return;
      pager.innerHTML = "";
      if (totalPages <= 1) return;
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
        b.textContent = label;
        if (disabled) b.disabled = true;
        if (active) b.classList.add("active");
        b.addEventListener("click", () => {
          if (disabled || page === currentPage) return;
          currentPage = page;
          renderFiltered(false);
        });
        pager.appendChild(b);
      };
      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);

      const maxButtons = 5;
      const start = Math.max(1, Math.min(currentPage - 2, totalPages - maxButtons + 1));
      const end = Math.min(totalPages, start + maxButtons - 1);
      for (let p = start; p <= end; p++) {
        addBtn(String(p), p, false, p === currentPage);
      }
      if (end < totalPages) {
        const ellipsis = document.createElement("span");
        ellipsis.textContent = "...";
        pager.appendChild(ellipsis);
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }

      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };

    const render = () => {
      const rows = filteredRows;
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const viewRows = rows.slice(startIdx, startIdx + pageSize);

      tbody.innerHTML = "";
      const adjustedRows = rows.map((r) => {
        const copy = { ...r };
        negateCols.forEach((k) => {
          copy[k] = adjustVal(k, copy[k]);
        });
        return copy;
      });
      const { totals, paoeSet, grupoSet } = computeTotals(adjustedRows);
      const pageRows = adjustedRows.slice(startIdx, startIdx + pageSize);
      pageRows.forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${r.uo || ""}</td>
          <td>${r.ug || ""}</td>
          <td>${r.funcao || ""}</td>
          <td>${r.subfuncao || ""}</td>
          <td>${r.programa || ""}</td>
          <td>${r.projeto_atividade || ""}</td>
          <td>${r.regional || ""}</td>
          <td>${r.natureza_despesa || ""}</td>
          <td>${r.fonte_recurso || ""}</td>
          <td>${r.iduso ?? ""}</td>
          <td>${r.tipo_recurso || ""}</td>
          <td class="${numCls(r.dotacao_inicial)}">${fmt(r.dotacao_inicial)}</td>
          <td class="${numCls(r.cred_suplementar)}">${fmt(r.cred_suplementar)}</td>
          <td class="${numCls(r.cred_especial)}">${fmt(r.cred_especial)}</td>
          <td class="${numCls(r.cred_extraordinario)}">${fmt(r.cred_extraordinario)}</td>
          <td class="${numCls(r.reducao)}">${fmt(r.reducao)}</td>
          <td class="${numCls(r.cred_autorizado)}">${fmt(r.cred_autorizado)}</td>
          <td class="${numCls(r.bloqueado_conting)}">${fmt(r.bloqueado_conting)}</td>
          <td class="${numCls(r.reserva_empenho)}">${fmt(r.reserva_empenho)}</td>
          <td class="${numCls(r.saldo_destaque)}">${fmt(r.saldo_destaque)}</td>
          <td class="${numCls(r.saldo_dotacao)}">${fmt(r.saldo_dotacao)}</td>
          <td class="${numCls(r.empenhado)}">${fmt(r.empenhado)}</td>
          <td class="${numCls(r.liquidado)}">${fmt(r.liquidado)}</td>
          <td class="${numCls(r.a_liquidar)}">${fmt(r.a_liquidar)}</td>
          <td class="${numCls(r.valor_pago)}">${fmt(r.valor_pago)}</td>
          <td class="${numCls(r.valor_a_pagar)}">${fmt(r.valor_a_pagar)}</td>
        `;
        tbody.appendChild(tr);
      });
      // linha de totais
      const totalTr = document.createElement("tr");
      totalTr.innerHTML = `
        <td colspan="11"><strong>Totais (linhas filtradas)</strong></td>
        <td class="${numCls(totals.dotacao_inicial)}"><strong>${totals.dotacao_inicial.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.cred_suplementar)}"><strong>${totals.cred_suplementar.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.cred_especial)}"><strong>${totals.cred_especial.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.cred_extraordinario)}"><strong>${totals.cred_extraordinario.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.reducao)}"><strong>${totals.reducao.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.cred_autorizado)}"><strong>${totals.cred_autorizado.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.bloqueado_conting)}"><strong>${totals.bloqueado_conting.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.reserva_empenho)}"><strong>${totals.reserva_empenho.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.saldo_destaque)}"><strong>${totals.saldo_destaque.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.saldo_dotacao)}"><strong>${totals.saldo_dotacao.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.empenhado)}"><strong>${totals.empenhado.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.liquidado)}"><strong>${totals.liquidado.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.a_liquidar)}"><strong>${totals.a_liquidar.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.valor_pago)}"><strong>${totals.valor_pago.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.valor_a_pagar)}"><strong>${totals.valor_a_pagar.toLocaleString("pt-BR")}</strong></td>
      `;
      tbody.appendChild(totalTr);

      const paoeEl = document.getElementById("tot-paoe");
      const grupoEl = document.getElementById("tot-grupo");
      const credAutoEl = document.getElementById("tot-cred-autorizado");
      const bloqueadoEl = document.getElementById("tot-bloqueado");
      const tetoEl = document.getElementById("tot-teto");
      const saldoDotEl = document.getElementById("tot-saldo-dotacao");
      if (paoeEl) {
        if (paoeSet.size === 0) {
          paoeEl.textContent = "-";
        } else if (paoeSet.size > 10) {
          paoeEl.textContent = "Vários PAOEs";
        } else {
          paoeEl.textContent = Array.from(paoeSet).join(" * ");
        }
      }
      if (grupoEl) grupoEl.textContent = grupoSet.size ? Array.from(grupoSet).join("*") : "-";
      const formatVal = (el, val) => {
        if (!el) return;
        const n = Number(val || 0);
        el.textContent = n === 0 ? "-" : n.toLocaleString("pt-BR");
        el.classList.remove("pos", "neg");
        if (n > 0) el.classList.add("pos");
        if (n < 0) el.classList.add("neg");
      };
      formatVal(credAutoEl, totals.cred_autorizado);
      const bloqueadoVal = totals.bloqueado_conting;
      // bloquear cores no cred_autorizado e teto
      if (bloqueadoEl) formatVal(bloqueadoEl, bloqueadoVal);
      if (tetoEl) {
        const teto = totals.cred_autorizado + bloqueadoVal;
        tetoEl.textContent = Number(teto || 0).toLocaleString("pt-BR");
        tetoEl.classList.remove("pos", "neg");
      }
      if (credAutoEl) {
        credAutoEl.textContent = Number(totals.cred_autorizado || 0).toLocaleString("pt-BR");
        credAutoEl.classList.remove("pos", "neg");
      }
      formatVal(saldoDotEl, totals.saldo_dotacao);
      renderPagination(totalPages);
      toggleReportEmptyState({
        tableEl: table,
        emptyEl: emptyState,
        btnDownloadEl: btnDownload,
        pagerEl: pager,
        hasRows: rows.length > 0,
      });
    };

    const allData = { rows: [] };

    const colKeys = [
      "uo",
      "ug",
      "funcao",
      "subfuncao",
      "programa",
      "projeto_atividade",
      "regional",
      "natureza_despesa",
      "fonte_recurso",
      "iduso",
      "tipo_recurso",
      "dotacao_inicial",
      "cred_suplementar",
      "cred_especial",
      "cred_extraordinario",
      "reducao",
      "cred_autorizado",
      "bloqueado_conting",
      "reserva_empenho",
      "saldo_destaque",
      "saldo_dotacao",
      "empenhado",
      "liquidado",
      "a_liquidar",
      "valor_pago",
      "valor_a_pagar",
    ];

    const filterContainers = table.querySelectorAll(".filter-row [data-col]");
    const filters = Object.fromEntries(colKeys.map((k) => [k, new Set()]));
    const filterControls = {};

    const closeAllPanels = () => {
      Object.values(filterControls).forEach((ctrl) => {
        if (ctrl?.panel) ctrl.panel.classList.remove("open");
      });
    };

    const updateDisplay = (key) => {
      const set = filters[key] || new Set();
      const ctrl = filterControls[key];
      if (!ctrl) return;
      const map = ctrl.labelMap || {};
      if (ctrl.allCb) ctrl.allCb.checked = set.size === 0;
      (ctrl.optionCbs || []).forEach((cb) => {
        cb.checked = set.has(cb.dataset.val || "");
      });
      if (set.size === 0) {
        ctrl.label.textContent = "(Todos)";
      } else if (set.size <= 2) {
        ctrl.label.textContent = Array.from(set)
          .map((v) => map[v] || v)
          .join(", ");
      } else {
        ctrl.label.textContent = `${set.size} selecionados`;
      }
    };

    const buildFilter = (container, options, key) => {
      container.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "mf-wrapper";
      const display = document.createElement("button");
      display.type = "button";
      display.className = "mf-display";
      const label = document.createElement("span");
      label.textContent = "(Todos)";
      display.appendChild(label);
      const icon = document.createElement("i");
      icon.className = "bi bi-chevron-down";
      display.appendChild(icon);

      const panel = document.createElement("div");
      panel.className = "mf-panel";
      const search = document.createElement("input");
      search.type = "text";
      search.className = "mf-search";
      search.placeholder = "Buscar...";
      const list = document.createElement("div");
      list.className = "mf-options";

      const tempSelected = new Set(filters[key] || []);
      const allId = `${key}-all`;
      const allRow = document.createElement("label");
      allRow.className = "mf-option";
      const allCb = document.createElement("input");
      allCb.type = "checkbox";
      allCb.id = allId;
      allCb.dataset.val = "";
      allRow.appendChild(allCb);
      const allSpan = document.createElement("span");
      allSpan.textContent = "(Todos)";
      allRow.appendChild(allSpan);
      list.appendChild(allRow);

      const selectVisibleRow = document.createElement("label");
      selectVisibleRow.className = "mf-option mf-select-visible";
      const selectVisibleCb = document.createElement("input");
      selectVisibleCb.type = "checkbox";
      selectVisibleRow.appendChild(selectVisibleCb);
      const selectVisibleSpan = document.createElement("span");
      selectVisibleSpan.textContent = "Selecionar exibidos";
      selectVisibleRow.appendChild(selectVisibleSpan);
      list.appendChild(selectVisibleRow);

      const cbs = [];
      const labelMap = {};
      options.forEach((opt) => {
        const row = document.createElement("label");
        row.className = "mf-option";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        const norm = String(opt || "").toLowerCase();
        cb.dataset.val = norm;
        labelMap[norm] = opt;
        row.appendChild(cb);
        const txt = document.createElement("span");
        txt.textContent = opt;
        row.appendChild(txt);
        list.appendChild(row);
        cbs.push({ cb, txt, row, val: norm });
      });

      const syncUIFromTemp = () => {
        allCb.checked = tempSelected.size === 0;
        cbs.forEach(({ cb, val }) => {
          cb.checked = tempSelected.has(val);
        });
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        const allVisibleSelected = visible.length > 0 && visible.every(({ cb }) => cb.checked);
        selectVisibleCb.checked = allVisibleSelected;
      };

      const applyTempToFilters = () => {
        const set = filters[key];
        set.clear();
        tempSelected.forEach((v) => set.add(v));
        updateDisplay(key);
        renderFiltered();
      };

      const closePanel = () => panel.classList.remove("open");

      allCb.addEventListener("change", () => {
        if (allCb.checked) {
          tempSelected.clear();
          syncUIFromTemp();
        }
      });

      selectVisibleCb.addEventListener("change", () => {
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        if (selectVisibleCb.checked) {
          visible.forEach(({ val }) => tempSelected.add(val));
        } else {
          visible.forEach(({ val }) => tempSelected.delete(val));
        }
        allCb.checked = tempSelected.size === 0;
        syncUIFromTemp();
      });

      cbs.forEach(({ cb, val }) => {
        cb.addEventListener("change", () => {
          if (cb.checked) {
            tempSelected.add(val);
            allCb.checked = false;
          } else {
            tempSelected.delete(val);
          }
          syncUIFromTemp();
        });
      });

      search.addEventListener("input", () => {
        const term = search.value.toLowerCase();
        cbs.forEach(({ row, txt }) => {
          const match = txt.textContent.toLowerCase().includes(term);
          row.style.display = match ? "" : "none";
        });
        const allMatch = "(todos)".includes(term) || term === "";
        allRow.style.display = allMatch ? "" : "none";
        selectVisibleRow.style.display = "";
        syncUIFromTemp();
      });

      const actions = document.createElement("div");
      actions.className = "mf-actions";
      const cancelBtn = document.createElement("button");
      cancelBtn.type = "button";
      cancelBtn.className = "mf-btn ghost";
      cancelBtn.textContent = "Cancelar";
      const applyBtn = document.createElement("button");
      applyBtn.type = "button";
      applyBtn.className = "mf-btn primary";
      applyBtn.textContent = "Aplicar";

      cancelBtn.addEventListener("click", () => {
        tempSelected.clear();
        filters[key].forEach((v) => tempSelected.add(v));
        syncUIFromTemp();
        closePanel();
      });
      applyBtn.addEventListener("click", () => {
        applyTempToFilters();
        closePanel();
      });

      display.addEventListener("click", () => {
        const isOpen = panel.classList.contains("open");
        closeAllPanels();
        if (!isOpen) {
          panel.style.width = "";
          panel.style.height = "";
          tempSelected.clear();
          filters[key].forEach((v) => tempSelected.add(v));
          cbs.forEach(({ row }) => (row.style.display = ""));
          allRow.style.display = "";
          search.value = "";
          syncUIFromTemp();
          panel.classList.add("open");
        }
      });

      wrap.appendChild(display);
      panel.appendChild(search);
      panel.appendChild(list);
      actions.appendChild(cancelBtn);
      actions.appendChild(applyBtn);
      panel.appendChild(actions);
      wrap.appendChild(panel);
      container.appendChild(wrap);

      filterControls[key] = {
        panel,
        label,
        allCb,
        optionCbs: cbs.map((c) => c.cb),
        labelMap,
      };
      updateDisplay(key);
    };

    const setOptions = (rows = allData.rows) => {
      closeAllPanels();
      const uniques = colKeys.map(() => new Set());
      (rows || []).forEach((r) => {
        colKeys.forEach((k, idx) => {
          const v = r[k];
          if (v !== undefined && v !== null && v !== "") uniques[idx].add(String(v));
        });
      });
      filterContainers.forEach((container) => {
        const key = container.getAttribute("data-col");
        const idx = colKeys.indexOf(key);
        if (idx === -1) return;
        const opts = Array.from(uniques[idx]).sort((a, b) => a.localeCompare(b, "pt-BR"));
        buildFilter(container, opts, key);
      });
    };

    const renderFiltered = (resetPage = true) => {
      const filtered = allData.rows.filter((r) =>
        colKeys.every((k) => {
          const set = filters[k];
          if (!set || set.size === 0) return true;
          const val = r[k];
          const cmp = val === null || val === undefined ? "" : String(val).toLowerCase();
          return set.has(cmp);
        })
      );
      setOptions(filtered);
      filteredRows = filtered;
      if (resetPage) currentPage = 1;
      render();
    };

      if (!multiFilterClickBound) {
        document.addEventListener("click", (ev) => {
          if (!ev.target.closest(".mf-wrapper")) {
            closeAllPanels();
          }
      });
      multiFilterClickBound = true;
    }

    const load = async () => {
      if (meta) meta.textContent = "Carregando...";
      try {
        const res = await fetch("/api/relatorios/fip613");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar.");
        allData.rows = data.data || [];
        setOptions(allData.rows);
        filteredRows = allData.rows;
        render();
        if (meta) {
          const dt = formatAmazonLocalTime(data.data_arquivo);
          const user = data.user_email || "-";
          const uploaded = formatAmazonTime(data.uploaded_at);
          meta.innerHTML = `
            <div><strong>Última atualização</strong></div>
            <div>Enviado por: ${user}</div>
            <div>Upload em: ${uploaded}</div>
            <div>Data do download: ${dt}</div>
          `;
        }
      } catch (err) {
        if (meta) meta.textContent = err.message;
        console.error(err);
      }
    };

    load();

    if (btnReset) {
      btnReset.addEventListener("click", () => {
        Object.keys(filters).forEach((k) => filters[k].clear());
        setOptions(allData.rows);
        filteredRows = allData.rows;
        currentPage = 1;
        render();
      });
    }

    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        const val = parseInt(pageSizeSelect.value || "20", 10);
        pageSize = Number.isNaN(val) ? 20 : val;
        currentPage = 1;
        render();
      });
    }

    if (btnDownload) {
      btnDownload.addEventListener("click", () => {
        window.open("/api/relatorios/fip613/download", "_blank");
      });
    }
  }

  function initRelatorioPlan20() {
    const table = document.getElementById("plan20-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
    const emptyState = document.getElementById("plan20-empty");
    const meta = document.getElementById("plan20-relatorio-meta");
    const pager = document.getElementById("plan20-pagination");
    const pageSizeSelect = document.getElementById("plan20-page-size");
    const btnDownload = document.getElementById("plan20-download");
    const btnReset = document.getElementById("plan20-reset");
    const totExercicio = document.getElementById("plan20-tot-exercicio");
    const totValorTotal = document.getElementById("plan20-tot-valor-total");
    if (!table || !tbody) return;
    if (table.dataset.bound === "1") return;
    table.dataset.bound = "1";

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;
    let filteredRows = [];

    const numFmt = new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const fmtNum = (v) => {
      const n = Number(v);
      if (Number.isNaN(n)) return v ?? "";
      return numFmt.format(n);
    };

    const updateTotals = (rows) => {
      const exSet = new Set();
      let totalVal = 0;
      rows.forEach((r) => {
        if (r.exercicio !== undefined && r.exercicio !== null && r.exercicio !== "") {
          exSet.add(String(r.exercicio));
        }
        const v = Number(r.valor_total || 0);
        if (!Number.isNaN(v)) totalVal += v;
      });
      if (totExercicio) {
        totExercicio.textContent = exSet.size ? Array.from(exSet).sort((a, b) => a.localeCompare(b, "pt-BR")).join(" * ") : "-";
      }
      if (totValorTotal) {
        totValorTotal.textContent = numFmt.format(totalVal);
        totValorTotal.classList.remove("pos", "neg");
        if (totalVal > 0) totValorTotal.classList.add("pos");
        else if (totalVal < 0) totValorTotal.classList.add("neg");
      }
    };

    const renderPagination = (totalPages) => {
      if (!pager) return;
      pager.innerHTML = "";
      if (totalPages <= 1) return;
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
        b.textContent = label;
        if (disabled) b.disabled = true;
        if (active) b.classList.add("active");
        b.addEventListener("click", () => {
          if (disabled || page === currentPage) return;
          currentPage = page;
          renderFiltered(false);
        });
        pager.appendChild(b);
      };
      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);

      const maxButtons = 5;
      const start = Math.max(1, Math.min(currentPage - 2, totalPages - maxButtons + 1));
      const end = Math.min(totalPages, start + maxButtons - 1);
      for (let p = start; p <= end; p++) {
        addBtn(String(p), p, false, p === currentPage);
      }
      if (end < totalPages) {
        const ellipsis = document.createElement("span");
        ellipsis.textContent = "...";
        pager.appendChild(ellipsis);
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }

      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };

    const render = () => {
      const rows = filteredRows;
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const pageRows = rows.slice(startIdx, startIdx + pageSize);

      tbody.innerHTML = "";
      pageRows.forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${r.exercicio ?? ""}</td>
          <td>${r.chave_planejamento ?? ""}</td>
          <td>${r.regiao ?? ""}</td>
          <td>${r.subfuncao_ug ?? ""}</td>
          <td>${r.adj ?? ""}</td>
          <td>${r.macropolitica ?? ""}</td>
          <td>${r.pilar ?? ""}</td>
          <td>${r.eixo ?? ""}</td>
          <td>${r.politica_decreto ?? ""}</td>
          <td>${r.publico_transversal_chave ?? ""}</td>
          <td>${r.programa ?? ""}</td>
          <td>${r.funcao ?? ""}</td>
          <td>${r.unidade_orcamentaria ?? ""}</td>
          <td>${r.acao_paoe ?? ""}</td>
          <td>${r.subfuncao ?? ""}</td>
          <td>${r.objetivo_especifico ?? ""}</td>
          <td>${r.esfera ?? ""}</td>
          <td>${r.responsavel_acao ?? ""}</td>
          <td>${r.produto_acao ?? ""}</td>
          <td>${r.unid_medida_produto ?? ""}</td>
          <td>${r.regiao_produto ?? ""}</td>
          <td>${r.meta_produto ?? ""}</td>
          <td>${r.saldo_meta_produto ?? ""}</td>
          <td>${r.publico_transversal ?? ""}</td>
          <td>${r.subacao_entrega ?? ""}</td>
          <td>${r.responsavel ?? ""}</td>
          <td>${r.prazo ?? ""}</td>
          <td>${r.unid_gestora ?? ""}</td>
          <td>${r.unidade_setorial_planejamento ?? ""}</td>
          <td>${r.produto_subacao ?? ""}</td>
          <td>${r.unidade_medida ?? ""}</td>
          <td>${r.regiao_subacao ?? ""}</td>
          <td>${r.codigo ?? ""}</td>
          <td>${r.municipios_entrega ?? ""}</td>
          <td>${r.meta_subacao ?? ""}</td>
          <td>${r.detalhamento_produto ?? ""}</td>
          <td>${r.etapa ?? ""}</td>
          <td>${r.responsavel_etapa ?? ""}</td>
          <td>${r.prazo_etapa ?? ""}</td>
          <td>${r.regiao_etapa ?? ""}</td>
          <td>${r.natureza ?? ""}</td>
          <td>${r.cat_econ ?? ""}</td>
          <td>${r.grupo ?? ""}</td>
          <td>${r.modalidade ?? ""}</td>
          <td>${r.elemento ?? ""}</td>
          <td>${r.subelemento ?? ""}</td>
          <td>${r.fonte ?? ""}</td>
          <td>${r.idu ?? ""}</td>
          <td>${r.descricao_item_despesa ?? ""}</td>
          <td>${r.unid_medida_item ?? ""}</td>
          <td class="num">${fmtNum(r.quantidade)}</td>
          <td class="num">${fmtNum(r.valor_unitario)}</td>
          <td class="num">${fmtNum(r.valor_total)}</td>
        `;
        tbody.appendChild(tr);
      });

      renderPagination(totalPages);
      updateTotals(rows);
      toggleReportEmptyState({
        tableEl: table,
        emptyEl: emptyState,
        btnDownloadEl: btnDownload,
        pagerEl: pager,
        hasRows: rows.length > 0,
      });
    };

    const allData = { rows: [] };

    const colKeys = [
      "exercicio",
      "chave_planejamento",
      "regiao",
      "subfuncao_ug",
      "adj",
      "macropolitica",
      "pilar",
      "eixo",
      "politica_decreto",
      "publico_transversal_chave",
      "programa",
      "funcao",
      "unidade_orcamentaria",
      "acao_paoe",
      "subfuncao",
      "objetivo_especifico",
      "esfera",
      "responsavel_acao",
      "produto_acao",
      "unid_medida_produto",
      "regiao_produto",
      "meta_produto",
      "saldo_meta_produto",
      "publico_transversal",
      "subacao_entrega",
      "responsavel",
      "prazo",
      "unid_gestora",
      "unidade_setorial_planejamento",
      "produto_subacao",
      "unidade_medida",
      "regiao_subacao",
      "codigo",
      "municipios_entrega",
      "meta_subacao",
      "detalhamento_produto",
      "etapa",
      "responsavel_etapa",
      "prazo_etapa",
      "regiao_etapa",
      "natureza",
      "cat_econ",
      "grupo",
      "modalidade",
      "elemento",
      "subelemento",
      "fonte",
      "idu",
      "descricao_item_despesa",
      "unid_medida_item",
      "quantidade",
      "valor_unitario",
      "valor_total",
    ];

    const filterContainers = table.querySelectorAll(".filter-row [data-col]");
    const filters = Object.fromEntries(colKeys.map((k) => [k, new Set()]));
    const filterControls = {};

    const closeAllPanels = () => {
      Object.values(filterControls).forEach((ctrl) => {
        if (ctrl?.panel) ctrl.panel.classList.remove("open");
      });
    };

    const updateDisplay = (key) => {
      const set = filters[key] || new Set();
      const ctrl = filterControls[key];
      if (!ctrl) return;
      const map = ctrl.labelMap || {};
      if (ctrl.allCb) ctrl.allCb.checked = set.size === 0;
      (ctrl.optionCbs || []).forEach((cb) => {
        cb.checked = set.has(cb.dataset.val || "");
      });
      if (set.size === 0) {
        ctrl.label.textContent = "(Todos)";
      } else if (set.size <= 2) {
        ctrl.label.textContent = Array.from(set)
          .map((v) => map[v] || v)
          .join(", ");
      } else {
        ctrl.label.textContent = `${set.size} selecionados`;
      }
    };

    const buildFilter = (container, options, key) => {
      container.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "mf-wrapper";
      const display = document.createElement("button");
      display.type = "button";
      display.className = "mf-display";
      const label = document.createElement("span");
      label.textContent = "(Todos)";
      display.appendChild(label);
      const icon = document.createElement("i");
      icon.className = "bi bi-chevron-down";
      display.appendChild(icon);

      const panel = document.createElement("div");
      panel.className = "mf-panel";
      const search = document.createElement("input");
      search.type = "text";
      search.className = "mf-search";
      search.placeholder = "Buscar...";
      const list = document.createElement("div");
      list.className = "mf-options";

      const tempSelected = new Set(filters[key] || []);
      const allId = `${key}-all`;
      const allRow = document.createElement("label");
      allRow.className = "mf-option";
      const allCb = document.createElement("input");
      allCb.type = "checkbox";
      allCb.id = allId;
      allCb.dataset.val = "";
      allRow.appendChild(allCb);
      const allSpan = document.createElement("span");
      allSpan.textContent = "(Todos)";
      allRow.appendChild(allSpan);
      list.appendChild(allRow);

      const selectVisibleRow = document.createElement("label");
      selectVisibleRow.className = "mf-option mf-select-visible";
      const selectVisibleCb = document.createElement("input");
      selectVisibleCb.type = "checkbox";
      selectVisibleRow.appendChild(selectVisibleCb);
      const selectVisibleSpan = document.createElement("span");
      selectVisibleSpan.textContent = "Selecionar exibidos";
      selectVisibleRow.appendChild(selectVisibleSpan);
      list.appendChild(selectVisibleRow);

      const cbs = [];
      const labelMap = {};
      options.forEach((opt) => {
        const row = document.createElement("label");
        row.className = "mf-option";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        const norm = String(opt || "").toLowerCase();
        cb.dataset.val = norm;
        labelMap[norm] = opt;
        row.appendChild(cb);
        const txt = document.createElement("span");
        txt.textContent = opt;
        row.appendChild(txt);
        list.appendChild(row);
        cbs.push({ cb, txt, row, val: norm });
      });

      const syncUIFromTemp = () => {
        allCb.checked = tempSelected.size === 0;
        cbs.forEach(({ cb, val }) => {
          cb.checked = tempSelected.has(val);
        });
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        const allVisibleSelected = visible.length > 0 && visible.every(({ cb }) => cb.checked);
        selectVisibleCb.checked = allVisibleSelected;
      };

      const applyTempToFilters = () => {
        const set = filters[key];
        set.clear();
        tempSelected.forEach((v) => set.add(v));
        updateDisplay(key);
        renderFiltered();
      };

      const closePanel = () => panel.classList.remove("open");

      allCb.addEventListener("change", () => {
        if (allCb.checked) {
          tempSelected.clear();
          syncUIFromTemp();
        }
      });

      selectVisibleCb.addEventListener("change", () => {
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        if (selectVisibleCb.checked) {
          visible.forEach(({ val }) => tempSelected.add(val));
        } else {
          visible.forEach(({ val }) => tempSelected.delete(val));
        }
        allCb.checked = tempSelected.size === 0;
        syncUIFromTemp();
      });

      cbs.forEach(({ cb, val }) => {
        cb.addEventListener("change", () => {
          if (cb.checked) {
            tempSelected.add(val);
            allCb.checked = false;
          } else {
            tempSelected.delete(val);
          }
          syncUIFromTemp();
        });
      });

      search.addEventListener("input", () => {
        const term = search.value.toLowerCase();
        cbs.forEach(({ row, txt }) => {
          const match = txt.textContent.toLowerCase().includes(term);
          row.style.display = match ? "" : "none";
        });
        const allMatch = "(todos)".includes(term) || term === "";
        allRow.style.display = allMatch ? "" : "none";
        selectVisibleRow.style.display = "";
        syncUIFromTemp();
      });

      const actions = document.createElement("div");
      actions.className = "mf-actions";
      const cancelBtn = document.createElement("button");
      cancelBtn.type = "button";
      cancelBtn.className = "mf-btn ghost";
      cancelBtn.textContent = "Cancelar";
      const applyBtn = document.createElement("button");
      applyBtn.type = "button";
      applyBtn.className = "mf-btn primary";
      applyBtn.textContent = "Aplicar";

      cancelBtn.addEventListener("click", () => {
        tempSelected.clear();
        filters[key].forEach((v) => tempSelected.add(v));
        syncUIFromTemp();
        closePanel();
      });
      applyBtn.addEventListener("click", () => {
        applyTempToFilters();
        closePanel();
      });

      display.addEventListener("click", () => {
        const isOpen = panel.classList.contains("open");
        closeAllPanels();
        if (!isOpen) {
          panel.style.width = "";
          panel.style.height = "";
          tempSelected.clear();
          filters[key].forEach((v) => tempSelected.add(v));
          cbs.forEach(({ row }) => (row.style.display = ""));
          allRow.style.display = "";
          search.value = "";
          syncUIFromTemp();
          panel.classList.add("open");
        }
      });

      wrap.appendChild(display);
      panel.appendChild(search);
      panel.appendChild(list);
      actions.appendChild(cancelBtn);
      actions.appendChild(applyBtn);
      panel.appendChild(actions);
      wrap.appendChild(panel);
      container.appendChild(wrap);

      filterControls[key] = {
        panel,
        label,
        allCb,
        optionCbs: cbs.map((c) => c.cb),
        labelMap,
      };
      updateDisplay(key);
    };

    const setOptions = (rows = allData.rows) => {
      closeAllPanels();
      const uniques = colKeys.map(() => new Set());
      (rows || []).forEach((r) => {
        colKeys.forEach((k, idx) => {
          const v = r[k];
          if (v !== undefined && v !== null && v !== "") uniques[idx].add(String(v));
        });
      });
      filterContainers.forEach((container) => {
        const key = container.getAttribute("data-col");
        const idx = colKeys.indexOf(key);
        if (idx === -1) return;
        const opts = Array.from(uniques[idx]).sort((a, b) => a.localeCompare(b, "pt-BR"));
        buildFilter(container, opts, key);
      });
    };

    const renderFiltered = (resetPage = true) => {
      const filtered = allData.rows.filter((r) =>
        colKeys.every((k) => {
          const set = filters[k];
          if (!set || set.size === 0) return true;
          const val = r[k];
          const cmp = val === null || val === undefined ? "" : String(val).toLowerCase();
          return set.has(cmp);
        })
      );
      setOptions(filtered);
      filteredRows = filtered;
      if (resetPage) currentPage = 1;
      render();
    };

    if (!multiFilterClickBound) {
      document.addEventListener("click", (ev) => {
        if (!ev.target.closest(".mf-wrapper")) {
          closeAllPanels();
        }
      });
      multiFilterClickBound = true;
    }

    const load = async () => {
      if (meta) meta.textContent = "Carregando...";
      try {
        const res = await fetch("/api/relatorios/plan20-seduc");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar.");
        allData.rows = data.data || [];
        setOptions(allData.rows);
        filteredRows = allData.rows;
        render();
        if (meta) {
          const dt = formatAmazonLocalTime(data.data_arquivo);
          const user = data.user_email || "-";
          const uploaded = formatAmazonTime(data.uploaded_at);
          meta.innerHTML = `
            <div><strong>Última atualização</strong></div>
            <div>Enviado por: ${user}</div>
            <div>Upload em: ${uploaded}</div>
            <div>Data do download: ${dt}</div>
          `;
        }
      } catch (err) {
        if (meta) meta.textContent = err.message;
        console.error(err);
      }
    };

    load();

    if (btnReset) {
      btnReset.addEventListener("click", () => {
        Object.keys(filters).forEach((k) => filters[k].clear());
        setOptions(allData.rows);
        filteredRows = allData.rows;
        currentPage = 1;
        render();
      });
    }

    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        const val = parseInt(pageSizeSelect.value || "20", 10);
        pageSize = Number.isNaN(val) ? 20 : val;
        currentPage = 1;
        render();
      });
    }

    if (btnDownload) {
      btnDownload.addEventListener("click", () => {
        window.open("/api/relatorios/plan20-seduc/download", "_blank");
      });
    }
  }

  function initRelatorioPlan21Nger() {
    const table = document.getElementById("plan21-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
    const emptyState = document.getElementById("plan21-empty");
    const meta = document.getElementById("plan21-relatorio-meta");
    const pager = document.getElementById("plan21-pagination");
    const pageSizeSelect = document.getElementById("plan21-page-size");
    const btnDownload = document.getElementById("plan21-download");
    const btnReset = document.getElementById("plan21-reset");
    const totExercicio = document.getElementById("plan21-tot-exercicio");
    const totValorTotal = document.getElementById("plan21-tot-valor-total");
    if (!table || !tbody) return;
    if (table.dataset.bound === "1") return;
    table.dataset.bound = "1";

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;
    let filteredRows = [];

    const numFmt = new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const fmtNum = (v) => {
      const n = Number(v);
      if (Number.isNaN(n)) return v ?? "";
      return numFmt.format(n);
    };

    const updateTotals = (rows) => {
      const exSet = new Set();
      let totalVal = 0;
      rows.forEach((r) => {
        if (r.exercicio !== undefined && r.exercicio !== null && r.exercicio !== "") {
          exSet.add(String(r.exercicio));
        }
        const v = Number(r.valor_total || 0);
        if (!Number.isNaN(v)) totalVal += v;
      });
      if (totExercicio) {
        totExercicio.textContent = exSet.size
          ? Array.from(exSet).sort((a, b) => a.localeCompare(b, "pt-BR")).join(" * ")
          : "-";
      }
      if (totValorTotal) {
        totValorTotal.textContent = numFmt.format(totalVal);
        totValorTotal.classList.remove("pos", "neg");
        if (totalVal > 0) totValorTotal.classList.add("pos");
        else if (totalVal < 0) totValorTotal.classList.add("neg");
      }
    };

    const renderPagination = (totalPages) => {
      if (!pager) return;
      pager.innerHTML = "";
      if (totalPages <= 1) return;
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
        b.textContent = label;
        if (disabled) b.disabled = true;
        if (active) b.classList.add("active");
        b.addEventListener("click", () => {
          if (disabled || page === currentPage) return;
          currentPage = page;
          renderFiltered(false);
        });
        pager.appendChild(b);
      };
      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);

      const maxButtons = 5;
      const start = Math.max(1, Math.min(currentPage - 2, totalPages - maxButtons + 1));
      const end = Math.min(totalPages, start + maxButtons - 1);
      for (let p = start; p <= end; p++) {
        addBtn(String(p), p, false, p === currentPage);
      }
      if (end < totalPages) {
        const ellipsis = document.createElement("span");
        ellipsis.textContent = "...";
        pager.appendChild(ellipsis);
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }

      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };

    const render = () => {
      const rows = filteredRows;
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const pageRows = rows.slice(startIdx, startIdx + pageSize);

      tbody.innerHTML = "";
      pageRows.forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${r.exercicio ?? ""}</td>
          <td>${r.chave_planejamento ?? ""}</td>
          <td>${r.regiao ?? ""}</td>
          <td>${r.subfuncao_ug ?? ""}</td>
          <td>${r.adj ?? ""}</td>
          <td>${r.macropolitica ?? ""}</td>
          <td>${r.pilar ?? ""}</td>
          <td>${r.eixo ?? ""}</td>
          <td>${r.politica_decreto ?? ""}</td>
          <td>${r.publico_transversal_chave ?? ""}</td>
          <td>${r.programa ?? ""}</td>
          <td>${r.funcao ?? ""}</td>
          <td>${r.unidade_orcamentaria ?? ""}</td>
          <td>${r.acao_paoe ?? ""}</td>
          <td>${r.subfuncao ?? ""}</td>
          <td>${r.objetivo_especifico ?? ""}</td>
          <td>${r.esfera ?? ""}</td>
          <td>${r.responsavel_acao ?? ""}</td>
          <td>${r.produto_acao ?? ""}</td>
          <td>${r.unid_medida_produto ?? ""}</td>
          <td>${r.regiao_produto ?? ""}</td>
          <td>${r.meta_produto ?? ""}</td>
          <td>${r.meta_credito ?? ""}</td>
          <td>${r.meta_anulada ?? ""}</td>
          <td>${r.meta_atual ?? ""}</td>
          <td>${r.saldo_meta_produto ?? ""}</td>
          <td>${r.publico_transversal ?? ""}</td>
          <td>${r.subacao_entrega ?? ""}</td>
          <td>${r.responsavel ?? ""}</td>
          <td>${r.prazo ?? ""}</td>
          <td>${r.unid_gestora ?? ""}</td>
          <td>${r.unidade_setorial_planejamento ?? ""}</td>
          <td>${r.produto_subacao ?? ""}</td>
          <td>${r.unidade_medida ?? ""}</td>
          <td>${r.regiao_subacao ?? ""}</td>
          <td>${r.codigo ?? ""}</td>
          <td>${r.municipios_entrega ?? ""}</td>
          <td>${r.meta_subacao ?? ""}</td>
          <td>${r.detalhamento_produto ?? ""}</td>
          <td>${r.etapa ?? ""}</td>
          <td>${r.responsavel_etapa ?? ""}</td>
          <td>${r.prazo_etapa ?? ""}</td>
          <td>${r.regiao_etapa ?? ""}</td>
          <td>${r.natureza ?? ""}</td>
          <td>${r.cat_econ ?? ""}</td>
          <td>${r.grupo ?? ""}</td>
          <td>${r.modalidade ?? ""}</td>
          <td>${r.elemento ?? ""}</td>
          <td>${r.subelemento ?? ""}</td>
          <td>${r.fonte ?? ""}</td>
          <td>${r.idu ?? ""}</td>
          <td>${r.descricao_item_despesa ?? ""}</td>
          <td>${r.unid_medida_item ?? ""}</td>
          <td class="num">${fmtNum(r.quantidade)}</td>
          <td class="num">${fmtNum(r.valor_unitario)}</td>
          <td class="num">${fmtNum(r.valor_total)}</td>
          <td class="num">${fmtNum(r.suplementacao)}</td>
          <td class="num">${fmtNum(r.anulacao)}</td>
          <td class="num">${fmtNum(r.valor_atual)}</td>
        `;
        tbody.appendChild(tr);
      });

      renderPagination(totalPages);
      updateTotals(rows);
      toggleReportEmptyState({
        tableEl: table,
        emptyEl: emptyState,
        btnDownloadEl: btnDownload,
        pagerEl: pager,
        hasRows: rows.length > 0,
      });
    };

    const allData = { rows: [] };

    const colKeys = [
      "exercicio",
      "chave_planejamento",
      "regiao",
      "subfuncao_ug",
      "adj",
      "macropolitica",
      "pilar",
      "eixo",
      "politica_decreto",
      "publico_transversal_chave",
      "programa",
      "funcao",
      "unidade_orcamentaria",
      "acao_paoe",
      "subfuncao",
      "objetivo_especifico",
      "esfera",
      "responsavel_acao",
      "produto_acao",
      "unid_medida_produto",
      "regiao_produto",
      "meta_produto",
      "meta_credito",
      "meta_anulada",
      "meta_atual",
      "saldo_meta_produto",
      "publico_transversal",
      "subacao_entrega",
      "responsavel",
      "prazo",
      "unid_gestora",
      "unidade_setorial_planejamento",
      "produto_subacao",
      "unidade_medida",
      "regiao_subacao",
      "codigo",
      "municipios_entrega",
      "meta_subacao",
      "detalhamento_produto",
      "etapa",
      "responsavel_etapa",
      "prazo_etapa",
      "regiao_etapa",
      "natureza",
      "cat_econ",
      "grupo",
      "modalidade",
      "elemento",
      "subelemento",
      "fonte",
      "idu",
      "descricao_item_despesa",
      "unid_medida_item",
      "quantidade",
      "valor_unitario",
      "valor_total",
      "suplementacao",
      "anulacao",
      "valor_atual",
    ];

    const filterContainers = table.querySelectorAll(".filter-row [data-col]");
    const filters = Object.fromEntries(colKeys.map((k) => [k, new Set()]));
    const filterControls = {};

    const closeAllPanels = () => {
      Object.values(filterControls).forEach((ctrl) => {
        if (ctrl?.panel) ctrl.panel.classList.remove("open");
      });
    };

    const updateDisplay = (key) => {
      const set = filters[key] || new Set();
      const ctrl = filterControls[key];
      if (!ctrl) return;
      const map = ctrl.labelMap || {};
      if (ctrl.allCb) ctrl.allCb.checked = set.size === 0;
      (ctrl.optionCbs || []).forEach((cb) => {
        cb.checked = set.has(cb.dataset.val || "");
      });
      if (set.size === 0) {
        ctrl.label.textContent = "(Todos)";
      } else if (set.size <= 2) {
        ctrl.label.textContent = Array.from(set)
          .map((v) => map[v] || v)
          .join(", ");
      } else {
        ctrl.label.textContent = `${set.size} selecionados`;
      }
    };

    const buildFilter = (container, options, key) => {
      container.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "mf-wrapper";
      const display = document.createElement("button");
      display.type = "button";
      display.className = "mf-display";
      const label = document.createElement("span");
      label.textContent = "(Todos)";
      display.appendChild(label);
      const icon = document.createElement("i");
      icon.className = "bi bi-chevron-down";
      display.appendChild(icon);

      const panel = document.createElement("div");
      panel.className = "mf-panel";
      const search = document.createElement("input");
      search.type = "text";
      search.className = "mf-search";
      search.placeholder = "Buscar...";
      const list = document.createElement("div");
      list.className = "mf-options";

      const tempSelected = new Set(filters[key] || []);
      const allId = `${key}-all`;
      const allRow = document.createElement("label");
      allRow.className = "mf-option";
      const allCb = document.createElement("input");
      allCb.type = "checkbox";
      allCb.id = allId;
      allCb.dataset.val = "";
      allRow.appendChild(allCb);
      const allSpan = document.createElement("span");
      allSpan.textContent = "(Todos)";
      allRow.appendChild(allSpan);
      list.appendChild(allRow);

      const selectVisibleRow = document.createElement("label");
      selectVisibleRow.className = "mf-option mf-select-visible";
      const selectVisibleCb = document.createElement("input");
      selectVisibleCb.type = "checkbox";
      selectVisibleRow.appendChild(selectVisibleCb);
      const selectVisibleSpan = document.createElement("span");
      selectVisibleSpan.textContent = "Selecionar exibidos";
      selectVisibleRow.appendChild(selectVisibleSpan);
      list.appendChild(selectVisibleRow);

      const cbs = [];
      const labelMap = {};
      options.forEach((opt) => {
        const row = document.createElement("label");
        row.className = "mf-option";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        const norm = String(opt || "").toLowerCase();
        cb.dataset.val = norm;
        labelMap[norm] = opt;
        row.appendChild(cb);
        const txt = document.createElement("span");
        txt.textContent = opt;
        row.appendChild(txt);
        list.appendChild(row);
        cbs.push({ cb, txt, row, val: norm });
      });

      const syncUIFromTemp = () => {
        allCb.checked = tempSelected.size === 0;
        cbs.forEach(({ cb, val }) => {
          cb.checked = tempSelected.has(val);
        });
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        const allVisibleSelected = visible.length > 0 && visible.every(({ cb }) => cb.checked);
        selectVisibleCb.checked = allVisibleSelected;
      };

      const applyTempToFilters = () => {
        const set = filters[key];
        set.clear();
        tempSelected.forEach((v) => set.add(v));
        updateDisplay(key);
        renderFiltered();
      };

      const closePanel = () => panel.classList.remove("open");

      allCb.addEventListener("change", () => {
        if (allCb.checked) {
          tempSelected.clear();
          syncUIFromTemp();
        }
      });

      selectVisibleCb.addEventListener("change", () => {
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        if (selectVisibleCb.checked) {
          visible.forEach(({ val }) => tempSelected.add(val));
        } else {
          visible.forEach(({ val }) => tempSelected.delete(val));
        }
        allCb.checked = tempSelected.size === 0;
        syncUIFromTemp();
      });

      cbs.forEach(({ cb, val }) => {
        cb.addEventListener("change", () => {
          if (cb.checked) {
            tempSelected.add(val);
            allCb.checked = false;
          } else {
            tempSelected.delete(val);
          }
          syncUIFromTemp();
        });
      });

      search.addEventListener("input", () => {
        const term = search.value.toLowerCase();
        cbs.forEach(({ row, txt }) => {
          const match = txt.textContent.toLowerCase().includes(term);
          row.style.display = match ? "" : "none";
        });
        const allMatch = "(todos)".includes(term) || term === "";
        allRow.style.display = allMatch ? "" : "none";
        selectVisibleRow.style.display = "";
        syncUIFromTemp();
      });

      const actions = document.createElement("div");
      actions.className = "mf-actions";
      const cancelBtn = document.createElement("button");
      cancelBtn.type = "button";
      cancelBtn.className = "mf-btn ghost";
      cancelBtn.textContent = "Cancelar";
      const applyBtn = document.createElement("button");
      applyBtn.type = "button";
      applyBtn.className = "mf-btn primary";
      applyBtn.textContent = "Aplicar";

      cancelBtn.addEventListener("click", () => {
        tempSelected.clear();
        filters[key].forEach((v) => tempSelected.add(v));
        syncUIFromTemp();
        closePanel();
      });
      applyBtn.addEventListener("click", () => {
        applyTempToFilters();
        closePanel();
      });

      display.addEventListener("click", () => {
        const isOpen = panel.classList.contains("open");
        closeAllPanels();
        if (!isOpen) {
          panel.style.width = "";
          panel.style.height = "";
          tempSelected.clear();
          filters[key].forEach((v) => tempSelected.add(v));
          cbs.forEach(({ row }) => (row.style.display = ""));
          allRow.style.display = "";
          search.value = "";
          syncUIFromTemp();
          panel.classList.add("open");
        }
      });

      wrap.appendChild(display);
      panel.appendChild(search);
      panel.appendChild(list);
      actions.appendChild(cancelBtn);
      actions.appendChild(applyBtn);
      panel.appendChild(actions);
      wrap.appendChild(panel);
      container.appendChild(wrap);

      filterControls[key] = {
        panel,
        label,
        allCb,
        optionCbs: cbs.map((c) => c.cb),
        labelMap,
      };
      updateDisplay(key);
    };

    const setOptions = (rows = allData.rows) => {
      closeAllPanels();
      const uniques = colKeys.map(() => new Set());
      (rows || []).forEach((r) => {
        colKeys.forEach((k, idx) => {
          const v = r[k];
          if (v !== undefined && v !== null && v !== "") uniques[idx].add(String(v));
        });
      });
      filterContainers.forEach((container) => {
        const key = container.getAttribute("data-col");
        const idx = colKeys.indexOf(key);
        if (idx === -1) return;
        const opts = Array.from(uniques[idx]).sort((a, b) => a.localeCompare(b, "pt-BR"));
        buildFilter(container, opts, key);
      });
    };

    const renderFiltered = (resetPage = true) => {
      const filtered = allData.rows.filter((r) =>
        colKeys.every((k) => {
          const set = filters[k];
          if (!set || set.size === 0) return true;
          const val = r[k];
          const cmp = val === null || val === undefined ? "" : String(val).toLowerCase();
          return set.has(cmp);
        })
      );
      setOptions(filtered);
      filteredRows = filtered;
      if (resetPage) currentPage = 1;
      render();
    };

    if (!multiFilterClickBound) {
      document.addEventListener("click", (ev) => {
        if (!ev.target.closest(".mf-wrapper")) {
          closeAllPanels();
        }
      });
      multiFilterClickBound = true;
    }

    const load = async () => {
      if (meta) meta.textContent = "Carregando...";
      try {
        const res = await fetch("/api/relatorios/plan21-nger");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar.");
        allData.rows = data.data || [];
        setOptions(allData.rows);
        filteredRows = allData.rows;
        render();
        if (meta) {
          const dt = formatAmazonLocalTime(data.data_arquivo);
          const user = data.user_email || "-";
          const uploaded = formatAmazonTime(data.uploaded_at);
          meta.innerHTML = `
            <div><strong>Última atualização</strong></div>
            <div>Enviado por: ${user}</div>
            <div>Upload em: ${uploaded}</div>
            <div>Data do download: ${dt}</div>
          `;
        }
      } catch (err) {
        if (meta) meta.textContent = err.message;
        console.error(err);
      }
    };

    load();

    if (btnReset) {
      btnReset.addEventListener("click", () => {
        Object.keys(filters).forEach((k) => filters[k].clear());
        setOptions(allData.rows);
        filteredRows = allData.rows;
        currentPage = 1;
        render();
      });
    }

    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        const val = parseInt(pageSizeSelect.value || "20", 10);
        pageSize = Number.isNaN(val) ? 20 : val;
        currentPage = 1;
        render();
      });
    }

    if (btnDownload) {
      btnDownload.addEventListener("click", () => {
        window.open("/api/relatorios/plan21-nger/download", "_blank");
      });
    }
  }

  function initRelatorioEmp() {
    const table = document.getElementById("emp-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
    const emptyState = document.getElementById("emp-empty");
    const meta = document.getElementById("emp-relatorio-meta");
    const pager = document.getElementById("emp-pagination");
    const pageSizeSelect = document.getElementById("emp-page-size");
    const btnDownload = document.getElementById("emp-download");
    const btnReset = document.getElementById("emp-reset");
    const totExercicio = document.getElementById("emp-tot-exercicio");
    const totValorEmp = document.getElementById("emp-tot-valor-emp");
    const chaveHeader = document.getElementById("emp-col-chave");
    if (!table || !tbody) return;
    if (table.dataset.bound === "1") return;
    table.dataset.bound = "1";

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;
    let filteredRows = [];

    const numFmt = new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const fmtNum = (v) => {
      const n = Number(v);
      if (Number.isNaN(n)) return v ?? "";
      return numFmt.format(n);
    };

    const colKeys = [
      "chave_display",
      "regiao",
      "subfuncao_ug",
      "adj",
      "macropolitica",
      "pilar",
      "eixo",
      "politica_decreto",
      "exercicio",
      "numero_emp",
      "numero_ped",
      "valor_emp",
      "devolucao_gcv",
      "valor_emp_devolucao_gcv",
      "uo",
      "nome_unidade_orcamentaria",
      "ug",
      "nome_unidade_gestora",
      "dotacao_orcamentaria",
      "funcao",
      "subfuncao",
      "programa_governo",
      "paoe",
      "natureza_despesa",
      "cat_econ",
      "grupo",
      "modalidade",
      "elemento",
      "fonte",
      "iduso",
      "historico",
      "tipo_despesa",
      "credor",
      "nome_credor",
      "cpf_cnpj_credor",
      "categoria_credor",
      "tipo_empenho",
      "situacao",
      "data_emissao",
      "data_criacao",
      "numero_contrato",
      "numero_convenio",
    ];

    const filterContainers = table.querySelectorAll(".filter-row [data-col]");
    const allData = { rows: [] };
    const filters = Object.fromEntries(colKeys.map((k) => [k, new Set()]));
    const filterControls = {};

    const closeAllPanels = () => {
      Object.values(filterControls).forEach((ctrl) => {
        if (ctrl?.panel) ctrl.panel.classList.remove("open");
      });
    };

    const updateDisplay = (key) => {
      const set = filters[key] || new Set();
      const ctrl = filterControls[key];
      if (!ctrl) return;
      const map = ctrl.labelMap || {};
      if (ctrl.allCb) ctrl.allCb.checked = set.size === 0;
      (ctrl.optionCbs || []).forEach((cb) => {
        cb.checked = set.has(cb.dataset.val || "");
      });
      if (set.size === 0) {
        ctrl.label.textContent = "(Todos)";
      } else if (set.size <= 2) {
        ctrl.label.textContent = Array.from(set)
          .map((v) => map[v] || v)
          .join(", ");
      } else {
        ctrl.label.textContent = `${set.size} selecionados`;
      }
    };

    const buildFilter = (container, options, key) => {
      container.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "mf-wrapper";
      const display = document.createElement("button");
      display.type = "button";
      display.className = "mf-display";
      const label = document.createElement("span");
      label.textContent = "(Todos)";
      display.appendChild(label);
      const icon = document.createElement("i");
      icon.className = "bi bi-chevron-down";
      display.appendChild(icon);

      const panel = document.createElement("div");
      panel.className = "mf-panel";
      const search = document.createElement("input");
      search.type = "text";
      search.className = "mf-search";
      search.placeholder = "Buscar...";
      const list = document.createElement("div");
      list.className = "mf-options";

      const tempSelected = new Set(filters[key] || []);
      const allId = `${key}-all`;
      const allRow = document.createElement("label");
      allRow.className = "mf-option";
      const allCb = document.createElement("input");
      allCb.type = "checkbox";
      allCb.id = allId;
      allCb.dataset.val = "";
      allRow.appendChild(allCb);
      const allSpan = document.createElement("span");
      allSpan.textContent = "(Todos)";
      allRow.appendChild(allSpan);
      list.appendChild(allRow);

      const selectVisibleRow = document.createElement("label");
      selectVisibleRow.className = "mf-option mf-select-visible";
      const selectVisibleCb = document.createElement("input");
      selectVisibleCb.type = "checkbox";
      selectVisibleRow.appendChild(selectVisibleCb);
      const selectVisibleSpan = document.createElement("span");
      selectVisibleSpan.textContent = "Selecionar exibidos";
      selectVisibleRow.appendChild(selectVisibleSpan);
      list.appendChild(selectVisibleRow);

      const cbs = [];
      const labelMap = {};
      options.forEach((opt) => {
        const row = document.createElement("label");
        row.className = "mf-option";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        const norm = String(opt || "").toLowerCase();
        cb.dataset.val = norm;
        labelMap[norm] = opt;
        row.appendChild(cb);
        const txt = document.createElement("span");
        txt.textContent = opt;
        row.appendChild(txt);
        list.appendChild(row);
        cbs.push({ cb, txt, row, val: norm });
      });

      const syncUIFromTemp = () => {
        allCb.checked = tempSelected.size === 0;
        cbs.forEach(({ cb, val }) => {
          cb.checked = tempSelected.has(val);
        });
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        const allVisibleSelected = visible.length > 0 && visible.every(({ cb }) => cb.checked);
        selectVisibleCb.checked = allVisibleSelected;
      };

      const applyTempToFilters = () => {
        const set = filters[key];
        set.clear();
        tempSelected.forEach((v) => set.add(v));
        updateDisplay(key);
        renderFiltered();
      };

      const closePanel = () => panel.classList.remove("open");

      allCb.addEventListener("change", () => {
        if (allCb.checked) {
          tempSelected.clear();
          syncUIFromTemp();
        }
      });

      selectVisibleCb.addEventListener("change", () => {
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        if (selectVisibleCb.checked) {
          visible.forEach(({ val }) => tempSelected.add(val));
        } else {
          visible.forEach(({ val }) => tempSelected.delete(val));
        }
        allCb.checked = tempSelected.size === 0;
        syncUIFromTemp();
      });

      cbs.forEach(({ cb, val }) => {
        cb.addEventListener("change", () => {
          if (cb.checked) {
            tempSelected.add(val);
            allCb.checked = false;
          } else {
            tempSelected.delete(val);
          }
          syncUIFromTemp();
        });
      });

      search.addEventListener("input", () => {
        const term = search.value.toLowerCase();
        cbs.forEach(({ row, txt }) => {
          const match = txt.textContent.toLowerCase().includes(term);
          row.style.display = match ? "" : "none";
        });
        const allMatch = "(todos)".includes(term) || term === "";
        allRow.style.display = allMatch ? "" : "none";
        selectVisibleRow.style.display = "";
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        const allVisibleSelected = visible.length > 0 && visible.every(({ cb }) => cb.checked);
        selectVisibleCb.checked = allVisibleSelected;
      });

      const actions = document.createElement("div");
      actions.className = "mf-actions";
      const cancelBtn = document.createElement("button");
      cancelBtn.type = "button";
      cancelBtn.className = "mf-btn ghost";
      cancelBtn.textContent = "Cancelar";
      const applyBtn = document.createElement("button");
      applyBtn.type = "button";
      applyBtn.className = "mf-btn primary";
      applyBtn.textContent = "Aplicar";

      cancelBtn.addEventListener("click", () => {
        tempSelected.clear();
        filters[key].forEach((v) => tempSelected.add(v));
        syncUIFromTemp();
        closePanel();
      });
      applyBtn.addEventListener("click", () => {
        applyTempToFilters();
        closePanel();
      });

      display.addEventListener("click", () => {
        const isOpen = panel.classList.contains("open");
        closeAllPanels();
        if (!isOpen) {
          panel.style.width = "";
          panel.style.height = "";
          tempSelected.clear();
          filters[key].forEach((v) => tempSelected.add(v));
          cbs.forEach(({ row }) => (row.style.display = ""));
          allRow.style.display = "";
          search.value = "";
          syncUIFromTemp();
          panel.classList.add("open");
        }
      });

      wrap.appendChild(display);
      panel.appendChild(search);
      panel.appendChild(list);
      actions.appendChild(cancelBtn);
      actions.appendChild(applyBtn);
      panel.appendChild(actions);
      wrap.appendChild(panel);
      container.appendChild(wrap);

      filterControls[key] = {
        panel,
        label,
        allCb,
        optionCbs: cbs.map((c) => c.cb),
        labelMap,
      };
      updateDisplay(key);
    };

    const setOptions = (rows = allData.rows) => {
      closeAllPanels();
      const uniques = colKeys.map(() => new Set());
      (rows || []).forEach((r) => {
        colKeys.forEach((k, idx) => {
          uniques[idx].add((r[k] ?? "").toString());
        });
      });
      filterContainers.forEach((container) => {
        const key = container.getAttribute("data-col");
        const idx = colKeys.indexOf(key);
        if (idx === -1) return;
        const opts = Array.from(uniques[idx]).sort((a, b) => a.localeCompare(b, "pt-BR"));
        buildFilter(container, opts, key);
      });
    };

    const renderFiltered = (resetPage = true) => {
      const filtered = allData.rows.filter((r) =>
        colKeys.every((k) => {
          const set = filters[k];
          if (!set || set.size === 0) return true;
          const val = r[k];
          const cmp = val === null || val === undefined ? "" : String(val).toLowerCase();
          return set.has(cmp);
        })
      );
      setOptions(filtered);
      filteredRows = filtered;
      if (resetPage) currentPage = 1;
      render();
    };

    const renderPagination = (totalPages) => {
      if (!pager) return;
      pager.innerHTML = "";
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
        b.type = "button";
        b.className = "page-btn";
        b.textContent = label;
        if (disabled) b.disabled = true;
        if (active) b.classList.add("active");
        b.addEventListener("click", () => {
          if (disabled || page === currentPage) return;
          currentPage = page;
          render();
        });
        pager.appendChild(b);
      };
      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);
      const maxButtons = 5;
      let start = Math.max(1, currentPage - Math.floor(maxButtons / 2));
      let end = Math.min(totalPages, start + maxButtons - 1);
      if (end - start + 1 < maxButtons) {
        start = Math.max(1, end - maxButtons + 1);
      }
      if (start > 1) {
        addBtn("1", 1, false, currentPage === 1);
        if (start > 2) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          pager.appendChild(ellipsis);
        }
      }
      for (let p = start; p <= end; p += 1) {
        addBtn(String(p), p, false, p === currentPage);
      }
      if (end < totalPages) {
        const ellipsis = document.createElement("span");
        ellipsis.textContent = "...";
        pager.appendChild(ellipsis);
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }
      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };

    const updateTotals = (rows) => {
      const exSet = new Set();
      let totalVal = 0;
      rows.forEach((r) => {
        if (r.exercicio !== undefined && r.exercicio !== null && r.exercicio !== "") {
          exSet.add(String(r.exercicio));
        }
        const v = Number(r.valor_emp_devolucao_gcv || 0);
        if (!Number.isNaN(v)) totalVal += v;
      });
      if (totExercicio) {
        totExercicio.textContent = exSet.size
          ? Array.from(exSet).sort((a, b) => a.localeCompare(b, "pt-BR")).join(" | ")
          : "-";
      }
      if (totValorEmp) {
        totValorEmp.textContent = numFmt.format(totalVal);
        totValorEmp.classList.remove("pos", "neg");
        if (totalVal > 0) totValorEmp.classList.add("pos");
        else if (totalVal < 0) totValorEmp.classList.add("neg");
      }
    };

    const render = () => {
      const rows = filteredRows;
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const pageRows = rows.slice(startIdx, startIdx + pageSize);

      tbody.innerHTML = "";
      pageRows.forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${r.chave_display ?? ""}</td>
          <td>${r.regiao ?? ""}</td>
          <td>${r.subfuncao_ug ?? ""}</td>
          <td>${r.adj ?? ""}</td>
          <td>${r.macropolitica ?? ""}</td>
          <td>${r.pilar ?? ""}</td>
          <td>${r.eixo ?? ""}</td>
          <td>${r.politica_decreto ?? ""}</td>
          <td>${r.exercicio ?? ""}</td>
          <td>${r.numero_emp ?? ""}</td>
          <td>${r.numero_ped ?? ""}</td>
          <td class="num">${fmtNum(r.valor_emp)}</td>
          <td class="num">${fmtNum(r.devolucao_gcv)}</td>
          <td class="num">${fmtNum(r.valor_emp_devolucao_gcv)}</td>
          <td>${r.uo ?? ""}</td>
          <td>${r.nome_unidade_orcamentaria ?? ""}</td>
          <td>${r.ug ?? ""}</td>
          <td>${r.nome_unidade_gestora ?? ""}</td>
          <td>${r.dotacao_orcamentaria ?? ""}</td>
          <td>${r.funcao ?? ""}</td>
          <td>${r.subfuncao ?? ""}</td>
          <td>${r.programa_governo ?? ""}</td>
          <td>${r.paoe ?? ""}</td>
          <td>${r.natureza_despesa ?? ""}</td>
          <td>${r.cat_econ ?? ""}</td>
          <td>${r.grupo ?? ""}</td>
          <td>${r.modalidade ?? ""}</td>
          <td>${r.elemento ?? ""}</td>
          <td>${r.fonte ?? ""}</td>
          <td>${r.iduso ?? ""}</td>
          <td>${r.historico ?? ""}</td>
          <td>${r.tipo_despesa ?? ""}</td>
          <td>${r.credor ?? ""}</td>
          <td>${r.nome_credor ?? ""}</td>
          <td>${r.cpf_cnpj_credor ?? ""}</td>
          <td>${r.categoria_credor ?? ""}</td>
          <td>${r.tipo_empenho ?? ""}</td>
          <td>${r.situacao ?? ""}</td>
          <td>${r.data_emissao ?? ""}</td>
          <td>${r.data_criacao ?? ""}</td>
          <td>${r.numero_contrato ?? ""}</td>
          <td>${r.numero_convenio ?? ""}</td>
        `;
        tbody.appendChild(tr);
      });

      renderPagination(totalPages);
      updateTotals(rows);
      toggleReportEmptyState({
        tableEl: table,
        emptyEl: emptyState,
        btnDownloadEl: btnDownload,
        pagerEl: pager,
        hasRows: rows.length > 0,
      });
    };

    const load = async () => {
      if (meta) meta.textContent = "Carregando...";
      try {
        const res = await fetch("/api/relatorios/emp");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar.");
          allData.rows = (data.data || []).map((r) => {
            const chaveDisplay = r.chave || r.chave_planejamento || "";
            return { ...r, chave_display: chaveDisplay };
          });
        filteredRows = allData.rows;
        setOptions(allData.rows);
        render();
        if (meta) {
          const dt = formatAmazonLocalTime(data.data_arquivo);
          const user = data.user_email || "-";
          const uploaded = formatAmazonTime(data.uploaded_at);
          meta.innerHTML = `
            <div><strong>Última atualização</strong></div>
            <div>Enviado por: ${user}</div>
            <div>Upload em: ${uploaded}</div>
            <div>Data do download: ${dt}</div>
          `;
        }
        if (chaveHeader) {
          chaveHeader.textContent = "Chave de Planejamento/Chave";
        }
      } catch (err) {
        if (meta) meta.textContent = err.message;
        console.error(err);
      }
    };

    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        pageSize = parseInt(pageSizeSelect.value || "20", 10) || 20;
        currentPage = 1;
        render();
      });
    }

    if (btnDownload) {
      btnDownload.addEventListener("click", () => {
        window.open("/api/relatorios/emp/download", "_blank");
      });
    }

    if (btnReset) {
      btnReset.addEventListener("click", () => {
        closeAllPanels();
        Object.keys(filters).forEach((k) => filters[k].clear());
        setOptions(allData.rows);
        filteredRows = allData.rows;
        currentPage = 1;
        render();
      });
    }

    if (!multiFilterClickBound) {
      document.addEventListener("click", (ev) => {
        if (!ev.target.closest(".mf-wrapper")) {
          closeAllPanels();
        }
      });
      multiFilterClickBound = true;
    }

    load();
  }

  function initRelatorioEstEmp() {
    const table = document.getElementById("est-emp-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
    const emptyState = document.getElementById("est-emp-empty");
    const meta = document.getElementById("est-emp-relatorio-meta");
    const pager = document.getElementById("est-emp-pagination");
    const pageSizeSelect = document.getElementById("est-emp-page-size");
    const btnDownload = document.getElementById("est-emp-download");
    const btnReset = document.getElementById("est-emp-reset");
    const totExercicio = document.getElementById("est-emp-tot-exercicio");
    const totValor = document.getElementById("est-emp-tot-valor-est-emp");
    if (!table || !tbody) return;
    if (table.dataset.bound === "1") return;
    table.dataset.bound = "1";

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;
    let filteredRows = [];

    const numFmt = new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const fmtNum = (v) => {
      const n = Number(v);
      if (Number.isNaN(n)) return v ?? "";
      return numFmt.format(n);
    };

    const colKeys = [
      "exercicio",
      "numero_est",
      "numero_emp",
      "empenho_atual",
      "empenho_rp",
      "numero_ped",
      "valor_emp",
      "valor_est_emp_sem_aqs",
      "valor_est_emp_com_aqs",
      "valor_emp_liquido",
      "uo",
      "nome_unidade_orcamentaria",
      "ug",
      "nome_unidade_gestora",
      "dotacao_orcamentaria",
      "historico",
      "credor",
      "nome_credor",
      "cpf_cnpj_credor",
      "data_criacao",
      "data_emissao",
      "situacao",
      "rp",
    ];

    const filterContainers = table.querySelectorAll(".filter-row [data-col]");
    const allData = { rows: [] };
    const filters = Object.fromEntries(colKeys.map((k) => [k, new Set()]));
    const filterControls = {};

    const closeAllPanels = () => {
      Object.values(filterControls).forEach((ctrl) => {
        if (ctrl?.panel) ctrl.panel.classList.remove("open");
      });
    };

    const updateDisplay = (key) => {
      const set = filters[key] || new Set();
      const ctrl = filterControls[key];
      if (!ctrl) return;
      const map = ctrl.labelMap || {};
      if (ctrl.allCb) ctrl.allCb.checked = set.size === 0;
      (ctrl.optionCbs || []).forEach((cb) => {
        cb.checked = set.has(cb.dataset.val || "");
      });
      if (set.size === 0) {
        ctrl.label.textContent = "(Todos)";
      } else if (set.size <= 2) {
        ctrl.label.textContent = Array.from(set)
          .map((v) => map[v] || v)
          .join(", ");
      } else {
        ctrl.label.textContent = `${set.size} selecionados`;
      }
    };

    const buildFilter = (container, options, key) => {
      container.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "mf-wrapper";
      const display = document.createElement("button");
      display.type = "button";
      display.className = "mf-display";
      const label = document.createElement("span");
      label.textContent = "(Todos)";
      display.appendChild(label);
      const icon = document.createElement("i");
      icon.className = "bi bi-chevron-down";
      display.appendChild(icon);

      const panel = document.createElement("div");
      panel.className = "mf-panel";
      const search = document.createElement("input");
      search.type = "text";
      search.className = "mf-search";
      search.placeholder = "Buscar...";
      const list = document.createElement("div");
      list.className = "mf-options";

      const tempSelected = new Set(filters[key] || []);
      const allId = `${key}-all`;
      const allRow = document.createElement("label");
      allRow.className = "mf-option";
      const allCb = document.createElement("input");
      allCb.type = "checkbox";
      allCb.id = allId;
      allCb.dataset.val = "";
      allRow.appendChild(allCb);
      const allSpan = document.createElement("span");
      allSpan.textContent = "(Todos)";
      allRow.appendChild(allSpan);
      list.appendChild(allRow);

      const selectVisibleRow = document.createElement("label");
      selectVisibleRow.className = "mf-option mf-select-visible";
      const selectVisibleCb = document.createElement("input");
      selectVisibleCb.type = "checkbox";
      selectVisibleRow.appendChild(selectVisibleCb);
      const selectVisibleSpan = document.createElement("span");
      selectVisibleSpan.textContent = "Selecionar exibidos";
      selectVisibleRow.appendChild(selectVisibleSpan);
      list.appendChild(selectVisibleRow);

      const cbs = [];
      const labelMap = {};
      options.forEach((opt) => {
        const row = document.createElement("label");
        row.className = "mf-option";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        const norm = String(opt || "").toLowerCase();
        cb.dataset.val = norm;
        labelMap[norm] = opt;
        row.appendChild(cb);
        const txt = document.createElement("span");
        txt.textContent = opt;
        row.appendChild(txt);
        list.appendChild(row);
        cbs.push({ cb, txt, row, val: norm });
      });

      const syncUIFromTemp = () => {
        allCb.checked = tempSelected.size === 0;
        cbs.forEach(({ cb, val }) => {
          cb.checked = tempSelected.has(val);
        });
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        const allVisibleSelected = visible.length > 0 && visible.every(({ cb }) => cb.checked);
        selectVisibleCb.checked = allVisibleSelected;
      };

      const applyTempToFilters = () => {
        const set = filters[key];
        set.clear();
        tempSelected.forEach((v) => set.add(v));
        updateDisplay(key);
        renderFiltered();
      };

      const closePanel = () => panel.classList.remove("open");

      allCb.addEventListener("change", () => {
        if (allCb.checked) {
          tempSelected.clear();
          syncUIFromTemp();
        }
      });

      selectVisibleCb.addEventListener("change", () => {
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        if (selectVisibleCb.checked) {
          visible.forEach(({ val }) => tempSelected.add(val));
        } else {
          visible.forEach(({ val }) => tempSelected.delete(val));
        }
        allCb.checked = tempSelected.size === 0;
        syncUIFromTemp();
      });

      cbs.forEach(({ cb, val }) => {
        cb.addEventListener("change", () => {
          if (cb.checked) {
            tempSelected.add(val);
            allCb.checked = false;
          } else {
            tempSelected.delete(val);
          }
          syncUIFromTemp();
        });
      });

      search.addEventListener("input", () => {
        const term = search.value.toLowerCase();
        cbs.forEach(({ row, txt }) => {
          const match = txt.textContent.toLowerCase().includes(term);
          row.style.display = match ? "" : "none";
        });
        const allMatch = "(todos)".includes(term) || term === "";
        allRow.style.display = allMatch ? "" : "none";
        selectVisibleRow.style.display = "";
        syncUIFromTemp();
      });

      const actions = document.createElement("div");
      actions.className = "mf-actions";
      const cancelBtn = document.createElement("button");
      cancelBtn.type = "button";
      cancelBtn.className = "mf-btn ghost";
      cancelBtn.textContent = "Cancelar";
      const applyBtn = document.createElement("button");
      applyBtn.type = "button";
      applyBtn.className = "mf-btn primary";
      applyBtn.textContent = "Aplicar";

      cancelBtn.addEventListener("click", () => {
        tempSelected.clear();
        filters[key].forEach((v) => tempSelected.add(v));
        syncUIFromTemp();
        closePanel();
      });
      applyBtn.addEventListener("click", () => {
        applyTempToFilters();
        closePanel();
      });

      display.addEventListener("click", () => {
        const isOpen = panel.classList.contains("open");
        closeAllPanels();
        if (!isOpen) {
          panel.style.width = "";
          panel.style.height = "";
          tempSelected.clear();
          filters[key].forEach((v) => tempSelected.add(v));
          cbs.forEach(({ row }) => (row.style.display = ""));
          allRow.style.display = "";
          search.value = "";
          syncUIFromTemp();
          panel.classList.add("open");
        }
      });

      wrap.appendChild(display);
      panel.appendChild(search);
      panel.appendChild(list);
      actions.appendChild(cancelBtn);
      actions.appendChild(applyBtn);
      panel.appendChild(actions);
      wrap.appendChild(panel);
      container.appendChild(wrap);

      filterControls[key] = {
        panel,
        label,
        allCb,
        optionCbs: cbs.map((c) => c.cb),
        labelMap,
      };
      updateDisplay(key);
    };

    const setOptions = (rows = allData.rows) => {
      closeAllPanels();
      const uniques = colKeys.map(() => new Set());
      (rows || []).forEach((r) => {
        colKeys.forEach((k, idx) => {
          const v = r[k];
          if (v !== undefined && v !== null && v !== "") uniques[idx].add(String(v));
        });
      });
      filterContainers.forEach((container) => {
        const key = container.getAttribute("data-col");
        const idx = colKeys.indexOf(key);
        if (idx === -1) return;
        const opts = Array.from(uniques[idx]).sort((a, b) => a.localeCompare(b, "pt-BR"));
        buildFilter(container, opts, key);
      });
    };

    const updateTotals = (rows) => {
      const exSet = new Set();
      let totalVal = 0;
      rows.forEach((r) => {
        if (r.exercicio !== undefined && r.exercicio !== null && r.exercicio !== "") {
          exSet.add(String(r.exercicio));
        }
        const v = Number(r.valor_emp_liquido || 0);
        if (!Number.isNaN(v)) totalVal += v;
      });
      if (totExercicio) {
        totExercicio.textContent = exSet.size
          ? Array.from(exSet).sort((a, b) => a.localeCompare(b, "pt-BR")).join(" | ")
          : "-";
      }
      if (totValor) {
        totValor.textContent = numFmt.format(totalVal);
        totValor.classList.remove("pos", "neg");
        if (totalVal > 0) totValor.classList.add("pos");
        else if (totalVal < 0) totValor.classList.add("neg");
      }
    };

    const renderPagination = (totalPages) => {
      if (!pager) return;
      pager.innerHTML = "";

      const addBtn = (label, page, disabled, active = false) => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "page-btn";
        if (active) btn.classList.add("active");
        btn.disabled = disabled;
        btn.textContent = label;
        btn.addEventListener("click", () => {
          currentPage = page;
          render();
        });
        pager.appendChild(btn);
      };

      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);

      const maxBtns = 5;
      let startPage = Math.max(1, currentPage - Math.floor(maxBtns / 2));
      let endPage = Math.min(totalPages, startPage + maxBtns - 1);
      if (endPage - startPage + 1 < maxBtns) {
        startPage = Math.max(1, endPage - maxBtns + 1);
      }

      if (startPage > 1) {
        addBtn("1", 1, false, currentPage === 1);
        if (startPage > 2) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          pager.appendChild(ellipsis);
        }
      }

      for (let p = startPage; p <= endPage; p += 1) {
        addBtn(String(p), p, false, currentPage === p);
      }

      if (endPage < totalPages) {
        if (endPage < totalPages - 1) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          pager.appendChild(ellipsis);
        }
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }

      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };

    const render = () => {
      const rows = filteredRows;
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const pageRows = rows.slice(startIdx, startIdx + pageSize);

      tbody.innerHTML = "";
      pageRows.forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${r.exercicio ?? ""}</td>
          <td>${r.numero_est ?? ""}</td>
          <td>${r.numero_emp ?? ""}</td>
          <td>${r.empenho_atual ?? ""}</td>
          <td>${r.empenho_rp ?? ""}</td>
          <td>${r.numero_ped ?? ""}</td>
          <td class="num">${fmtNum(r.valor_emp)}</td>
          <td class="num">${fmtNum(r.valor_est_emp_sem_aqs)}</td>
          <td class="num">${fmtNum(r.valor_est_emp_com_aqs)}</td>
          <td class="num">${fmtNum(r.valor_emp_liquido)}</td>
          <td>${r.uo ?? ""}</td>
          <td>${r.nome_unidade_orcamentaria ?? ""}</td>
          <td>${r.ug ?? ""}</td>
          <td>${r.nome_unidade_gestora ?? ""}</td>
          <td>${r.dotacao_orcamentaria ?? ""}</td>
          <td>${r.historico ?? ""}</td>
          <td>${r.credor ?? ""}</td>
          <td>${r.nome_credor ?? ""}</td>
          <td>${r.cpf_cnpj_credor ?? ""}</td>
          <td>${r.data_criacao ?? ""}</td>
          <td>${r.data_emissao ?? ""}</td>
          <td>${r.situacao ?? ""}</td>
          <td>${r.rp ?? ""}</td>
        `;
        tbody.appendChild(tr);
      });

      renderPagination(totalPages);
      updateTotals(rows);
      toggleReportEmptyState({
        tableEl: table,
        emptyEl: emptyState,
        btnDownloadEl: btnDownload,
        pagerEl: pager,
        hasRows: rows.length > 0,
      });
    };

    const renderFiltered = (resetPage = true) => {
      const filtered = allData.rows.filter((r) =>
        colKeys.every((k) => {
          const set = filters[k];
          if (!set || set.size === 0) return true;
          const val = r[k];
          const cmp = val === null || val === undefined ? "" : String(val).toLowerCase();
          return set.has(cmp);
        })
      );
      setOptions(filtered);
      filteredRows = filtered;
      if (resetPage) currentPage = 1;
      render();
    };

    if (!multiFilterClickBound) {
      document.addEventListener("click", (ev) => {
        if (!ev.target.closest(".mf-wrapper")) {
          closeAllPanels();
        }
      });
      multiFilterClickBound = true;
    }

    const load = async () => {
      if (meta) meta.textContent = "Carregando...";
      try {
        const res = await fetch("/api/relatorios/est-emp");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar.");
        allData.rows = data.data || [];
        setOptions(allData.rows);
        filteredRows = allData.rows;
        render();
        if (meta) {
          const dt = formatAmazonLocalTime(data.data_arquivo);
          const user = data.user_email || "-";
          const uploaded = formatAmazonTime(data.uploaded_at);
          meta.innerHTML = `
            <div><strong>Última atualização</strong></div>
            <div>Enviado por: ${user}</div>
            <div>Upload em: ${uploaded}</div>
            <div>Data do download: ${dt}</div>
          `;
        }
      } catch (err) {
        if (meta) meta.textContent = err.message;
        console.error(err);
      }
    };

    load();

    if (btnReset) {
      btnReset.addEventListener("click", () => {
        Object.keys(filters).forEach((k) => filters[k].clear());
        setOptions(allData.rows);
        filteredRows = allData.rows;
        currentPage = 1;
        render();
      });
    }

    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        const val = parseInt(pageSizeSelect.value || "20", 10);
        pageSize = Number.isNaN(val) ? 20 : val;
        currentPage = 1;
        render();
      });
    }

    if (btnDownload) {
      btnDownload.addEventListener("click", () => {
        window.open("/api/relatorios/est-emp/download", "_blank");
      });
    }
  }

  function initRelatorioPed() {
    const table = document.getElementById("ped-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
    const emptyState = document.getElementById("ped-empty");
    const meta = document.getElementById("ped-relatorio-meta");
    const pager = document.getElementById("ped-pagination");
    const pageSizeSelect = document.getElementById("ped-page-size");
    const btnDownload = document.getElementById("ped-download");
    const btnReset = document.getElementById("ped-reset");
    const totExercicio = document.getElementById("ped-tot-exercicio");
    const totValorPed = document.getElementById("ped-tot-valor-ped");
    const chaveHeader = document.getElementById("ped-col-chave");
    if (!table || !tbody) return;
    if (table.dataset.bound === "1") return;
    table.dataset.bound = "1";

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;
    let filteredRows = [];

    const numFmt = new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const fmtNum = (v) => {
      const n = Number(v);
      if (Number.isNaN(n)) return v ?? "";
      return numFmt.format(n);
    };

    const colKeys = [
      "chave_display",
      "regiao",
      "subfuncao_ug",
      "adj",
      "macropolitica",
      "pilar",
      "eixo",
      "politica_decreto",
      "exercicio",
      "numero_ped",
      "numero_ped_estorno",
      "numero_emp",
      "numero_cad",
      "numero_noblist",
      "numero_os",
      "convenio",
      "numero_processo_orcamentario_pagamento",
      "valor_ped",
      "valor_estorno",
      "indicativo_licitacao_exercicios_anteriores",
      "data_licitacao",
      "liberado_fisco_estadual",
      "situacao",
      "uo",
      "nome_unidade_orcamentaria",
      "ug",
      "nome_unidade_gestora",
      "data_solicitacao",
      "data_criacao",
      "tipo_empenho",
      "dotacao_orcamentaria",
      "funcao",
      "subfuncao",
      "programa_governo",
      "paoe",
      "natureza_despesa",
      "cat_econ",
      "grupo",
      "modalidade",
      "elemento",
      "nome_elemento",
      "fonte",
      "iduso",
      "numero_emenda_ep",
      "autor_emenda_ep",
      "numero_cac",
      "licitacao",
      "usuario_responsavel",
      "historico",
      "credor",
      "nome_credor",
      "data_autorizacao",
      "data_hora_cadastro_autorizacao",
      "tipo_despesa",
      "numero_abj",
      "numero_processo_sequestro_judicial",
      "indicativo_entrega_imediata",
      "indicativo_contrato",
      "codigo_uo_extinta",
      "devolucao_gcv",
      "mes_competencia_folha_pagamento",
      "exercicio_competencia_folha",
      "obrigacao_patronal",
      "tipo_obrigacao_patronal",
      "numero_nla",
    ];

    const filterContainers = table.querySelectorAll(".filter-row [data-col]");
    const allData = { rows: [] };
    const filters = Object.fromEntries(colKeys.map((k) => [k, new Set()]));
    const filterControls = {};

    const closeAllPanels = () => {
      Object.values(filterControls).forEach((ctrl) => {
        if (ctrl?.panel) ctrl.panel.classList.remove("open");
      });
    };

    const updateDisplay = (key) => {
      const set = filters[key] || new Set();
      const ctrl = filterControls[key];
      if (!ctrl) return;
      const map = ctrl.labelMap || {};
      if (ctrl.allCb) ctrl.allCb.checked = set.size === 0;
      (ctrl.optionCbs || []).forEach((cb) => {
        cb.checked = set.has(cb.dataset.val || "");
      });
      if (set.size === 0) {
        ctrl.label.textContent = "(Todos)";
      } else if (set.size <= 2) {
        ctrl.label.textContent = Array.from(set)
          .map((v) => map[v] || v)
          .join(", ");
      } else {
        ctrl.label.textContent = `${set.size} selecionados`;
      }
    };

    const buildFilter = (container, options, key) => {
      container.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "mf-wrapper";
      const display = document.createElement("button");
      display.type = "button";
      display.className = "mf-display";
      const label = document.createElement("span");
      label.textContent = "(Todos)";
      display.appendChild(label);
      const icon = document.createElement("i");
      icon.className = "bi bi-chevron-down";
      display.appendChild(icon);

      const panel = document.createElement("div");
      panel.className = "mf-panel";
      const search = document.createElement("input");
      search.type = "text";
      search.className = "mf-search";
      search.placeholder = "Buscar...";
      const list = document.createElement("div");
      list.className = "mf-options";

      const tempSelected = new Set(filters[key] || []);
      const allId = `${key}-all`;
      const allRow = document.createElement("label");
      allRow.className = "mf-option";
      const allCb = document.createElement("input");
      allCb.type = "checkbox";
      allCb.id = allId;
      allCb.dataset.val = "";
      allRow.appendChild(allCb);
      const allSpan = document.createElement("span");
      allSpan.textContent = "(Todos)";
      allRow.appendChild(allSpan);
      list.appendChild(allRow);

      const selectVisibleRow = document.createElement("label");
      selectVisibleRow.className = "mf-option mf-select-visible";
      const selectVisibleCb = document.createElement("input");
      selectVisibleCb.type = "checkbox";
      selectVisibleRow.appendChild(selectVisibleCb);
      const selectVisibleSpan = document.createElement("span");
      selectVisibleSpan.textContent = "Selecionar exibidos";
      selectVisibleRow.appendChild(selectVisibleSpan);
      list.appendChild(selectVisibleRow);

      const cbs = [];
      const labelMap = {};
      options.forEach((opt) => {
        const row = document.createElement("label");
        row.className = "mf-option";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        const norm = String(opt || "").toLowerCase();
        cb.dataset.val = norm;
        labelMap[norm] = opt;
        row.appendChild(cb);
        const txt = document.createElement("span");
        txt.textContent = opt;
        row.appendChild(txt);
        list.appendChild(row);
        cbs.push({ cb, txt, row, val: norm });
      });

      const syncUIFromTemp = () => {
        allCb.checked = tempSelected.size === 0;
        cbs.forEach(({ cb, val }) => {
          cb.checked = tempSelected.has(val);
        });
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        const allVisibleSelected = visible.length > 0 && visible.every(({ cb }) => cb.checked);
        selectVisibleCb.checked = allVisibleSelected;
      };

      const applyTempToFilters = () => {
        const set = filters[key];
        set.clear();
        tempSelected.forEach((v) => set.add(v));
        updateDisplay(key);
        renderFiltered();
      };

      const closePanel = () => panel.classList.remove("open");

      allCb.addEventListener("change", () => {
        if (allCb.checked) {
          tempSelected.clear();
          syncUIFromTemp();
        }
      });

      selectVisibleCb.addEventListener("change", () => {
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        if (selectVisibleCb.checked) {
          visible.forEach(({ val }) => tempSelected.add(val));
        } else {
          visible.forEach(({ val }) => tempSelected.delete(val));
        }
        allCb.checked = tempSelected.size === 0;
        syncUIFromTemp();
      });

      cbs.forEach(({ cb, val }) => {
        cb.addEventListener("change", () => {
          if (cb.checked) {
            tempSelected.add(val);
            allCb.checked = false;
          } else {
            tempSelected.delete(val);
          }
          syncUIFromTemp();
        });
      });

      search.addEventListener("input", () => {
        const term = search.value.toLowerCase();
        cbs.forEach(({ row, txt }) => {
          const match = txt.textContent.toLowerCase().includes(term);
          row.style.display = match ? "" : "none";
        });
        const allMatch = "(todos)".includes(term) || term === "";
        allRow.style.display = allMatch ? "" : "none";
        selectVisibleRow.style.display = "";
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        const allVisibleSelected = visible.length > 0 && visible.every(({ cb }) => cb.checked);
        selectVisibleCb.checked = allVisibleSelected;
      });

      const actions = document.createElement("div");
      actions.className = "mf-actions";
      const cancelBtn = document.createElement("button");
      cancelBtn.type = "button";
      cancelBtn.className = "mf-btn ghost";
      cancelBtn.textContent = "Cancelar";
      const applyBtn = document.createElement("button");
      applyBtn.type = "button";
      applyBtn.className = "mf-btn primary";
      applyBtn.textContent = "Aplicar";

      cancelBtn.addEventListener("click", () => {
        tempSelected.clear();
        filters[key].forEach((v) => tempSelected.add(v));
        syncUIFromTemp();
        closePanel();
      });
      applyBtn.addEventListener("click", () => {
        applyTempToFilters();
        closePanel();
      });

      display.addEventListener("click", () => {
        const isOpen = panel.classList.contains("open");
        closeAllPanels();
        if (!isOpen) {
          panel.style.width = "";
          panel.style.height = "";
          tempSelected.clear();
          filters[key].forEach((v) => tempSelected.add(v));
          cbs.forEach(({ row }) => (row.style.display = ""));
          allRow.style.display = "";
          search.value = "";
          syncUIFromTemp();
          panel.classList.add("open");
        }
      });

      wrap.appendChild(display);
      panel.appendChild(search);
      panel.appendChild(list);
      actions.appendChild(cancelBtn);
      actions.appendChild(applyBtn);
      panel.appendChild(actions);
      wrap.appendChild(panel);
      container.appendChild(wrap);

      filterControls[key] = {
        panel,
        label,
        allCb,
        optionCbs: cbs.map((c) => c.cb),
        labelMap,
      };
      updateDisplay(key);
    };

    const setOptions = (rows = allData.rows) => {
      closeAllPanels();
      const uniques = colKeys.map(() => new Set());
      (rows || []).forEach((r) => {
        colKeys.forEach((k, idx) => {
          uniques[idx].add((r[k] ?? "").toString());
        });
      });
      filterContainers.forEach((container) => {
        const key = container.getAttribute("data-col");
        const idx = colKeys.indexOf(key);
        if (idx === -1) return;
        const opts = Array.from(uniques[idx]).sort((a, b) => a.localeCompare(b, "pt-BR"));
        buildFilter(container, opts, key);
      });
    };

    const renderFiltered = (resetPage = true) => {
      const filtered = allData.rows.filter((r) =>
        colKeys.every((k) => {
          const set = filters[k];
          if (!set || set.size === 0) return true;
          const val = r[k];
          const cmp = val === null || val === undefined ? "" : String(val).toLowerCase();
          return set.has(cmp);
        })
      );
      setOptions(filtered);
      filteredRows = filtered;
      if (resetPage) currentPage = 1;
      render();
    };

    const renderPagination = (totalPages) => {
      if (!pager) return;
      pager.innerHTML = "";
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
        b.type = "button";
        b.className = "page-btn";
        b.textContent = label;
        if (disabled) b.disabled = true;
        if (active) b.classList.add("active");
        b.addEventListener("click", () => {
          if (disabled || page === currentPage) return;
          currentPage = page;
          render();
        });
        pager.appendChild(b);
      };
      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);
      const maxButtons = 5;
      let start = Math.max(1, currentPage - Math.floor(maxButtons / 2));
      let end = Math.min(totalPages, start + maxButtons - 1);
      if (end - start + 1 < maxButtons) {
        start = Math.max(1, end - maxButtons + 1);
      }
      if (start > 1) {
        addBtn("1", 1, false, currentPage === 1);
        if (start > 2) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          pager.appendChild(ellipsis);
        }
      }
      for (let p = start; p <= end; p += 1) {
        addBtn(String(p), p, false, p === currentPage);
      }
      if (end < totalPages) {
        const ellipsis = document.createElement("span");
        ellipsis.textContent = "...";
        pager.appendChild(ellipsis);
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }
      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };

    const updateTotals = (rows) => {
      const exSet = new Set();
      let totalVal = 0;
      rows.forEach((r) => {
        if (r.exercicio !== undefined && r.exercicio !== null && r.exercicio !== "") {
          exSet.add(String(r.exercicio));
        }
        const v = Number(r.valor_ped || 0);
        if (!Number.isNaN(v)) totalVal += v;
      });
      if (totExercicio) {
        totExercicio.textContent = exSet.size
          ? Array.from(exSet).sort((a, b) => a.localeCompare(b, "pt-BR")).join(" | ")
          : "-";
      }
      if (totValorPed) {
        totValorPed.textContent = numFmt.format(totalVal);
        totValorPed.classList.remove("pos", "neg");
        if (totalVal > 0) totValorPed.classList.add("pos");
        else if (totalVal < 0) totValorPed.classList.add("neg");
      }
    };

    const render = () => {
      const rows = filteredRows;
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const pageRows = rows.slice(startIdx, startIdx + pageSize);

      tbody.innerHTML = "";
      pageRows.forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${r.chave_display ?? ""}</td>
          <td>${r.regiao ?? ""}</td>
          <td>${r.subfuncao_ug ?? ""}</td>
          <td>${r.adj ?? ""}</td>
          <td>${r.macropolitica ?? ""}</td>
          <td>${r.pilar ?? ""}</td>
          <td>${r.eixo ?? ""}</td>
          <td>${r.politica_decreto ?? ""}</td>
          <td>${r.exercicio ?? ""}</td>
          <td>${r.numero_ped ?? ""}</td>
          <td>${r.numero_ped_estorno ?? ""}</td>
          <td>${r.numero_emp ?? ""}</td>
          <td>${r.numero_cad ?? ""}</td>
          <td>${r.numero_noblist ?? ""}</td>
          <td>${r.numero_os ?? ""}</td>
          <td>${r.convenio ?? ""}</td>
          <td>${r.numero_processo_orcamentario_pagamento ?? ""}</td>
          <td class="num">${fmtNum(r.valor_ped)}</td>
          <td class="num">${fmtNum(r.valor_estorno)}</td>
          <td>${r.indicativo_licitacao_exercicios_anteriores ?? ""}</td>
          <td>${r.data_licitacao ?? ""}</td>
          <td>${r.liberado_fisco_estadual ?? ""}</td>
          <td>${r.situacao ?? ""}</td>
          <td>${r.uo ?? ""}</td>
          <td>${r.nome_unidade_orcamentaria ?? ""}</td>
          <td>${r.ug ?? ""}</td>
          <td>${r.nome_unidade_gestora ?? ""}</td>
          <td>${r.data_solicitacao ?? ""}</td>
          <td>${r.data_criacao ?? ""}</td>
          <td>${r.tipo_empenho ?? ""}</td>
          <td>${r.dotacao_orcamentaria ?? ""}</td>
          <td>${r.funcao ?? ""}</td>
          <td>${r.subfuncao ?? ""}</td>
          <td>${r.programa_governo ?? ""}</td>
          <td>${r.paoe ?? ""}</td>
          <td>${r.natureza_despesa ?? ""}</td>
          <td>${r.cat_econ ?? ""}</td>
          <td>${r.grupo ?? ""}</td>
          <td>${r.modalidade ?? ""}</td>
          <td>${r.elemento ?? ""}</td>
          <td>${r.nome_elemento ?? ""}</td>
          <td>${r.fonte ?? ""}</td>
          <td>${r.iduso ?? ""}</td>
          <td>${r.numero_emenda_ep ?? ""}</td>
          <td>${r.autor_emenda_ep ?? ""}</td>
          <td>${r.numero_cac ?? ""}</td>
          <td>${r.licitacao ?? ""}</td>
          <td>${r.usuario_responsavel ?? ""}</td>
          <td>${r.historico ?? ""}</td>
          <td>${r.credor ?? ""}</td>
          <td>${r.nome_credor ?? ""}</td>
          <td>${r.data_autorizacao ?? ""}</td>
          <td>${r.data_hora_cadastro_autorizacao ?? ""}</td>
          <td>${r.tipo_despesa ?? ""}</td>
          <td>${r.numero_abj ?? ""}</td>
          <td>${r.numero_processo_sequestro_judicial ?? ""}</td>
          <td>${r.indicativo_entrega_imediata ?? ""}</td>
          <td>${r.indicativo_contrato ?? ""}</td>
          <td>${r.codigo_uo_extinta ?? ""}</td>
          <td>${r.devolucao_gcv ?? ""}</td>
          <td>${r.mes_competencia_folha_pagamento ?? ""}</td>
          <td>${r.exercicio_competencia_folha ?? ""}</td>
          <td>${r.obrigacao_patronal ?? ""}</td>
          <td>${r.tipo_obrigacao_patronal ?? ""}</td>
          <td>${r.numero_nla ?? ""}</td>
        `;
        tbody.appendChild(tr);
      });
      renderPagination(totalPages);
      updateTotals(rows);
      toggleReportEmptyState({
        tableEl: table,
        emptyEl: emptyState,
        btnDownloadEl: btnDownload,
        pagerEl: pager,
        hasRows: rows.length > 0,
      });
    };

    const load = async () => {
      if (meta) meta.textContent = "Carregando...";
      try {
        const res = await fetch("/api/relatorios/ped");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar.");
        allData.rows = (data.data || []).map((r) => {
          const chaveDisplay = r.chave || r.chave_planejamento || "";
          return { ...r, chave_display: chaveDisplay };
        });
        filteredRows = allData.rows;
        setOptions(allData.rows);
        render();
        if (meta) {
          const dt = formatAmazonLocalTime(data.data_arquivo);
          const user = data.user_email || "-";
          const uploaded = formatAmazonTime(data.uploaded_at);
          meta.innerHTML = `
            <div><strong>Última atualização</strong></div>
            <div>Enviado por: ${user}</div>
            <div>Upload em: ${uploaded}</div>
            <div>Data do download: ${dt}</div>
          `;
        }
        if (chaveHeader) {
          chaveHeader.textContent = "Chave de Planejamento/Chave";
        }
      } catch (err) {
        if (meta) meta.textContent = err.message;
        console.error(err);
      }
    };

    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        pageSize = parseInt(pageSizeSelect.value || "20", 10) || 20;
        render();
      });
    }

    if (btnDownload) {
      btnDownload.addEventListener("click", () => {
        window.open("/api/relatorios/ped/download", "_blank");
      });
    }

    if (btnReset) {
      btnReset.addEventListener("click", () => {
        closeAllPanels();
        Object.keys(filters).forEach((k) => filters[k].clear());
        setOptions(allData.rows);
        filteredRows = allData.rows;
        currentPage = 1;
        render();
      });
    }

    if (!multiFilterClickBound) {
      document.addEventListener("click", (ev) => {
        if (!ev.target.closest(".mf-wrapper")) {
          closeAllPanels();
        }
      });
      multiFilterClickBound = true;
    }

    load();
  }


  function initRelatorioNob() {
    const table = document.getElementById("nob-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
    const emptyState = document.getElementById("nob-empty");
    const meta = document.getElementById("nob-relatorio-meta");
    const pager = document.getElementById("nob-pagination");
    const pageSizeSelect = document.getElementById("nob-page-size");
    const btnDownload = document.getElementById("nob-download");
    const btnReset = document.getElementById("nob-reset");
    const totExercicio = document.getElementById("nob-tot-exercicio");
    const totValorNob = document.getElementById("nob-tot-valor-nob");
    if (!table || !tbody) return;
    if (table.dataset.bound === "1") return;
    table.dataset.bound = "1";

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;
    let filteredRows = [];

    const numFmt = new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const fmtNum = (v) => {
      const n = Number(v);
      if (Number.isNaN(n)) return v ?? "";
      return numFmt.format(n);
    };

    const colKeys = [
      "exercicio",
      "numero_nob",
      "numero_nob_estorno",
      "numero_liq",
      "numero_emp",
      "empenho_atual",
      "empenho_rp",
      "numero_ped",
      "valor_nob",
      "devolucao_gcv",
      "valor_nob_gcv",
      "uo",
      "ug",
      "dotacao_orcamentaria",
      "funcao",
      "subfuncao",
      "programa_governo",
      "paoe",
      "natureza_despesa",
      "cat_econ",
      "grupo",
      "modalidade",
      "elemento",
      "nome_elemento_despesa",
      "fonte",
      "nome_fonte_recurso",
      "iduso",
      "historico_liq",
      "nome_credor_principal",
      "cpf_cnpj_credor_principal",
      "credor",
      "nome_credor",
      "cpf_cnpj_credor",
      "data_nob",
      "data_cadastro_nob",
      "data_hora_cadastro_liq",
    ];

    const filterContainers = table.querySelectorAll(".filter-row [data-col]");
    const allData = { rows: [] };
    const filters = Object.fromEntries(colKeys.map((k) => [k, new Set()]));
    const filterControls = {};

    const closeAllPanels = () => {
      Object.values(filterControls).forEach((ctrl) => {
        if (ctrl?.panel) ctrl.panel.classList.remove("open");
      });
    };

    const updateDisplay = (key) => {
      const set = filters[key] || new Set();
      const ctrl = filterControls[key];
      if (!ctrl) return;
      const map = ctrl.labelMap || {};
      if (ctrl.allCb) ctrl.allCb.checked = set.size === 0;
      (ctrl.optionCbs || []).forEach((cb) => {
        cb.checked = set.has(cb.dataset.val || "");
      });
      if (set.size === 0) {
        ctrl.label.textContent = "(Todos)";
      } else if (set.size <= 2) {
        ctrl.label.textContent = Array.from(set)
          .map((v) => map[v] || v)
          .join(", ");
      } else {
        ctrl.label.textContent = `${set.size} selecionados`;
      }
    };

    const buildFilter = (container, options, key) => {
      container.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "mf-wrapper";
      const display = document.createElement("button");
      display.type = "button";
      display.className = "mf-display";
      const label = document.createElement("span");
      label.textContent = "(Todos)";
      display.appendChild(label);
      const icon = document.createElement("i");
      icon.className = "bi bi-chevron-down";
      display.appendChild(icon);

      const panel = document.createElement("div");
      panel.className = "mf-panel";
      const search = document.createElement("input");
      search.type = "text";
      search.className = "mf-search";
      search.placeholder = "Buscar...";
      const list = document.createElement("div");
      list.className = "mf-options";

      const tempSelected = new Set(filters[key] || []);
      const allId = `${key}-all`;
      const allRow = document.createElement("label");
      allRow.className = "mf-option";
      const allCb = document.createElement("input");
      allCb.type = "checkbox";
      allCb.id = allId;
      allCb.dataset.val = "";
      allRow.appendChild(allCb);
      const allSpan = document.createElement("span");
      allSpan.textContent = "(Todos)";
      allRow.appendChild(allSpan);
      list.appendChild(allRow);

      const selectVisibleRow = document.createElement("label");
      selectVisibleRow.className = "mf-option mf-select-visible";
      const selectVisibleCb = document.createElement("input");
      selectVisibleCb.type = "checkbox";
      selectVisibleRow.appendChild(selectVisibleCb);
      const selectVisibleSpan = document.createElement("span");
      selectVisibleSpan.textContent = "Selecionar exibidos";
      selectVisibleRow.appendChild(selectVisibleSpan);
      list.appendChild(selectVisibleRow);

      const cbs = [];
      const labelMap = {};
      options.forEach((opt) => {
        const row = document.createElement("label");
        row.className = "mf-option";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        const norm = String(opt || "").toLowerCase();
        cb.dataset.val = norm;
        labelMap[norm] = opt;
        row.appendChild(cb);
        const txt = document.createElement("span");
        txt.textContent = opt;
        row.appendChild(txt);
        list.appendChild(row);
        cbs.push({ cb, txt, row, val: norm });
      });

      const syncUIFromTemp = () => {
        allCb.checked = tempSelected.size === 0;
        cbs.forEach(({ cb, val }) => {
          cb.checked = tempSelected.has(val);
        });
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        const allVisibleSelected = visible.length > 0 && visible.every(({ cb }) => cb.checked);
        selectVisibleCb.checked = allVisibleSelected;
      };

      const applyTempToFilters = () => {
        const set = filters[key];
        set.clear();
        tempSelected.forEach((v) => set.add(v));
        updateDisplay(key);
        renderFiltered();
      };

      const closePanel = () => panel.classList.remove("open");

      allCb.addEventListener("change", () => {
        if (allCb.checked) {
          tempSelected.clear();
          syncUIFromTemp();
        }
      });

      selectVisibleCb.addEventListener("change", () => {
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        if (selectVisibleCb.checked) {
          visible.forEach(({ val }) => tempSelected.add(val));
        } else {
          visible.forEach(({ val }) => tempSelected.delete(val));
        }
        allCb.checked = tempSelected.size === 0;
        syncUIFromTemp();
      });

      cbs.forEach(({ cb, val }) => {
        cb.addEventListener("change", () => {
          if (cb.checked) {
            tempSelected.add(val);
            allCb.checked = false;
          } else {
            tempSelected.delete(val);
          }
          syncUIFromTemp();
        });
      });

      search.addEventListener("input", () => {
        const term = search.value.toLowerCase();
        cbs.forEach(({ row, txt }) => {
          const match = txt.textContent.toLowerCase().includes(term);
          row.style.display = match ? "" : "none";
        });
        const allMatch = "(todos)".includes(term) || term === "";
        allRow.style.display = allMatch ? "" : "none";
        selectVisibleRow.style.display = "";
        syncUIFromTemp();
      });

      const actions = document.createElement("div");
      actions.className = "mf-actions";
      const cancelBtn = document.createElement("button");
      cancelBtn.type = "button";
      cancelBtn.className = "mf-btn ghost";
      cancelBtn.textContent = "Cancelar";
      const applyBtn = document.createElement("button");
      applyBtn.type = "button";
      applyBtn.className = "mf-btn primary";
      applyBtn.textContent = "Aplicar";

      cancelBtn.addEventListener("click", () => {
        tempSelected.clear();
        filters[key].forEach((v) => tempSelected.add(v));
        syncUIFromTemp();
        closePanel();
      });
      applyBtn.addEventListener("click", () => {
        applyTempToFilters();
        closePanel();
      });

      display.addEventListener("click", () => {
        const isOpen = panel.classList.contains("open");
        closeAllPanels();
        if (!isOpen) {
          panel.style.width = "";
          panel.style.height = "";
          tempSelected.clear();
          filters[key].forEach((v) => tempSelected.add(v));
          cbs.forEach(({ row }) => (row.style.display = ""));
          allRow.style.display = "";
          search.value = "";
          syncUIFromTemp();
          panel.classList.add("open");
        }
      });

      wrap.appendChild(display);
      panel.appendChild(search);
      panel.appendChild(list);
      actions.appendChild(cancelBtn);
      actions.appendChild(applyBtn);
      panel.appendChild(actions);
      wrap.appendChild(panel);
      container.appendChild(wrap);

      filterControls[key] = {
        panel,
        label,
        allCb,
        optionCbs: cbs.map((c) => c.cb),
        labelMap,
      };
      updateDisplay(key);
    };

    const setOptions = (rows = allData.rows) => {
      closeAllPanels();
      const uniques = colKeys.map(() => new Set());
      (rows || []).forEach((r) => {
        colKeys.forEach((k, idx) => {
          uniques[idx].add((r[k] ?? "").toString());
        });
      });
      filterContainers.forEach((container) => {
        const key = container.getAttribute("data-col");
        const idx = colKeys.indexOf(key);
        if (idx === -1) return;
        const opts = Array.from(uniques[idx]).sort((a, b) => a.localeCompare(b, "pt-BR"));
        buildFilter(container, opts, key);
      });
    };

    const renderFiltered = (resetPage = true) => {
      const filtered = allData.rows.filter((r) =>
        colKeys.every((k) => {
          const set = filters[k];
          if (!set || set.size === 0) return true;
          const val = r[k];
          const cmp = val === null || val === undefined ? "" : String(val).toLowerCase();
          return set.has(cmp);
        })
      );
      setOptions(filtered);
      filteredRows = filtered;
      if (resetPage) currentPage = 1;
      render();
    };

    const renderPagination = (totalPages) => {
      if (!pager) return;
      pager.innerHTML = "";
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
        b.type = "button";
        b.className = "page-btn";
        b.textContent = label;
        if (disabled) b.disabled = true;
        if (active) b.classList.add("active");
        b.addEventListener("click", () => {
          if (disabled || page === currentPage) return;
          currentPage = page;
          render();
        });
        pager.appendChild(b);
      };
      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);
      const maxButtons = 5;
      let start = Math.max(1, currentPage - Math.floor(maxButtons / 2));
      let end = Math.min(totalPages, start + maxButtons - 1);
      if (end - start + 1 < maxButtons) {
        start = Math.max(1, end - maxButtons + 1);
      }
      if (start > 1) {
        addBtn("1", 1, false, currentPage === 1);
        if (start > 2) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          pager.appendChild(ellipsis);
        }
      }
      for (let p = start; p <= end; p += 1) {
        addBtn(String(p), p, false, p === currentPage);
      }
      if (end < totalPages) {
        const ellipsis = document.createElement("span");
        ellipsis.textContent = "...";
        pager.appendChild(ellipsis);
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }
      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };

    const updateTotals = (rows) => {
      const exSet = new Set();
      let totalVal = 0;
      rows.forEach((r) => {
        if (r.exercicio !== undefined && r.exercicio !== null && r.exercicio !== "") {
          exSet.add(String(r.exercicio));
        }
        const v = Number(r.valor_nob_gcv || 0);
        if (!Number.isNaN(v)) totalVal += v;
      });
      if (totExercicio) {
        totExercicio.textContent = exSet.size
          ? Array.from(exSet).sort((a, b) => a.localeCompare(b, "pt-BR")).join(" | ")
          : "-";
      }
      if (totValorNob) {
        totValorNob.textContent = numFmt.format(totalVal);
        totValorNob.classList.remove("pos", "neg");
        if (totalVal > 0) totValorNob.classList.add("pos");
        else if (totalVal < 0) totValorNob.classList.add("neg");
      }
    };

    const render = () => {
      const rows = filteredRows;
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const pageRows = rows.slice(startIdx, startIdx + pageSize);

      tbody.innerHTML = "";
      pageRows.forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${r.exercicio ?? ""}</td>
          <td>${r.numero_nob ?? ""}</td>
          <td>${r.numero_nob_estorno ?? ""}</td>
          <td>${r.numero_liq ?? ""}</td>
          <td>${r.numero_emp ?? ""}</td>
          <td>${r.empenho_atual ?? ""}</td>
          <td>${r.empenho_rp ?? ""}</td>
          <td>${r.numero_ped ?? ""}</td>
          <td class="num">${fmtNum(r.valor_nob)}</td>
          <td class="num">${fmtNum(r.devolucao_gcv)}</td>
          <td class="num">${fmtNum(r.valor_nob_gcv)}</td>
          <td>${r.uo ?? ""}</td>
          <td>${r.ug ?? ""}</td>
          <td>${r.dotacao_orcamentaria ?? ""}</td>
          <td>${r.funcao ?? ""}</td>
          <td>${r.subfuncao ?? ""}</td>
          <td>${r.programa_governo ?? ""}</td>
          <td>${r.paoe ?? ""}</td>
          <td>${r.natureza_despesa ?? ""}</td>
          <td>${r.cat_econ ?? ""}</td>
          <td>${r.grupo ?? ""}</td>
          <td>${r.modalidade ?? ""}</td>
          <td>${r.elemento ?? ""}</td>
          <td>${r.nome_elemento_despesa ?? ""}</td>
          <td>${r.fonte ?? ""}</td>
          <td>${r.nome_fonte_recurso ?? ""}</td>
          <td>${r.iduso ?? ""}</td>
          <td>${r.historico_liq ?? ""}</td>
          <td>${r.nome_credor_principal ?? ""}</td>
          <td>${r.cpf_cnpj_credor_principal ?? ""}</td>
          <td>${r.credor ?? ""}</td>
          <td>${r.nome_credor ?? ""}</td>
          <td>${r.cpf_cnpj_credor ?? ""}</td>
          <td>${r.data_nob ?? ""}</td>
          <td>${r.data_cadastro_nob ?? ""}</td>
          <td>${r.data_hora_cadastro_liq ?? ""}</td>
        `;
        tbody.appendChild(tr);
      });

      renderPagination(totalPages);
      updateTotals(rows);
      toggleReportEmptyState({
        tableEl: table,
        emptyEl: emptyState,
        btnDownloadEl: btnDownload,
        pagerEl: pager,
        hasRows: rows.length > 0,
      });
    };

    const load = async () => {
      if (meta) meta.textContent = "Carregando...";
      try {
        const res = await fetch("/api/relatorios/nob");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar.");
        allData.rows = data.data || [];
        filteredRows = allData.rows;
        setOptions(allData.rows);
        render();
        if (meta) {
          const dt = formatAmazonLocalTime(data.data_arquivo);
          const user = data.user_email || "-";
          const uploaded = formatAmazonTime(data.uploaded_at);
          meta.innerHTML = `
            <div><strong>Última atualização</strong></div>
            <div>Enviado por: ${user}</div>
            <div>Upload em: ${uploaded}</div>
            <div>Data do download: ${dt}</div>
          `;
        }
      } catch (err) {
        if (meta) meta.textContent = err.message;
        console.error(err);
      }
    };

    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        pageSize = parseInt(pageSizeSelect.value || "20", 10) || 20;
        render();
      });
    }

    if (btnDownload) {
      btnDownload.addEventListener("click", () => {
        window.open("/api/relatorios/nob/download", "_blank");
      });
    }

    if (btnReset) {
      btnReset.addEventListener("click", () => {
        closeAllPanels();
        Object.keys(filters).forEach((k) => filters[k].clear());
        setOptions(allData.rows);
        filteredRows = allData.rows;
        currentPage = 1;
        render();
      });
    }

    if (!multiFilterClickBound) {
      document.addEventListener("click", (ev) => {
        if (!ev.target.closest(".mf-wrapper")) {
          closeAllPanels();
        }
      });
      multiFilterClickBound = true;
    }

    load();
  }

  function initMetaFisicaPlan21() {
    const form = document.getElementById("form-meta-fisica");
    const msg = document.getElementById("meta-fisica-msg");
    if (!form || !msg) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const selects = {
      exercicio: document.getElementById("meta-fisica-exercicio"),
      unidade_orcamentaria: document.getElementById("meta-fisica-uo"),
      programa: document.getElementById("meta-fisica-programa"),
      acao_paoe: document.getElementById("meta-fisica-acao"),
      adj_solicitante: document.getElementById("meta-fisica-adj-solicitante"),
      produto_acao: document.getElementById("meta-fisica-produto"),
      unid_medida_produto: document.getElementById("meta-fisica-unidade-medida"),
    };
    const tbody = document.getElementById("meta-fisica-tbody");
    const addRowBtn = document.getElementById("meta-fisica-add-row");
    const clearBtn = document.getElementById("meta-fisica-clear");
    const consultBtn = document.getElementById("meta-fisica-consultar");
    const justificativaInput = document.getElementById("meta-fisica-justificativa");
    const metaPage = document.getElementById("meta-fisica-page");
    const metaViewMode = String(metaPage?.dataset.viewMode || "formulario");
    const metaIsConsultaView = metaViewMode === "consultar";
    const metaPendingActionKey = "spo.metaFisica.pendingAction";
    const metaRowSnapshot = (row) => (row ? Object.fromEntries(Object.entries(row.dataset || {})) : {});
    const ensureMetaPendingRow = (pending) => {
      const pendingId = String(pending?.id || "").trim();
      let row = pendingId
        ? summaryBody?.querySelector(`.meta-fisica-summary-row[data-id="${CSS.escape(pendingId)}"]`)
        : null;
      if (!row && summaryBody && pending?.dataset) {
        row = document.createElement("tr");
        row.className = "meta-fisica-summary-row selected";
        row.style.display = "none";
        Object.entries(pending.dataset || {}).forEach(([key, value]) => {
          row.dataset[key] = value == null ? "" : String(value);
        });
        summaryBody.appendChild(row);
      }
      return row;
    };
    const summaryBox = document.getElementById("meta-fisica-summary");
    const summaryBody = document.querySelector("#meta-fisica-summary-table tbody");
    const summaryTable = document.getElementById("meta-fisica-summary-table");
    const summaryTableWrap = summaryBox ? summaryBox.querySelector(".table-responsive") : null;
    const pageSizeSelect = document.getElementById("meta-fisica-page-size");
    const paginationEl = document.getElementById("meta-fisica-pagination");
    const filterField = document.getElementById("meta-fisica-filtro-campo");
    const filterOp = document.getElementById("meta-fisica-filtro-operador");
    const filterValue = document.getElementById("meta-fisica-filtro-valor");
    const filterAdd = document.getElementById("meta-fisica-filtro-add");
    const filterList = document.getElementById("meta-fisica-filtro-list");
    const filterRemove = document.getElementById("meta-fisica-filtro-remove");
    const filterClear = document.getElementById("meta-fisica-filtro-clear");
    const filterCancel = document.getElementById("meta-fisica-filtro-cancel");
    const filterApply = document.getElementById("meta-fisica-filtro-apply");
    const filterMsg = document.getElementById("meta-fisica-filtro-msg");
    const approveBtn = document.getElementById("meta-fisica-approve");
    const editBtn = document.getElementById("meta-fisica-edit");
    const deleteBtn = document.getElementById("meta-fisica-delete");
    const printBtn = document.getElementById("meta-fisica-print");
    const editBadge = document.getElementById("meta-fisica-editing-badge");
    const saveBtn = document.getElementById("meta-fisica-save");
    const totalMetaPtaEl = document.getElementById("meta-fisica-total-meta-pta");
    const totalMetaAjustadaEl = document.getElementById("meta-fisica-total-meta-ajustada");
    const approvalFields = document.getElementById("meta-fisica-aprovacao-fields");
    const approvalQuestionLabel = document.getElementById("meta-fisica-aprovacao-pergunta");
    const approvalJustificativa = document.getElementById("meta-fisica-justificativa-aprovacao");
    const approvalRadios = form
      ? Array.from(form.querySelectorAll('input[name="meta-fisica-aprovada"]'))
      : [];
    const currentUserPerfilId = String(metaPage?.dataset?.userPerfilId || "").trim();
    const nivelAtual = parseInt(String(metaPage?.dataset?.userNivel || userNivel || "").trim(), 10);
    const canApprove = nivelAtual === 1 || nivelAtual === 2;
    const rowRequiredKeys = [
      "unidade_orcamentaria",
      "programa",
      "acao_paoe",
      "produto_acao",
      "unid_medida_produto",
    ];
    const MAX_META_FISICA_ROWS = 13;

    let tableRows = [];
    let hasConsulted = false;
    let lastQueryHadRows = false;
    let regionCatalog = [];
    let editingMetaId = "";
    let editingControle = "";
    let approvalMode = false;
    let approvingMetaId = "";
    let approvingControle = "";
    const defaultSaveLabel = saveBtn ? saveBtn.textContent : "Salvar";
    const criteria = [];
    let criteriaSelected = -1;
    let summaryPageSize = parseInt(pageSizeSelect?.value || "5", 10) || 5;
    let summaryCurrentPage = 1;
    let loadOptionsAbortController = null;
    let optionRowsCatalog = [];
    const fieldLabels = {
      controle_meta: "Controle de Meta",
      exercicio: "Exercício",
      status_aprovacao: "Status",
      acao_paoe: "Ação/PAOE",
      programa: "Programa",
      produto_acao: "Produto da Ação",
      regiao_produto: "Região PTA/LOA",
    };
    const cascadeOptionKeys = [
      "unidade_orcamentaria",
      "programa",
      "acao_paoe",
      "produto_acao",
      "unid_medida_produto",
    ];
    const opLabels = {
      eq: "Igual a",
      contains: "Contém",
      gt: "Maior que",
      lt: "Menor que",
      gte: "Maior igual a",
      lte: "Menor igual a",
    };
    let justificativaProtectedPrefix = "";
    const getMetaJustificativaPrefix = (controle) => {
      const txt = String(controle || "").trim();
      return txt ? `${txt}* ` : "";
    };
    const stripAnyMetaJustificativaPrefix = (value) =>
      String(value || "").replace(/^META\.[^*\r\n]+\*\s*/i, "").trimStart();
    const formatJustificativaWithControle = (controle, value) => {
      const prefix = getMetaJustificativaPrefix(controle);
      const text = stripAnyMetaJustificativaPrefix(value);
      return prefix ? `${prefix}${text}` : text;
    };
    const setJustificativaProtectedValue = (controle, value) => {
      if (!justificativaInput) return;
      justificativaProtectedPrefix = getMetaJustificativaPrefix(controle);
      justificativaInput.value = justificativaProtectedPrefix
        ? `${justificativaProtectedPrefix}${stripAnyMetaJustificativaPrefix(value)}`
        : String(value || "");
      if (justificativaProtectedPrefix && document.activeElement === justificativaInput) {
        const pos = Math.max(justificativaInput.selectionStart || 0, justificativaProtectedPrefix.length);
        justificativaInput.setSelectionRange(pos, pos);
      }
    };
    const clearJustificativaProtection = () => {
      justificativaProtectedPrefix = "";
    };
    const getJustificativaEditableText = () =>
      stripAnyMetaJustificativaPrefix(justificativaInput?.value || "").trim();
    const keepJustificativaPrefixProtected = () => {
      if (!justificativaInput || !justificativaProtectedPrefix) return;
      if (!String(justificativaInput.value || "").startsWith(justificativaProtectedPrefix)) {
        justificativaInput.value = `${justificativaProtectedPrefix}${stripAnyMetaJustificativaPrefix(justificativaInput.value)}`;
      }
      const minPos = justificativaProtectedPrefix.length;
      const start = justificativaInput.selectionStart || 0;
      const end = justificativaInput.selectionEnd || 0;
      if (start < minPos || end < minPos) {
        justificativaInput.setSelectionRange(Math.max(start, minPos), Math.max(end, minPos));
      }
    };

    let msgClearTimer = null;
    const setMsg = (text, isError = false) => {
      if (msgClearTimer) {
        clearTimeout(msgClearTimer);
        msgClearTimer = null;
      }
      msg.textContent = text || "";
      msg.style.whiteSpace = "pre-line";
      msg.classList.toggle("text-error", !!isError);
    };
    const clearMsgOnUserAction = () => {
      if (!msg?.textContent) return;
      msg.textContent = "";
      msg.classList.remove("text-error");
    };
    const setFilterMsg = (text, isError = false) => {
      if (!filterMsg) return;
      filterMsg.textContent = text || "";
      filterMsg.classList.toggle("text-error", !!isError);
    };
    const esc = (v) =>
      String(v ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    const normalizeRegionKey = (value) => {
      const raw = String(value || "").trim();
      if (!raw) return "";
      const digits = raw.replace(/\D/g, "");
      if (digits) {
        return String(parseInt(digits, 10));
      }
      return raw.toLowerCase();
    };
    const parseDec = (value) => {
      if (value === null || value === undefined) return null;
      const raw = String(value).trim();
      if (!raw) return null;
      let cleaned = raw.replace(/\s+/g, "");
      const hasDot = cleaned.includes(".");
      const hasComma = cleaned.includes(",");
      if (hasDot && hasComma) {
        if (cleaned.lastIndexOf(",") > cleaned.lastIndexOf(".")) {
          cleaned = cleaned.replace(/\./g, "").replace(",", ".");
        } else {
          cleaned = cleaned.replace(/,/g, "");
        }
      } else if (hasComma) {
        cleaned = cleaned.replace(",", ".");
      }
      const n = Number(cleaned);
      return Number.isFinite(n) ? n : null;
    };
    const fmtNum = (value) => {
      if (value === null || value === undefined || value === "") return "";
      const n = Number(value);
      if (!Number.isFinite(n)) return "";
      return n.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    };
    const formatPrintDate = (value) => {
      if (!value) return "";
      const d = new Date(value);
      if (Number.isNaN(d.getTime())) return String(value);
      return d.toLocaleString("pt-BR");
    };
    const inputNumValue = (value) => {
      const n = parseDec(value);
      if (n === null) return "";
      return String(n);
    };
    const formatDecimalPtBr = (value) => {
      const n = parseDec(value);
      if (n === null) return "";
      return n.toLocaleString("pt-BR", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
    };
    const unformatDecimalPtBr = (value) => {
      const n = parseDec(value);
      if (n === null) return "";
      return String(n).replace(".", ",");
    };
    const sanitizeDecimalInput = (value) => {
      let raw = String(value ?? "").replace(/[^\d,.]/g, "");
      const firstSepIdx = raw.search(/[,.]/);
      if (firstSepIdx >= 0) {
        const intPart = raw.slice(0, firstSepIdx).replace(/[,.]/g, "");
        const decPart = raw.slice(firstSepIdx + 1).replace(/[,.]/g, "");
        return `${intPart}${decPart ? `,${decPart}` : ","}`;
      }
      return raw;
    };
    const getUnidadeTipo = () => String(selects.unid_medida_produto?.value || "").trim().toLowerCase();
    const sanitizeByUnidade = (value) => {
      if (getUnidadeTipo() === "unidade") {
        return String(value ?? "").replace(/[^\d]/g, "");
      }
      return sanitizeDecimalInput(value);
    };
    const enforceMinPercentual = (value, isFinal = false) => {
      if (getUnidadeTipo() !== "percentual") return String(value ?? "");
      const raw = String(value ?? "").trim();
      if (!raw) return raw;
      const n = parseDec(raw);
      if (n === null) return raw;
      const isTypingPrefix =
        !isFinal && (raw === "0" || raw === "0," || raw === "0." || /[,.]$/.test(raw));
      if (isTypingPrefix) return raw;
      if (n < 0.1) return "0,1";
      return raw;
    };
    const formatByUnidade = (value) => {
      const n = parseDec(value);
      if (n === null) return "";
      if (getUnidadeTipo() === "unidade") {
        return String(Math.trunc(n));
      }
      return formatDecimalPtBr(value);
    };
    const validateValorByUnidade = (value, fieldLabel) => {
      const raw = String(value ?? "").trim();
      if (!raw) return "";
      const n = parseDec(raw);
      if (n === null) return `${fieldLabel} inválido.`;
      if (n === 0) return "";
      if (getUnidadeTipo() === "percentual" && n > 0 && n < 0.1) {
        return `${fieldLabel} mínimo é 0,1 para unidade de medida Percentual.`;
      }
      if (getUnidadeTipo() === "unidade" && n > 0 && !Number.isInteger(n)) {
        return `${fieldLabel} deve ser inteiro quando unidade de medida for Unidade.`;
      }
      return "";
    };
    const rowAdjusted = (row) => {
      const metaBaseRaw = parseDec(row.meta_produto);
      const creditoRaw = parseDec(sumFieldItems(row, "meta_credito"));
      const anuladaRaw = parseDec(sumFieldItems(row, "meta_anulada"));
      if (row.is_novo && metaBaseRaw === null && creditoRaw === null && anuladaRaw === null) {
        return "";
      }
      if (!row.is_novo && metaBaseRaw === null) return "";
      const metaBase = metaBaseRaw === null ? 0 : metaBaseRaw;
      const credito = creditoRaw || 0;
      const anulada = anuladaRaw || 0;
      return metaBase + credito - anulada;
    };
    const getTableTotals = () => {
      let totalMetaPta = 0;
      let totalCredito = 0;
      let totalAnulada = 0;
      let totalAjustada = 0;
      tableRows.forEach((row) => {
        const metaBase = parseDec(row?.meta_produto);
        if (metaBase !== null) totalMetaPta += metaBase;
        const credito = parseDec(sumFieldItems(row, "meta_credito"));
        if (credito !== null) totalCredito += credito;
        const anulada = parseDec(sumFieldItems(row, "meta_anulada"));
        if (anulada !== null) totalAnulada += anulada;
        const ajustada = parseDec(rowAdjusted(row));
        if (ajustada !== null) totalAjustada += ajustada;
      });
      return {
        meta_pta: totalMetaPta,
        acrescimo: totalCredito,
        reducao: totalAnulada,
        meta_ajustada: totalAjustada,
      };
    };
    const renderResumoTotais = () => {
      const totals = getTableTotals();
      if (totalMetaPtaEl) totalMetaPtaEl.textContent = fmtNum(totals.meta_pta) || "0,00";
      if (totalMetaAjustadaEl) totalMetaAjustadaEl.textContent = fmtNum(totals.meta_ajustada) || "0,00";
    };
    const refreshTotalsDisplay = () => {
      const totals = getTableTotals();
      const values = {
        meta_pta: totals.meta_pta,
        acrescimo: totals.acrescimo,
        reducao: totals.reducao,
        meta_ajustada: totals.meta_ajustada,
      };
      Object.entries(values).forEach(([key, rawValue]) => {
        const input = tbody?.querySelector(`.meta-fisica-total-row input[data-total-field="${key}"]`);
        if (!input) return;
        input.value = fmtNum(rawValue) || "0,00";
      });
      renderResumoTotais();
    };
    const openMetaFisicaSaveConfirmModal = ({ totalMetaPta, totalMetaAjustada, message }) =>
      new Promise((resolve) => {
        const existing = document.getElementById("meta-fisica-save-confirm-overlay");
        if (existing) existing.remove();
        const overlay = document.createElement("div");
        overlay.className = "modal-overlay";
        overlay.id = "meta-fisica-save-confirm-overlay";
        overlay.innerHTML = `
          <div class="modal-card meta-fisica-save-confirm-modal" role="dialog" aria-modal="true" aria-labelledby="meta-fisica-save-confirm-title">
            <div class="modal-header">
              <img src="/static/img/logo.jpg" alt="Logo" class="modal-logo" />
              <div class="modal-header-text">
                <div class="modal-header-title">Sistema de Planejamento e Orçamento</div>
                <div class="modal-header-subtitle">SPO-NGER-SEDUCMT</div>
              </div>
            </div>
            <div class="modal-body">
              <div class="modal-title" id="meta-fisica-save-confirm-title">Confirmação de Salvamento da Meta Física</div>
              <table class="table meta-fisica-save-confirm-table">
                <tbody>
                  <tr>
                    <th>Total META PTA/LOA</th>
                    <td>${esc(fmtNum(totalMetaPta) || "0,00")}</td>
                  </tr>
                  <tr>
                    <th>Total META AJUSTADA</th>
                    <td>${esc(fmtNum(totalMetaAjustada) || "0,00")}</td>
                  </tr>
                </tbody>
              </table>
              <p class="meta-fisica-save-confirm-msg">${esc(message || "")}</p>
            </div>
            <div class="modal-footer meta-fisica-save-confirm-footer">
              <button type="button" class="btn btn-danger sm" data-mf-confirm-action="cancelar">Cancelar</button>
              <button type="button" class="btn btn-primary sm" data-mf-confirm-action="salvar">Salvar</button>
            </div>
          </div>
        `;
        const finish = (ok) => {
          document.removeEventListener("keydown", onKeyDown, true);
          overlay.remove();
          resolve(!!ok);
        };
        const onKeyDown = (ev) => {
          if (ev.key === "Escape") {
            ev.preventDefault();
            finish(false);
          }
        };
        overlay.addEventListener("click", (ev) => {
          if (ev.target === overlay) finish(false);
        });
        overlay.querySelector('[data-mf-confirm-action="cancelar"]')?.addEventListener("click", () => finish(false));
        overlay.querySelector('[data-mf-confirm-action="salvar"]')?.addEventListener("click", () => finish(true));
        document.addEventListener("keydown", onKeyDown, true);
        document.body.appendChild(overlay);
      });
    const openMetaFisicaNegativeTotalsModal = ({ produtoAcao, linhas }) =>
      new Promise((resolve) => {
        const existing = document.getElementById("meta-fisica-negative-totals-overlay");
        if (existing) existing.remove();
        const overlay = document.createElement("div");
        overlay.className = "modal-overlay";
        overlay.id = "meta-fisica-negative-totals-overlay";
        const produtoTxt = String(produtoAcao || "").trim() || "(Produto da Ação não informado)";
        const linhasHtml = (Array.isArray(linhas) ? linhas : [])
          .map((txt) => `<li>${esc(String(txt || ""))}</li>`)
          .join("");
        overlay.innerHTML = `
          <div class="modal-card meta-fisica-save-confirm-modal" role="dialog" aria-modal="true" aria-labelledby="meta-fisica-negative-title">
            <div class="modal-header">
              <img src="/static/img/logo.jpg" alt="Logo" class="modal-logo" />
              <div class="modal-header-text">
                <div class="modal-header-title">Sistema de Planejamento e Orçamento</div>
                <div class="modal-header-subtitle">SPO-NGER-SEDUCMT</div>
              </div>
            </div>
            <div class="modal-body">
              <div class="modal-title" id="meta-fisica-negative-title">Meta Física do Produto ${esc(produtoTxt)} está negativa. Por favor realize os ajustes necessários.</div>
              <ul style="margin: 8px 0 0 18px; padding: 0;">${linhasHtml}</ul>
            </div>
            <div class="modal-footer meta-fisica-save-confirm-footer">
              <button type="button" class="btn btn-primary sm" data-mf-neg-action="ok">Ok</button>
            </div>
          </div>
        `;
        const finish = () => {
          document.removeEventListener("keydown", onKeyDown, true);
          overlay.remove();
          resolve(true);
        };
        const onKeyDown = (ev) => {
          if (ev.key === "Escape") {
            ev.preventDefault();
            finish();
          }
        };
        overlay.addEventListener("click", (ev) => {
          if (ev.target === overlay) finish();
        });
        overlay.querySelector('[data-mf-neg-action="ok"]')?.addEventListener("click", finish);
        document.addEventListener("keydown", onKeyDown, true);
        document.body.appendChild(overlay);
      });
    const openMetaFisicaPendingModal = ({ controle }) =>
      new Promise((resolve) => {
        const existing = document.getElementById("meta-fisica-pending-overlay");
        if (existing) existing.remove();
        const overlay = document.createElement("div");
        overlay.className = "modal-overlay";
        overlay.id = "meta-fisica-pending-overlay";
        const controleTxt = String(controle || "").trim() || "(registro aguardando)";
        overlay.innerHTML = `
          <div class="modal-card meta-fisica-save-confirm-modal" role="dialog" aria-modal="true" aria-labelledby="meta-fisica-pending-title">
            <div class="modal-header">
              <img src="/static/img/logo.jpg" alt="Logo" class="modal-logo" />
              <div class="modal-header-text">
                <div class="modal-header-title">Sistema de Planejamento e Orçamento</div>
                <div class="modal-header-subtitle">SPO-NGER-SEDUCMT</div>
              </div>
            </div>
            <div class="modal-body">
              <div class="modal-title" id="meta-fisica-pending-title">Alteração de Meta Física aguardando aprovação</div>
              <p class="meta-fisica-save-confirm-msg">
                Já existe um registro de alteração de meta aguardando aprovação para estes filtros: ${esc(controleTxt)}.
              </p>
              <p class="meta-fisica-save-confirm-msg">
                Só é possível cadastrar outro ajuste após a aprovação ou rejeição do registro atual.
              </p>
            </div>
            <div class="modal-footer meta-fisica-save-confirm-footer">
              <button type="button" class="btn btn-primary sm" data-mf-pending-action="ok">Ok</button>
            </div>
          </div>
        `;
        const finish = () => {
          document.removeEventListener("keydown", onKeyDown, true);
          overlay.remove();
          resolve(true);
        };
        const onKeyDown = (ev) => {
          if (ev.key === "Escape") {
            ev.preventDefault();
            finish();
          }
        };
        overlay.addEventListener("click", (ev) => {
          if (ev.target === overlay) finish();
        });
        overlay.querySelector('[data-mf-pending-action="ok"]')?.addEventListener("click", finish);
        document.addEventListener("keydown", onKeyDown, true);
        document.body.appendChild(overlay);
      });
    const getFieldItems = (row, field) => {
      const key = field === "meta_credito" ? "meta_credito_items" : "meta_anulada_items";
      const arr = Array.isArray(row[key]) ? row[key].map((v) => String(v ?? "")) : [];
      if (arr.length) return arr;
      const fallback = String(row[field] ?? "");
      if (!fallback && !row?.is_novo) return [];
      return [fallback];
    };
    const setFieldItems = (row, field, items) => {
      const key = field === "meta_credito" ? "meta_credito_items" : "meta_anulada_items";
      row[key] =
        Array.isArray(items) && items.length
          ? items.map((v) => String(v ?? ""))
          : (row?.is_novo ? [""] : []);
      row[field] = sumFieldItems(row, field);
    };
    const cloneRowForRollback = (row) => {
      if (!row || typeof row !== "object") return null;
      const copy = { ...row };
      [
        "meta_credito_items",
        "meta_anulada_items",
        "plan21_ids",
      ].forEach((k) => {
        copy[k] = Array.isArray(row[k]) ? row[k].map((v) => String(v ?? "")) : [];
      });
      return copy;
    };
    const restoreRowFromSnapshot = (row, snapshot) => {
      if (!row || !snapshot) return;
      Object.keys(row).forEach((k) => {
        delete row[k];
      });
      Object.assign(row, cloneRowForRollback(snapshot) || {});
    };
    const sumFieldItems = (row, field) => {
      const items = getFieldItems(row, field);
      let total = 0;
      items.forEach((item) => {
        const n = parseDec(item);
        if (n !== null) total += n;
      });
      if (!Number.isFinite(total) || total === 0) return "";
      return String(total).replace(".", ",");
    };
    const canAddMovement = (row, field) => {
      if (row?.[`allow_start_${field}`]) return true;
      const ownHas = getFieldItems(row, field).some((item) => {
        const n = parseDec(item);
        return n !== null && n > 0;
      });
      if (ownHas) return true;
      if (!row?.allow_cross_add) return false;
      const otherField = field === "meta_credito" ? "meta_anulada" : "meta_credito";
      return getFieldItems(row, otherField).some((item) => {
        const n = parseDec(item);
        return n !== null && n > 0;
      });
    };
    const getMovementCountForField = (row, field) => {
      const items = getFieldItems(row, field);
      const hasValue = items.some((v) => String(v ?? "").trim() !== "");
      if (!hasValue && items.length <= 1) {
        return row?.[`entry_enable_${field}`] ? 1 : 0;
      }
      return items.length;
    };
    const getLockedItemCount = (row, field) => {
      const key = field === "meta_credito" ? "lock_meta_credito_count" : "lock_meta_anulada_count";
      const raw = Number(row?.[key] ?? 0);
      if (!Number.isFinite(raw) || raw <= 0) return 0;
      const itemsLen = getFieldItems(row, field).length;
      return Math.min(Math.trunc(raw), itemsLen);
    };
    const getUnlockedItemsCount = (row, field) => {
      const itemsLen = getFieldItems(row, field).length;
      const locked = getLockedItemCount(row, field);
      return Math.max(0, itemsLen - locked);
    };
    const hasActiveEditableMovement = (row) => {
      const creditoAtivo =
        !!row?.entry_enable_meta_credito &&
        getUnlockedItemsCount(row, "meta_credito") > 0;
      const anuladaAtiva =
        !!row?.entry_enable_meta_anulada &&
        getUnlockedItemsCount(row, "meta_anulada") > 0;
      return creditoAtivo || anuladaAtiva;
    };
    const setLockedItemCount = (row, field, count) => {
      const keyCount = field === "meta_credito" ? "lock_meta_credito_count" : "lock_meta_anulada_count";
      const keyLock = field === "meta_credito" ? "lock_meta_credito" : "lock_meta_anulada";
      const itemsLen = getFieldItems(row, field).length;
      const safe = Math.max(0, Math.min(Number.isFinite(Number(count)) ? Math.trunc(Number(count)) : 0, itemsLen));
      row[keyCount] = safe;
      row[keyLock] = safe > 0;
    };
    const freezeExistingMovements = (row) => {
      if (!row?.__preFreezeSnapshot) {
        row.__preFreezeSnapshot = {
          lock_meta_credito_count: getLockedItemCount(row, "meta_credito"),
          lock_meta_anulada_count: getLockedItemCount(row, "meta_anulada"),
          entry_enable_meta_credito: !!row.entry_enable_meta_credito,
          entry_enable_meta_anulada: !!row.entry_enable_meta_anulada,
          allow_start_meta_credito: !!row.allow_start_meta_credito,
          allow_start_meta_anulada: !!row.allow_start_meta_anulada,
        };
      }
      // Materializa as linhas exatamente como estavam renderizadas antes do freeze
      // para não colapsar histórico/movimentação em uma única linha.
      const materializeAlignedItems = (field) => {
        const items = getFieldItems(row, field);
        const lockCount = Math.min(getLockedItemCount(row, field), items.length);
        const { histRows, movRows } = getLineBlockInfo(row);
        const histVals = items.slice(0, lockCount);
        const movVals = items.slice(lockCount);
        const aligned = [];
        for (let i = 0; i < histRows; i += 1) {
          aligned.push(i < histVals.length ? String(histVals[i] ?? "") : "");
        }
        for (let i = 0; i < movRows; i += 1) {
          aligned.push(i < movVals.length ? String(movVals[i] ?? "") : "");
        }
        return aligned.length ? aligned : [""];
      };
      const creditoAligned = materializeAlignedItems("meta_credito");
      const anuladaAligned = materializeAlignedItems("meta_anulada");
      const totalRowsBeforeFreeze = Math.max(creditoAligned.length, anuladaAligned.length, 1);
      while (creditoAligned.length < totalRowsBeforeFreeze) creditoAligned.push("");
      while (anuladaAligned.length < totalRowsBeforeFreeze) anuladaAligned.push("");
      setFieldItems(row, "meta_credito", creditoAligned);
      setFieldItems(row, "meta_anulada", anuladaAligned);
      setLockedItemCount(row, "meta_credito", totalRowsBeforeFreeze);
      setLockedItemCount(row, "meta_anulada", totalRowsBeforeFreeze);
      row.entry_enable_meta_credito = false;
      row.entry_enable_meta_anulada = false;
    };
    const restoreFrozenMovementsIfPossible = (row) => {
      const snap = row?.__preFreezeSnapshot;
      if (!snap) return;
      if (hasActiveEditableMovement(row)) return;
      setLockedItemCount(row, "meta_credito", snap.lock_meta_credito_count || 0);
      setLockedItemCount(row, "meta_anulada", snap.lock_meta_anulada_count || 0);
      row.entry_enable_meta_credito = !!snap.entry_enable_meta_credito;
      row.entry_enable_meta_anulada = !!snap.entry_enable_meta_anulada;
      row.allow_start_meta_credito = !!snap.allow_start_meta_credito;
      row.allow_start_meta_anulada = !!snap.allow_start_meta_anulada;
      delete row.__preFreezeSnapshot;
    };
    const getLineBlockInfo = (row) => {
      const creditoItems = getFieldItems(row, "meta_credito");
      const anuladaItems = getFieldItems(row, "meta_anulada");
      const creditoLock = Math.min(getLockedItemCount(row, "meta_credito"), creditoItems.length);
      const anuladaLock = Math.min(getLockedItemCount(row, "meta_anulada"), anuladaItems.length);
      const creditoMov = Math.max(creditoItems.length - creditoLock, 0);
      const anuladaMov = Math.max(anuladaItems.length - anuladaLock, 0);
      const histRows = Math.max(creditoLock, anuladaLock);
      const movRows = Math.max(creditoMov, anuladaMov);
      return { histRows, movRows, total: histRows + movRows };
    };
    const buildAlignedFieldLines = (row, field) => {
      const items = getFieldItems(row, field);
      const lockCount = Math.min(getLockedItemCount(row, field), items.length);
      const { histRows, movRows } = getLineBlockInfo(row);
      const histVals = items.slice(0, lockCount);
      const movVals = items.slice(lockCount);
      const lines = [];
      for (let i = 0; i < histRows; i += 1) {
        if (i < histVals.length) {
          lines.push({ val: histVals[i], itemIdx: i, isLocked: true });
        } else {
          lines.push({ val: "", itemIdx: -1, isLocked: true });
        }
      }
      for (let i = 0; i < movRows; i += 1) {
        if (i < movVals.length) {
          lines.push({ val: movVals[i], itemIdx: lockCount + i, isLocked: false });
        } else {
          lines.push({ val: "", itemIdx: -1, isLocked: false });
        }
      }
      return lines;
    };
    const getMovementRowsCount = (row) => getLineBlockInfo(row).total;
    const getMovementLabel = (row, movementIdx) => {
      const { histRows } = getLineBlockInfo(row);
      return movementIdx < histRows ? "Histórico" : "Movimentação";
    };
    const buildAdjustedLinesHtml = (row) => {
      const baseRaw = parseDec(row.meta_produto);
      if (row.is_novo) {
        return `<input type="text" class="meta-fisica-cell" data-field="meta_atual" value="${esc(fmtNum(rowAdjusted(row)))}" readonly />`;
      }
      if (baseRaw === null) {
        return `<input type="text" class="meta-fisica-cell" data-field="meta_atual" value="" readonly />`;
      }
      const base = baseRaw;
      const creditoLines = buildAlignedFieldLines(row, "meta_credito");
      const anuladaLines = buildAlignedFieldLines(row, "meta_anulada");
      const movementRows = Math.max(creditoLines.length, anuladaLines.length);
      let acumulado = base;
      let html = `<div class="meta-fisica-adjust-line">
        <input type="text" class="meta-fisica-cell" data-field="meta_atual" value="${esc(fmtNum(base))}" readonly />
      </div>`;
      for (let i = 0; i < movementRows; i += 1) {
        const c = parseDec(creditoLines[i]?.val) || 0;
        const a = parseDec(anuladaLines[i]?.val) || 0;
        acumulado += c - a;
        html += `<div class="meta-fisica-adjust-line">
          <input type="text" class="meta-fisica-cell" data-field="meta_atual" value="${esc(fmtNum(acumulado))}" readonly />
        </div>`;
      }
      return html;
    };
    const updateAdjustedDisplay = (tr, row) => {
      const wrap = tr?.querySelector(".meta-fisica-adjust-wrap");
      if (!wrap) return;
      wrap.innerHTML = buildAdjustedLinesHtml(row);
    };
    const buildMovementCell = (row, idx, field, readOnly = false, allowAdd = false) => {
      const items = getFieldItems(row, field);
      const fieldCss = field === "meta_credito" ? "meta-fisica-cell-credito" : "meta-fisica-cell-anulada";
      const rowAdjustedVal = parseDec(rowAdjusted(row));
      const blockReducaoByAdjusted = field === "meta_anulada" && rowAdjustedVal !== null && rowAdjustedVal <= 0;
      const canAdd = allowAdd && !blockReducaoByAdjusted && canAddMovement(row, field);
      const lockedCount = getLockedItemCount(row, field);
      const entryEnabled = !!row?.[`entry_enable_${field}`];
      const isBlockedReductionOnNewRow = row.is_novo && field === "meta_anulada";
      if (row.is_novo) {
        const disabledNovo = readOnly || isBlockedReductionOnNewRow ? "readonly" : "";
        const inputsNovo = items
          .map((val, itemIdx) => `<div class="meta-fisica-multi-line">
            <input type="text" inputmode="decimal" class="meta-fisica-cell ${fieldCss}" data-field="${field}" data-item-idx="${itemIdx}" value="${esc(formatByUnidade(val))}" ${disabledNovo} />
            ${itemIdx > 0 && !readOnly ? `<button type="button" class="meta-fisica-item-remove" data-remove-field="${field}" data-remove-idx="${itemIdx}" title="Remover lançamento">-</button>` : ""}
          </div>`)
          .join("");
        return `<div class="meta-fisica-multi-wrap">${inputsNovo}
          ${allowAdd && !readOnly && !isBlockedReductionOnNewRow ? `<button type="button" class="meta-fisica-item-add" data-add-field="${field}" ${canAdd ? "" : "disabled"} title="Novo lançamento">+</button>` : ""}
        </div>`;
      }

      const lines = buildAlignedFieldLines(row, field);
      let inputs = `<div class="meta-fisica-multi-line">
        <input type="text" class="meta-fisica-cell ${fieldCss}" value="" readonly />
      </div>`;
      for (let lineIdx = 0; lineIdx < lines.length; lineIdx += 1) {
        const line = lines[lineIdx];
        const val = line?.val ?? "";
        const mappedIdx = Number.isInteger(line?.itemIdx) ? line.itemIdx : -1;
        const isLocked = !!line?.isLocked;
        const isUnlockedItem = mappedIdx >= 0 && !isLocked;
        const disabled = readOnly || isLocked || !isUnlockedItem || (isUnlockedItem && !entryEnabled) ? "readonly" : "";
        const labelText = getMovementLabel(row, lineIdx);
        inputs += `<div class="meta-fisica-multi-line">
          <span class="meta-fisica-mov-label">${labelText}</span>
          <input type="text" inputmode="decimal" class="meta-fisica-cell ${fieldCss}" data-field="${field}" data-item-idx="${mappedIdx}" value="${esc(formatByUnidade(val))}" ${disabled} />
          ${
            !readOnly &&
            isUnlockedItem &&
            (!!row?.has_new_movement || !!entryEnabled)
              ? `<button type="button" class="meta-fisica-item-remove" data-remove-field="${field}" data-remove-idx="${mappedIdx}" title="Remover lançamento">-</button>`
              : ""
          }
        </div>`;
      }
      return `<div class="meta-fisica-multi-wrap">${inputs}
        ${allowAdd ? `<button type="button" class="meta-fisica-item-add" data-add-field="${field}" ${canAdd ? "" : "disabled"} title="Novo lançamento">+</button>` : ""}
      </div>`;
    };
    const validateDuplicateRegions = () => {
      const seen = new Set();
      for (const row of tableRows) {
        const key = normalizeRegionKey(row.regiao_produto);
        if (!key) continue;
        if (seen.has(key)) return row.regiao_produto;
        seen.add(key);
      }
      return "";
    };
    const getCatalogCode = (item) =>
      normalizeRegionKey(item?.codigo || item?.value || item?.label || "");
    const getUsedRegionCodes = (exceptIdx = null) => {
      const used = new Set();
      tableRows.forEach((row, idx) => {
        if (exceptIdx !== null && idx === exceptIdx) return;
        const key = normalizeRegionKey(row.regiao_produto);
        if (key) used.add(key);
      });
      return used;
    };
    const getAvailableRegionOptions = (rowIdx) => {
      const used = getUsedRegionCodes(rowIdx);
      return regionCatalog.filter((item) => {
        const code = getCatalogCode(item);
        if (!code) return false;
        return !used.has(code);
      });
    };
    const applySelectOptions = (select, values, keepValue = true) => {
      if (!select) return;
      const current = keepValue ? select.value : "";
      const normalizedValues = (values || []).map((val) => String(val));
      select.innerHTML = '<option value="">Selecione...</option>';
      normalizedValues.forEach((val) => {
        const opt = document.createElement("option");
        opt.value = String(val);
        opt.textContent = String(val);
        select.appendChild(opt);
      });
      if (current && normalizedValues.includes(current)) {
        select.value = current;
      }
    };
    const setSelectValueFallback = (select, value) => {
      if (!select) return;
      const text = String(value || "").trim();
      if (text && !Array.from(select.options || []).some((opt) => opt.value === text)) {
        const opt = document.createElement("option");
        opt.value = text;
        opt.textContent = text;
        opt.dataset.preserved = "1";
        select.appendChild(opt);
      }
      select.value = text;
    };
    const metaSelectDatasetMap = {
      exercicio: "exercicio",
      unidade_orcamentaria: "uo",
      programa: "programa",
      acao_paoe: "acaoPaoe",
      adj_solicitante: "adjSolicitante",
      produto_acao: "produtoAcao",
      unid_medida_produto: "unidMedidaProduto",
    };
    const applyMetaFiltersFromSummaryRow = (row) => {
      Object.entries(selects).forEach(([key, el]) => {
        if (!el) return;
        const dataKey = metaSelectDatasetMap[key];
        const value = String((dataKey && row?.dataset?.[dataKey]) || "").trim();
        setSelectValueFallback(el, value);
      });
    };
    const refreshCascadeOptionsFromCatalog = () => {
      if (!Array.isArray(optionRowsCatalog) || !optionRowsCatalog.length) return false;
      const selected = {};
      cascadeOptionKeys.forEach((k) => {
        selected[k] = String(selects[k]?.value || "").trim();
      });
      cascadeOptionKeys.forEach((targetKey) => {
        const values = [];
        optionRowsCatalog.forEach((row) => {
          if (!row) return;
          for (const otherKey of cascadeOptionKeys) {
            if (otherKey === targetKey) continue;
            const sel = selected[otherKey];
            if (sel && String(row[otherKey] || "").trim() !== sel) {
              return;
            }
          }
          const val = String(row[targetKey] || "").trim();
          if (val) values.push(val);
        });
        applySelectOptions(selects[targetKey], Array.from(new Set(values)).sort((a, b) => a.localeCompare(b, "pt-BR")), true);
      });
      return true;
    };
    const hasAllRowFiltersSelected = () =>
      rowRequiredKeys.every((key) => String(selects[key]?.value || "").trim() !== "");
    const getRowsEmptyMessage = () => {
      if (!hasAllRowFiltersSelected()) {
        return "Preencha todos os filtros obrigatórios para carregar as regiões.";
      }
      if (!hasConsulted) {
        return "Clique em Consultar para carregar as regiões.";
      }
      if (!lastQueryHadRows) {
        return "Nenhuma região encontrada para os filtros informados.";
      }
      return "Sem registros.";
    };

    const parsePositiveList = (list) => (Array.isArray(list) ? list : [])
      .map((v) => parseDec(v))
      .filter((n) => n !== null && n > 0);
    const parseAlignedList = (list) => (Array.isArray(list) ? list : [])
      .map((v) => {
        const n = parseDec(v);
        return n !== null && n > 0 ? n : null;
      });
    const sumPositiveAligned = (list) => (Array.isArray(list) ? list : [])
      .reduce((acc, n) => acc + ((n !== null && n > 0) ? n : 0), 0);

    const normalizeMetaLinhaSeries = (linha) => {
      const histCreditoItems = parseAlignedList(linha?.meta_credito_historico_items);
      const histAnuladaItems = parseAlignedList(linha?.meta_anulada_historico_items);
      const histCreditoScalar = parseDec(linha?.meta_credito_historico);
      const histAnuladaScalar = parseDec(linha?.meta_anulada_historico);

      const rawMovCreditoList = Array.isArray(linha?.meta_credito_mov_items)
        ? linha.meta_credito_mov_items
        : [];
      const rawMovAnuladaList = Array.isArray(linha?.meta_anulada_mov_items)
        ? linha.meta_anulada_mov_items
        : [];
      // Tratar lista vazia como "sem lista de movimento" para permitir
      // reconstrução via full-hist em registros legados.
      const hasCreditoMovList = rawMovCreditoList.length > 0;
      const hasAnuladaMovList = rawMovAnuladaList.length > 0;
      const fullCredito = parsePositiveList(linha?.meta_credito_items);
      const fullAnulada = parsePositiveList(linha?.meta_anulada_items);
      let movCredito = parseAlignedList(rawMovCreditoList);
      let movAnulada = parseAlignedList(rawMovAnuladaList);

      // Fallback legado: só reconstrói movimento via "full - histórico"
      // quando a lista explícita de movimento não existe no payload.
      if (!hasCreditoMovList && fullCredito.length) {
        const histCredPosCount = histCreditoItems.filter((n) => n !== null && n > 0).length;
        movCredito = fullCredito.slice(Math.min(histCredPosCount, fullCredito.length));
        if (!histCreditoItems.length && histCreditoScalar !== null && histCreditoScalar > 0) {
          let consumed = false;
          movCredito = movCredito.filter((n) => {
            if (!consumed && Math.abs(n - histCreditoScalar) < 0.000001) {
              consumed = true;
              return false;
            }
            return true;
          });
        }
      }
      if (!hasAnuladaMovList && fullAnulada.length) {
        const histAnuPosCount = histAnuladaItems.filter((n) => n !== null && n > 0).length;
        movAnulada = fullAnulada.slice(Math.min(histAnuPosCount, fullAnulada.length));
        if (!histAnuladaItems.length && histAnuladaScalar !== null && histAnuladaScalar > 0) {
          let consumed = false;
          movAnulada = movAnulada.filter((n) => {
            if (!consumed && Math.abs(n - histAnuladaScalar) < 0.000001) {
              consumed = true;
              return false;
            }
            return true;
          });
        }
      }

      if (!histCreditoItems.some((n) => n !== null && n > 0) && histCreditoScalar !== null && histCreditoScalar > 0) {
        histCreditoItems.push(histCreditoScalar);
      }
      if (!histAnuladaItems.some((n) => n !== null && n > 0) && histAnuladaScalar !== null && histAnuladaScalar > 0) {
        histAnuladaItems.push(histAnuladaScalar);
      }
      if (
        !movCredito.some((n) => n !== null && n > 0) &&
        !movAnulada.some((n) => n !== null && n > 0) &&
        !histCreditoItems.some((n) => n !== null && n > 0) &&
        !histAnuladaItems.some((n) => n !== null && n > 0)
      ) {
        const metaBase = parseDec(linha?.meta_produto);
        const metaAtual = parseDec(linha?.meta_atual);
        if (metaBase !== null && metaAtual !== null) {
          const delta = metaAtual - metaBase;
          if (delta > 0.000001) movCredito = [delta];
          if (delta < -0.000001) movAnulada = [Math.abs(delta)];
        }
      }

      const creditoSeries = [...histCreditoItems, ...movCredito];
      const anuladaSeries = [...histAnuladaItems, ...movAnulada];
      const hasAny =
        creditoSeries.some((n) => n !== null && n > 0) ||
        anuladaSeries.some((n) => n !== null && n > 0);
      return {
        histCreditoItems,
        histAnuladaItems,
        movCredito,
        movAnulada,
        creditoSeries,
        anuladaSeries,
        hasAny,
      };
    };

    const extractLinhaMovHist = (linha) => {
      const normalized = normalizeMetaLinhaSeries(linha);
      return {
        histCreditoItems: normalized.histCreditoItems,
        histAnuladaItems: normalized.histAnuladaItems,
        movCredito: normalized.movCredito,
        movAnulada: normalized.movAnulada,
      };
    };

    const buildMetaFisicaPrintTable = (meta) => {
      const linhas = Array.isArray(meta?.linhas) ? meta.linhas : [];
      let totalMetaPta = 0;
      let totalAcrescimo = 0;
      let totalReducao = 0;
      let totalMetaAjustada = 0;
      const toAlignedNums = (list) => (Array.isArray(list) ? list : []).map((v) => {
        const n = parseDec(v);
        return n !== null && n > 0 ? n : null;
      });
      const extractLinhaForPrint = (linha) => {
        let histCreditoItems = toAlignedNums(linha?.meta_credito_historico_items);
        let histAnuladaItems = toAlignedNums(linha?.meta_anulada_historico_items);
        let movCredito = toAlignedNums(linha?.meta_credito_mov_items);
        let movAnulada = toAlignedNums(linha?.meta_anulada_mov_items);
        return { histCreditoItems, histAnuladaItems, movCredito, movAnulada };
      };
      const buildAdjustmentRows = (histCreditoItems, histAnuladaItems, movCredito, movAnulada) => {
        const rawRows = [];
        const histRowsCount = Math.max(histCreditoItems.length, histAnuladaItems.length);
        for (let i = 0; i < histRowsCount; i += 1) {
          const c = histCreditoItems[i] ?? null;
          const a = histAnuladaItems[i] ?? null;
          if ((c > 0) || (a > 0)) rawRows.push({ c, a, label: "Histórico" });
        }
        const movRowsCount = Math.max(movCredito.length, movAnulada.length);
        for (let i = 0; i < movRowsCount; i += 1) {
          const c = movCredito[i] ?? null;
          const a = movAnulada[i] ?? null;
          if ((c > 0) || (a > 0)) rawRows.push({ c, a, label: "Movimentação" });
        }
        return rawRows;
      };
      const linhasHtml = linhas
        .map((l) => {
          try {
          const reg = l?.regiao_produto || "";
          const metaBase = parseDec(l?.meta_produto) ?? 0;
          let acumulado = metaBase;
          let { histCreditoItems, histAnuladaItems, movCredito, movAnulada } = extractLinhaForPrint(l);

          const creditoSeries = [...histCreditoItems, ...movCredito];
          const anuladaSeries = [...histAnuladaItems, ...movAnulada];
          totalMetaPta += metaBase;
          totalAcrescimo += sumPositiveAligned(creditoSeries);
          totalReducao += sumPositiveAligned(anuladaSeries);

          const rows = [];
          rows.push(`<tr>
            <td>${esc(reg)}</td>
            <td>${esc(fmtNum(metaBase))}</td>
            <td></td>
            <td></td>
            <td>${esc(fmtNum(acumulado))}</td>
          </tr>`);

          const classifiedRows = buildAdjustmentRows(
            histCreditoItems,
            histAnuladaItems,
            movCredito,
            movAnulada
          );
          for (const adjRow of classifiedRows) {
            const c = adjRow.c ?? null;
            const a = adjRow.a ?? null;
            acumulado += (c || 0) - (a || 0);
            const rowClass = adjRow.label === "Movimentação" ? ' class="print-movimentacao-row"' : "";
            rows.push(`<tr${rowClass}>
              <td colspan="2">${esc(adjRow.label)}</td>
              <td>${esc(c > 0 ? fmtNum(c) : "")}</td>
              <td>${esc(a > 0 ? fmtNum(a) : "")}</td>
              <td>${esc(fmtNum(acumulado))}</td>
            </tr>`);
          }

          totalMetaAjustada += acumulado;

          return rows.join("");
          } catch (lineErr) {
            console.error("Erro ao montar linha da impressão meta física:", lineErr, l);
            const reg = l?.regiao_produto || "";
            const metaBase = parseDec(l?.meta_produto) ?? 0;
            return `<tr>
              <td>${esc(reg)}</td>
              <td>${esc(fmtNum(metaBase))}</td>
              <td></td>
              <td></td>
              <td>${esc(fmtNum(metaBase))}</td>
            </tr>`;
          }
        })
        .join("");

      const motivoRejeicao = String(meta?.motivo_rejeicao || "").trim();
      const showMotivoRejeicao = String(meta?.status_aprovacao || "").trim().toLowerCase() === "rejeitado" && motivoRejeicao;
      return `
        <section class="print-section">
          <table class="print-table print-data-table">
            <tbody>
              <tr><th>Exercício</th><td>${esc(meta?.exercicio || "")}</td></tr>
              <tr><th>UO</th><td>${esc(meta?.unidade_orcamentaria || "")}</td></tr>
              <tr><th>Programa de Governo</th><td>${esc(meta?.programa || "")}</td></tr>
              <tr><th>Ação/PAOE</th><td>${esc(meta?.acao_paoe || "")}</td></tr>
              <tr><th>Adjunta Solicitante</th><td>${esc(meta?.adj_solicitante || "")}</td></tr>
              <tr><th>Produto da Ação</th><td>${esc(meta?.produto_acao || "")}</td></tr>
              <tr><th>Unidade de Medida</th><td>${esc(meta?.unid_medida_produto || "")}</td></tr>
            </tbody>
          </table>
        </section>
        <section class="print-section print-metas-section">
          <div class="print-section-title">Tabela de Metas</div>
          <table class="print-table print-metas-table">
            <thead>
              <tr>
                <th>REGIÃO PTA/LOA</th>
                <th>META PTA/LOA</th>
                <th>ACRÉSCIMO</th>
                <th>REDUÇÃO</th>
                <th>META AJUSTADA</th>
              </tr>
            </thead>
            <tbody>
              ${linhasHtml || '<tr><td colspan="5">Sem linhas</td></tr>'}
              <tr class="print-total-row">
                <td>TOTAIS</td>
                <td>${esc(fmtNum(totalMetaPta))}</td>
                <td>${esc(fmtNum(totalAcrescimo))}</td>
                <td>${esc(fmtNum(totalReducao))}</td>
                <td>${esc(fmtNum(totalMetaAjustada))}</td>
              </tr>
            </tbody>
          </table>
        </section>
        <section class="print-section print-justificativa-section">
          <table class="print-table print-data-table">
            <tbody>
              <tr><th>Justificativa</th><td>${esc(formatJustificativaWithControle(meta?.controle, meta?.justificativa || ""))}</td></tr>
              ${showMotivoRejeicao ? `<tr><th>Motivo da Rejeição</th><td>${esc(motivoRejeicao)}</td></tr>` : ""}
            </tbody>
          </table>
        </section>
      `;
    };

    const openMetaFisicaPrintPopup = (meta, targetWin = null) => {
      const controle = meta?.controle || "";
      const criadoEm = formatPrintDate(meta?.criado_em || "");
      const usuarioNome = meta?.usuario_nome || "";
      const usuarioPerfil = meta?.usuario_perfil || String(metaPage?.dataset?.userPerfil || "").trim();
      const status = String(meta?.status_aprovacao || "").trim().toLowerCase();
      const aprovadoPorNome = String(meta?.aprovado_por_nome || "").trim();
      const aprovadoPorPerfil = String(meta?.aprovado_por_perfil || "").trim();
      const dataAprovacao = formatPrintDate(meta?.data_aprovacao || "");
      const footerLine2 = [usuarioNome, usuarioPerfil, criadoEm ? `cadastrado em ${criadoEm}` : "", controle]
        .map((p) => String(p || "").trim())
        .filter(Boolean)
        .join(" - ");
      const statusEventoLabel =
        status === "rejeitado"
          ? "rejeitado em"
          : status === "aprovado"
            ? "aprovado em"
            : "aprovado em";
      const footerLine3 = [
        [aprovadoPorNome, aprovadoPorPerfil].map((p) => String(p || "").trim()).filter(Boolean).join(" - "),
        dataAprovacao ? `${statusEventoLabel} ${dataAprovacao}` : "",
      ]
        .map((p) => String(p || "").trim())
        .filter(Boolean)
        .join(" - ");
      let watermarkText = "";
      if (status === "aguardando") watermarkText = "AGUARDANDO";
      if (status === "rejeitado") watermarkText = "Rejeitado";
      const html = `<!doctype html>
  <html>
  <head>
    <meta charset="utf-8" />
    <title>Alterar Meta Física</title>
    <style>
      @page { margin: 8mm 5mm 10mm; }
      body { font-family: Arial, sans-serif; color: #000; margin: 0 12px; padding: 0; }
      .print-page-header { position: fixed; top: 0; left: 12px; right: 12px; background: #fff; z-index: 2; }
      .print-header { display: flex; align-items: center; justify-content: space-between; padding: 4px 0 3px; border-bottom: 1px dashed #000; }
      .print-brand { display: flex; align-items: center; gap: 8px; }
      .print-brand img { height: 34px; }
      .print-brand-title { font-weight: 700; font-size: 13px; }
      .print-brand-subtitle { font-size: 10px; color: #333; }
      .print-title-row { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin: 4px 0 8px; }
      .print-title { text-align: center; font-weight: 700; flex: 1; text-transform: uppercase; }
      .print-title-key { min-width: 150px; font-size: 10px; }
      .print-title-date { min-width: 150px; text-align: right; font-size: 10px; }
      .print-footer { position: fixed; left: 12px; right: 12px; bottom: 4px; border-top: 1px dashed #000; background: #fff; font-size: 9px; padding-top: 3px; display: flex; align-items: center; justify-content: space-between; gap: 8px; z-index: 2; }
      .print-footer img { height: 26px; }
      .print-footer-text { flex: 1; text-align: center; line-height: 1.15; }
      .print-body { margin-top: 92px; padding-bottom: 46px; }
      .print-section { break-inside: auto; page-break-inside: auto; margin-bottom: 10px; }
      .print-section-title { border: 1px solid #000; border-bottom: 0; background: #dddddd; box-shadow: inset 0 0 0 9999px #dddddd; font-size: 10px; font-weight: 700; padding: 6px 8px; -webkit-print-color-adjust: exact; print-color-adjust: exact; color-adjust: exact; }
      .print-table { width: 100%; border-collapse: collapse; margin-bottom: 12px; table-layout: fixed; }
      .print-table th, .print-table td { border: 1px solid #000; padding: 6px 8px; text-align: left; font-size: 10px; vertical-align: top; word-break: break-word; }
      .print-table th { width: 26%; background: #dddddd; box-shadow: inset 0 0 0 9999px #dddddd; -webkit-print-color-adjust: exact; print-color-adjust: exact; color-adjust: exact; }
      .print-metas-table { margin-bottom: 0; }
      .print-metas-table thead { display: table-header-group; }
      .print-metas-table th, .print-metas-table td { padding: 4px 6px; font-size: 9px; text-align: center; vertical-align: middle; }
      .print-metas-table th { width: auto; background: #e5e5e5; box-shadow: inset 0 0 0 9999px #e5e5e5; -webkit-print-color-adjust: exact; print-color-adjust: exact; color-adjust: exact; }
      .print-metas-table tr { break-inside: avoid; page-break-inside: avoid; }
      .print-data-table tr, .print-justificativa-section tr { break-inside: avoid; page-break-inside: avoid; }
      .print-movimentacao-row td { background: #eeeeee; box-shadow: inset 0 0 0 9999px #eeeeee; -webkit-print-color-adjust: exact; print-color-adjust: exact; color-adjust: exact; }
      .print-total-row td { font-weight: 700; background: #dddddd; box-shadow: inset 0 0 0 9999px #dddddd; -webkit-print-color-adjust: exact; print-color-adjust: exact; color-adjust: exact; }
      .print-watermark { position: fixed; top: 45%; left: 50%; transform: translate(-50%, -50%) rotate(-30deg); font-size: 60px; color: rgba(0,0,0,0.12); font-family: "Arial Black", Arial, sans-serif; text-transform: uppercase; white-space: pre-line; text-align: center; pointer-events: none; }
      @media print {
        .print-page-header { top: 0; left: 0; right: 0; }
        .print-footer { bottom: 0; left: 0; right: 0; }
        .print-body { margin-top: 88px; padding-bottom: 38px; }
      }
    </style>
  </head>
  <body>
    ${watermarkText ? `<div class="print-watermark">${watermarkText}</div>` : ""}
    <div class="print-page-header">
      <div class="print-header">
        <div class="print-brand">
          <img src="/static/img/logo.jpg" alt="Logo" />
          <div class="print-brand-text">
            <div class="print-brand-title">Sistema de Planejamento e Orçamento</div>
            <div class="print-brand-subtitle">SPO-NGER-SEDUCMT</div>
          </div>
        </div>
      </div>
      <div class="print-title-row">
        <div class="print-title-key">${esc(controle)}</div>
        <div class="print-title">Alterar Meta Física</div>
        <div class="print-title-date">${esc(criadoEm)}</div>
      </div>
    </div>
    <div class="print-body">
      ${buildMetaFisicaPrintTable(meta)}
    </div>
    <div class="print-footer">
      <img src="/static/img/logo.jpg" alt="Logo" />
      <div class="print-footer-text">
        ${footerLine2 ? `<div>${esc(footerLine2)}</div>` : ""}
        ${footerLine3 ? `<div>${esc(footerLine3)}</div>` : ""}
      </div>
      <img src="/static/img/logoseduc.jpg" alt="Logo Seduc" />
    </div>
  </body>
  </html>`;
      const win = targetWin && !targetWin.closed ? targetWin : window.open("", "_blank");
      if (!win) {
        setMsg("Popup bloqueado. Libere o navegador para imprimir.", true);
        return;
      }
      win.document.open();
      win.document.write(html);
      win.document.close();
      win.focus();
      setTimeout(() => {
        win.print();
      }, 300);
    };

    const prepareMetaFisicaPrintWindow = () => {
      const win = window.open("", "_blank");
      if (!win) return null;
      try {
        win.document.open();
        win.document.write("<!doctype html><html><head><meta charset=\"utf-8\" /><title>Preparando impressão...</title></head><body>Preparando impressão...</body></html>");
        win.document.close();
      } catch (err) {
        console.error(err);
      }
      return win;
    };

    const getSummaryRows = () => Array.from(summaryBody?.querySelectorAll(".meta-fisica-summary-row") || []);

    const parseMaybeNumber = (value) => {
      if (value === null || value === undefined) return { raw: "", num: null };
      const raw = String(value).trim();
      if (!raw) return { raw, num: null };
      const num = Number(raw.replace(/\./g, "").replace(",", "."));
      return Number.isNaN(num) ? { raw, num: null } : { raw, num };
    };

    const compareValues = (left, right, op) => {
      const l = parseMaybeNumber(left);
      const r = parseMaybeNumber(right);
      if (l.num !== null && r.num !== null) {
        if (op === "eq") return l.num === r.num;
        if (op === "gt") return l.num > r.num;
        if (op === "lt") return l.num < r.num;
        if (op === "gte") return l.num >= r.num;
        if (op === "lte") return l.num <= r.num;
      }
      const lraw = l.raw.toLowerCase();
      const rraw = r.raw.toLowerCase();
      const cmp = lraw.localeCompare(rraw, "pt-BR", { sensitivity: "base" });
      if (op === "eq") return cmp === 0;
      if (op === "contains") return lraw.includes(rraw);
      if (op === "gt") return cmp > 0;
      if (op === "lt") return cmp < 0;
      if (op === "gte") return cmp >= 0;
      if (op === "lte") return cmp <= 0;
      return false;
    };

    const normalizeDigits = (value) => {
      const raw = String(value || "");
      const match = raw.match(/\d+(?:[.,]\d+)?/);
      if (match) return match[0].replace(".", ",");
      return raw;
    };

    const getRowFieldValue = (row, field) => {
      if (!row) return "";
      if (field === "controle_meta") return row.dataset.controleMeta || "";
      if (field === "exercicio") return row.dataset.exercicio || "";
      if (field === "status_aprovacao") return row.dataset.statusAprovacao || "";
      if (field === "acao_paoe") return row.dataset.acaoPaoe || "";
      if (field === "programa") return row.dataset.programa || "";
      if (field === "produto_acao") return row.dataset.produtoAcao || "";
      if (field === "regiao_produto") return row.dataset.regioesPreview || "";
      return "";
    };

    const compareField = (field, rowVal, targetVal, op) => {
      if (field === "acao_paoe" || field === "programa") {
        return compareValues(normalizeDigits(rowVal), normalizeDigits(targetVal), op);
      }
      return compareValues(rowVal, targetVal, op);
    };

    const renderCriteria = () => {
      if (!filterList) return;
      filterList.innerHTML = "";
      criteria.forEach((c, idx) => {
        const li = document.createElement("li");
        const label = fieldLabels[c.field] || c.field;
        const op = opLabels[c.op] || c.op;
        li.textContent = `${label} ${op} ${c.value}`;
        li.dataset.index = String(idx);
        if (idx === criteriaSelected) {
          li.style.borderColor = "var(--primary)";
        }
        li.addEventListener("click", () => {
          criteriaSelected = idx;
          renderCriteria();
        });
        filterList.appendChild(li);
      });
    };

    const setResultsVisible = (show) => {
      if (!summaryBox) return;
      summaryBox.classList.toggle("dotacao-summary-hidden", !show);
      summaryBox.classList.toggle("consulta-summary-hidden", !show);
      if (!show) {
        getSummaryRows().forEach((row) => row.classList.remove("selected"));
        if (paginationEl) paginationEl.innerHTML = "";
        if (summaryTableWrap) {
          summaryTableWrap.style.height = "";
          summaryTableWrap.style.maxHeight = "";
        }
      }
    };

    const getFilteredSummaryRows = () => {
      const rows = getSummaryRows();
      if (!criteria.length) return rows;
      return rows.filter((row) =>
        criteria.every((c) => compareField(c.field, getRowFieldValue(row, c.field), c.value, c.op))
      );
    };

    const updateSummaryViewportHeight = (rowsOnPage) => {
      if (!summaryTableWrap) return;
      const headerHeight = summaryTable?.tHead?.offsetHeight || 40;
      const sampleRow =
        summaryBody?.querySelector(".meta-fisica-summary-row") ||
        summaryBody?.querySelector("tr");
      const rowHeight = sampleRow?.offsetHeight || 36;
      const visibleRows = Math.max(0, Number(rowsOnPage || 0));
      if (visibleRows === 0) {
        const emptyHeight = Math.max(72, headerHeight + 24);
        summaryTableWrap.style.height = `${emptyHeight}px`;
        summaryTableWrap.style.maxHeight = `${emptyHeight}px`;
        return;
      }
      const contentHeight = headerHeight + (rowHeight * visibleRows) + 10;
      const viewportCap = Math.max(220, Math.floor(window.innerHeight * 0.52));
      const finalHeight = Math.min(contentHeight, viewportCap);
      summaryTableWrap.style.height = `${finalHeight}px`;
      summaryTableWrap.style.maxHeight = `${finalHeight}px`;
    };

    const renderSummaryPagination = (totalPages) => {
      if (!paginationEl) return;
      paginationEl.innerHTML = "";
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
        b.type = "button";
        b.className = "page-btn";
        b.textContent = label;
        if (disabled) b.disabled = true;
        if (active) b.classList.add("active");
        b.addEventListener("click", () => {
          if (disabled || page === summaryCurrentPage) return;
          summaryCurrentPage = page;
          renderSummaryPage();
        });
        paginationEl.appendChild(b);
      };
      addBtn("<<", 1, summaryCurrentPage === 1);
      addBtn("<", Math.max(1, summaryCurrentPage - 1), summaryCurrentPage === 1);
      const maxButtons = 5;
      let start = Math.max(1, summaryCurrentPage - Math.floor(maxButtons / 2));
      let end = Math.min(totalPages, start + maxButtons - 1);
      if (end - start + 1 < maxButtons) {
        start = Math.max(1, end - maxButtons + 1);
      }
      if (start > 1) {
        addBtn("1", 1, false, summaryCurrentPage === 1);
        if (start > 2) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          paginationEl.appendChild(ellipsis);
        }
      }
      for (let p = start; p <= end; p += 1) {
        addBtn(String(p), p, false, p === summaryCurrentPage);
      }
      if (end < totalPages) {
        const ellipsis = document.createElement("span");
        ellipsis.textContent = "...";
        paginationEl.appendChild(ellipsis);
        addBtn(String(totalPages), totalPages, false, summaryCurrentPage === totalPages);
      }
      addBtn(">", Math.min(totalPages, summaryCurrentPage + 1), summaryCurrentPage === totalPages);
      addBtn(">>", totalPages, summaryCurrentPage === totalPages);
    };

    const renderSummaryPage = () => {
      const rows = getSummaryRows();
      const filtered = getFilteredSummaryRows();
      rows.forEach((row) => {
        row.style.display = "none";
        row.classList.remove("selected");
      });
      const totalPages = Math.max(1, Math.ceil(filtered.length / summaryPageSize));
      if (summaryCurrentPage > totalPages) summaryCurrentPage = totalPages;
      const startIdx = (summaryCurrentPage - 1) * summaryPageSize;
      const pageRows = filtered.slice(startIdx, startIdx + summaryPageSize);
      pageRows.forEach((row) => {
        row.style.display = "";
      });
      updateSummaryViewportHeight(pageRows.length);
      renderSummaryPagination(totalPages);
    };

    const applyCriteriaToResults = (resetPage = true) => {
      if (resetPage) summaryCurrentPage = 1;
      const filtered = getFilteredSummaryRows();
      renderSummaryPage();
      if (!filtered.length) {
        setFilterMsg("Nenhum registro encontrado para os critérios informados.", true);
      }
    };

    const selectSummaryRow = (row) => {
      getSummaryRows().forEach((el) => el.classList.remove("selected"));
      if (row && row.style.display !== "none") row.classList.add("selected");
      updateSummaryActionButtons();
    };

    const resetEditMode = () => {
      editingMetaId = "";
      editingControle = "";
      approvalMode = false;
      approvingMetaId = "";
      approvingControle = "";
      if (approvalFields) approvalFields.style.display = "none";
      if (approvalJustificativa) {
        approvalJustificativa.value = "";
        approvalJustificativa.required = false;
      }
      approvalRadios.forEach((r) => {
        r.checked = r.value === "sim";
      });
      if (saveBtn) saveBtn.textContent = defaultSaveLabel;
      const controls = [
        ...Object.values(selects).filter((el) => el && el !== selects.exercicio),
        consultBtn,
        addRowBtn,
        clearBtn,
        justificativaInput,
      ].filter(Boolean);
      controls.forEach((el) => {
        el.disabled = false;
      });
      if (selects.exercicio) {
        selects.exercicio.disabled = true;
      }
      if (editBadge) {
        editBadge.textContent = "";
        editBadge.style.display = "none";
      }
    };

    const setEditMode = (metaId, controle) => {
      editingMetaId = String(metaId || "").trim();
      editingControle = String(controle || "").trim();
      approvalMode = false;
      approvingMetaId = "";
      approvingControle = "";
      if (approvalFields) approvalFields.style.display = "none";
      if (approvalJustificativa) {
        approvalJustificativa.value = "";
        approvalJustificativa.required = false;
      }
      if (saveBtn) saveBtn.textContent = defaultSaveLabel;
      const controls = [
        ...Object.values(selects).filter((el) => el && el !== selects.exercicio),
        consultBtn,
        addRowBtn,
        clearBtn,
        justificativaInput,
      ].filter(Boolean);
      controls.forEach((el) => {
        el.disabled = false;
      });
      if (selects.exercicio) {
        selects.exercicio.disabled = true;
      }
      if (!editBadge) return;
      if (!editingMetaId) {
        editBadge.textContent = "";
        editBadge.style.display = "none";
        return;
      }
      editBadge.textContent = `- Edição do registro ${editingControle || `<id:${editingMetaId}>`}`;
      editBadge.style.display = "inline";
    };

    const setApprovalMode = (metaId, controle) => {
      approvalMode = true;
      approvingMetaId = String(metaId || "").trim();
      approvingControle = String(controle || "").trim();
      editingMetaId = "";
      editingControle = "";
      if (approvalFields) approvalFields.style.display = "";
      if (approvalQuestionLabel) {
        approvalQuestionLabel.textContent = approvingControle
          ? `*Deseja aprovar o registro ${approvingControle}?`
          : "*Deseja aprovar o registro (Controle de Meta)?";
      }
      if (approvalJustificativa) {
        approvalJustificativa.value = "";
        approvalJustificativa.required = true;
      }
      approvalRadios.forEach((r) => {
        r.checked = r.value === "sim";
      });
      if (saveBtn) saveBtn.textContent = "Confirmar";
      const controls = [
        ...Object.values(selects).filter(Boolean),
        consultBtn,
        addRowBtn,
        clearBtn,
        justificativaInput,
      ].filter(Boolean);
      controls.forEach((el) => {
        el.disabled = true;
      });
      if (editBadge) {
        editBadge.textContent = `- Aprovação do registro ${approvingControle || `<id:${approvingMetaId}>`}`;
        editBadge.style.display = "inline";
      }
    };

    const updateSummaryActionButtons = () => {
      const selected = summaryBody?.querySelector(".meta-fisica-summary-row.selected");
      if (approveBtn) approveBtn.disabled = !selected || !canApprove;
      if (deleteBtn) deleteBtn.disabled = !selected;
      if (editBtn) editBtn.disabled = !selected;
    };

    if (justificativaInput) {
      justificativaInput.addEventListener("keydown", (ev) => {
        if (!justificativaProtectedPrefix) return;
        const minPos = justificativaProtectedPrefix.length;
        const start = justificativaInput.selectionStart || 0;
        const end = justificativaInput.selectionEnd || 0;
        const isDeleteKey = ev.key === "Backspace" || ev.key === "Delete";
        const touchesPrefix =
          start < minPos ||
          (ev.key === "Backspace" && start <= minPos && start === end) ||
          (ev.key === "Delete" && start < minPos);
        if (isDeleteKey && touchesPrefix) {
          ev.preventDefault();
          justificativaInput.setSelectionRange(minPos, minPos);
        }
      });
      ["input", "focus", "click", "keyup", "select"].forEach((eventName) => {
        justificativaInput.addEventListener(eventName, keepJustificativaPrefixProtected);
      });
    }

    const parseSummaryLinhas = (row) => {
      const raw = row?.getAttribute("data-linhas") || "[]";
      try {
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
      } catch (err) {
        return [];
      }
    };
    const fetchMetaLinhasById = async (metaId) => {
      const id = String(metaId || "").trim();
      if (!id) return [];
      try {
        const res = await fetch(`/api/meta-fisica/${encodeURIComponent(id)}/linhas`, {
          headers: { "X-Requested-With": "fetch" },
        });
        const data = await res.json();
        if (!res.ok) return [];
        return Array.isArray(data?.linhas) ? data.linhas : [];
      } catch (err) {
        return [];
      }
    };

    const buildSummaryMetaForPrint = (row) => {
      const linhasRaw = parseSummaryLinhas(row);
      const linhas = linhasRaw.map((l) => ({ ...(l || {}) }));
      const selectedId = Number(row?.dataset?.id || "0");
      const selectedStatus = String(row?.dataset?.statusAprovacao || "").trim().toLowerCase();

      const normalizeMatchText = (value) =>
        String(value || "")
          .normalize("NFD")
          .replace(/[\u0300-\u036f]/g, "")
          .replace(/\s+/g, " ")
          .trim()
          .toLowerCase();
      const sameContext = (candidateRow) => {
        const fields = ["exercicio", "uo", "programa", "acaoPaoe", "produtoAcao", "unidMedidaProduto"];
        return fields.every((f) => normalizeMatchText(candidateRow?.dataset?.[f] || "") === normalizeMatchText(row?.dataset?.[f] || ""));
      };
      const toSeries = (linha, field) => {
        const listKey = field === "credito" ? "meta_credito_items" : "meta_anulada_items";
        const scalarKey = field === "credito" ? "meta_credito" : "meta_anulada";
        const arr = parsePositiveList(linha?.[listKey]);
        if (arr.length) return arr;
        const scalar = parseDec(linha?.[scalarKey]);
        return scalar !== null && scalar > 0 ? [scalar] : [];
      };
      const _eqNum = (a, b) => Math.abs((Number(a) || 0) - (Number(b) || 0)) < 0.000001;
      const splitByPrevious = (prevSeries, currSeries) => {
        if (!currSeries.length) return { hist: [], mov: [] };
        if (!prevSeries.length) return { hist: [], mov: currSeries.slice() };
        let prefix = 0;
        while (
          prefix < prevSeries.length &&
          prefix < currSeries.length &&
          _eqNum(prevSeries[prefix], currSeries[prefix])
        ) {
          prefix += 1;
        }
        return {
          hist: currSeries.slice(0, prefix),
          mov: currSeries.slice(prefix),
        };
      };

      try {
        const hasStructuredLists = linhas.some((cl) =>
          Array.isArray(cl?.meta_credito_historico_items) ||
          Array.isArray(cl?.meta_anulada_historico_items) ||
          Array.isArray(cl?.meta_credito_mov_items) ||
          Array.isArray(cl?.meta_anulada_mov_items)
        );
        if (selectedId > 0 && !hasStructuredLists) {
          const approvedBeforeRows = getSummaryRows()
            .filter((r) => {
              const id = Number(r?.dataset?.id || "0");
              if (!(id > 0) || id >= selectedId) return false;
              if (String(r?.dataset?.statusAprovacao || "").trim().toLowerCase() !== "aprovado") return false;
              return sameContext(r);
            })
            .sort((a, b) => Number(a?.dataset?.id || "0") - Number(b?.dataset?.id || "0"));

          const regionState = {};
          approvedBeforeRows.forEach((approvedRow) => {
            const priorLinhas = parseSummaryLinhas(approvedRow);
            priorLinhas.forEach((pl) => {
              const key = normalizeRegionKey(String(pl?.regiao_produto || "").trim());
              if (!key) return;
              const prevFullCred = Array.isArray(regionState[key]?.fullCred) ? regionState[key].fullCred : [];
              const prevFullAnu = Array.isArray(regionState[key]?.fullAnu) ? regionState[key].fullAnu : [];
              const currCred = toSeries(pl, "credito");
              const currAnu = toSeries(pl, "anulada");
              const splitCred = splitByPrevious(prevFullCred, currCred);
              const splitAnu = splitByPrevious(prevFullAnu, currAnu);
              const hasMov = splitCred.mov.length > 0 || splitAnu.mov.length > 0;
              if (!regionState[key] || hasMov) {
                regionState[key] = {
                  fullCred: currCred,
                  fullAnu: currAnu,
                  histCred: splitCred.hist,
                  histAnu: splitAnu.hist,
                  movCred: splitCred.mov,
                  movAnu: splitAnu.mov,
                };
              } else {
                regionState[key].fullCred = currCred;
                regionState[key].fullAnu = currAnu;
              }
            });
          });

          linhas.forEach((cl) => {
            const key = normalizeRegionKey(String(cl?.regiao_produto || "").trim());
            if (!key) return;
            const prevFullCred = Array.isArray(regionState[key]?.fullCred) ? regionState[key].fullCred : [];
            const prevFullAnu = Array.isArray(regionState[key]?.fullAnu) ? regionState[key].fullAnu : [];
            const currCred = toSeries(cl, "credito");
            const currAnu = toSeries(cl, "anulada");
            const splitCred = splitByPrevious(prevFullCred, currCred);
            const splitAnu = splitByPrevious(prevFullAnu, currAnu);
            const hasCurrentMov = splitCred.mov.length > 0 || splitAnu.mov.length > 0;

            let useHistCred = splitCred.hist;
            let useHistAnu = splitAnu.hist;
            let useMovCred = splitCred.mov;
            let useMovAnu = splitAnu.mov;

            // Se o registro atual não mexeu na região, preserva o último
            // snapshot que teve movimento para manter a leitura consistente.
            if (!hasCurrentMov && regionState[key]) {
              useHistCred = Array.isArray(regionState[key].histCred) ? regionState[key].histCred : [];
              useHistAnu = Array.isArray(regionState[key].histAnu) ? regionState[key].histAnu : [];
              useMovCred = Array.isArray(regionState[key].movCred) ? regionState[key].movCred : [];
              useMovAnu = Array.isArray(regionState[key].movAnu) ? regionState[key].movAnu : [];
            } else {
              regionState[key] = {
                fullCred: currCred,
                fullAnu: currAnu,
                histCred: splitCred.hist,
                histAnu: splitAnu.hist,
                movCred: splitCred.mov,
                movAnu: splitAnu.mov,
              };
            }

            cl.meta_credito_historico_items = useHistCred.map((n) => String(n));
            cl.meta_credito_mov_items = useMovCred.map((n) => String(n));
            cl.meta_anulada_historico_items = useHistAnu.map((n) => String(n));
            cl.meta_anulada_mov_items = useMovAnu.map((n) => String(n));
            cl.meta_credito_historico = useHistCred.length
              ? String(useHistCred.reduce((acc, n) => acc + n, 0))
              : "";
            cl.meta_anulada_historico = useHistAnu.length
              ? String(useHistAnu.reduce((acc, n) => acc + n, 0))
              : "";
            cl.has_historico_movimento = useHistCred.length > 0 || useHistAnu.length > 0;
          });
        }
      } catch (err) {
        console.error("Falha ao separar histórico/movimentação para impressão:", err);
      }

      return {
        controle: row?.dataset?.controleMeta || "",
        status_aprovacao: selectedStatus,
        aprovado_por: String(row?.dataset?.aprovadoPor || "").trim(),
        aprovado_por_nome: String(row?.dataset?.aprovadoPorNome || "").trim(),
        aprovado_por_perfil: String(row?.dataset?.aprovadoPorPerfil || "").trim(),
        data_aprovacao: String(row?.dataset?.dataAprovacao || "").trim(),
        motivo_rejeicao: String(row?.dataset?.motivoRejeicao || "").trim(),
        criado_em: row?.dataset?.criadoEm || "",
        usuario_nome: String(row?.dataset?.usuarioNome || "").trim(),
        usuario_perfil: String(row?.dataset?.usuarioPerfil || "").trim(),
        exercicio: row?.dataset?.exercicio || "",
        unidade_orcamentaria: row?.dataset?.uo || "",
        programa: row?.dataset?.programa || "",
        acao_paoe: row?.dataset?.acaoPaoe || "",
        adj_solicitante: row?.dataset?.adjSolicitante || "",
        produto_acao: row?.dataset?.produtoAcao || "",
        unid_medida_produto: row?.dataset?.unidMedidaProduto || "",
        justificativa: row?.dataset?.justificativa || "",
        linhas,
      };
    };

    const buildCurrentFormMetaForPrint = (selectedRow) => {
      const linhas = (Array.isArray(tableRows) ? tableRows : [])
        .filter((row) => !row?.is_novo)
        .map((row) => {
          const creditoItemsAll = getFieldItems(row, "meta_credito");
          const anuladaItemsAll = getFieldItems(row, "meta_anulada");
          const creditoLockCount = getLockedItemCount(row, "meta_credito");
          const anuladaLockCount = getLockedItemCount(row, "meta_anulada");
          const toAligned = (v) => {
            const n = parseDec(v);
            return n !== null && n > 0 ? String(v ?? "").trim() : "";
          };
          const creditoHist = creditoItemsAll
            .map((v, idx) => (row.lock_meta_credito && idx < creditoLockCount ? toAligned(v) : null))
            .filter((v) => v !== null);
          const anuladaHist = anuladaItemsAll
            .map((v, idx) => (row.lock_meta_anulada && idx < anuladaLockCount ? toAligned(v) : null))
            .filter((v) => v !== null);
          const creditoMov = creditoItemsAll
            .map((v, idx) => (!(row.lock_meta_credito && idx < creditoLockCount) ? toAligned(v) : null))
            .filter((v) => v !== null);
          const anuladaMov = anuladaItemsAll
            .map((v, idx) => (!(row.lock_meta_anulada && idx < anuladaLockCount) ? toAligned(v) : null))
            .filter((v) => v !== null);
          return {
            regiao_produto: String(row?.regiao_produto || "").trim(),
            meta_produto: row?.meta_produto ?? "",
            meta_credito_items: creditoItemsAll.map((v) => toAligned(v)),
            meta_anulada_items: anuladaItemsAll.map((v) => toAligned(v)),
            meta_credito_historico_items: creditoHist,
            meta_anulada_historico_items: anuladaHist,
            meta_credito_mov_items: creditoMov,
            meta_anulada_mov_items: anuladaMov,
          };
        });
      return {
        controle: selectedRow?.dataset?.controleMeta || "",
        status_aprovacao: String(selectedRow?.dataset?.statusAprovacao || "").trim(),
        aprovado_por: String(selectedRow?.dataset?.aprovadoPor || "").trim(),
        aprovado_por_nome: String(selectedRow?.dataset?.aprovadoPorNome || "").trim(),
        aprovado_por_perfil: String(selectedRow?.dataset?.aprovadoPorPerfil || "").trim(),
        data_aprovacao: String(selectedRow?.dataset?.dataAprovacao || "").trim(),
        motivo_rejeicao: String(selectedRow?.dataset?.motivoRejeicao || "").trim(),
        criado_em: selectedRow?.dataset?.criadoEm || "",
        usuario_nome: String(selectedRow?.dataset?.usuarioNome || "").trim(),
        usuario_perfil: String(selectedRow?.dataset?.usuarioPerfil || "").trim(),
        exercicio: selects.exercicio?.value || selectedRow?.dataset?.exercicio || "",
        unidade_orcamentaria: selects.unidade_orcamentaria?.value || selectedRow?.dataset?.uo || "",
        programa: selects.programa?.value || selectedRow?.dataset?.programa || "",
        acao_paoe: selects.acao_paoe?.value || selectedRow?.dataset?.acaoPaoe || "",
        adj_solicitante: selects.adj_solicitante?.value || selectedRow?.dataset?.adjSolicitante || "",
        produto_acao: selects.produto_acao?.value || selectedRow?.dataset?.produtoAcao || "",
        unid_medida_produto: selects.unid_medida_produto?.value || selectedRow?.dataset?.unidMedidaProduto || "",
        justificativa: String(justificativaInput?.value || "").trim() || selectedRow?.dataset?.justificativa || "",
        linhas,
      };
    };

    const _lockCountByBaseline = (items, baselineTotal) => {
      const base = Number(baselineTotal || 0);
      if (!Number.isFinite(base) || base <= 0) return 0;
      let acc = 0;
      let count = 0;
      for (const raw of items) {
        const n = parseDec(raw);
        if (!(n > 0)) continue;
        acc += n;
        count += 1;
        if (acc >= base - 0.000001) break;
      }
      return count;
    };

    const buildEditableRowsFromSummary = (linhas, baselineByRegion = {}) =>
      (Array.isArray(linhas) ? linhas : []).map((row) => {
        const isNovo = !!row?.is_novo;
        const hasStructuredLists =
          Array.isArray(row?.meta_credito_historico_items) ||
          Array.isArray(row?.meta_anulada_historico_items) ||
          Array.isArray(row?.meta_credito_mov_items) ||
          Array.isArray(row?.meta_anulada_mov_items);
        const normalized = hasStructuredLists ? normalizeMetaLinhaSeries(row) : null;
        // Preserva a estrutura de linhas (incluindo vazios de pareamento)
        // para não colapsar históricos de colunas opostas na mesma linha.
        const creditoItems = normalized
          ? normalized.creditoSeries.map((n) => (n !== null && n > 0 ? String(n) : ""))
          : (Array.isArray(row?.meta_credito_items)
            ? row.meta_credito_items.map((v) => String(v ?? ""))
            : []);
        const anuladaItems = normalized
          ? normalized.anuladaSeries.map((n) => (n !== null && n > 0 ? String(n) : ""))
          : (Array.isArray(row?.meta_anulada_items)
            ? row.meta_anulada_items.map((v) => String(v ?? ""))
            : []);
        const creditoMovItems = Array.isArray(row?.meta_credito_mov_items)
          ? row.meta_credito_mov_items.map((v) => String(v ?? ""))
          : [];
        const anuladaMovItems = Array.isArray(row?.meta_anulada_mov_items)
          ? row.meta_anulada_mov_items.map((v) => String(v ?? ""))
          : [];
        const creditoHistItemsRaw = Array.isArray(row?.meta_credito_historico_items)
          ? row.meta_credito_historico_items.map((v) => String(v ?? ""))
          : [];
        const anuladaHistItemsRaw = Array.isArray(row?.meta_anulada_historico_items)
          ? row.meta_anulada_historico_items.map((v) => String(v ?? ""))
          : [];
        const creditoHistorico = parseDec(row?.meta_credito_historico);
        const anuladaHistorico = parseDec(row?.meta_anulada_historico);
        const hasHistoricoMov = !!row?.has_historico_movimento;
        const creditoPositivos = creditoItems.filter((v) => (parseDec(v) || 0) > 0);
        const anuladaPositivos = anuladaItems.filter((v) => (parseDec(v) || 0) > 0);
        const hasCreditoMovList = Array.isArray(row?.meta_credito_mov_items);
        const hasAnuladaMovList = Array.isArray(row?.meta_anulada_mov_items);
        const creditoMovLen = creditoMovItems.filter((v) => (parseDec(v) || 0) > 0).length;
        const anuladaMovLen = anuladaMovItems.filter((v) => (parseDec(v) || 0) > 0).length;
        const hasCurrentMovement = creditoMovLen > 0 || anuladaMovLen > 0;
        let lockCreditoCount = 0;
        let lockAnuladaCount = 0;
        const regKey = normalizeRegionKey(String(row?.regiao_produto || ""));
        const baseline = baselineByRegion?.[regKey] || {};
        const baselineCredito = parseDec(baseline.meta_credito);
        const baselineAnulada = parseDec(baseline.meta_anulada);

        if (!isNovo) {
          // Regra principal: bloquear somente o que veio do plan21_nger (baseline da consulta).
          if (normalized && normalized.histCreditoItems.length > 0) {
            lockCreditoCount = normalized.histCreditoItems.length;
          } else if ((baselineCredito || 0) > 0 && creditoPositivos.length > 0) {
            lockCreditoCount = _lockCountByBaseline(creditoItems, baselineCredito);
          } else if (hasHistoricoMov && creditoPositivos.length > 0) {
            // Fallback para registros legados sem baseline encontrado.
            const histCountByItems = creditoHistItemsRaw.filter((v) => {
              const n = parseDec(v);
              return n !== null && n > 0;
            }).length;
            if (histCountByItems > 0) {
              lockCreditoCount = histCountByItems;
            } else if ((creditoHistorico || 0) > 0 && hasCreditoMovList && creditoMovLen <= creditoPositivos.length) {
              lockCreditoCount = Math.max(0, creditoPositivos.length - creditoMovLen);
            } else if ((creditoHistorico || 0) > 0) {
              lockCreditoCount = 1;
            }
          }

          if (normalized && normalized.histAnuladaItems.length > 0) {
            lockAnuladaCount = normalized.histAnuladaItems.length;
          } else if ((baselineAnulada || 0) > 0 && anuladaPositivos.length > 0) {
            lockAnuladaCount = _lockCountByBaseline(anuladaItems, baselineAnulada);
          } else if (hasHistoricoMov && anuladaPositivos.length > 0) {
            // Fallback para registros legados sem baseline encontrado.
            const histCountByItems = anuladaHistItemsRaw.filter((v) => {
              const n = parseDec(v);
              return n !== null && n > 0;
            }).length;
            if (histCountByItems > 0) {
              lockAnuladaCount = histCountByItems;
            } else if ((anuladaHistorico || 0) > 0 && hasAnuladaMovList && anuladaMovLen <= anuladaPositivos.length) {
              lockAnuladaCount = Math.max(0, anuladaPositivos.length - anuladaMovLen);
            } else if ((anuladaHistorico || 0) > 0) {
              lockAnuladaCount = 1;
            }
          }
        }
        const creditoEditableCount = Math.max(0, creditoPositivos.length - lockCreditoCount);
        const anuladaEditableCount = Math.max(0, anuladaPositivos.length - lockAnuladaCount);
        let activeMovementField = "";
        if (hasCurrentMovement) {
          if (creditoMovLen > 0 && anuladaMovLen <= 0) {
            activeMovementField = "meta_credito";
          } else if (anuladaMovLen > 0 && creditoMovLen <= 0) {
            activeMovementField = "meta_anulada";
          }
        }
        const hasUnlockedCredito = creditoItems
          .slice(lockCreditoCount)
          .some((v) => {
            const n = parseDec(v);
            return n !== null && n > 0;
          });
        const hasUnlockedAnulada = anuladaItems
          .slice(lockAnuladaCount)
          .some((v) => {
            const n = parseDec(v);
            return n !== null && n > 0;
          });
        return {
          regiao_produto: String(row?.regiao_produto || "").trim(),
          meta_produto: row?.meta_produto ?? "",
          meta_credito: "",
          meta_anulada: "",
          meta_credito_items: creditoItems.length ? creditoItems : (isNovo ? [""] : []),
          meta_anulada_items: isNovo ? [""] : (anuladaItems.length ? anuladaItems : []),
          lock_meta_credito: lockCreditoCount > 0,
          lock_meta_anulada: lockAnuladaCount > 0,
          lock_meta_credito_count: lockCreditoCount,
          lock_meta_anulada_count: lockAnuladaCount,
          allow_add_meta_credito: isNovo ? true : true,
          allow_add_meta_anulada: isNovo ? false : true,
          allow_start_meta_credito: isNovo ? true : !hasCurrentMovement,
          allow_start_meta_anulada: isNovo ? true : !hasCurrentMovement,
          allow_cross_add: !isNovo,
          entry_enable_meta_credito: creditoEditableCount > 0 || hasUnlockedCredito,
          entry_enable_meta_anulada: anuladaEditableCount > 0 || hasUnlockedAnulada,
          has_new_movement: hasCurrentMovement || hasUnlockedCredito || hasUnlockedAnulada,
          active_movement_field: activeMovementField,
          is_novo: isNovo,
          plan21_nger_id: row?.plan21_nger_id || null,
          plan21_ids: Array.isArray(row?.plan21_ids) ? row.plan21_ids : [],
        };
      });

    const fetchPlanBaselineByRegion = async () => {
      const url = new URL("/api/meta-fisica/options", window.location.origin);
      url.searchParams.set("include_rows", "1");
      url.searchParams.set("include_history", "0");
      Object.entries(selects).forEach(([key, el]) => {
        if (!el) return;
        const val = String(el.value || "").trim();
        if (val) url.searchParams.set(key, val);
      });
      const res = await fetch(url.toString(), { headers: { "X-Requested-With": "fetch" } });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Falha ao carregar baseline do PTA.");
      const map = {};
      const rows = Array.isArray(data?.rows) ? data.rows : [];
      rows.forEach((r) => {
        const key = normalizeRegionKey(String(r?.regiao_produto || ""));
        if (!key) return;
        map[key] = {
          meta_credito: r?.meta_credito || "",
          meta_anulada: r?.meta_anulada || "",
        };
      });
      return map;
    };

    const renderRows = () => {
      if (!tbody) return;
      if (!tableRows.length) {
        tbody.innerHTML =
          `<tr><td colspan="5" class="muted">${esc(getRowsEmptyMessage())}</td></tr>`;
        renderResumoTotais();
        return;
      }
      const totals = getTableTotals();
      const rowsHtml = tableRows
        .map((row, idx) => {
          const forceReadOnly = approvalMode;
          const readOnlyNew = row.is_novo ? "" : "readonly";
          const creditoReadOnly = forceReadOnly;
          const anuladaReadOnly = forceReadOnly;
          const creditoAllowAdd = forceReadOnly ? false : !!row.allow_add_meta_credito;
          const anuladaAllowAdd = forceReadOnly ? false : !!row.allow_add_meta_anulada;
          let regionFieldHtml = "";
          if (row.is_novo && !forceReadOnly) {
            const options = getAvailableRegionOptions(idx);
            const selectedKey = normalizeRegionKey(row.regiao_produto);
            const hasSelected = selectedKey && options.some((opt) => getCatalogCode(opt) === selectedKey);
            let selectOptions = '<option value="">Selecione...</option>';
            if (selectedKey && !hasSelected && row.regiao_produto) {
              selectOptions += `<option value="${esc(row.regiao_produto)}">${esc(row.regiao_produto)}</option>`;
            }
            selectOptions += options
              .map((opt) => {
                const code = String(opt?.codigo || "").trim();
                return `<option value="${esc(code)}">${esc(code)}</option>`;
              })
              .join("");
            regionFieldHtml = `
              <div class="meta-fisica-region-new-wrap">
                <select class="meta-fisica-cell meta-fisica-region-select" data-field="regiao_produto" ${options.length ? "" : "disabled"}>
                  ${selectOptions}
                </select>
                <button type="button" class="meta-fisica-remove-row-btn" data-remove-row="${idx}" title="Remover linha">-</button>
              </div>
            `;
          } else {
            regionFieldHtml = `<input type="text" class="meta-fisica-cell" data-field="regiao_produto" value="${esc(row.regiao_produto || "")}" readonly />`;
          }
          return `
            <tr data-idx="${idx}">
              <td>${regionFieldHtml}</td>
              <td><input type="text" class="meta-fisica-cell" data-field="meta_produto" value="${esc(fmtNum(row.meta_produto))}" readonly /></td>
              <td>${buildMovementCell(row, idx, "meta_credito", creditoReadOnly, creditoAllowAdd)}</td>
              <td>${buildMovementCell(row, idx, "meta_anulada", anuladaReadOnly, anuladaAllowAdd)}</td>
              <td><div class="meta-fisica-adjust-wrap">${buildAdjustedLinesHtml(row)}</div></td>
            </tr>
          `;
        })
        .join("");
      tbody.innerHTML = `${rowsHtml}
        <tr class="meta-fisica-total-row">
          <td><strong>TOTAIS</strong></td>
          <td><input type="text" class="meta-fisica-cell" data-total-field="meta_pta" value="${esc(fmtNum(totals.meta_pta))}" readonly /></td>
          <td><input type="text" class="meta-fisica-cell meta-fisica-cell-credito" data-total-field="acrescimo" value="${esc(fmtNum(totals.acrescimo))}" readonly /></td>
          <td><input type="text" class="meta-fisica-cell meta-fisica-cell-anulada" data-total-field="reducao" value="${esc(fmtNum(totals.reducao))}" readonly /></td>
          <td><input type="text" class="meta-fisica-cell" data-total-field="meta_ajustada" value="${esc(fmtNum(totals.meta_ajustada))}" readonly /></td>
        </tr>`;
      tableRows.forEach((row, idx) => {
        if (!row.is_novo) return;
        const tr = tbody.querySelector(`tr[data-idx="${idx}"]`);
        const select = tr ? tr.querySelector('select[data-field="regiao_produto"]') : null;
        if (!select) return;
        const desired = String(row.regiao_produto || "").trim();
        if (desired && Array.from(select.options).some((o) => o.value === desired)) {
          select.value = desired;
        }
      });
      refreshTotalsDisplay();
    };

    const loadOptions = async (loadRows = false, includeOptionRows = false) => {
      if (selects.exercicio) {
        selects.exercicio.disabled = true;
      }
      if (loadOptionsAbortController) {
        try {
          loadOptionsAbortController.abort();
        } catch (e) {
          // noop
        }
      }
      loadOptionsAbortController = new AbortController();
      const controller = loadOptionsAbortController;
      const signal = controller.signal;
      try {
        const url = new URL("/api/meta-fisica/options", window.location.origin);
        if (loadRows) {
          url.searchParams.set("include_rows", "1");
          url.searchParams.set("include_history", "1");
        } else {
          url.searchParams.set("include_rows", "0");
          url.searchParams.set("include_history", "0");
        }
        if (includeOptionRows) {
          url.searchParams.set("include_option_rows", "1");
        }
        const currentMetaId = String(editingMetaId || approvingMetaId || "").trim();
        if (currentMetaId) {
          url.searchParams.set("meta_id", currentMetaId);
        }
        Object.entries(selects).forEach(([key, el]) => {
          if (!el) return;
          const val = String(el.value || "").trim();
          if (val) url.searchParams.set(key, val);
        });
        const res = await fetch(url.toString(), {
          headers: { "X-Requested-With": "fetch" },
          signal,
        });
        if (signal.aborted) return;
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar opções.");
        if (signal.aborted) return;
        regionCatalog = Array.isArray(data.regioes_catalog) ? data.regioes_catalog : [];
        if (Array.isArray(data.option_rows) && data.option_rows.length) {
          optionRowsCatalog = data.option_rows;
        }
        if (loadRows && data.pending_meta) {
          tableRows = [];
          lastQueryHadRows = false;
          renderRows();
          return { pendingMeta: data.pending_meta };
        }

        applySelectOptions(selects.exercicio, data.options?.exercicio || [], false);
        if (selects.exercicio) {
          selects.exercicio.value = (data.current_year || "");
          selects.exercicio.disabled = true;
        }
        applySelectOptions(selects.unidade_orcamentaria, data.options?.unidade_orcamentaria || []);
        applySelectOptions(selects.programa, data.options?.programa || []);
        applySelectOptions(selects.acao_paoe, data.options?.acao_paoe || []);
        applySelectOptions(selects.adj_solicitante, data.options?.adj_solicitante || []);
        applySelectOptions(selects.produto_acao, data.options?.produto_acao || []);
        applySelectOptions(selects.unid_medida_produto, data.options?.unid_medida_produto || []);

        const manualRows = tableRows.filter((row) => row.is_novo);
        if (!hasAllRowFiltersSelected()) {
          tableRows = [];
          lastQueryHadRows = false;
          renderRows();
          return;
        }
        if (!loadRows) {
          tableRows = manualRows;
          lastQueryHadRows = false;
          renderRows();
          return;
        }

        const loadedRows = (data.rows || []).map((row) => {
          const normalized = normalizeMetaLinhaSeries(row);
          const creditoItems = normalized.creditoSeries.map((n) => (n !== null && n > 0 ? String(n) : ""));
          const anuladaItems = normalized.anuladaSeries.map((n) => (n !== null && n > 0 ? String(n) : ""));
          const lockedCreditoCount = normalized.histCreditoItems.length;
          const lockedAnuladaCount = normalized.histAnuladaItems.length;
          const lockCreditoFromDb = lockedCreditoCount > 0;
          const lockAnuladaFromDb = lockedAnuladaCount > 0;
          const hasMovFromDb =
            normalized.movCredito.some((n) => n !== null && n > 0) ||
            normalized.movAnulada.some((n) => n !== null && n > 0);
          return {
            regiao_produto: row.regiao_produto || "",
            meta_produto: row.meta_produto || "",
            meta_credito: "",
            meta_anulada: "",
            meta_credito_items: creditoItems,
            meta_anulada_items: anuladaItems,
            lock_meta_credito: lockCreditoFromDb,
            lock_meta_anulada: lockAnuladaFromDb,
            lock_meta_credito_count: lockedCreditoCount,
            lock_meta_anulada_count: lockedAnuladaCount,
            allow_add_meta_credito: true,
            allow_add_meta_anulada: true,
            allow_start_meta_credito: !hasMovFromDb,
            allow_start_meta_anulada: !hasMovFromDb,
            allow_cross_add: true,
            entry_enable_meta_credito: false,
            entry_enable_meta_anulada: false,
            is_novo: false,
            plan21_nger_id: row.plan21_nger_id || null,
            plan21_ids: Array.isArray(row.plan21_ids) ? row.plan21_ids : [],
          };
        });
        tableRows = [...loadedRows, ...manualRows];
        lastQueryHadRows = loadedRows.length > 0;
        renderRows();
        return { data };
      } catch (err) {
        if (err?.name === "AbortError") return;
        console.error(err);
        setMsg(err.message || "Falha ao carregar opções.", true);
      } finally {
        if (loadOptionsAbortController === controller) {
          loadOptionsAbortController = null;
        }
        if (selects.exercicio) {
          selects.exercicio.disabled = true;
        }
      }
      return null;
    };


    if (tbody) {
      tbody.addEventListener("focusin", (ev) => {
        const input = ev.target.closest("input[data-field]");
        if (!input) return;
        const field = input.dataset.field;
        if (field !== "meta_credito" && field !== "meta_anulada") return;
        if (getUnidadeTipo() !== "unidade") {
          input.value = unformatDecimalPtBr(input.value);
        }
      });

      tbody.addEventListener("input", (ev) => {
        clearMsgOnUserAction();
        const input = ev.target.closest("input[data-field]");
        if (!input) return;
        const tr = input.closest("tr[data-idx]");
        if (!tr) return;
        const idx = Number(tr.dataset.idx || "-1");
        if (!Number.isInteger(idx) || idx < 0 || idx >= tableRows.length) return;
        const field = input.dataset.field;
        const itemIdx = Number(input.dataset.itemIdx || "0");
        if (field === "meta_credito" || field === "meta_anulada") {
          const itemIdx = Number(input.dataset.itemIdx || "0");
          if (!Number.isInteger(itemIdx) || itemIdx < 0) return;
          const lockCount = getLockedItemCount(tableRows[idx], field);
          const isLocked =
            itemIdx >= 0 &&
            itemIdx < lockCount &&
            ((field === "meta_credito" && !!tableRows[idx].lock_meta_credito) ||
              (field === "meta_anulada" && !!tableRows[idx].lock_meta_anulada));
          if (isLocked) return;
          let masked = sanitizeByUnidade(input.value);
          masked = enforceMinPercentual(masked, false);
          input.value = masked;
          const items = getFieldItems(tableRows[idx], field);
          items[itemIdx] = masked;
          setFieldItems(tableRows[idx], field, items);
        } else {
          tableRows[idx][field] = input.value;
        }
        let valueRuleErr = "";
        if (field === "meta_credito" || field === "meta_anulada") {
          const fieldLabel = field === "meta_credito" ? "Acréscimo" : "Redução";
          const items = getFieldItems(tableRows[idx], field);
          const lockCount = getLockedItemCount(tableRows[idx], field);
          for (let i = 0; i < items.length; i += 1) {
            if (
              ((field === "meta_credito" && tableRows[idx].lock_meta_credito) ||
                (field === "meta_anulada" && tableRows[idx].lock_meta_anulada)) &&
              i < lockCount
            ) {
              continue;
            }
            const item = items[i];
            valueRuleErr = validateValorByUnidade(item, fieldLabel);
            if (valueRuleErr) break;
          }
        }
        const dup = validateDuplicateRegions();
        if (dup) {
          setMsg(`A região ${dup} já existe na tabela.`, true);
        } else if (valueRuleErr) {
          setMsg(valueRuleErr, true);
        } else {
          setMsg("");
        }
        updateAdjustedDisplay(tr, tableRows[idx]);
        refreshTotalsDisplay();
      });

      tbody.addEventListener("change", (ev) => {
        clearMsgOnUserAction();
        const select = ev.target.closest('select[data-field="regiao_produto"]');
        if (!select) return;
        const tr = select.closest("tr[data-idx]");
        if (!tr) return;
        const idx = Number(tr.dataset.idx || "-1");
        if (!Number.isInteger(idx) || idx < 0 || idx >= tableRows.length) return;
        tableRows[idx].regiao_produto = String(select.value || "").trim();
        const dup = validateDuplicateRegions();
        if (dup) {
          setMsg(`A região ${dup} já existe na tabela.`, true);
        } else {
          setMsg("");
        }
        renderRows();
      });

      tbody.addEventListener("focusout", (ev) => {
        clearMsgOnUserAction();
        const input = ev.target.closest("input[data-field]");
        if (!input) return;
        const tr = input.closest("tr[data-idx]");
        if (!tr) return;
        const idx = Number(tr.dataset.idx || "-1");
        if (!Number.isInteger(idx) || idx < 0 || idx >= tableRows.length) return;
        const field = input.dataset.field;
        if (field !== "meta_credito" && field !== "meta_anulada") return;
        const itemIdx = Number(input.dataset.itemIdx || "0");
        if (!Number.isInteger(itemIdx) || itemIdx < 0) return;
        const lockCount = getLockedItemCount(tableRows[idx], field);
        const isLocked =
          itemIdx >= 0 &&
          itemIdx < lockCount &&
          ((field === "meta_credito" && !!tableRows[idx].lock_meta_credito) ||
            (field === "meta_anulada" && !!tableRows[idx].lock_meta_anulada));
        if (isLocked) return;
        let masked = sanitizeByUnidade(input.value);
        masked = enforceMinPercentual(masked, true);
        const items = getFieldItems(tableRows[idx], field);
        items[itemIdx] = masked;
        setFieldItems(tableRows[idx], field, items);
        input.value = formatByUnidade(masked);
        updateAdjustedDisplay(tr, tableRows[idx]);
        refreshTotalsDisplay();
      });

      tbody.addEventListener("click", (ev) => {
        clearMsgOnUserAction();
        const removeRowBtn = ev.target.closest("[data-remove-row]");
        if (removeRowBtn) {
          const idx = Number(removeRowBtn.getAttribute("data-remove-row") || "-1");
          if (!Number.isInteger(idx) || idx < 0 || idx >= tableRows.length) return;
          if (!tableRows[idx]?.is_novo) return;
          tableRows.splice(idx, 1);
          const dup = validateDuplicateRegions();
          if (dup) {
            setMsg(`A região ${dup} já existe na tabela.`, true);
          } else {
            setMsg("");
          }
          renderRows();
          return;
        }
        const addBtn = ev.target.closest("[data-add-field]");
        if (addBtn) {
          const tr = addBtn.closest("tr[data-idx]");
          if (!tr) return;
          const idx = Number(tr.dataset.idx || "-1");
          if (!Number.isInteger(idx) || idx < 0 || idx >= tableRows.length) return;
          const field = addBtn.getAttribute("data-add-field");
          if (field !== "meta_credito" && field !== "meta_anulada") return;
          const allowAdd =
            (field === "meta_credito" && !!tableRows[idx].allow_add_meta_credito) ||
            (field === "meta_anulada" && !!tableRows[idx].allow_add_meta_anulada);
          if (!allowAdd) return;
          if (!tableRows[idx].is_novo && hasActiveEditableMovement(tableRows[idx])) {
            setMsg("Remova primeiro a movimentação atual da região antes de adicionar uma nova sublinha.", true);
            return;
          }
          if (!canAddMovement(tableRows[idx], field)) {
            setMsg("Preencha um lançamento antes de adicionar outro.", true);
            return;
          }
          // Snapshot da região para rollback atômico se remover a sublinha nova.
          if (!tableRows[idx].__new_movement_snapshot) {
            tableRows[idx].__new_movement_snapshot = cloneRowForRollback(tableRows[idx]);
          }
          if (!tableRows[idx].is_novo) {
            // Ao criar nova sublinha em linha existente, todo conteúdo anterior
            // passa a ser histórico/bloqueado; apenas a nova sublinha fica editável.
            freezeExistingMovements(tableRows[idx]);
          }
          const oppositeField = field === "meta_credito" ? "meta_anulada" : "meta_credito";
          tableRows[idx][`allow_start_${field}`] = false;
          tableRows[idx][`allow_start_${oppositeField}`] = false;
          tableRows[idx][`entry_enable_${field}`] = true;
          // Regra de negócio: ajuste por vez (somente a coluna clicada fica ativa).
          tableRows[idx][`entry_enable_${oppositeField}`] = false;
          tableRows[idx].active_movement_field = field;
          tableRows[idx].has_new_movement = true;
          const items = getFieldItems(tableRows[idx], field);
          items.push("");
          setFieldItems(tableRows[idx], field, items);
          renderRows();
          return;
        }
        const removeBtn = ev.target.closest("[data-remove-field]");
        if (removeBtn) {
          const tr = removeBtn.closest("tr[data-idx]");
          if (!tr) return;
          const idx = Number(tr.dataset.idx || "-1");
          if (!Number.isInteger(idx) || idx < 0 || idx >= tableRows.length) return;
          const field = removeBtn.getAttribute("data-remove-field");
          const removeIdx = Number(removeBtn.getAttribute("data-remove-idx") || "-1");
          if (field !== "meta_credito" && field !== "meta_anulada") return;
          if (removeIdx < 0) return;
          // Remoção atômica apenas da movimentação nova criada nesta edição.
          if (
            !tableRows[idx]?.is_novo &&
            !!tableRows[idx]?.has_new_movement &&
            (
              !tableRows[idx]?.active_movement_field ||
              tableRows[idx].active_movement_field === field
            )
          ) {
            const snapshot = tableRows[idx].__new_movement_snapshot;
            if (snapshot) {
              restoreRowFromSnapshot(tableRows[idx], snapshot);
            } else {
              const items = getFieldItems(tableRows[idx], field);
              if (removeIdx >= items.length) return;
              items.splice(removeIdx, 1);
              setFieldItems(tableRows[idx], field, items);
              const lockCountAfter = getLockedItemCount(tableRows[idx], field);
              const unlockedHasValue = getFieldItems(tableRows[idx], field)
                .slice(lockCountAfter)
                .some((item) => {
                  const n = parseDec(item);
                  return n !== null && n > 0;
                });
              tableRows[idx][`entry_enable_${field}`] = unlockedHasValue;
            }
            const hasCreditoUnlocked = getFieldItems(tableRows[idx], "meta_credito")
              .slice(getLockedItemCount(tableRows[idx], "meta_credito"))
              .some((item) => {
                const n = parseDec(item);
                return n !== null && n > 0;
              });
            const hasAnuladaUnlocked = getFieldItems(tableRows[idx], "meta_anulada")
              .slice(getLockedItemCount(tableRows[idx], "meta_anulada"))
              .some((item) => {
                const n = parseDec(item);
                return n !== null && n > 0;
              });
            tableRows[idx].has_new_movement = hasCreditoUnlocked || hasAnuladaUnlocked;
            tableRows[idx].active_movement_field = hasCreditoUnlocked
              ? "meta_credito"
              : (hasAnuladaUnlocked ? "meta_anulada" : "");
            delete tableRows[idx].__new_movement_snapshot;
            renderRows();
            return;
          }
          if (!tableRows[idx]?.is_novo) {
            setMsg("Não é permitido remover movimentações já validadas da base. Remova apenas a sublinha nova criada na edição.", true);
            return;
          }
          const items = getFieldItems(tableRows[idx], field);
          if (removeIdx >= items.length) return;
          items.splice(removeIdx, 1);
          setFieldItems(tableRows[idx], field, items);
          const stillHasAny = getFieldItems(tableRows[idx], field).some((item) => {
            const n = parseDec(item);
            return n !== null && n > 0;
          });
          if (!stillHasAny) {
            tableRows[idx][`entry_enable_${field}`] = false;
            tableRows[idx][`allow_start_${field}`] = true;
            if (tableRows[idx].active_movement_field === field) {
              tableRows[idx].active_movement_field = "";
            }
          }
          restoreFrozenMovementsIfPossible(tableRows[idx]);
          renderRows();
        }
      });
    }

    if (addRowBtn) {
      addRowBtn.addEventListener("click", () => {
        clearMsgOnUserAction();
        if (!hasAllRowFiltersSelected()) {
          setMsg("Preencha os filtros obrigatórios e clique em Consultar antes de adicionar linha.", true);
          return;
        }
        if (!hasConsulted) {
          setMsg("Clique em Consultar antes de adicionar nova linha.", true);
          return;
        }
        if (tableRows.length >= MAX_META_FISICA_ROWS) {
          setMsg(`Limite máximo de ${MAX_META_FISICA_ROWS} linhas atingido.`, true);
          return;
        }
        const available = getAvailableRegionOptions(null);
        if (!available.length) {
          setMsg("Não há regiões disponíveis para adicionar.", true);
          return;
        }
        tableRows.push({
          regiao_produto: "",
          meta_produto: "",
          meta_credito: "",
          meta_anulada: "",
          meta_credito_items: [""],
          meta_anulada_items: [""],
          lock_meta_credito: false,
          lock_meta_anulada: false,
          lock_meta_credito_count: 0,
          lock_meta_anulada_count: 0,
          allow_add_meta_credito: false,
          allow_add_meta_anulada: false,
          is_novo: true,
          plan21_nger_id: null,
          plan21_ids: [],
        });
        renderRows();
      });
    }

    if (clearBtn) {
      clearBtn.addEventListener("click", async () => {
        clearMsgOnUserAction();
        resetEditMode();
        if (justificativaInput) justificativaInput.value = "";
        Object.entries(selects).forEach(([key, el]) => {
          if (!el) return;
          if (key === "exercicio") return;
          el.value = "";
        });
        tableRows = [];
        hasConsulted = false;
        lastQueryHadRows = false;
        await loadOptions(false);
        setMsg("");
      });
    }

    if (consultBtn) {
      consultBtn.addEventListener("click", async () => {
        clearMsgOnUserAction();
        resetEditMode();
        if (!hasAllRowFiltersSelected()) {
          setMsg("Preencha todos os filtros obrigatórios antes de consultar.", true);
          hasConsulted = false;
          lastQueryHadRows = false;
          renderRows();
          return;
        }
        hasConsulted = true;
        setMsg("");
        const loadResult = await loadOptions(true);
        if (loadResult?.pendingMeta) {
          hasConsulted = false;
          await openMetaFisicaPendingModal({
            controle: loadResult.pendingMeta.controle || "",
          });
          return;
        }
        const totalsAfterConsult = getTableTotals();
        const totalPta = parseDec(totalsAfterConsult.meta_pta);
        const totalAjustada = parseDec(totalsAfterConsult.meta_ajustada);
        const linhasNegativas = [];
        if (totalPta !== null && totalPta < 0) {
          linhasNegativas.push(`META PTA/LOA está negativo (${fmtNum(totalPta)}).`);
        }
        if (totalAjustada !== null && totalAjustada < 0) {
          linhasNegativas.push(`META AJUSTADA está negativo (${fmtNum(totalAjustada)}).`);
        }
        if (linhasNegativas.length) {
          const produtoSelecionado = selects.produto_acao?.selectedOptions?.[0]?.textContent
            || selects.produto_acao?.value
            || "";
          await openMetaFisicaNegativeTotalsModal({
            produtoAcao: produtoSelecionado,
            linhas: linhasNegativas,
          });
        }
      });
    }

    Object.values(selects).forEach((el) => {
      if (!el || el === selects.exercicio || el === selects.adj_solicitante) return;
      el.addEventListener("change", () => {
        clearMsgOnUserAction();
        resetEditMode();
        hasConsulted = false;
        lastQueryHadRows = false;
        const usedCatalog = refreshCascadeOptionsFromCatalog();
        if (!usedCatalog) {
          loadOptions(false, true);
        } else {
          renderRows();
        }
      });
    });
    if (selects.adj_solicitante) {
      selects.adj_solicitante.addEventListener("change", () => {
        clearMsgOnUserAction();
        resetEditMode();
      });
    }

    getSummaryRows().forEach((row) => {
      row.addEventListener("click", () => {
        selectSummaryRow(row);
      });
    });

    renderCriteria();
    setResultsVisible(false);
    updateSummaryActionButtons();

    if (filterAdd) {
      filterAdd.addEventListener("click", () => {
        const field = String(filterField?.value || "");
        const op = String(filterOp?.value || "eq");
        const value = String(filterValue?.value || "").trim();
        if (!field) {
          setFilterMsg("Selecione um campo.", true);
          return;
        }
        if (!value) {
          setFilterMsg("Informe um valor.", true);
          return;
        }
        if (field !== "exercicio" && !criteria.some((c) => c.field === "exercicio")) {
          setFilterMsg("Informe o critério de Exercício antes dos demais.", true);
          return;
        }
        criteria.push({ field, op, value });
        criteriaSelected = criteria.length - 1;
        renderCriteria();
        setFilterMsg("");
        if (filterValue) filterValue.value = "";
      });
    }

    if (filterRemove) {
      filterRemove.addEventListener("click", () => {
        if (criteriaSelected < 0 || criteriaSelected >= criteria.length) {
          setFilterMsg("Selecione um critério para remover.", true);
          return;
        }
        criteria.splice(criteriaSelected, 1);
        criteriaSelected = -1;
        renderCriteria();
        setResultsVisible(false);
        setFilterMsg("");
      });
    }

    if (filterClear) {
      filterClear.addEventListener("click", () => {
        criteria.length = 0;
        criteriaSelected = -1;
        renderCriteria();
        setResultsVisible(false);
        setFilterMsg("");
      });
    }

    if (filterCancel) {
      filterCancel.addEventListener("click", () => {
        criteria.length = 0;
        criteriaSelected = -1;
        renderCriteria();
        setResultsVisible(false);
        getSummaryRows().forEach((row) => {
          row.style.display = "";
          row.classList.remove("selected");
        });
        updateSummaryActionButtons();
        if (filterField) filterField.value = "";
        if (filterOp) filterOp.value = "eq";
        if (filterValue) filterValue.value = "";
        setFilterMsg("");
      });
    }

    if (filterApply) {
      filterApply.addEventListener("click", () => {
        if (!criteria.some((c) => c.field === "exercicio")) {
          setFilterMsg("Informe o critério de Exercício antes de consultar.", true);
          return;
        }
        setResultsVisible(true);
        applyCriteriaToResults(true);
        if (getFilteredSummaryRows().length) {
          setFilterMsg("");
        }
        updateSummaryActionButtons();
      });
    }

    if (approveBtn) {
      approveBtn.style.display = canApprove ? "" : "none";
      approveBtn.addEventListener("click", async () => {
        const selected = summaryBody?.querySelector(".meta-fisica-summary-row.selected");
        if (!selected) {
          setFilterMsg("Selecione um registro para aprovar.", true);
          return;
        }
        if (!canApprove) {
          setFilterMsg("Usuário sem permissão para aprovar o registro atual.", true);
          return;
        }
        const controle = String(selected.dataset.controleMeta || "").trim() || "(sem controle)";
        const status = String(selected.dataset.statusAprovacao || "").trim().toLowerCase();
        if (status !== "aguardando") {
          setFilterMsg(`Somente registros com status Aguardando podem ser aprovados (${controle}).`, true);
          return;
        }
        if (metaIsConsultaView) {
          sessionStorage.setItem(
            metaPendingActionKey,
            JSON.stringify({
              action: "approve",
              id: selected.dataset.id || "",
              dataset: metaRowSnapshot(selected),
            })
          );
          await loadPage("cadastrar/plan_21-nger/meta_fisica/formulario");
          return;
        }

        applyMetaFiltersFromSummaryRow(selected);
        if (justificativaInput) {
          setJustificativaProtectedValue(controle, selected.dataset.justificativa || "");
        }
        await loadOptions(false);
        applyMetaFiltersFromSummaryRow(selected);
        let baselineByRegion = {};
        try {
          baselineByRegion = await fetchPlanBaselineByRegion();
        } catch (err) {
          console.error(err);
        }
        const linhasFromApi = await fetchMetaLinhasById(selected.dataset.id || "");
        const linhas = linhasFromApi.length ? linhasFromApi : parseSummaryLinhas(selected);
        tableRows = buildEditableRowsFromSummary(linhas, baselineByRegion);
        hasConsulted = true;
        lastQueryHadRows = tableRows.length > 0;
        setApprovalMode(selected.dataset.id || "", controle);
        renderRows();
        setMsg("");
        setFilterMsg("");
      });
    }

    if (editBtn) {
      editBtn.addEventListener("click", async () => {
        const selected = summaryBody?.querySelector(".meta-fisica-summary-row.selected");
        if (!selected) {
          setFilterMsg("Selecione um registro para editar.", true);
          return;
        }
        const controle = String(selected.dataset.controleMeta || "").trim() || "(sem controle)";
        const status = String(selected.dataset.statusAprovacao || "").trim().toLowerCase();
        if (status !== "aguardando") {
          setFilterMsg(`Somente registros com status Aguardando podem ser editados (${controle}).`, true);
          return;
        }
        const criadorPerfilId = String(selected.dataset.criadorPerfilId || "").trim();
        if (!criadorPerfilId || !currentUserPerfilId || currentUserPerfilId !== criadorPerfilId) {
          setFilterMsg(`Usuário sem permissão para editar registro ${controle}.`, true);
          return;
        }
        if (metaIsConsultaView) {
          sessionStorage.setItem(
            metaPendingActionKey,
            JSON.stringify({
              action: "edit",
              id: selected.dataset.id || "",
              dataset: metaRowSnapshot(selected),
            })
          );
          await loadPage("cadastrar/plan_21-nger/meta_fisica/formulario");
          return;
        }

        applyMetaFiltersFromSummaryRow(selected);
        if (justificativaInput) {
          setJustificativaProtectedValue(controle, selected.dataset.justificativa || "");
        }
        await loadOptions(false);
        applyMetaFiltersFromSummaryRow(selected);
        let baselineByRegion = {};
        try {
          baselineByRegion = await fetchPlanBaselineByRegion();
        } catch (err) {
          console.error(err);
        }
        const linhasFromApi = await fetchMetaLinhasById(selected.dataset.id || "");
        const linhas = linhasFromApi.length ? linhasFromApi : parseSummaryLinhas(selected);
        tableRows = buildEditableRowsFromSummary(linhas, baselineByRegion);
        hasConsulted = true;
        lastQueryHadRows = tableRows.length > 0;
        setEditMode(selected.dataset.id || "", controle);
        renderRows();
        setMsg("");
        setFilterMsg("");
      });
    }

    if (deleteBtn) {
      deleteBtn.addEventListener("click", async () => {
        const selected = summaryBody?.querySelector(".meta-fisica-summary-row.selected");
        if (!selected) {
          setFilterMsg("Selecione um registro para excluir.", true);
          return;
        }
        const controle = String(selected.dataset.controleMeta || "").trim() || "(sem controle)";
        const status = String(selected.dataset.statusAprovacao || "").trim().toLowerCase();
        if (status !== "aguardando") {
          setFilterMsg(`Somente registros com status Aguardando podem ser excluídos (${controle}).`, true);
          return;
        }
        const criadorPerfilId = String(selected.dataset.criadorPerfilId || "").trim();
        if (!currentUserPerfilId || currentUserPerfilId !== criadorPerfilId) {
          setFilterMsg(`Usuário sem permissão para excluir registro ${controle}.`, true);
          return;
        }
        const metaId = String(selected.dataset.id || "").trim();
        if (!metaId) {
          setFilterMsg(`Registro inválido para exclusão (${controle}).`, true);
          return;
        }
        try {
          const res = await fetch(`/api/meta-fisica/${encodeURIComponent(metaId)}`, {
            method: "DELETE",
            headers: { "X-Requested-With": "fetch" },
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || `Falha ao excluir registro ${controle}.`);
          showToast(data.message || `Registro ${controle} excluído com sucesso.`, "success");
          await loadPage("cadastrar/plan_21-nger/meta_fisica/consultar");
        } catch (err) {
          setFilterMsg(err.message || `Falha ao excluir registro ${controle}.`, true);
        }
      });
    }

    if (printBtn) {
      printBtn.addEventListener("click", async () => {
        const selected = summaryBody?.querySelector(".meta-fisica-summary-row.selected");
        if (!selected) {
          setFilterMsg("Selecione um registro para imprimir.", true);
          return;
        }
        try {
          const selectedId = String(selected?.dataset?.id || "").trim();
          const linhasFromApi = await fetchMetaLinhasById(selectedId);
          const linhasFallback = parseSummaryLinhas(selected);
          if (!linhasFromApi.length && !linhasFallback.length) {
            throw new Error("Não foi possível carregar as linhas estruturadas da impressão.");
          }
          const useCurrentFormState =
            !!selectedId &&
            !!editingMetaId &&
            selectedId === String(editingMetaId).trim() &&
            Array.isArray(tableRows) &&
            tableRows.length > 0;
          const meta = useCurrentFormState
            ? buildCurrentFormMetaForPrint(selected)
            : (() => {
              const base = buildSummaryMetaForPrint(selected);
              if (linhasFromApi.length) base.linhas = linhasFromApi;
              else if (linhasFallback.length) base.linhas = linhasFallback;
              return base;
            })();
          openMetaFisicaPrintPopup(meta);
        } catch (err) {
          console.error(err);
          const msg = String(err?.message || "Falha ao gerar impressão para o registro selecionado.");
          setFilterMsg(msg, true);
        }
      });
    }

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      if (approvalMode) {
        const registroId = String(approvingMetaId || "").trim();
        const controle = String(approvingControle || "").trim() || "(sem controle)";
        if (!registroId) {
          setMsg("Registro inválido para aprovação.", true);
          return;
        }
        const aprovadoRadio = approvalRadios.find((r) => r.checked);
        const aprovado = String(aprovadoRadio?.value || "").trim().toLowerCase();
        if (!["sim", "nao"].includes(aprovado)) {
          setMsg("Selecione Sim ou Não para concluir a aprovação.", true);
          return;
        }
        const justificativaAprovacao = String(approvalJustificativa?.value || "").trim();
        if (!justificativaAprovacao) {
          setMsg("Informe a justificativa da decisão.", true);
          return;
        }
        try {
          const pendingPrintWin = prepareMetaFisicaPrintWindow();
          setMsg("Processando aprovação...");
          const res = await fetch(`/api/meta-fisica/${encodeURIComponent(registroId)}/aprovar`, {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify({
              meta_aprovada: aprovado,
              motivo_rejeicao: justificativaAprovacao,
            }),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || `Falha ao aprovar registro ${controle}.`);
          const selected = summaryBody?.querySelector(".meta-fisica-summary-row.selected");
          const metaPrint = selected ? buildSummaryMetaForPrint(selected) : {};
          metaPrint.controle = controle;
          metaPrint.status_aprovacao = aprovado === "sim" ? "Aprovado" : "Rejeitado";
          metaPrint.motivo_rejeicao = aprovado === "nao" ? justificativaAprovacao : "";
          metaPrint.data_aprovacao = data?.data_aprovacao || "";
          metaPrint.aprovado_por_nome = String(metaPage?.dataset?.userNome || "").trim();
          metaPrint.aprovado_por_perfil = String(metaPage?.dataset?.userPerfil || "").trim();
          openMetaFisicaPrintPopup(metaPrint, pendingPrintWin || null);
          showToast(data.message || `Registro ${controle} processado com sucesso.`, "success");
          resetEditMode();
          await loadPage("cadastrar/plan_21-nger/meta_fisica/consultar");
        } catch (err) {
          console.error(err);
          setMsg(err.message || `Falha ao aprovar registro ${controle}.`, true);
        }
        return;
      }
      if (!form.checkValidity()) {
        form.reportValidity();
        return;
      }
      const dup = validateDuplicateRegions();
      if (dup) {
        setMsg(`A região ${dup} já existe na tabela.`, true);
        return;
      }
      const validationErrors = [];
      const addValidationError = (message) => {
        const txt = String(message || "").trim();
        if (!txt) return;
        if (!validationErrors.includes(txt)) validationErrors.push(txt);
      };
      const rowsPayload = [];
      for (const row of tableRows) {
        const regiao = String(row.regiao_produto || "").trim();
        if (!regiao) {
          addValidationError("Selecione a região em todas as novas linhas adicionadas.");
          continue;
        }
        if (!row.is_novo && parseDec(row.meta_produto) === null) {
          addValidationError(`Meta PTA/LOA inválida para a região ${regiao}.`);
        }
        const ajustadaRegiao = parseDec(rowAdjusted(row));
        if (ajustadaRegiao !== null && ajustadaRegiao < 0) {
          addValidationError(
            `A Meta Ajustada da região ${regiao} não pode ser negativa (${fmtNum(ajustadaRegiao)}).`
          );
        }
        const creditoItemsAll = getFieldItems(row, "meta_credito");
        const anuladaItemsAll = getFieldItems(row, "meta_anulada");
        const creditoLockCount = getLockedItemCount(row, "meta_credito");
        const anuladaLockCount = getLockedItemCount(row, "meta_anulada");
        const creditoHistorico =
          row.lock_meta_credito && creditoLockCount > 0
            ? creditoItemsAll
              .slice(0, creditoLockCount)
              .reduce((acc, v) => {
                const n = parseDec(v);
                return n !== null && n > 0 ? acc + n : acc;
              }, 0)
            : "";
        const anuladaHistorico =
          row.lock_meta_anulada && anuladaLockCount > 0
            ? anuladaItemsAll
              .slice(0, anuladaLockCount)
              .reduce((acc, v) => {
                const n = parseDec(v);
                return n !== null && n > 0 ? acc + n : acc;
              }, 0)
            : "";
        const toAlignedToken = (v) => {
          const n = parseDec(v);
          return n !== null && n > 0 ? String(v ?? "").trim() : "";
        };
        const creditoMovItems = creditoItemsAll
          .map((v, idx) => (!(row.lock_meta_credito && idx < creditoLockCount) ? toAlignedToken(v) : null))
          .filter((v) => v !== null);
        const anuladaMovItems = anuladaItemsAll
          .map((v, idx) => (!(row.lock_meta_anulada && idx < anuladaLockCount) ? toAlignedToken(v) : null))
          .filter((v) => v !== null);
        const creditoHistoricoItems = creditoItemsAll
          .map((v, idx) => (row.lock_meta_credito && idx < creditoLockCount ? toAlignedToken(v) : null))
          .filter((v) => v !== null);
        const anuladaHistoricoItems = anuladaItemsAll
          .map((v, idx) => (row.lock_meta_anulada && idx < anuladaLockCount ? toAlignedToken(v) : null))
          .filter((v) => v !== null);
        rowsPayload.push({
          regiao_produto: regiao,
          meta_produto: row.meta_produto,
          meta_credito: sumFieldItems(row, "meta_credito") || "",
          meta_anulada: row.is_novo ? "" : sumFieldItems(row, "meta_anulada") || "",
          meta_credito_items: creditoItemsAll.filter((v) => {
            const n = parseDec(v);
            return n !== null && n > 0;
          }),
          meta_credito_mov_items: creditoMovItems,
          meta_credito_historico_items: creditoHistoricoItems,
          meta_credito_historico: creditoHistorico ? String(creditoHistorico).replace(".", ",") : "",
          meta_anulada_items: row.is_novo
            ? []
            : anuladaItemsAll.filter((v) => {
              const n = parseDec(v);
              return n !== null && n > 0;
            }),
          meta_anulada_mov_items: row.is_novo ? [] : anuladaMovItems,
          meta_anulada_historico_items: row.is_novo ? [] : anuladaHistoricoItems,
          meta_anulada_historico: row.is_novo ? "" : (anuladaHistorico ? String(anuladaHistorico).replace(".", ",") : ""),
          has_historico_movimento: !!(creditoHistorico || anuladaHistorico),
          is_novo: !!row.is_novo,
          plan21_nger_id: row.plan21_nger_id || null,
          plan21_ids: Array.isArray(row.plan21_ids) ? row.plan21_ids : [],
        });
      }
      if (!rowsPayload.length) {
        if (!validationErrors.length) {
          addValidationError("Adicione ao menos uma linha de meta física.");
        }
      }
      for (const row of tableRows) {
        const regiaoAtual = String(row?.regiao_produto || "").trim();
        if (!regiaoAtual) continue;
        const creditoItems = getFieldItems(row, "meta_credito");
        const creditoLockCount = getLockedItemCount(row, "meta_credito");
        const creditoUnlockedStart = Math.max(0, creditoLockCount);
        const creditoUnlocked = creditoItems.slice(creditoUnlockedStart);
        const creditoUnlockedHasValue = creditoUnlocked.some((v) => {
          const n = parseDec(v);
          return n !== null && n > 0;
        });
        const requireUnlockedCredito = row.is_novo ||
          (row.active_movement_field === "meta_credito" && !creditoUnlockedHasValue);
        for (let i = creditoUnlockedStart; i < creditoItems.length; i += 1) {
          if (row.lock_meta_credito && i < creditoLockCount) continue;
          const item = creditoItems[i];
          const creditoNum = parseDec(item);
          if (
            requireUnlockedCredito &&
            (creditoNum === null || creditoNum <= 0)
          ) {
            addValidationError(`Preencha todos os lançamentos de Acréscimo adicionados na região ${regiaoAtual} antes de salvar.`);
            break;
          }
          if (creditoNum === null) continue;
          const errCredito = validateValorByUnidade(item, "Acréscimo");
          if (errCredito) {
            addValidationError(`${errCredito} Região: ${regiaoAtual}.`);
            break;
          }
        }
        const anuladaItems = getFieldItems(row, "meta_anulada");
        const anuladaLockCount = getLockedItemCount(row, "meta_anulada");
        const anuladaUnlockedStart = Math.max(0, anuladaLockCount);
        const anuladaUnlocked = anuladaItems.slice(anuladaUnlockedStart);
        const anuladaUnlockedHasValue = anuladaUnlocked.some((v) => {
          const n = parseDec(v);
          return n !== null && n > 0;
        });
        const requireUnlockedAnulada = !row.is_novo
          ? row.active_movement_field === "meta_anulada" && !anuladaUnlockedHasValue
          : anuladaUnlocked.length > 1
          || anuladaItems.some((v) => {
            const n = parseDec(v);
            return n !== null && n > 0;
          });
        for (let i = anuladaUnlockedStart; i < anuladaItems.length; i += 1) {
          if (row.lock_meta_anulada && i < anuladaLockCount) continue;
          const item = anuladaItems[i];
          const anuladaNum = parseDec(item);
          if (
            requireUnlockedAnulada &&
            (anuladaNum === null || anuladaNum <= 0)
          ) {
            addValidationError(`Preencha todos os lançamentos de Redução adicionados na região ${regiaoAtual} antes de salvar.`);
            break;
          }
          if (anuladaNum === null) continue;
          const errAnulada = validateValorByUnidade(item, "Redução");
          if (errAnulada) {
            addValidationError(`${errAnulada} Região: ${regiaoAtual}.`);
            break;
          }
        }
      }

      const justificativaText = getJustificativaEditableText();
      if (!justificativaText) {
        addValidationError("Informe a justificativa para salvar a Meta Física.");
      }

      const totalsForValidation = getTableTotals();
      const totalMetaAjustadaValid = parseDec(totalsForValidation.meta_ajustada);
      if (totalMetaAjustadaValid !== null && totalMetaAjustadaValid < 0) {
        addValidationError(
          `O Total da Meta Ajustada não pode ser negativo (${fmtNum(totalMetaAjustadaValid)}).`
        );
      }

      if (validationErrors.length) {
        if (justificativaInput && !justificativaText) {
          justificativaInput.value = "";
          justificativaInput.focus();
          if (typeof justificativaInput.reportValidity === "function") {
            justificativaInput.reportValidity();
          }
        }
        setMsg(validationErrors.join("\n"), true);
        return;
      }

      const payload = {
        meta_id: editingMetaId || "",
        exercicio: selects.exercicio?.value || "",
        unidade_orcamentaria: selects.unidade_orcamentaria?.value || "",
        programa: selects.programa?.value || "",
        acao_paoe: selects.acao_paoe?.value || "",
        adj_solicitante: selects.adj_solicitante?.value || "",
        produto_acao: selects.produto_acao?.value || "",
        unid_medida_produto: selects.unid_medida_produto?.value || "",
        justificativa: justificativaText,
        rows: rowsPayload,
      };

      const totalsBeforeSave = getTableTotals();
      const totalMetaPta = Number(totalsBeforeSave.meta_pta || 0);
      const totalMetaAjustada = Number(totalsBeforeSave.meta_ajustada || 0);
      const totalsAreEqual = Math.abs(totalMetaAjustada - totalMetaPta) < 0.000001;
      const confirmationMessage = totalsAreEqual
        ? "A META AJUSTADA está igual à META PTA/LOA.\nTodas as alterações nas metas físicas das regiões  (acréscimos/reduções) estão devidamente justificadas?\nDeseja salvar as alterações?"
        : "A META AJUSTADA está diferente da META PTA/LOA.\nA divergência entre as metas físicas das regiões (acréscimos/reduções) devem ser devidamente justificadas.\nDeseja salvar as alterações?";
      const saveConfirmed = await openMetaFisicaSaveConfirmModal({
        totalMetaPta,
        totalMetaAjustada,
        message: confirmationMessage,
      });
      if (!saveConfirmed) {
        setMsg("Salvamento cancelado.");
        return;
      }

      try {
        const pendingPrintWin = prepareMetaFisicaPrintWindow();
        setMsg("Salvando...");
        const res = await fetch("/api/meta-fisica", {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao salvar.");
        const acao = editingMetaId ? "atualizada" : "salva";
        showToast(`Meta física ${acao} com sucesso. Registros: ${data.count || 0}.`, "success");
        if (data.meta_fisica) {
          openMetaFisicaPrintPopup(data.meta_fisica, pendingPrintWin || null);
        }
        resetEditMode();
        await loadPage("cadastrar/plan_21-nger/meta_fisica/consultar");
      } catch (err) {
        console.error(err);
        setMsg(err.message || "Falha ao salvar.", true);
      }
    });

    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        summaryPageSize = parseInt(pageSizeSelect.value || "5", 10) || 5;
        if (
          summaryBox &&
          !summaryBox.classList.contains("dotacao-summary-hidden") &&
          !summaryBox.classList.contains("consulta-summary-hidden")
        ) {
          applyCriteriaToResults(false);
        }
      });
    }
    window.addEventListener("resize", () => {
      if (
        summaryBox &&
        !summaryBox.classList.contains("dotacao-summary-hidden") &&
        !summaryBox.classList.contains("consulta-summary-hidden")
      ) {
        renderSummaryPage();
      }
    });

    resetEditMode();
    loadOptions(false, true);
    if (!metaIsConsultaView) {
      const pendingRaw = sessionStorage.getItem(metaPendingActionKey);
      if (pendingRaw) {
        sessionStorage.removeItem(metaPendingActionKey);
        try {
          const pending = JSON.parse(pendingRaw);
          const row = ensureMetaPendingRow(pending);
          if (row) {
            summaryBody?.querySelectorAll(".meta-fisica-summary-row.selected").forEach((r) => {
              r.classList.remove("selected");
            });
            row.classList.add("selected");
            if (pending.action === "approve") {
              if (approveBtn) approveBtn.disabled = false;
              approveBtn?.click();
            } else {
              if (editBtn) editBtn.disabled = false;
              editBtn?.click();
            }
          } else {
            setMsg("Registro selecionado na consulta não foi encontrado.", true);
          }
        } catch (err) {
          console.error(err);
        }
      }
    }
  }

  function initTetoOrcamentarioDashboard() {
    const root = document.getElementById("teto-dashboard");
    if (!root || root.dataset.bound === "1") return;
    root.dataset.bound = "1";
    const dashboardResizeObserver =
      typeof ResizeObserver !== "undefined"
        ? new ResizeObserver(() => resizeTetoDashboardCharts())
        : null;
    dashboardResizeObserver?.observe(root);

    const state = { momp: [], politicas: [], filters: {} };
    let chartFilterTimer = null;
    const filterEls = Array.from(root.querySelectorAll("[data-filter]"));
    const filterEmptyLabels = Object.fromEntries(
      filterEls.map((el) => [el.dataset.filter, el.options[0]?.textContent || "Todos"])
    );
    const statusEl = document.getElementById("teto-dashboard-status");
    const activeFiltersEl = document.getElementById("teto-active-filters");
    const money = new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" });
    const number = new Intl.NumberFormat("pt-BR");
    const oldChartPalette = [
      "#345feb", "#048075", "#f8cb2e", "#f58220", "#ea1d2c", "#6a1b9a",
      "#008FFB", "#9C27B0", "#FF4560", "#00E396", "#FEB019", "#775DD0",
    ];
    const groupColors = {
      "1": "#345feb",
      "3": "#048075",
      "4": "#f8cb2e",
    };
    const adjColors = {
      SAGP: "#ea1d2c",
      SAAS: "#048075",
      SAGE: "#f58220",
      SAIP: "#008FFB",
      SAGR: "#6a1b9a",
      GAB: "#f8cb2e",
      SARC: "#9C27B0",
      SAEX: "#345feb",
    };
    const politicalKeys = ["regiao", "subfuncao", "paoe", "adj", "macropolitica", "pilar", "eixo", "politica"];

    const clean = (value) => String(value ?? "").trim();
    const codeOf = (value) => {
      const text = clean(value);
      return text.includes(" - ") ? text.split(" - ")[0].trim() : text;
    };
    const subfunctionOf = (value) => codeOf(value).split(".")[0].trim();
    const unique = (values) =>
      Array.from(new Set(values.map(clean).filter(Boolean))).sort((a, b) =>
        a.localeCompare(b, "pt-BR", { numeric: true, sensitivity: "base" })
      );
    const sum = (rows) => rows.reduce((total, row) => total + Number(row.valor || 0), 0);
    const groupSum = (rows, key, valueKey = "valor") => {
      const grouped = new Map();
      rows.forEach((row) => {
        const label = clean(row[key]) || "Não informado";
        grouped.set(label, (grouped.get(label) || 0) + Number(row[valueKey] || 0));
      });
      return Array.from(grouped, ([label, valor]) => ({ label, valor }))
        .sort((a, b) => b.valor - a.valor);
    };
    const escapeHtml = (value) =>
      clean(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    const percent = (value, total) => (total ? `${((value / total) * 100).toFixed(2).replace(".", ",")}%` : "-");
    const plotConfig = { responsive: true, displaylogo: false, modeBarButtonsToRemove: ["lasso2d"] };
    const plotLayout = (extra = {}) => ({
      margin: { t: 20, r: 24, b: 70, l: 70 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: { family: "Arial, sans-serif", size: 11, color: getComputedStyle(document.body).getPropertyValue("--text").trim() || "#24323d" },
      separators: ",.",
      ...extra,
    });

    const setStatus = (message, isError = false) => {
      if (!statusEl) return;
      statusEl.textContent = message || "";
      statusEl.classList.toggle("text-error", isError);
    };

    const readFilters = () => {
      filterEls.forEach((el) => {
        state.filters[el.dataset.filter] = clean(el.value);
      });
      return state.filters;
    };

    const isPoliticalMode = (filters = state.filters) =>
      politicalKeys.some((key) => Boolean(filters[key]));

    const filteredData = (filtersInput = null) => {
      const filters = filtersInput || readFilters();
      let momp = state.momp.filter((row) =>
        (!filters.exercicio || row.exercicio === filters.exercicio) &&
        (!filters.fonte || row.fonte === filters.fonte) &&
        (!filters.grupo || row.grupo === filters.grupo) &&
        (!filters.subgrupo || row.subgrupo === filters.subgrupo)
      );
      const validIds = new Set(momp.map((row) => row.id));
      let politicas = state.politicas.filter((row) => validIds.has(row.momp_id));
      politicas = politicas.filter((row) =>
        (!filters.regiao || row.regiao === filters.regiao) &&
        (!filters.subfuncao || subfunctionOf(row.subfuncao) === filters.subfuncao) &&
        (!filters.paoe || row.paoe === filters.paoe) &&
        (!filters.adj || row.adj === filters.adj) &&
        (!filters.macropolitica || row.macropolitica === filters.macropolitica) &&
        (!filters.pilar || row.pilar === filters.pilar) &&
        (!filters.eixo || row.eixo === filters.eixo) &&
        (!filters.politica || row.politica === filters.politica)
      );

      if (isPoliticalMode(filters)) {
        const policyMompIds = new Set(politicas.map((row) => row.momp_id));
        momp = momp.filter((row) => policyMompIds.has(row.id));
      }
      const mompById = new Map(momp.map((row) => [row.id, row]));
      const joined = politicas
        .map((row) => ({ ...mompById.get(row.momp_id), ...row, valor: Number(row.valor || 0) }))
        .filter((row) => row.id && mompById.has(row.momp_id));
      return { momp, politicas, joined, policyMode: isPoliticalMode(filters) };
    };

    const optionValuesFor = (key, data) => {
      const sources = {
        exercicio: () => data.momp.map((row) => row.exercicio),
        fonte: () => data.momp.map((row) => row.fonte),
        grupo: () => data.momp.map((row) => row.grupo),
        subgrupo: () => data.momp.map((row) => row.subgrupo),
        regiao: () => data.politicas.map((row) => row.regiao),
        subfuncao: () => data.politicas.map((row) => subfunctionOf(row.subfuncao)),
        paoe: () => data.politicas.map((row) => row.paoe),
        adj: () => data.politicas.map((row) => row.adj),
        macropolitica: () => data.politicas.map((row) => row.macropolitica),
        pilar: () => data.politicas.map((row) => row.pilar),
        eixo: () => data.politicas.map((row) => row.eixo),
        politica: () => data.politicas.map((row) => row.politica),
      };
      return unique(sources[key]?.() || []);
    };

    const refreshFilterOptions = () => {
      // Duas passagens estabilizam as listas quando uma combinação deixa
      // alguma seleção anterior sem correspondência.
      for (let pass = 0; pass < 2; pass += 1) {
        filterEls.forEach((el) => {
          const key = el.dataset.filter;
          const current = clean(state.filters[key] ?? el.value);
          const candidateFilters = { ...state.filters, [key]: "" };
          const values = optionValuesFor(key, filteredData(candidateFilters));
          el.innerHTML = `<option value="">${escapeHtml(filterEmptyLabels[key] || "Todos")}</option>`;
          values.forEach((value) => {
            const option = document.createElement("option");
            option.value = value;
            option.textContent = value;
            el.appendChild(option);
          });
          if (current && values.includes(current)) {
            el.value = current;
            state.filters[key] = current;
          } else {
            el.value = "";
            state.filters[key] = "";
          }
        });
      }
    };

    const renderActiveFilters = () => {
      if (!activeFiltersEl) return;
      const labels = {
        exercicio: "Exercício", regiao: "Região", subfuncao: "Subfunção", grupo: "Grupo",
        subgrupo: "Tipificação", paoe: "PAOE", fonte: "Fonte", adj: "ADJ",
        macropolitica: "Macropolítica", pilar: "Pilar", eixo: "Eixo", politica: "Política",
      };
      activeFiltersEl.innerHTML = Object.entries(state.filters)
        .filter(([, value]) => value)
        .map(([key, value]) => `<span class="teto-filter-chip">${escapeHtml(labels[key])}: ${escapeHtml(value)}</span>`)
        .join("");
    };

    const emptyPlot = (id, message) => {
      const el = document.getElementById(id);
      if (!el || typeof Plotly === "undefined") return;
      Plotly.react(el, [], plotLayout({
        xaxis: { visible: false },
        yaxis: { visible: false },
        annotations: [{ text: message, x: 0.5, y: 0.5, xref: "paper", yref: "paper", showarrow: false }],
      }), plotConfig);
    };

    const bindPlotFilter = (id, key, valueResolver) => {
      const el = document.getElementById(id);
      if (!el || typeof el.on !== "function") return;
      if (typeof el.removeAllListeners === "function") el.removeAllListeners("plotly_click");
      el.on("plotly_click", (event) => {
        const point = event?.points?.[0];
        const value = clean(valueResolver(point));
        const select = root.querySelector(`[data-filter="${key}"]`);
        if (!value || !select) return;
        const option = Array.from(select.options).find((item) => item.value === value);
        if (!option) return;
        readFilters();
        const nextValue = state.filters[key] === value ? "" : value;
        select.value = nextValue;
        state.filters[key] = nextValue;
        window.clearTimeout(chartFilterTimer);
        chartFilterTimer = window.setTimeout(() => render(), 0);
      });
    };

    const renderKpis = ({ momp, politicas, joined, policyMode }) => {
      const base = policyMode ? joined : momp;
      const total = sum(base);
      const fontes = new Set(base.map((row) => clean(row.fonte)).filter(Boolean)).size;
      const grupos = new Set(base.map((row) => clean(row.grupo)).filter(Boolean)).size;
      const paoes = new Set(politicas.map((row) => clean(row.paoe)).filter(Boolean)).size;
      document.getElementById("teto-kpi-total").textContent = money.format(total);
      document.getElementById("teto-kpi-fontes").textContent = number.format(fontes);
      document.getElementById("teto-kpi-grupos").textContent = number.format(grupos);
      document.getElementById("teto-kpi-paoes").textContent = number.format(paoes);
    };

    const renderCharts = (data) => {
      if (typeof Plotly === "undefined") {
        setStatus("Não foi possível carregar a biblioteca de gráficos.", true);
        return;
      }
      const base = data.policyMode ? data.joined : data.momp;
      const byGroup = groupSum(base, "grupo");
      const groupEl = document.getElementById("teto-chart-grupo");
      if (!byGroup.length || !sum(byGroup)) {
        emptyPlot("teto-chart-grupo", "Sem dados para o grupo de despesa");
      } else {
        Plotly.react(groupEl, [{
          type: "pie",
          labels: byGroup.map((row) => row.label),
          values: byGroup.map((row) => row.valor),
          customdata: byGroup.map((row) => row.label),
          textinfo: "percent",
          textposition: "outside",
          marker: {
            colors: byGroup.map((row, index) =>
              groupColors[codeOf(row.label)] || oldChartPalette[index % oldChartPalette.length]
            ),
            line: { color: "#ffffff", width: 1 },
          },
          hovertemplate: "<b>%{label}</b><br>R$ %{value:,.2f}<br>%{percent}<extra></extra>",
        }], plotLayout({ margin: { t: 8, r: 20, b: 80, l: 20 }, legend: { orientation: "h", y: -0.18 } }), plotConfig);
        bindPlotFilter("teto-chart-grupo", "grupo", (point) => point?.customdata);
      }

      const byAdj = groupSum(data.politicas, "adj");
      if (!byAdj.length || !sum(byAdj)) {
        emptyPlot("teto-chart-adj", "Sem dados para ADJ");
      } else {
        const adjChart = document.getElementById("teto-chart-adj");
        Plotly.purge(adjChart);
        Plotly.newPlot(adjChart, [{
          type: "treemap",
          labels: byAdj.map((row) => row.label),
          parents: byAdj.map(() => ""),
          values: byAdj.map((row) => row.valor),
          customdata: byAdj.map((row) => row.label),
          marker: {
            colors: byAdj.map((row, index) =>
              adjColors[codeOf(row.label)] || oldChartPalette[index % oldChartPalette.length]
            ),
          },
          textinfo: "label",
          hovertemplate: "<b>%{label}</b><br>R$ %{value:,.2f}<extra></extra>",
        }], plotLayout({ margin: { t: 8, r: 8, b: 8, l: 8 } }), plotConfig);
        bindPlotFilter("teto-chart-adj", "adj", (point) => point?.customdata);
      }

      const byMacro = groupSum(data.politicas, "macropolitica");
      const macroTotal = sum(byMacro);
      if (!byMacro.length || !macroTotal) {
        emptyPlot("teto-chart-macro", "Sem dados para macropolíticas");
      } else {
        let running = 0;
        const accumulated = byMacro.map((row) => {
          running += row.valor;
          return running / macroTotal;
        });
        Plotly.react("teto-chart-macro", [
          {
            type: "bar", name: "Teto (R$)", x: byMacro.map((row) => row.label),
            y: byMacro.map((row) => row.valor), customdata: byMacro.map((row) => row.label),
            marker: { color: "#636EFA" }, hovertemplate: "<b>%{x}</b><br>R$ %{y:,.2f}<extra></extra>",
          },
          {
            type: "scatter", name: "Acumulado (%)", x: byMacro.map((row) => row.label),
            y: accumulated, mode: "lines+markers", yaxis: "y2", line: { color: "#EF553B" },
            hovertemplate: "<b>%{x}</b><br>%{y:.1%}<extra></extra>",
          },
        ], plotLayout({
          xaxis: { tickangle: -35, automargin: true },
          yaxis: { title: "Teto (R$)", rangemode: "tozero" },
          yaxis2: { title: "Acumulado", overlaying: "y", side: "right", range: [0, 1], tickformat: ".0%" },
          legend: { orientation: "h", y: -0.38 },
          shapes: [{ type: "line", xref: "paper", x0: 0, x1: 1, yref: "y2", y0: 0.8, y1: 0.8, line: { color: "#c23b3b", dash: "dash" } }],
        }), plotConfig);
        bindPlotFilter("teto-chart-macro", "macropolitica", (point) => point?.customdata || point?.x);
      }

      const byPaoe = groupSum(data.politicas, "paoe");
      if (!byPaoe.length || !sum(byPaoe)) {
        emptyPlot("teto-chart-paoe", "Sem dados para ação/PAOE");
      } else {
        const paoeCodes = byPaoe.map((row) => codeOf(row.label));
        const paoeValues = byPaoe.map((row) => row.valor);
        const paoeCategories = [...paoeCodes, "Total"];
        Plotly.react("teto-chart-paoe", [{
          type: "waterfall",
          x: paoeCategories,
          y: [...paoeValues, sum(byPaoe)],
          measure: [...byPaoe.map(() => "relative"), "total"],
          customdata: [
            ...byPaoe.map((row) => [row.label, money.format(row.valor)]),
            ["", money.format(sum(byPaoe))],
          ],
          textposition: "outside",
          increasing: { marker: { color: "#3D9970" } },
          decreasing: { marker: { color: "#FF4136" } },
          totals: { marker: { color: "#0074D9" } },
          connector: { line: { color: "#7b8790", width: 1 } },
          hovertemplate: "<b>%{x}</b><br>%{customdata[1]}<extra></extra>",
        }], plotLayout({
          margin: { t: 20, r: 24, b: 85, l: 70 },
          xaxis: {
            type: "category",
            categoryorder: "array",
            categoryarray: paoeCategories,
            tickangle: -45,
            automargin: true,
            showgrid: true,
          },
          yaxis: { title: "Teto (R$)", rangemode: "tozero", showgrid: true },
        }), plotConfig);
        bindPlotFilter("teto-chart-paoe", "paoe", (point) => point?.customdata?.[0]);
      }

      const years = unique(base.map((row) => row.exercicio));
      const groups = unique(base.map((row) => row.grupo));
      if (!years.length || !groups.length) {
        emptyPlot("teto-chart-exercicio", "Sem dados para comparação por exercício");
      } else {
        const traces = groups.map((group, index) => ({
          type: "bar",
          name: codeOf(group),
          x: years,
          y: years.map((year) => sum(base.filter((row) => row.exercicio === year && row.grupo === group))),
          customdata: years.map(() => group),
          marker: {
            color: groupColors[codeOf(group)] || oldChartPalette[index % oldChartPalette.length],
          },
          hovertemplate: `<b>Exercício %{x}</b><br>${escapeHtml(group)}<br>R$ %{y:,.2f}<extra></extra>`,
        }));
        traces.push({
          type: "scatter", name: "Total Geral", mode: "lines+markers", x: years,
          y: years.map((year) => sum(base.filter((row) => row.exercicio === year))),
          line: { color: "#B620E0", width: 2 },
          hovertemplate: "<b>Exercício %{x}</b><br>Total: R$ %{y:,.2f}<extra></extra>",
        });
        Plotly.react("teto-chart-exercicio", traces, plotLayout({
          barmode: "group",
          xaxis: {
            title: "Exercício",
            type: "category",
            categoryorder: "array",
            categoryarray: years,
            tickmode: "array",
            tickvals: years,
            ticktext: years,
          },
          yaxis: { title: "Teto (R$)", rangemode: "tozero" },
          legend: { orientation: "h", y: -0.25 },
        }), plotConfig);
        bindPlotFilter("teto-chart-exercicio", "grupo", (point) => point?.customdata);
      }
    };

    const renderTable = (tableId, rows) => {
      const tbody = document.querySelector(`#${tableId} tbody`);
      if (!tbody) return;
      tbody.innerHTML = rows.map((row) => {
        const rowClass = row.total ? "teto-row-total" : row.child ? "teto-row-child" : "";
        return `<tr class="${rowClass}">${row.cells.map((cell) => `<td>${escapeHtml(cell)}</td>`).join("")}</tr>`;
      }).join("");
    };

    const renderTables = (data) => {
      const base = data.policyMode ? data.joined : data.momp;
      const total = sum(base);
      const sourceRows = groupSum(base, "fonte").map((row) => ({
        cells: [codeOf(row.label), money.format(row.valor), percent(row.valor, total)],
      }));
      sourceRows.push({ total: true, cells: ["Total Geral", money.format(total), total ? "100,00%" : "-"] });
      renderTable("teto-table-fonte", sourceRows);

      const groupRows = [];
      groupSum(base, "grupo").forEach((group) => {
        groupRows.push({ cells: [group.label, money.format(group.valor), percent(group.valor, total)] });
        groupSum(base.filter((row) => clean(row.grupo) === group.label), "subgrupo").forEach((subgroup) => {
          groupRows.push({ child: true, cells: [`↳ ${subgroup.label}`, money.format(subgroup.valor), percent(subgroup.valor, total)] });
        });
      });
      groupRows.push({ total: true, cells: ["Total Geral", money.format(total), total ? "100,00%" : "-"] });
      renderTable("teto-table-grupo", groupRows);

      const years = unique(base.map((row) => row.exercicio)).slice(0, 3);
      const qompHead = document.querySelector("#teto-table-qomp thead");
      const qompBody = document.querySelector("#teto-table-qomp tbody");
      if (!qompHead || !qompBody) return;
      qompHead.innerHTML = `<tr><th>Fonte</th><th>Grupo de despesa / Tipificação</th>${years
        .map((year) => `<th class="teto-qomp-value-col">Teto anual (${escapeHtml(year)})</th><th>Perc. (%) (${escapeHtml(year)})</th>`).join("")}</tr>`;
      const totalsByYear = Object.fromEntries(years.map((year) => [year, sum(base.filter((row) => row.exercicio === year))]));
      const qompRows = [];
      unique(base.map((row) => row.fonte)).forEach((fonte) => {
        const sourceBase = base.filter((row) => row.fonte === fonte);
        unique(sourceBase.map((row) => row.grupo)).forEach((grupo) => {
          const groupBase = sourceBase.filter((row) => row.grupo === grupo);
          qompRows.push({
            source: true,
            cells: [codeOf(fonte), grupo, ...years.flatMap((year) => {
              const value = sum(groupBase.filter((row) => row.exercicio === year));
              return [money.format(value), percent(value, totalsByYear[year])];
            })],
          });
          unique(groupBase.map((row) => row.subgrupo)).forEach((subgrupo) => {
            const subgroupBase = groupBase.filter((row) => row.subgrupo === subgrupo);
            qompRows.push({
              child: true,
              cells: ["", `↳ ${subgrupo}`, ...years.flatMap((year) => {
                const value = sum(subgroupBase.filter((row) => row.exercicio === year));
                return [money.format(value), percent(value, totalsByYear[year])];
              })],
            });
          });
        });
      });
      qompRows.push({
        total: true,
        cells: ["", "Total Geral", ...years.flatMap((year) => [money.format(totalsByYear[year]), totalsByYear[year] ? "100,00%" : "-"])],
      });
      qompBody.innerHTML = qompRows.map((row) => {
        const rowClass = row.total
          ? "teto-row-total"
          : row.child
            ? "teto-row-child"
            : row.source
              ? "teto-row-source"
              : "";
        return `<tr class="${rowClass}">${row.cells.map((cell, index) =>
          `<td${index >= 2 && index % 2 === 0 ? ' class="teto-qomp-value-col"' : ""}>${escapeHtml(cell)}</td>`
        ).join("")}</tr>`;
      }).join("");
    };

    const render = () => {
      readFilters();
      refreshFilterOptions();
      const data = filteredData(state.filters);
      renderActiveFilters();
      renderKpis(data);
      renderCharts(data);
      renderTables(data);
      const valueRows = data.policyMode ? data.joined.length : data.momp.length;
      setStatus(`${number.format(valueRows)} registros considerados. Base monetária: ${data.policyMode ? "políticas orçamentárias" : "MOMP"}.`);
    };

    const load = async () => {
      setStatus("Carregando dados...");
      try {
        const response = await fetch("/api/paineis-dashboards/teto-orcamentario", {
          headers: { "X-Requested-With": "fetch" },
        });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.error || "Falha ao carregar o painel.");
        state.momp = Array.isArray(payload.momp) ? payload.momp : [];
        state.politicas = Array.isArray(payload.politicas) ? payload.politicas : [];
        render();
      } catch (error) {
        setStatus(error.message || "Falha ao carregar o painel.", true);
      }
    };

    filterEls.forEach((el) => el.addEventListener("change", render));
    document.getElementById("teto-dashboard-clear")?.addEventListener("click", () => {
      filterEls.forEach((el) => { el.value = ""; });
      render();
    });
    document.getElementById("teto-dashboard-refresh")?.addEventListener("click", load);
    root.querySelectorAll(".teto-view-btn").forEach((button) => {
      button.addEventListener("click", () => {
        const view = button.dataset.view;
        root.dataset.activeView = view;
        root.querySelectorAll(".teto-view-btn").forEach((item) => {
          const active = item === button;
          item.classList.toggle("active", active);
          item.setAttribute("aria-selected", String(active));
        });
        root.querySelectorAll("[data-view-panel]").forEach((panel) => {
          const active = panel.dataset.viewPanel === view;
          panel.hidden = !active;
          panel.classList.toggle("active", active);
        });
        if (view === "graficos" && typeof Plotly !== "undefined") {
          resizeTetoDashboardCharts();
        }
      });
    });

    load();
  }

  function initRoute(route) {
    if (route === "dashboard") {
      initDashboard();
    }
    if (route === "atualizar/personalizar-spo") {
      initPersonalizarSpo();
    }
    if (route === "atualizar/governanca-resultados/programacao-pta2027") {
      initPta2027Integration();
    }
    if (route === "usuarios" || route === "usuarios/cadastrar") {
      initUsuariosForm();
    }
    if (route === "usuarios/editar") {
      initUsuariosEditar();
    }
    if (route === "usuarios/perfil") {
      initPerfis();
    }
    if (route === "usuarios/senha") {
      initUsuariosSenha();
    }
    if (route === "usuarios/api-acessos") {
      initApiAcessos();
    }
    if (route === "painel") {
      initPainel();
    }
    if (route === "atualizar/fip613") {
      initFip613();
    }
    if (route === "atualizar/ped") {
      initPed();
    }
    if (route === "atualizar/emp") {
      initEmp();
    }
    if (route === "atualizar/est-emp") {
      initEstEmp();
    }
    if (route === "atualizar/nob") {
      initNob();
    }
    if (route === "atualizar/plan20-seduc") {
      initPlan20();
    }
    if (route === "atualizar/teto-seduc") {
      initTetoSeduc();
    }
    if (route === "atualizar/chave_planejamento_regra") {
      initChavePlanejamentoRegra();
    }
    if (route.startsWith("atualizar/estrutura-planejamento/")) {
      initEstruturaPlanejamento();
    }
    if (route === "atualizar/estrutura-planejamento/componentes") {
      initEstruturaComponentes();
    }
    if (route === "atualizar/estrutura-planejamento/modelos-chave") {
      initModelosChave();
    }
    if (route === "atualizar/estrutura-planejamento/catalogo-chave") {
      initCatalogoChave();
    }
    if (route === "atualizar/estrutura-planejamento/replicar-exercicio") {
      initReplicarExercicio();
    }
    if (route === "cadastrar/dotacao" || route.startsWith("cadastrar/dotacao/")) {
      initDotacao();
    }
    if (route === "cadastrar/est-dotacao" || route.startsWith("cadastrar/est-dotacao/")) {
      initEstDotacao();
    }
    if (route === "cadastrar/plan_21-nger/meta_fisica" || route.startsWith("cadastrar/plan_21-nger/meta_fisica/")) {
      initMetaFisicaPlan21();
    }
    if (route === "cadastrar/plan_21-nger/subacao" || route.startsWith("cadastrar/plan_21-nger/subacao/")) {
      initSubacaoPlan21();
    }
    if (route === "cadastrar/plan_21-nger/etapa" || route.startsWith("cadastrar/plan_21-nger/etapa/")) {
      initEtapaPlan21();
    }
    if (route === "relatorios/fip613") {
      initRelatorioFip();
    }
    if (route === "relatorios/emp") {
      initRelatorioEmp();
    }
    if (route === "relatorios/dotacao") {
      initRelatorioDotacao();
    }
    if (route === "relatorios/est-dotacao") {
      initRelatorioEstDotacao();
    }
    if (route === "relatorios/est-emp") {
      initRelatorioEstEmp();
    }
    if (route === "relatorios/nob") {
      initRelatorioNob();
    }
    if (route === "relatorios/ped") {
      initRelatorioPed();
    }
    if (route === "relatorios/plan20-seduc") {
      initRelatorioPlan20();
    }
    if (route === "relatorios/plan21-nger") {
      initRelatorioPlan21Nger();
    }
    if (route === "relatorios/estrutura-planejamento") {
      initRelatorioEstruturaPlanejamento();
    }
    if (route === "paineis-dashboards/teto-orcamentario") {
      initTetoOrcamentarioDashboard();
    }
  }

  function initRelatorioDotacao() {
    const table = document.getElementById("dotacao-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
    const emptyState = document.getElementById("dotacao-empty");
    const pager = document.getElementById("dotacao-pagination");
    const pageSizeSelect = document.getElementById("dotacao-page-size");
    const btnDownload = document.getElementById("dotacao-relatorio-download");
    const btnReset = document.getElementById("dotacao-relatorio-reset");
    if (!table || !tbody) return;
    if (table.dataset.bound === "1") return;
    table.dataset.bound = "1";

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;
    let filteredRows = [];

    const numFmt = new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const fmtNum = (v) => {
      const n = Number(v);
      if (Number.isNaN(n)) return v ?? "";
      return numFmt.format(n);
    };
    const fmtDateTime = (val) => {
      if (!val) return "";
      const d = new Date(val);
      if (Number.isNaN(d.getTime())) return val;
      return d.toLocaleString("pt-BR");
    };

    const colKeys = [
      "exercicio",
      "status_aprovacao",
      "adjunta_solicitante",
      "adj_concedente",
      "chave_dotacao",
      "chave_planejamento",
      "valor_dotacao",
      "valor_estorno",
      "valor_ped_emp",
      "valor_atual",
      "situacao",
      "uo",
      "programa",
      "acao_paoe",
      "produto",
      "ug",
      "regiao",
      "subacao_entrega",
      "etapa",
      "natureza_despesa",
      "elemento",
      "subelemento",
      "fonte",
      "iduso",
      "justificativa_historico",
      "usuario_nome_perfil",
      "criado_em",
      "alterado_em",
      "aprovado_por_nome_perfil",
      "data_aprovacao",
      "motivo_rejeicao",
    ];

    const filterContainers = table.querySelectorAll(".filter-row [data-col]");
    const allData = { rows: [] };
    const filters = Object.fromEntries(colKeys.map((k) => [k, new Set()]));
    const filterControls = {};

    const closeAllPanels = () => {
      Object.values(filterControls).forEach((ctrl) => {
        if (ctrl?.panel) ctrl.panel.classList.remove("open");
      });
    };

    const updateDisplay = (key) => {
      const set = filters[key] || new Set();
      const ctrl = filterControls[key];
      if (!ctrl) return;
      const map = ctrl.labelMap || {};
      if (ctrl.allCb) ctrl.allCb.checked = set.size === 0;
      (ctrl.optionCbs || []).forEach((cb) => {
        cb.checked = set.has(cb.dataset.val || "");
      });
      if (set.size === 0) {
        ctrl.label.textContent = "(Todos)";
      } else if (set.size <= 2) {
        ctrl.label.textContent = Array.from(set)
          .map((v) => map[v] || v)
          .join(", ");
      } else {
        ctrl.label.textContent = `${set.size} selecionados`;
      }
    };

    const buildFilter = (container, options, key) => {
      container.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "mf-wrapper";
      const display = document.createElement("button");
      display.type = "button";
      display.className = "mf-display";
      const label = document.createElement("span");
      label.textContent = "(Todos)";
      display.appendChild(label);
      const icon = document.createElement("i");
      icon.className = "bi bi-chevron-down";
      display.appendChild(icon);

      const panel = document.createElement("div");
      panel.className = "mf-panel";
      const search = document.createElement("input");
      search.type = "text";
      search.className = "mf-search";
      search.placeholder = "Buscar...";
      const list = document.createElement("div");
      list.className = "mf-options";

      const tempSelected = new Set(filters[key] || []);
      const allRow = document.createElement("label");
      allRow.className = "mf-option";
      const allCb = document.createElement("input");
      allCb.type = "checkbox";
      allCb.dataset.val = "";
      allRow.appendChild(allCb);
      const allSpan = document.createElement("span");
      allSpan.textContent = "(Todos)";
      allRow.appendChild(allSpan);
      list.appendChild(allRow);

      const selectVisibleRow = document.createElement("label");
      selectVisibleRow.className = "mf-option mf-select-visible";
      const selectVisibleCb = document.createElement("input");
      selectVisibleCb.type = "checkbox";
      selectVisibleRow.appendChild(selectVisibleCb);
      const selectVisibleSpan = document.createElement("span");
      selectVisibleSpan.textContent = "Selecionar exibidos";
      selectVisibleRow.appendChild(selectVisibleSpan);
      list.appendChild(selectVisibleRow);

      const cbs = [];
      const labelMap = {};
      options.forEach((opt) => {
        const row = document.createElement("label");
        row.className = "mf-option";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        const norm = String(opt || "").toLowerCase();
        cb.dataset.val = norm;
        labelMap[norm] = opt;
        row.appendChild(cb);
        const txt = document.createElement("span");
        txt.textContent = opt;
        row.appendChild(txt);
        list.appendChild(row);
        cbs.push({ cb, txt, row, val: norm });
      });

      const syncUIFromTemp = () => {
        allCb.checked = tempSelected.size === 0;
        cbs.forEach(({ cb, val }) => {
          cb.checked = tempSelected.has(val);
        });
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        const allVisibleSelected = visible.length > 0 && visible.every(({ cb }) => cb.checked);
        selectVisibleCb.checked = allVisibleSelected;
      };

      const applyTempToFilters = () => {
        const set = filters[key];
        set.clear();
        tempSelected.forEach((v) => set.add(v));
        updateDisplay(key);
        renderFiltered();
      };

      const closePanel = () => panel.classList.remove("open");

      allCb.addEventListener("change", () => {
        if (allCb.checked) {
          tempSelected.clear();
          syncUIFromTemp();
        }
      });

      selectVisibleCb.addEventListener("change", () => {
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        if (selectVisibleCb.checked) {
          visible.forEach(({ val }) => tempSelected.add(val));
        } else {
          visible.forEach(({ val }) => tempSelected.delete(val));
        }
        allCb.checked = tempSelected.size === 0;
        syncUIFromTemp();
      });

      cbs.forEach(({ cb, val }) => {
        cb.addEventListener("change", () => {
          if (cb.checked) {
            tempSelected.add(val);
            allCb.checked = false;
          } else {
            tempSelected.delete(val);
          }
          syncUIFromTemp();
        });
      });

      search.addEventListener("input", () => {
        const term = search.value.toLowerCase();
        cbs.forEach(({ row, txt }) => {
          const match = txt.textContent.toLowerCase().includes(term);
          row.style.display = match ? "" : "none";
        });
        const allMatch = "(todos)".includes(term) || term === "";
        allRow.style.display = allMatch ? "" : "none";
        selectVisibleRow.style.display = "";
        syncUIFromTemp();
      });

      const actions = document.createElement("div");
      actions.className = "mf-actions";
      const cancelBtn = document.createElement("button");
      cancelBtn.type = "button";
      cancelBtn.className = "mf-btn ghost";
      cancelBtn.textContent = "Cancelar";
      const applyBtn = document.createElement("button");
      applyBtn.type = "button";
      applyBtn.className = "mf-btn primary";
      applyBtn.textContent = "Aplicar";

      cancelBtn.addEventListener("click", () => {
        tempSelected.clear();
        filters[key].forEach((v) => tempSelected.add(v));
        syncUIFromTemp();
        closePanel();
      });
      applyBtn.addEventListener("click", () => {
        applyTempToFilters();
        closePanel();
      });

      display.addEventListener("click", () => {
        const isOpen = panel.classList.contains("open");
        closeAllPanels();
        if (!isOpen) {
          panel.style.width = "";
          panel.style.height = "";
          tempSelected.clear();
          filters[key].forEach((v) => tempSelected.add(v));
          cbs.forEach(({ row }) => (row.style.display = ""));
          allRow.style.display = "";
          search.value = "";
          syncUIFromTemp();
          panel.classList.add("open");
        }
      });

      wrap.appendChild(display);
      panel.appendChild(search);
      panel.appendChild(list);
      actions.appendChild(cancelBtn);
      actions.appendChild(applyBtn);
      panel.appendChild(actions);
      wrap.appendChild(panel);
      container.appendChild(wrap);

      filterControls[key] = {
        panel,
        label,
        allCb,
        optionCbs: cbs.map((c) => c.cb),
        labelMap,
      };
      updateDisplay(key);
    };

    const setOptions = (rows = allData.rows) => {
      closeAllPanels();
      const uniques = colKeys.map(() => new Set());
      (rows || []).forEach((r) => {
        colKeys.forEach((k, idx) => {
          const v = r[k];
          if (v !== undefined && v !== null && v !== "") uniques[idx].add(String(v));
        });
      });
      filterContainers.forEach((container) => {
        const key = container.getAttribute("data-col");
        const idx = colKeys.indexOf(key);
        if (idx === -1) return;
        const opts = Array.from(uniques[idx]).sort((a, b) => a.localeCompare(b, "pt-BR"));
        buildFilter(container, opts, key);
      });
    };

    const renderPagination = (totalPages) => {
      if (!pager) return;
      pager.innerHTML = "";
      const addBtn = (label, page, disabled, active = false) => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "page-btn";
        if (active) btn.classList.add("active");
        btn.disabled = disabled;
        btn.textContent = label;
        btn.addEventListener("click", () => {
          currentPage = page;
          render();
        });
        pager.appendChild(btn);
      };
      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);
      const maxBtns = 5;
      let startPage = Math.max(1, currentPage - Math.floor(maxBtns / 2));
      let endPage = Math.min(totalPages, startPage + maxBtns - 1);
      if (endPage - startPage + 1 < maxBtns) {
        startPage = Math.max(1, endPage - maxBtns + 1);
      }
      if (startPage > 1) {
        addBtn("1", 1, false, currentPage === 1);
        if (startPage > 2) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          pager.appendChild(ellipsis);
        }
      }
      for (let p = startPage; p <= endPage; p += 1) {
        addBtn(String(p), p, false, currentPage === p);
      }
      if (endPage < totalPages) {
        if (endPage < totalPages - 1) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          pager.appendChild(ellipsis);
        }
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }
      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };

    const render = () => {
      const rows = filteredRows;
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const pageRows = rows.slice(startIdx, startIdx + pageSize);

      tbody.innerHTML = "";
      pageRows.forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${r.exercicio ?? ""}</td>
          <td>${r.status_aprovacao ?? ""}</td>
          <td>${r.adjunta_solicitante ?? ""}</td>
          <td>${r.adj_concedente ?? ""}</td>
          <td>${r.chave_dotacao ?? ""}</td>
          <td>${r.chave_planejamento ?? ""}</td>
          <td class="num">${fmtNum(r.valor_dotacao)}</td>
          <td class="num">${fmtNum(r.valor_estorno)}</td>
          <td class="num">${fmtNum(r.valor_ped_emp)}</td>
          <td class="num">${fmtNum(r.valor_atual)}</td>
          <td>${r.situacao ?? ""}</td>
          <td>${r.uo ?? ""}</td>
          <td>${r.programa ?? ""}</td>
          <td>${r.acao_paoe ?? ""}</td>
          <td>${r.produto ?? ""}</td>
          <td>${r.ug ?? ""}</td>
          <td>${r.regiao ?? ""}</td>
          <td>${r.subacao_entrega ?? ""}</td>
          <td>${r.etapa ?? ""}</td>
          <td>${r.natureza_despesa ?? ""}</td>
          <td>${r.elemento ?? ""}</td>
          <td>${r.subelemento ?? ""}</td>
          <td>${r.fonte ?? ""}</td>
          <td>${r.iduso ?? ""}</td>
          <td>${r.justificativa_historico ?? ""}</td>
          <td>${r.usuario_nome_perfil ?? ""}</td>
          <td>${fmtDateTime(r.criado_em)}</td>
          <td>${fmtDateTime(r.alterado_em)}</td>
          <td>${r.aprovado_por_nome_perfil ?? ""}</td>
          <td>${fmtDateTime(r.data_aprovacao)}</td>
          <td>${r.motivo_rejeicao ?? ""}</td>
        `;
        tbody.appendChild(tr);
      });

      renderPagination(totalPages);
      toggleReportEmptyState({
        tableEl: table,
        emptyEl: emptyState,
        btnDownloadEl: btnDownload,
        pagerEl: pager,
        hasRows: rows.length > 0,
      });
    };

    const renderFiltered = (resetPage = true) => {
      const filtered = allData.rows.filter((r) =>
        colKeys.every((k) => {
          const set = filters[k];
          if (!set || set.size === 0) return true;
          const val = r[k];
          const cmp = val === null || val === undefined ? "" : String(val).toLowerCase();
          return set.has(cmp);
        })
      );
      setOptions(filtered);
      filteredRows = filtered;
      if (resetPage) currentPage = 1;
      render();
    };

    if (!multiFilterClickBound) {
      document.addEventListener("click", (ev) => {
        if (!ev.target.closest(".mf-wrapper")) {
          closeAllPanels();
        }
      });
      multiFilterClickBound = true;
    }

    const load = async () => {
      try {
        const res = await fetch("/api/relatorios/dotacao");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar.");
        allData.rows = data.data || [];
        setOptions(allData.rows);
        filteredRows = allData.rows;
        render();
      } catch (err) {
        console.error(err);
      }
    };

    load();

    if (btnReset) {
      btnReset.addEventListener("click", () => {
        Object.keys(filters).forEach((k) => filters[k].clear());
        setOptions(allData.rows);
        filteredRows = allData.rows;
        currentPage = 1;
        render();
      });
    }

    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        const val = parseInt(pageSizeSelect.value || "20", 10);
        pageSize = Number.isNaN(val) ? 20 : val;
        currentPage = 1;
        render();
      });
    }

    if (btnDownload) {
      btnDownload.addEventListener("click", () => {
        window.open("/api/relatorios/dotacao/download", "_blank");
      });
    }
  }

  function initRelatorioEstDotacao() {
    const table = document.getElementById("est-dotacao-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
    const tableWrap = table ? table.closest(".table-responsive") : null;
    const tableFoot = table ? table.closest(".card")?.querySelector(".table-foot") : null;
    const emptyState = document.getElementById("est-dotacao-empty");
    const pager = document.getElementById("est-dotacao-pagination");
    const pageSizeSelect = document.getElementById("est-dotacao-page-size");
    const btnDownload = document.getElementById("est-dotacao-relatorio-download");
    const btnReset = document.getElementById("est-dotacao-relatorio-reset");
    if (!table || !tbody) return;
    if (table.dataset.bound === "1") return;
    table.dataset.bound = "1";

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;
    let filteredRows = [];

    const numFmt = new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const fmtNum = (v) => {
      const n = Number(v);
      if (Number.isNaN(n)) return v ?? "";
      return numFmt.format(n);
    };
    const fmtDateTime = (val) => {
      if (!val) return "";
      const d = new Date(val);
      if (Number.isNaN(d.getTime())) return val;
      return d.toLocaleString("pt-BR");
    };

    const colKeys = [
      "exercicio",
      "status_aprovacao",
      "adjunta_solicitante",
      "chave_dotacao",
      "chave_planejamento",
      "valor_dotacao",
      "valor_a_ser_est",
      "saldo_dotacao_apos",
      "situacao",
      "uo",
      "programa",
      "acao_paoe",
      "produto",
      "ug",
      "regiao",
      "subacao_entrega",
      "etapa",
      "natureza_despesa",
      "elemento",
      "subelemento",
      "fonte",
      "iduso",
      "justificativa",
      "usuario_nome_perfil",
      "criado_em",
      "alterado_em",
      "aprovado_por_nome_perfil",
      "data_aprovacao",
      "motivo_rejeicao",
    ];

    const filterContainers = table.querySelectorAll(".filter-row [data-col]");
    const allData = { rows: [] };
    const filters = Object.fromEntries(colKeys.map((k) => [k, new Set()]));
    const filterControls = {};

    const updateDownloadState = () => {
      if (!btnDownload) return;
      btnDownload.disabled = filteredRows.length === 0;
    };

    const toggleEmptyState = (showEmpty) => {
      if (emptyState) emptyState.hidden = !showEmpty;
      if (tableWrap) tableWrap.style.display = showEmpty ? "none" : "";
      if (tableFoot) tableFoot.style.display = showEmpty ? "none" : "";
    };

    const closeAllPanels = () => {
      Object.values(filterControls).forEach((ctrl) => {
        if (ctrl?.panel) ctrl.panel.classList.remove("open");
      });
    };

    const updateDisplay = (key) => {
      const set = filters[key] || new Set();
      const ctrl = filterControls[key];
      if (!ctrl) return;
      const map = ctrl.labelMap || {};
      if (ctrl.allCb) ctrl.allCb.checked = set.size === 0;
      (ctrl.optionCbs || []).forEach((cb) => {
        cb.checked = set.has(cb.dataset.val || "");
      });
      if (set.size === 0) {
        ctrl.label.textContent = "(Todos)";
      } else if (set.size <= 2) {
        ctrl.label.textContent = Array.from(set)
          .map((v) => map[v] || v)
          .join(", ");
      } else {
        ctrl.label.textContent = `${set.size} selecionados`;
      }
    };

    const buildFilter = (container, options, key) => {
      container.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "mf-wrapper";
      const display = document.createElement("button");
      display.type = "button";
      display.className = "mf-display";
      const label = document.createElement("span");
      label.textContent = "(Todos)";
      display.appendChild(label);
      const icon = document.createElement("i");
      icon.className = "bi bi-chevron-down";
      display.appendChild(icon);

      const panel = document.createElement("div");
      panel.className = "mf-panel";
      const search = document.createElement("input");
      search.type = "text";
      search.className = "mf-search";
      search.placeholder = "Buscar...";
      const list = document.createElement("div");
      list.className = "mf-options";

      const tempSelected = new Set(filters[key] || []);
      const allRow = document.createElement("label");
      allRow.className = "mf-option";
      const allCb = document.createElement("input");
      allCb.type = "checkbox";
      allCb.dataset.val = "";
      allRow.appendChild(allCb);
      const allSpan = document.createElement("span");
      allSpan.textContent = "(Todos)";
      allRow.appendChild(allSpan);
      list.appendChild(allRow);

      const selectVisibleRow = document.createElement("label");
      selectVisibleRow.className = "mf-option mf-select-visible";
      const selectVisibleCb = document.createElement("input");
      selectVisibleCb.type = "checkbox";
      selectVisibleRow.appendChild(selectVisibleCb);
      const selectVisibleSpan = document.createElement("span");
      selectVisibleSpan.textContent = "Selecionar exibidos";
      selectVisibleRow.appendChild(selectVisibleSpan);
      list.appendChild(selectVisibleRow);

      const cbs = [];
      const labelMap = {};
      options.forEach((opt) => {
        const row = document.createElement("label");
        row.className = "mf-option";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        const norm = String(opt || "").toLowerCase();
        cb.dataset.val = norm;
        labelMap[norm] = opt;
        row.appendChild(cb);
        const txt = document.createElement("span");
        txt.textContent = opt;
        row.appendChild(txt);
        list.appendChild(row);
        cbs.push({ cb, txt, row, val: norm });
      });

      const syncUIFromTemp = () => {
        allCb.checked = tempSelected.size === 0;
        cbs.forEach(({ cb, val }) => {
          cb.checked = tempSelected.has(val);
        });
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        const allVisibleSelected = visible.length > 0 && visible.every(({ cb }) => cb.checked);
        selectVisibleCb.checked = allVisibleSelected;
      };

      const applyTempToFilters = () => {
        const set = filters[key];
        set.clear();
        tempSelected.forEach((v) => set.add(v));
        updateDisplay(key);
        renderFiltered();
      };

      const closePanel = () => panel.classList.remove("open");

      allCb.addEventListener("change", () => {
        if (allCb.checked) {
          tempSelected.clear();
          syncUIFromTemp();
        }
      });

      selectVisibleCb.addEventListener("change", () => {
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        if (selectVisibleCb.checked) {
          visible.forEach(({ val }) => tempSelected.add(val));
        } else {
          visible.forEach(({ val }) => tempSelected.delete(val));
        }
        allCb.checked = tempSelected.size === 0;
        syncUIFromTemp();
      });

      cbs.forEach(({ cb, val }) => {
        cb.addEventListener("change", () => {
          if (cb.checked) {
            tempSelected.add(val);
            allCb.checked = false;
          } else {
            tempSelected.delete(val);
          }
          syncUIFromTemp();
        });
      });

      search.addEventListener("input", () => {
        const term = search.value.toLowerCase();
        cbs.forEach(({ row, txt }) => {
          const match = txt.textContent.toLowerCase().includes(term);
          row.style.display = match ? "" : "none";
        });
        const allMatch = "(todos)".includes(term) || term === "";
        allRow.style.display = allMatch ? "" : "none";
        selectVisibleRow.style.display = "";
        syncUIFromTemp();
      });

      const actions = document.createElement("div");
      actions.className = "mf-actions";
      const cancelBtn = document.createElement("button");
      cancelBtn.type = "button";
      cancelBtn.className = "mf-btn ghost";
      cancelBtn.textContent = "Cancelar";
      const applyBtn = document.createElement("button");
      applyBtn.type = "button";
      applyBtn.className = "mf-btn primary";
      applyBtn.textContent = "Aplicar";

      cancelBtn.addEventListener("click", () => {
        tempSelected.clear();
        filters[key].forEach((v) => tempSelected.add(v));
        syncUIFromTemp();
        closePanel();
      });
      applyBtn.addEventListener("click", () => {
        applyTempToFilters();
        closePanel();
      });

      display.addEventListener("click", () => {
        const isOpen = panel.classList.contains("open");
        closeAllPanels();
        if (!isOpen) {
          panel.style.width = "";
          panel.style.height = "";
          tempSelected.clear();
          filters[key].forEach((v) => tempSelected.add(v));
          cbs.forEach(({ row }) => (row.style.display = ""));
          allRow.style.display = "";
          search.value = "";
          syncUIFromTemp();
          panel.classList.add("open");
        }
      });

      wrap.appendChild(display);
      panel.appendChild(search);
      panel.appendChild(list);
      actions.appendChild(cancelBtn);
      actions.appendChild(applyBtn);
      panel.appendChild(actions);
      wrap.appendChild(panel);
      container.appendChild(wrap);

      filterControls[key] = {
        panel,
        label,
        allCb,
        optionCbs: cbs.map((c) => c.cb),
        labelMap,
      };
      updateDisplay(key);
    };

    const setOptions = (rows = allData.rows) => {
      closeAllPanels();
      const uniques = colKeys.map(() => new Set());
      (rows || []).forEach((r) => {
        colKeys.forEach((k, idx) => {
          const v = r[k];
          if (v !== undefined && v !== null && v !== "") uniques[idx].add(String(v));
        });
      });
      filterContainers.forEach((container) => {
        const key = container.getAttribute("data-col");
        const idx = colKeys.indexOf(key);
        if (idx === -1) return;
        const opts = Array.from(uniques[idx]).sort((a, b) => a.localeCompare(b, "pt-BR"));
        buildFilter(container, opts, key);
      });
    };

    const renderPagination = (totalPages) => {
      if (!pager) return;
      pager.innerHTML = "";
      const addBtn = (label, page, disabled, active = false) => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "page-btn";
        if (active) btn.classList.add("active");
        btn.disabled = disabled;
        btn.textContent = label;
        btn.addEventListener("click", () => {
          currentPage = page;
          render();
        });
        pager.appendChild(btn);
      };
      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);
      const maxBtns = 5;
      let startPage = Math.max(1, currentPage - Math.floor(maxBtns / 2));
      let endPage = Math.min(totalPages, startPage + maxBtns - 1);
      if (endPage - startPage + 1 < maxBtns) {
        startPage = Math.max(1, endPage - maxBtns + 1);
      }
      if (startPage > 1) {
        addBtn("1", 1, false, currentPage === 1);
        if (startPage > 2) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          pager.appendChild(ellipsis);
        }
      }
      for (let p = startPage; p <= endPage; p += 1) {
        addBtn(String(p), p, false, currentPage === p);
      }
      if (endPage < totalPages) {
        if (endPage < totalPages - 1) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          pager.appendChild(ellipsis);
        }
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }
      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };

    const render = () => {
      const rows = filteredRows;
      if (!rows.length) {
        tbody.innerHTML = "";
        if (pager) pager.innerHTML = "";
        toggleEmptyState(true);
        updateDownloadState();
        return;
      }

      toggleEmptyState(false);
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const pageRows = rows.slice(startIdx, startIdx + pageSize);

      tbody.innerHTML = "";
      pageRows.forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${r.exercicio ?? ""}</td>
          <td>${r.status_aprovacao ?? ""}</td>
          <td>${r.adjunta_solicitante ?? ""}</td>
          <td>${r.chave_dotacao ?? ""}</td>
          <td>${r.chave_planejamento ?? ""}</td>
          <td class="num">${fmtNum(r.valor_dotacao)}</td>
          <td class="num">${fmtNum(r.valor_a_ser_est)}</td>
          <td class="num">${fmtNum(r.saldo_dotacao_apos)}</td>
          <td>${r.situacao ?? ""}</td>
          <td>${r.uo ?? ""}</td>
          <td>${r.programa ?? ""}</td>
          <td>${r.acao_paoe ?? ""}</td>
          <td>${r.produto ?? ""}</td>
          <td>${r.ug ?? ""}</td>
          <td>${r.regiao ?? ""}</td>
          <td>${r.subacao_entrega ?? ""}</td>
          <td>${r.etapa ?? ""}</td>
          <td>${r.natureza_despesa ?? ""}</td>
          <td>${r.elemento ?? ""}</td>
          <td>${r.subelemento ?? ""}</td>
          <td>${r.fonte ?? ""}</td>
          <td>${r.iduso ?? ""}</td>
          <td>${r.justificativa ?? ""}</td>
          <td>${r.usuario_nome_perfil ?? ""}</td>
          <td>${fmtDateTime(r.criado_em)}</td>
          <td>${fmtDateTime(r.alterado_em)}</td>
          <td>${r.aprovado_por_nome_perfil ?? ""}</td>
          <td>${fmtDateTime(r.data_aprovacao)}</td>
          <td>${r.motivo_rejeicao ?? ""}</td>
        `;
        tbody.appendChild(tr);
      });

      renderPagination(totalPages);
      updateDownloadState();
    };

    const renderFiltered = (resetPage = true) => {
      const filtered = allData.rows.filter((r) =>
        colKeys.every((k) => {
          const set = filters[k];
          if (!set || set.size === 0) return true;
          const val = r[k];
          const cmp = val === null || val === undefined ? "" : String(val).toLowerCase();
          return set.has(cmp);
        })
      );
      setOptions(filtered);
      filteredRows = filtered;
      if (resetPage) currentPage = 1;
      render();
    };

    if (!multiFilterClickBound) {
      document.addEventListener("click", (ev) => {
        if (!ev.target.closest(".mf-wrapper")) {
          closeAllPanels();
        }
      });
      multiFilterClickBound = true;
    }

    const load = async () => {
      try {
        const res = await fetch("/api/relatorios/est-dotacao");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar.");
        allData.rows = data.data || [];
        setOptions(allData.rows);
        filteredRows = allData.rows;
        render();
      } catch (err) {
        console.error(err);
      }
    };

    load();

    if (btnReset) {
      btnReset.addEventListener("click", () => {
        Object.keys(filters).forEach((k) => filters[k].clear());
        setOptions(allData.rows);
        filteredRows = allData.rows;
        currentPage = 1;
        render();
      });
    }

    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        const val = parseInt(pageSizeSelect.value || "20", 10);
        pageSize = Number.isNaN(val) ? 20 : val;
        currentPage = 1;
        render();
      });
    }

    if (btnDownload) {
      btnDownload.addEventListener("click", () => {
        if (!filteredRows.length) return;
        window.open("/api/relatorios/est-dotacao/download", "_blank");
      });
    }
  }

  function initEstDotacao() {
    const tipoSelect = document.getElementById("est-dotacao-tipo");
    const filterField = document.getElementById("est-dotacao-filtro-campo");
    const filterOp = document.getElementById("est-dotacao-filtro-operador");
    const filterValue = document.getElementById("est-dotacao-filtro-valor");
    const filterAdd = document.getElementById("est-dotacao-filtro-add");
    const filterList = document.getElementById("est-dotacao-filtro-list");
    const filterRemove = document.getElementById("est-dotacao-filtro-remove");
    const filterClear = document.getElementById("est-dotacao-filtro-clear");
    const filterCancel = document.getElementById("est-dotacao-filtro-cancel");
    const filterApply = document.getElementById("est-dotacao-filtro-apply");
    const filterMsg = document.getElementById("est-dotacao-filtro-msg");
    const dotacaoTable = document.getElementById("est-dotacao-summary-table-dotacao");
    const estornoTable = document.getElementById("est-dotacao-summary-table-estorno");
    const summaryBody = dotacaoTable ? dotacaoTable.querySelector("tbody") : null;
    const cadastrarBtn = document.getElementById("est-dotacao-cadastrar");
    const editBtn = document.getElementById("est-dotacao-edit");
    const deleteBtn = document.getElementById("est-dotacao-delete");
    const approveBtn = document.getElementById("est-dotacao-approve");
    const form = document.getElementById("est-dotacao-form");
    const estIdInput = document.getElementById("est-dotacao-id");
    const approvalFields = document.getElementById("est-dotacao-aprovacao-fields");
    const approvalRadios = form ? Array.from(form.querySelectorAll('input[name="estorno-aprovado"]')) : [];
    const approvalJustificativa = document.getElementById("est-dotacao-justificativa-aprovacao");
    const situacaoSelect = document.getElementById("est-dotacao-situacao");
    const exercicioSelect = document.getElementById("est-dotacao-exercicio");
    const adjuntaInput = document.getElementById("est-dotacao-adjunta");
    const uoInput = document.getElementById("est-dotacao-uo");
    const programaInput = document.getElementById("est-dotacao-programa");
    const acaoInput = document.getElementById("est-dotacao-acao");
    const produtoInput = document.getElementById("est-dotacao-produto");
    const chaveInput = document.getElementById("est-dotacao-chave");
    const regiaoInput = document.getElementById("est-dotacao-regiao");
    const ugInput = document.getElementById("est-dotacao-ug");
    const naturezaInput = document.getElementById("est-dotacao-natureza");
    const elementoInput = document.getElementById("est-dotacao-elemento");
    const subelementoInput = document.getElementById("est-dotacao-subelemento");
    const fonteInput = document.getElementById("est-dotacao-fonte");
    const idusoInput = document.getElementById("est-dotacao-iduso");
    const subacaoInput = document.getElementById("est-dotacao-subacao");
    const etapaInput = document.getElementById("est-dotacao-etapa");
    const valorDotacaoInput = document.getElementById("est-dotacao-valor-dotacao");
    const valorEstornoInput = document.getElementById("est-dotacao-valor-estorno");
    const saldoInput = document.getElementById("est-dotacao-saldo");
    const justificativaInput = document.getElementById("est-dotacao-justificativa");
    const msg = document.getElementById("est-dotacao-msg");
    const estPage = document.getElementById("est-dotacao-page");
    const pageMode = String(estPage?.dataset?.viewMode || "formulario").trim();
    const editBadge = document.getElementById("est-dotacao-editing-badge");
    const currentUserPerfilId = String(estPage?.dataset?.userPerfilId || userPerfilId || "").trim();
    const currentUserId = estPage?.dataset?.userId || "";
    const pageSizeSelect = document.getElementById("est-dotacao-page-size");
    const paginationEl = document.getElementById("est-dotacao-pagination");
    const summaryBox = document.getElementById("est-dotacao-summary");
    if (!summaryBody || !dotacaoTable || !estornoTable) return;

    const criteria = [];
    let criteriaSelected = -1;
    const fieldLabels = {
      exercicio: "Exercício",
      chaveDotacao: "Controle de Dotação",
      adjunta: "Adjunta Solicitante",
      programa: "Programa",
      paoe: "Ação/PAOE",
    };
    const opLabels = {
      eq: "Igual a",
      contains: "Contém",
      gt: "Maior que",
      lt: "Menor que",
      gte: "Maior igual a",
      lte: "Menor igual a",
    };

    const setFilterMsg = (text, isError = false) => {
      if (!filterMsg) return;
      filterMsg.textContent = text || "";
      if (isError) filterMsg.classList.add("text-error");
      else filterMsg.classList.remove("text-error");
    };

    const parseMaybeNumber = (value) => {
      if (value === null || value === undefined) return { raw: "", num: null };
      const raw = String(value).trim();
      if (!raw) return { raw, num: null };
      const num = Number(raw.replace(",", "."));
      return Number.isNaN(num) ? { raw, num: null } : { raw, num };
    };

    const compareValues = (left, right, op) => {
      const l = parseMaybeNumber(left);
      const r = parseMaybeNumber(right);
      if (l.num !== null && r.num !== null) {
        if (op === "eq") return l.num === r.num;
        if (op === "gt") return l.num > r.num;
        if (op === "lt") return l.num < r.num;
        if (op === "gte") return l.num >= r.num;
        if (op === "lte") return l.num <= r.num;
      }
      const lraw = l.raw.toLowerCase();
      const rraw = r.raw.toLowerCase();
      const cmp = lraw.localeCompare(rraw, "pt-BR", { sensitivity: "base" });
      if (op === "eq") return cmp === 0;
      if (op === "contains") return lraw.includes(rraw);
      if (op === "gt") return cmp > 0;
      if (op === "lt") return cmp < 0;
      if (op === "gte") return cmp >= 0;
      if (op === "lte") return cmp <= 0;
      return false;
    };

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;
    let isEditMode = false;
    let isApprovalMode = false;
    const pendingStorageKey = "spo.estDotacao.pendingAction";

    const getActiveTable = () => (tipoSelect?.value === "estorno" ? estornoTable : dotacaoTable);
    const getRows = () => Array.from(getActiveTable().querySelectorAll(".dotacao-summary-row"));
    const getAllRows = () => Array.from(document.querySelectorAll("#est-dotacao-summary .dotacao-summary-row"));
    const rowSnapshot = (row) => (row ? Object.fromEntries(Object.entries(row.dataset || {})) : {});
    const rowFromSnapshot = (dataset) => ({ dataset: dataset || {} });
    const openEstornoFormulario = (action, row) => {
      try {
        sessionStorage.setItem(
          pendingStorageKey,
          JSON.stringify({
            action,
            dataset: rowSnapshot(row),
          })
        );
      } catch (err) {
        // noop
      }
      loadPage("cadastrar/est-dotacao/formulario");
    };
    const updateEditBadge = (action, row) => {
      if (!editBadge) return;
      const controle = String(row?.dataset?.chaveDotacao || row?.dataset?.id || "").trim();
      if (!controle || !action) {
        editBadge.textContent = "";
        editBadge.style.display = "none";
        return;
      }
      if (action === "approve") {
        editBadge.textContent = `- Aprovação do registro ${controle}`;
      } else if (action === "edit") {
        editBadge.textContent = `- Edição do registro ${controle}`;
      } else {
        editBadge.textContent = `- Novo estorno da dotação ${controle}`;
      }
      editBadge.style.display = "inline";
    };

    const renderCriteria = () => {
      if (!filterList) return;
      filterList.innerHTML = "";
      criteria.forEach((c, idx) => {
        const li = document.createElement("li");
        const label = fieldLabels[c.field] || c.field;
        const op = opLabels[c.op] || c.op;
        li.textContent = `${label} ${op} ${c.value}`;
        li.dataset.index = String(idx);
        if (idx === criteriaSelected) {
          li.style.borderColor = "var(--primary)";
        }
        li.addEventListener("click", () => {
          criteriaSelected = idx;
          renderCriteria();
        });
        filterList.appendChild(li);
      });
    };

    const getFilteredRows = () => {
      const rows = getRows();
      if (!criteria.length) return rows;
      return rows.filter((row) =>
        criteria.every((c) => {
          const field = c.field;
          const rowVal = row.dataset[field] || "";
          return compareValues(rowVal, c.value, c.op);
        })
      );
    };

    const renderPagination = (totalPages) => {
      if (!paginationEl) return;
      paginationEl.innerHTML = "";
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
        b.type = "button";
        b.className = "page-btn";
        b.textContent = label;
        if (disabled) b.disabled = true;
        if (active) b.classList.add("active");
        b.addEventListener("click", () => {
          if (disabled || page === currentPage) return;
          currentPage = page;
          renderSummaryPage();
          setFilterMsg("");
        });
        paginationEl.appendChild(b);
      };
      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);
      const maxButtons = 5;
      let start = Math.max(1, currentPage - Math.floor(maxButtons / 2));
      let end = Math.min(totalPages, start + maxButtons - 1);
      if (end - start + 1 < maxButtons) {
        start = Math.max(1, end - maxButtons + 1);
      }
      if (start > 1) {
        addBtn("1", 1, false, currentPage === 1);
        if (start > 2) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          paginationEl.appendChild(ellipsis);
        }
      }
      for (let p = start; p <= end; p += 1) {
        addBtn(String(p), p, false, p === currentPage);
      }
      if (end < totalPages) {
        const ellipsis = document.createElement("span");
        ellipsis.textContent = "...";
        paginationEl.appendChild(ellipsis);
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }
      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };

    const renderSummaryPage = () => {
      const allRows = getRows();
      const filtered = getFilteredRows();
      const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const pageRows = filtered.slice(startIdx, startIdx + pageSize);
      allRows.forEach((row) => {
        row.style.display = "none";
        row.classList.remove("selected");
      });
      pageRows.forEach((row) => {
        row.style.display = "";
      });
      renderPagination(totalPages);
    };

    const setResultsVisible = (show) => {
      if (!summaryBox) return;
      summaryBox.classList.toggle("dotacao-summary-hidden", !show);
      summaryBox.classList.toggle("consulta-summary-hidden", !show);
      if (!show && paginationEl) paginationEl.innerHTML = "";
    };

    const applyCriteriaToResults = () => {
      currentPage = 1;
      renderSummaryPage();
    };

    const formatSummaryValues = () => {
      getAllRows().forEach((row) => {
        const cell = row.querySelector(".dotacao-summary-valor");
        if (!cell) return;
        const raw = row.dataset.valor || cell.textContent || "";
        cell.textContent = formatPtBr(parsePtBr(raw) || 0);
      });
    };

    const formatPtBr = (value) => {
      const n = Number(value || 0);
      if (Number.isNaN(n)) return "";
      return new Intl.NumberFormat("pt-BR", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }).format(n);
    };

    const parsePtBr = (value) => {
      if (value === null || value === undefined) return null;
      const raw = String(value).trim();
      if (!raw) return null;
      if (raw.includes(",")) {
        const cleaned = raw.replace(/\./g, "").replace(",", ".");
        const num = Number(cleaned);
        return Number.isNaN(num) ? null : num;
      }
      const num = Number(raw);
      return Number.isNaN(num) ? null : num;
    };

    const setCurrentYear = () => {
      if (!exercicioSelect) return;
      const year = new Intl.DateTimeFormat("pt-BR", { timeZone: "America/Manaus", year: "numeric" }).format(
        new Date()
      );
      exercicioSelect.innerHTML = `<option value="">Selecione...</option><option value="${year}">${year}</option>`;
      exercicioSelect.value = "";
    };

    const setExercicioValue = (value) => {
      if (!exercicioSelect) return;
      const v = String(value || "").trim();
      if (!v) {
        exercicioSelect.value = "";
        return;
      }
      const has = Array.from(exercicioSelect.options).some((opt) => opt.value === v);
      if (!has) {
        exercicioSelect.insertAdjacentHTML("beforeend", `<option value="${v}">${v}</option>`);
      }
      exercicioSelect.value = v;
    };

    const updateSaldo = () => {
      const vDot = parsePtBr(valorDotacaoInput?.value || "") || 0;
      const vEst = parsePtBr(valorEstornoInput?.value || "") || 0;
      if (saldoInput) saldoInput.value = formatPtBr(vDot - vEst);
    };

    const updateEstornoMode = () => {
      if (!situacaoSelect || !valorEstornoInput) return;
      const total = situacaoSelect.value === "Estorno Total";
      if (total) {
        valorEstornoInput.value = valorDotacaoInput?.value || "";
        valorEstornoInput.disabled = true;
      } else {
        valorEstornoInput.disabled = false;
      }
      updateSaldo();
    };

    const setEditFieldsEnabled = (enabled) => {
      if (situacaoSelect) situacaoSelect.disabled = !enabled;
      if (exercicioSelect) exercicioSelect.disabled = !enabled;
      if (valorEstornoInput) valorEstornoInput.disabled = !enabled;
      if (justificativaInput) justificativaInput.readOnly = !enabled;
      if (enabled) updateEstornoMode();
    };

    const setApprovalMode = (enabled) => {
      isApprovalMode = enabled;
      if (approvalFields) approvalFields.style.display = enabled ? "" : "none";
      if (approvalJustificativa) approvalJustificativa.value = "";
      if (approvalRadios.length) approvalRadios[0].checked = true;
      setEditFieldsEnabled(!enabled);
      if (approvalJustificativa) approvalJustificativa.disabled = !enabled;
    };

    const applyTipo = () => {
      const isEstorno = tipoSelect?.value === "estorno";
      dotacaoTable.style.display = isEstorno ? "none" : "";
      estornoTable.style.display = isEstorno ? "" : "none";
      if (cadastrarBtn) cadastrarBtn.style.display = isEstorno ? "none" : "";
      if (editBtn) editBtn.style.display = isEstorno ? "" : "none";
      if (deleteBtn) deleteBtn.style.display = isEstorno ? "" : "none";
      if (approveBtn) approveBtn.style.display = isEstorno ? "" : "none";
      setResultsVisible(false);
      renderCriteria();
    };

    formatSummaryValues();
    applyTipo();
    setCurrentYear();

    if (situacaoSelect) {
      situacaoSelect.addEventListener("change", updateEstornoMode);
    }
    if (valorEstornoInput) {
      valorEstornoInput.addEventListener("input", updateSaldo);
      valorEstornoInput.addEventListener("blur", () => {
        const num = parsePtBr(valorEstornoInput.value || "");
        valorEstornoInput.value = num === null ? "" : formatPtBr(num);
        updateSaldo();
      });
    }

    const selectRow = (row) => {
      getAllRows().forEach((el) => el.classList.remove("selected"));
      if (row) row.classList.add("selected");
    };
    const fillFormFromDotacao = (selected) => {
      if (!selected) return;
      const adjunta = String(selected.dataset.adjunta || "").trim();
      if (adjuntaInput) adjuntaInput.value = adjunta;
      if (uoInput) uoInput.value = selected.dataset.uo || "";
      if (programaInput) programaInput.value = selected.dataset.programa || "";
      if (acaoInput) acaoInput.value = selected.dataset.paoe || "";
      if (produtoInput) produtoInput.value = selected.dataset.produto || "";
      if (chaveInput) chaveInput.value = selected.dataset.chavePlanejamento || "";
      if (regiaoInput) regiaoInput.value = selected.dataset.regiao || "";
      if (ugInput) ugInput.value = selected.dataset.ug || "";
      if (naturezaInput) naturezaInput.value = selected.dataset.natureza || "";
      if (elementoInput) elementoInput.value = selected.dataset.elemento || "";
      if (subelementoInput) subelementoInput.value = selected.dataset.subelemento || "";
      if (fonteInput) fonteInput.value = selected.dataset.fonte || "";
      if (idusoInput) idusoInput.value = selected.dataset.iduso || "";
      if (subacaoInput) subacaoInput.value = selected.dataset.subacao || "";
      if (etapaInput) etapaInput.value = selected.dataset.etapa || "";
      if (valorDotacaoInput) {
        const v = selected.dataset.valor || "";
        const num = parsePtBr(v);
        valorDotacaoInput.value = num === null ? v : formatPtBr(num);
      }
      if (form) form.dataset.chaveDotacao = selected.dataset.chaveDotacao || "";
      updateEstornoMode();
      if (justificativaInput) justificativaInput.value = "";
      setCurrentYear();
      if (estIdInput) estIdInput.value = "";
      isEditMode = false;
      setApprovalMode(false);
      updateEditBadge("new", selected);
      if (msg) msg.textContent = "";
      setFilterMsg("");
    };

    getAllRows().forEach((row) => {
      row.addEventListener("click", () => {
        selectRow(row);
      });
    });
    setResultsVisible(false);

    if (filterAdd) {
      filterAdd.addEventListener("click", () => {
        const field = String(filterField?.value || "");
        const op = String(filterOp?.value || "eq");
        const value = String(filterValue?.value || "").trim();
        if (!field) {
          setFilterMsg("Selecione um campo.", true);
          return;
        }
        if (!value) {
          setFilterMsg("Informe um valor.", true);
          return;
        }
        if (field !== "exercicio" && !criteria.some((c) => c.field === "exercicio")) {
          setFilterMsg("Informe um critério de Exercício antes dos demais.", true);
          return;
        }
        criteria.push({ field, op, value });
        criteriaSelected = criteria.length - 1;
        renderCriteria();
        setFilterMsg("");
        if (filterValue) filterValue.value = "";
      });
    }

    if (filterRemove) {
      filterRemove.addEventListener("click", () => {
        if (criteriaSelected < 0 || criteriaSelected >= criteria.length) {
          setFilterMsg("Selecione um critério para remover.", true);
          return;
        }
        criteria.splice(criteriaSelected, 1);
        criteriaSelected = -1;
        renderCriteria();
        setResultsVisible(false);
        setFilterMsg("");
      });
    }

    if (filterClear) {
      filterClear.addEventListener("click", () => {
        criteria.length = 0;
        criteriaSelected = -1;
        renderCriteria();
        setResultsVisible(false);
        setFilterMsg("");
      });
    }

    if (filterCancel) {
      filterCancel.addEventListener("click", () => {
        criteria.length = 0;
        criteriaSelected = -1;
        renderCriteria();
        setResultsVisible(false);
        if (filterField) filterField.value = "";
        if (filterOp) filterOp.value = "eq";
        if (filterValue) filterValue.value = "";
        setFilterMsg("");
      });
    }

    if (filterApply) {
      filterApply.addEventListener("click", () => {
        if (!criteria.some((c) => c.field == "exercicio")) {
          setFilterMsg("Informe o critério de Exercício antes de consultar.", true);
          return;
        }
        setResultsVisible(true);
        applyCriteriaToResults();
        setFilterMsg("");
      });
    }

    if (tipoSelect) {
      tipoSelect.addEventListener("change", () => {
        applyTipo();
      });
    }

    if (cadastrarBtn) {
      cadastrarBtn.addEventListener("click", () => {
        const selected = summaryBody?.querySelector(".dotacao-summary-row.selected");
        if (!selected) {
          setFilterMsg("Selecione um registro para cadastrar estorno.", true);
          return;
        }
        if (!currentUserPerfilId || currentUserPerfilId !== String(selected.dataset.perfilId || "").trim()) {
          setFilterMsg("Usuário sem permissão de cadastrar estorno.", true);
          return;
        }
        if (pageMode === "consultar") {
          openEstornoFormulario("new", selected);
          return;
        }
        fillFormFromDotacao(selected);
      });
    }

    const fillFormFromEstorno = (row) => {
      if (!row) return;
      if (estIdInput) estIdInput.value = row.dataset.id || "";
      isEditMode = true;
      setApprovalMode(false);
      if (adjuntaInput) adjuntaInput.value = row.dataset.adjunta || "";
      if (uoInput) uoInput.value = row.dataset.uo || "";
      if (programaInput) programaInput.value = row.dataset.programa || "";
      if (acaoInput) acaoInput.value = row.dataset.paoe || "";
      if (produtoInput) produtoInput.value = row.dataset.produto || "";
      if (chaveInput) chaveInput.value = row.dataset.chavePlanejamento || "";
      if (regiaoInput) regiaoInput.value = row.dataset.regiao || "";
      if (ugInput) ugInput.value = row.dataset.ug || "";
      if (naturezaInput) naturezaInput.value = row.dataset.natureza || "";
      if (elementoInput) elementoInput.value = row.dataset.elemento || "";
      if (subelementoInput) subelementoInput.value = row.dataset.subelemento || "";
      if (fonteInput) fonteInput.value = row.dataset.fonte || "";
      if (idusoInput) idusoInput.value = row.dataset.iduso || "";
      if (subacaoInput) subacaoInput.value = row.dataset.subacao || "";
      if (etapaInput) etapaInput.value = row.dataset.etapa || "";
      if (valorDotacaoInput) {
        const v = row.dataset.valordotacao || "";
        const num = parsePtBr(v);
        valorDotacaoInput.value = num === null ? v : formatPtBr(num);
      }
      if (form) form.dataset.chaveDotacao = row.dataset.chaveDotacao || "";
      if (valorEstornoInput) {
        const v = row.dataset.valorestorno || "";
        const num = parsePtBr(v);
        valorEstornoInput.value = num === null ? v : formatPtBr(num);
      }
      if (saldoInput) {
        const v = row.dataset.saldoestorno || "";
        const num = parsePtBr(v);
        saldoInput.value = num === null ? v : formatPtBr(num);
      }
      if (justificativaInput) justificativaInput.value = row.dataset.justificativa || "";
      if (situacaoSelect) situacaoSelect.value = row.dataset.situacao || "";
      setExercicioValue(row.dataset.exercicio || "");
      updateEstornoMode();
      if (msg) msg.textContent = "";
      setFilterMsg("");
    };
    const restorePendingFormulario = () => {
      if (pageMode !== "formulario") return;
      let pending = null;
      try {
        pending = JSON.parse(sessionStorage.getItem(pendingStorageKey) || "null");
        sessionStorage.removeItem(pendingStorageKey);
      } catch (err) {
        pending = null;
      }
      if (!pending || !pending.dataset) return;
      const row = rowFromSnapshot(pending.dataset);
      if (pending.action === "new") {
        fillFormFromDotacao(row);
        return;
      }
      if (pending.action === "edit") {
        fillFormFromEstorno(row);
        setEditFieldsEnabled(true);
        updateEditBadge("edit", row);
        return;
      }
      if (pending.action === "approve") {
        fillFormFromEstorno(row);
        setApprovalMode(true);
        updateEditBadge("approve", row);
      }
    };
    restorePendingFormulario();

    const canEditOrDeleteEstorno = (row) => {
      if (!row) return false;
      const status = String(row.dataset.status || "").trim().toLowerCase();
      if (status && status !== "aguardando") {
        setFilterMsg("Somente estornos com status Aguardando podem ser alterados.", true);
        return false;
      }
      const criadorPerfilId = String(row.dataset.criadorPerfilId || "").trim();
      if (!criadorPerfilId || !currentUserPerfilId || currentUserPerfilId !== criadorPerfilId) {
        setFilterMsg("Usuário sem permissão para alterar o estorno atual.", true);
        return false;
      }
      return true;
    };

    const canDeleteEstorno = (row) => {
      if (!row) return false;
      const status = String(row.dataset.status || "").trim().toLowerCase();
      if (status && status !== "aguardando") {
        setFilterMsg("Somente estornos com status Aguardando podem ser excluídos.", true);
        return false;
      }
      const criadorPerfilId = String(row.dataset.criadorPerfilId || "").trim();
      if (!criadorPerfilId || !currentUserPerfilId || currentUserPerfilId !== criadorPerfilId) {
        setFilterMsg("Usuário sem permissão para excluir o estorno atual.", true);
        return false;
      }
      return true;
    };

    if (editBtn) {
      editBtn.addEventListener("click", () => {
        const selected = estornoTable?.querySelector(".dotacao-summary-row.selected");
        if (!selected) {
          setFilterMsg("Selecione um registro para editar.", true);
          return;
        }
        if (!canEditOrDeleteEstorno(selected)) return;
        if (pageMode === "consultar") {
          openEstornoFormulario("edit", selected);
          return;
        }
        fillFormFromEstorno(selected);
        setEditFieldsEnabled(true);
        updateEditBadge("edit", selected);
      });
    }

    if (deleteBtn) {
      deleteBtn.addEventListener("click", async () => {
        const selected = estornoTable?.querySelector(".dotacao-summary-row.selected");
        if (!selected) {
          setFilterMsg("Selecione um registro para excluir.", true);
          return;
        }
        if (!canDeleteEstorno(selected)) return;
        const estId = selected.dataset.id;
        if (!estId) {
          setFilterMsg("Registro inválido para exclusão.", true);
          return;
        }
        try {
          const res = await fetch(`/api/est-dotacao/${encodeURIComponent(estId)}`, {
            method: "DELETE",
            headers: { "X-Requested-With": "fetch" },
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao excluir.");
          await loadPage("cadastrar/est-dotacao/consultar");
        } catch (err) {
          setFilterMsg(err.message || "Falha ao excluir.", true);
        }
      });
    }

    if (approveBtn) {
      approveBtn.addEventListener("click", () => {
        const selected = estornoTable?.querySelector(".dotacao-summary-row.selected");
        if (!selected) {
          setFilterMsg("Selecione um registro para aprovar.", true);
          return;
        }
        const status = String(selected.dataset.status || "").trim().toLowerCase();
        if (status && status !== "aguardando") {
          setFilterMsg("Somente estornos com status Aguardando podem ser aprovados.", true);
          return;
        }
        const adjunta = String(selected.dataset.adjunta || "").trim();
        if (!currentUserPerfilId || currentUserPerfilId !== String(selected.dataset.perfilId || "").trim()) {
          setFilterMsg("Usuário sem permissão para aprovar o estorno atual.", true);
          return;
        }
        if (pageMode === "consultar") {
          openEstornoFormulario("approve", selected);
          return;
        }
        fillFormFromEstorno(selected);
        setApprovalMode(true);
        updateEditBadge("approve", selected);
      });
    }

    if (form) {
      form.addEventListener("submit", async (ev) => {
        ev.preventDefault();
        const selected = summaryBody?.querySelector(".dotacao-summary-row.selected");
        const editId = String(estIdInput?.value || "").trim();
        const useEdit = isEditMode && editId;
        const loadedChaveDotacao = String(form?.dataset?.chaveDotacao || "").trim();
        if (isApprovalMode) {
          if (!editId) {
            if (msg) {
              msg.textContent = "Registro inválido para aprovação.";
              msg.classList.add("text-error");
            }
            return;
          }
          const aprovado = approvalRadios.find((r) => r.checked)?.value || "sim";
          const justificativa = String(approvalJustificativa?.value || "").trim();
          if (!justificativa) {
            if (msg) {
              msg.textContent = "Justificativa obrigatória.";
              msg.classList.add("text-error");
            }
            return;
          }
          try {
            const res = await fetch(`/api/est-dotacao/${encodeURIComponent(editId)}/aprovar`, {
              method: "POST",
              headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
              body: JSON.stringify({ estorno_aprovado: aprovado, motivo_rejeicao: justificativa }),
            });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || "Falha ao aprovar.");
            if (msg) msg.textContent = data.message || "Estorno atualizado.";
            await loadPage("cadastrar/est-dotacao/consultar");
          } catch (err) {
            if (msg) {
              msg.textContent = err.message || "Falha ao aprovar.";
              msg.classList.add("text-error");
            }
          }
          return;
        }
        if (!useEdit && !loadedChaveDotacao && (!selected || selected.dataset.kind !== "dotacao")) {
          if (msg) {
            msg.textContent = "Carregue uma dotação antes de salvar o estorno.";
            msg.classList.add("text-error");
          }
          return;
        }
        if (msg) {
          msg.textContent = "Salvando...";
          msg.classList.remove("text-error");
        }
        const payload = {
          exercicio: exercicioSelect?.value || "",
          adjunta: adjuntaInput?.value || "",
          chave_planejamento: chaveInput?.value || "",
          chave_dotacao: useEdit
            ? form?.dataset?.chaveDotacao || ""
            : loadedChaveDotacao || summaryBody?.querySelector(".dotacao-summary-row.selected")?.dataset.chaveDotacao || "",
          uo: uoInput?.value || "",
          programa: programaInput?.value || "",
          acao_paoe: acaoInput?.value || "",
          produto: produtoInput?.value || "",
          ug: ugInput?.value || "",
          regiao: regiaoInput?.value || "",
          subacao_entrega: subacaoInput?.value || "",
          etapa: etapaInput?.value || "",
          natureza_despesa: naturezaInput?.value || "",
          elemento: elementoInput?.value || "",
          subelemento: subelementoInput?.value || "",
          fonte: fonteInput?.value || "",
          iduso: idusoInput?.value || "",
          valor_dotacao: valorDotacaoInput?.value || "",
          valor_a_ser_est: valorEstornoInput?.value || "",
          saldo_dotacao_apos: saldoInput?.value || "",
          justificativa: justificativaInput?.value || "",
          situacao: situacaoSelect?.value || "",
        };
        try {
          const url = useEdit ? `/api/est-dotacao/${encodeURIComponent(editId)}` : "/api/est-dotacao";
          const method = useEdit ? "PUT" : "POST";
          const res = await fetch(url, {
            method,
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify(payload),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao salvar.");
          if (msg) msg.textContent = data.message || "Estorno cadastrado.";
          await loadPage("cadastrar/est-dotacao/consultar");
        } catch (err) {
          if (msg) {
            msg.textContent = err.message || "Falha ao salvar.";
            msg.classList.add("text-error");
          }
        }
      });
    }

    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        pageSize = parseInt(pageSizeSelect.value || "20", 10) || 20;
        if (summaryBox && summaryBox.style.display !== "none") {
          renderSummaryPage();
        }
      });
    }
  }

  function initDashboard({ forceShow = false } = {}) {
    const recordModal = document.getElementById("emp-record-modal");
    if (recordModal) {
      const renderDigits = (container) => {
        if (!container) return;
        const rawVal = container.dataset.value ?? "0";
        const minLen = parseInt(container.dataset.minLen || "3", 10) || 3;
        let text = String(rawVal);
        if (!/^\d+$/.test(text)) text = "0";
        if (text.length < minLen) text = text.padStart(minLen, "0");
        container.innerHTML = "";
        for (const ch of text) {
          const span = document.createElement("span");
          span.className = "digit";
          span.textContent = ch;
          container.appendChild(span);
        }
      };

      const formatEmpRecordStamp = (value) => {
        const date = parseManausLocal(value);
        if (!date) return "-";
        const parts = new Intl.DateTimeFormat("pt-BR", {
          timeZone: AMAZON_TZ,
          day: "2-digit",
          month: "2-digit",
          year: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
        }).formatToParts(date);
        const map = Object.fromEntries(parts.map((p) => [p.type, p.value]));
        const dia = `${map.day}/${map.month}/${map.year}`;
        const hora = `${map.hour}h${map.minute}m`;
        return `${dia} às ${hora}`;
      };

      const diasEl = recordModal.querySelector(".emp-record-digits");
      const recEl = recordModal.querySelector(".emp-record-digits.small");
      renderDigits(diasEl);
      renderDigits(recEl);

      const auditEl = recordModal.querySelector(".emp-record-audit");
      if (auditEl) {
        const penult = recordModal.dataset.penult || auditEl.dataset.penult;
        const ult = recordModal.dataset.ult || auditEl.dataset.ult;
        if (penult || ult) {
          const penultText = penult ? formatEmpRecordStamp(penult) : "-";
          const ultText = ult ? formatEmpRecordStamp(ult) : "-";
          auditEl.textContent = `Atualizado de ${penultText} a ${ultText}`;
        } else {
          auditEl.textContent = "Nenhuma atualizacao registrada.";
        }
      }
    }

    const modal = document.getElementById("dotacao-aguardando-modal");
    const hasItems = modal ? modal.dataset.hasItems === "1" : false;
    const closeBtn = modal ? document.getElementById("dotacao-aguardando-close") : null;
    if (closeBtn && modal) {
      closeBtn.addEventListener("click", () => {
        modal.style.display = "none";
      });
    }
    const showDotacaoModal = () => {
      if (modal && hasItems) modal.style.display = "flex";
    };

    if (recordModal) {
      const recordClose = document.getElementById("emp-record-close");
      if (recordClose) {
        recordClose.addEventListener("click", () => {
          recordModal.style.display = "none";
          showDotacaoModal();
        });
      }
    }

    if (forceShow) {
      if (recordModal) {
        recordModal.style.display = "none";
      }
      showDotacaoModal();
      return;
    }
    const nav = performance.getEntriesByType("navigation")[0];
    const isReload = nav && nav.type === "reload";
    if (!isReload) {
      if (recordModal && recordModal.dataset.showLogin === "1") {
        recordModal.style.display = "flex";
      } else {
        showDotacaoModal();
      }
    }
  }

  function initEtapaPlan21() {
    const page = document.getElementById("etapa-page");
    const form = document.getElementById("form-etapa");
    const msg = document.getElementById("etapa-msg");
    if (!page || !form || !msg) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const currentUserPerfilId = String(page.dataset.userPerfilId || userPerfilId || "").trim();
    const nivelAtual = parseInt(String(page.dataset.userNivel || userNivel || "").trim(), 10);
    const etapaViewMode = String(page.dataset.viewMode || "formulario");
    const etapaIsConsultaView = etapaViewMode === "consultar";
    const etapaPendingActionKey = "spo.etapa.pendingAction";
    const fields = {
      id: document.getElementById("etapa-id"),
      plan21Id: document.getElementById("etapa-plan21-id"),
      exercicio: document.getElementById("etapa-exercicio"),
      uo: document.getElementById("etapa-uo"),
      programa: document.getElementById("etapa-programa"),
      produto: document.getElementById("etapa-produto-acao"),
      acao: document.getElementById("etapa-acao"),
      subacao: document.getElementById("etapa-subacao"),
      adj: document.getElementById("etapa-adj-solicitante"),
      etapaSelect: document.getElementById("etapa-select"),
      origem: document.getElementById("etapa-origem"),
      responsavelOrigem: document.getElementById("etapa-responsavel-origem"),
      prazoOrigem: document.getElementById("etapa-prazo-origem"),
      municipio: document.getElementById("etapa-municipio"),
      etapaNova: document.getElementById("etapa-nova"),
      responsavelNovo: document.getElementById("etapa-responsavel-novo"),
      cpfNovo: document.getElementById("etapa-cpf-novo"),
      emailNovo: document.getElementById("etapa-email-novo"),
      inicioNovo: document.getElementById("etapa-data-inicio-novo"),
      fimNovo: document.getElementById("etapa-data-fim-novo"),
      justificativa: document.getElementById("etapa-justificativa"),
      responsavelNger: document.getElementById("etapa-responsavel-nger"),
      justificativaAprovacao: document.getElementById("etapa-justificativa-aprovacao"),
    };
    const formSection = document.getElementById("etapa-form-section");
    const formTitle = document.getElementById("etapa-form-title");
    const origemGrid = document.getElementById("etapa-origem-grid");
    const selectWrap = document.getElementById("etapa-select-wrap");
    const cadastrarQuestion = document.getElementById("etapa-cadastrar-question");
    const excluirQuestion = document.getElementById("etapa-excluir-question");
    const consultarWrap = document.getElementById("etapa-consultar-wrap");
    const consultarBtn = document.getElementById("etapa-consultar");
    const approvalFields = document.getElementById("etapa-aprovacao-fields");
    const novoFieldWraps = Array.from(form.querySelectorAll("[data-etapa-novo-field]"));
    const municipioWrap = document.getElementById("etapa-municipio-wrap");
    const saveBtn = document.getElementById("etapa-save");
    const clearBtn = document.getElementById("etapa-clear");
    const editBadge = document.getElementById("etapa-editing-badge");
    const summary = document.getElementById("etapa-summary");
    const table = document.getElementById("etapa-summary-table");
    const tbody = table ? table.querySelector("tbody") : null;
    const resultsPlaceholder = document.getElementById("etapa-results-placeholder");
    const resultsTableWrap = document.getElementById("etapa-results-table-wrap");
    const resultsFooter = document.getElementById("etapa-results-footer");
    const pageSizeSelect = document.getElementById("etapa-page-size");
    const paginationEl = document.getElementById("etapa-pagination");
    const approveBtn = document.getElementById("etapa-approve");
    const editBtn = document.getElementById("etapa-edit");
    const deleteBtn = document.getElementById("etapa-delete");
    const printBtn = document.getElementById("etapa-print");
    const filterField = document.getElementById("etapa-filtro-campo");
    const filterForm = document.getElementById("etapa-filtro-form");
    const filterOp = document.getElementById("etapa-filtro-operador");
    const filterValue = document.getElementById("etapa-filtro-valor");
    const filterAdd = document.getElementById("etapa-filtro-add");
    const filterList = document.getElementById("etapa-filtro-list");
    const filterRemove = document.getElementById("etapa-filtro-remove");
    const filterClear = document.getElementById("etapa-filtro-clear");
    const filterCancel = document.getElementById("etapa-filtro-cancel");
    const filterApply = document.getElementById("etapa-filtro-apply");
    const filterMsg = document.getElementById("etapa-filtro-msg");
    let selectedRow = null;
    let approvalMode = false;
    let approvalDisabledSnapshot = null;
    let pageSize = parseInt(pageSizeSelect?.value || "5", 10) || 5;
    let currentPage = 1;
    const filters = [];
    let filterSelected = -1;
    const optionsCache = new Map();
    let optionRowsCatalog = [];
    let adjOptionsCatalog = [];
    let responsaveisNgerCatalog = [];
    let etapaNumberPrefix = "";

    const esc = (value) =>
      String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    const setMsg = (text, isError = false) => {
      const rawText = String(text || "");
      if (rawText.startsWith("Não é possível excluir a Etapa")) {
        let html = esc(rawText)
          .replace(
            /(Não é possível excluir a Etapa &quot;)(.*?)(&quot;, pois existem )/,
            "$1<strong>$2</strong>$3"
          )
          .replace(/Memórias de Cálculo/g, "<strong>Memórias de Cálculo</strong>");
        msg.innerHTML = html;
      } else {
        msg.textContent = rawText;
      }
      msg.classList.toggle("text-error", Boolean(isError));
    };
    const getRadio = (name) => form.querySelector(`input[name="${name}"]:checked`)?.value || "";
    const setRadio = (name, value) => {
      const input = form.querySelector(`input[name="${name}"][value="${value}"]`);
      if (input) input.checked = true;
    };
    const clearRadio = (name) => {
      form.querySelectorAll(`input[name="${name}"]`).forEach((input) => {
        input.checked = false;
      });
    };
    const fillSelect = (select, values, placeholder = "Selecione...") => {
      if (!select) return;
      const prev = select.value;
      select.innerHTML = `<option value="">${placeholder}</option>`;
      (values || []).forEach((item) => {
        const opt = document.createElement("option");
        if (typeof item === "string") {
          opt.value = item;
          opt.textContent = item;
        } else {
          opt.value = item.value ?? item.label ?? "";
          opt.textContent = item.label ?? item.value ?? "";
          if (item.id) opt.dataset.id = item.id;
          if (item.responsavel_etapa) opt.dataset.responsavelEtapa = item.responsavel_etapa;
          if (item.prazo_etapa) opt.dataset.prazoEtapa = item.prazo_etapa;
        }
        select.appendChild(opt);
      });
      if (prev && Array.from(select.options).some((opt) => opt.value === prev)) {
        select.value = prev;
      }
    };
    const setSelectValueFallback = (select, value) => {
      if (!select) return;
      const text = String(value || "");
      if (text && !Array.from(select.options).some((opt) => opt.value === text)) {
        const opt = document.createElement("option");
        opt.value = text;
        opt.textContent = text;
        select.appendChild(opt);
      }
      select.value = text;
    };
    const filterParams = () => {
      const params = new URLSearchParams();
      if (fields.exercicio?.value) params.set("exercicio", fields.exercicio.value);
      return params;
    };
    const extractMunicipioNameEtapa = (label, value) => {
      const raw = String(label || "").trim();
      if (!raw) return String(value || "").trim();
      const parts = raw.split(" - ");
      return parts.length >= 2 ? parts.slice(1).join(" - ").trim() : raw;
    };
    const buildEtapaPrefix = (municipioLabel, municipioValue) => {
      const valueRaw = String(municipioValue || "").trim();
      if (!valueRaw) return "";
      const labelRaw = String(municipioLabel || "").trim().toLowerCase();
      const valueLower = valueRaw.toLowerCase();
      if (
        valueRaw.startsWith("5100000") ||
        valueLower === "estado" ||
        valueLower.includes("estado mato grosso") ||
        labelRaw === "estado" ||
        labelRaw.includes("estado mato grosso")
      ) {
        return "";
      }
      const name = extractMunicipioNameEtapa(municipioLabel, municipioValue);
      return name ? `${name} * ` : "";
    };
    const stripEtapaLeadingNumber = (value) =>
      String(value || "").replace(/^\s*\d+\s*[-–—]\s*/, "");
    const extractEtapaLeadingNumberPrefix = (value) => {
      const match = String(value || "").match(/^\s*(\d+\s*[-–—]\s*)/);
      return match ? match[1].replace(/[–—]/g, "-") : "";
    };
    const currentEtapaRequiredPrefix = () => {
      const numberPrefix = mode() === "alterar" ? etapaNumberPrefix : "";
      if (!fields.municipio || mode() === "excluir") return numberPrefix;
      const selected = fields.municipio.selectedOptions?.[0];
      const municipioPrefix = buildEtapaPrefix(selected?.textContent || "", fields.municipio.value || "");
      return `${municipioPrefix}${numberPrefix}`;
    };
    const cleanEtapaSuffixForPrefix = (value) => {
      let suffix = String(value || "");
      const municipioPrefixes = Array.from(fields.municipio?.options || [])
        .map((opt) => buildEtapaPrefix(opt.textContent || "", opt.value || ""))
        .filter(Boolean);
      municipioPrefixes.forEach((p) => {
        if (suffix.startsWith(p)) suffix = suffix.slice(p.length).trimStart();
      });
      if (etapaNumberPrefix && suffix.startsWith(etapaNumberPrefix)) {
        suffix = suffix.slice(etapaNumberPrefix.length).trimStart();
      }
      return stripEtapaLeadingNumber(suffix);
    };
    const applyEtapaPrefix = () => {
      if (!fields.etapaNova || mode() === "excluir") return;
      const prefix = currentEtapaRequiredPrefix();
      if (!prefix) return;
      const current = String(fields.etapaNova.value || "");
      if (!current) {
        fields.etapaNova.value = prefix;
        return;
      }
      if (current.startsWith(prefix)) return;
      const suffix = cleanEtapaSuffixForPrefix(current);
      fields.etapaNova.value = `${prefix}${suffix}`.slice(0, 260);
    };
    const protectEtapaPrefix = () => {
      if (!fields.etapaNova || mode() === "excluir") return;
      const prefix = currentEtapaRequiredPrefix();
      if (!prefix) return;
      const current = String(fields.etapaNova.value || "");
      if (current.startsWith(prefix)) return;
      const cleaned = cleanEtapaSuffixForPrefix(current.replace(prefix.trim(), "").trimStart());
      fields.etapaNova.value = `${prefix}${cleaned}`.slice(0, 260);
      try {
        fields.etapaNova.setSelectionRange(prefix.length, prefix.length);
      } catch (e) {
        // noop
      }
    };
    const uniqueSorted = (values) =>
      Array.from(new Set((values || []).map((v) => String(v || "").trim()).filter(Boolean))).sort((a, b) =>
        a.localeCompare(b, "pt-BR", { sensitivity: "base" })
      );
    const catalogRowsFor = (skipKey = "") => {
      const selected = {
        unidade_orcamentaria: fields.uo?.value || "",
        programa: fields.programa?.value || "",
        produto_acao: fields.produto?.value || "",
        acao_paoe: fields.acao?.value || "",
        subacao_entrega: fields.subacao?.value || "",
      };
      return optionRowsCatalog.filter((row) =>
        Object.entries(selected).every(([key, value]) => {
          if (key === skipKey || !value) return true;
          return String(row[key] || "") === value;
        })
      );
    };
    const refreshCascadeOptionsFromCatalog = () => {
      if (!optionRowsCatalog.length) return false;
      fillSelect(fields.uo, uniqueSorted(catalogRowsFor("unidade_orcamentaria").map((r) => r.unidade_orcamentaria)));
      fillSelect(fields.programa, uniqueSorted(catalogRowsFor("programa").map((r) => r.programa)));
      fillSelect(fields.produto, uniqueSorted(catalogRowsFor("produto_acao").map((r) => r.produto_acao)));
      fillSelect(fields.acao, uniqueSorted(catalogRowsFor("acao_paoe").map((r) => r.acao_paoe)));
      fillSelect(fields.subacao, uniqueSorted(catalogRowsFor("subacao_entrega").map((r) => r.subacao_entrega)));
      fillSelect(fields.adj, adjOptionsCatalog);
      const etapaRows = catalogRowsFor("");
      const municipioMap = new Map();
      etapaRows.forEach((row) => {
        const municipio = String(row.municipios_entrega || "").trim();
        const codigo = String(row.codigo || "").trim();
        if (!municipio) return;
        const key = `${codigo}::${municipio}`;
        if (!municipioMap.has(key)) {
          municipioMap.set(key, {
            value: municipio,
            label: codigo ? `${codigo} - ${municipio}` : municipio,
            codigo,
          });
        }
      });
      const municipios = Array.from(municipioMap.values());
      fillSelect(fields.municipio, municipios);
      if (municipioWrap) municipioWrap.style.display = municipios.length > 1 ? "" : "none";
      if (fields.municipio) {
        if (municipios.length === 1) {
          fields.municipio.value = municipios[0].value;
        } else if (municipios.length > 1) {
          fields.municipio.value = "";
        }
      }
      const seenEtapas = new Set();
      const etapas = [];
      etapaRows.forEach((row) => {
        const etapa = String(row.etapa || "").trim();
        if (!etapa || seenEtapas.has(etapa)) return;
        seenEtapas.add(etapa);
        etapas.push({
          id: row.id,
          value: etapa,
          label: etapa,
          responsavel_etapa: row.responsavel_etapa || "",
          prazo_etapa: row.prazo_etapa || "",
        });
      });
      fillSelect(fields.etapaSelect, etapas);
      return true;
    };
    const loadOptions = async () => {
      if (fields.exercicio) fields.exercicio.disabled = true;
      const params = filterParams().toString();
      let data = optionsCache.get(params);
      if (!data) {
        const res = await fetch(`/api/etapa/options?${params}`);
        data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar opcoes.");
        optionsCache.set(params, data);
      }
      optionRowsCatalog = Array.isArray(data.option_rows) ? data.option_rows : [];
      adjOptionsCatalog = data.adj_solicitante || [];
      responsaveisNgerCatalog = data.responsaveis_nger || [];
      fillSelect(fields.exercicio, data.exercicio || []);
      if (fields.exercicio) {
        fields.exercicio.value = (data.exercicio || [])[0] || fields.exercicio.value || "";
        fields.exercicio.disabled = true;
      }
      if (!refreshCascadeOptionsFromCatalog()) {
        fillSelect(fields.produto, data.produto_acao || []);
        fillSelect(fields.uo, data.unidade_orcamentaria || []);
        fillSelect(fields.programa, data.programa || []);
        fillSelect(fields.acao, data.acao_paoe || []);
        fillSelect(fields.subacao, data.subacao_entrega || []);
        fillSelect(fields.adj, data.adj_solicitante || []);
        fillSelect(fields.etapaSelect, data.etapas || []);
      }
      fillSelect(fields.responsavelNger, responsaveisNgerCatalog);
    };
    const mode = () => {
      if (getRadio("etapa_alterar") === "sim") return "alterar";
      if (getRadio("etapa_cadastrar") === "sim") return "cadastrar";
      if (getRadio("etapa_excluir") === "sim") return "excluir";
      return "";
    };
    const hideEtapaForm = () => {
      if (formSection) formSection.style.display = "none";
      if (origemGrid) origemGrid.style.display = "none";
      if (approvalFields && !approvalMode) approvalFields.style.display = "none";
    };
    const setEtapaApprovalProtected = (enabled) => {
      if (enabled && approvalDisabledSnapshot) {
        setEtapaApprovalProtected(false);
      }
      const controls = Array.from(form.querySelectorAll("button, input, select, textarea"));
      if (enabled) {
        approvalDisabledSnapshot = new Map();
      }
      controls.forEach((el) => {
        if (el.type === "hidden") return;
        if (approvalFields && approvalFields.contains(el)) {
          el.disabled = false;
          return;
        }
        if (enabled && approvalDisabledSnapshot) {
          approvalDisabledSnapshot.set(el, el.disabled);
          el.disabled = true;
          return;
        }
        if (!enabled && approvalDisabledSnapshot?.has(el)) {
          el.disabled = approvalDisabledSnapshot.get(el);
        }
      });
      if (!enabled) {
        approvalDisabledSnapshot = null;
      }
      if (saveBtn) saveBtn.disabled = false;
      if (clearBtn) clearBtn.disabled = Boolean(enabled);
      if (approvalFields) {
        approvalFields.querySelectorAll("input, select, textarea, button").forEach((el) => {
          el.disabled = false;
        });
      }
    };
    const resetEtapaFormFields = ({ keepEtapaSelect = false } = {}) => {
      if (fields.id) fields.id.value = "";
      if (fields.plan21Id) fields.plan21Id.value = "";
      [
        fields.origem,
        fields.responsavelOrigem,
        fields.prazoOrigem,
        fields.etapaNova,
        fields.responsavelNovo,
        fields.cpfNovo,
        fields.emailNovo,
        fields.inicioNovo,
        fields.fimNovo,
        fields.justificativa,
        fields.responsavelNger,
        fields.justificativaAprovacao,
      ].forEach((el) => {
        if (el) el.value = "";
      });
      etapaNumberPrefix = "";
      if (fields.municipio) fields.municipio.value = "";
      if (fields.etapaSelect && !keepEtapaSelect) fields.etapaSelect.value = "";
      if (editBadge) {
        editBadge.textContent = "";
        editBadge.style.display = "none";
      }
      if (saveBtn) saveBtn.textContent = "Salvar";
    };
    const syncQuestions = () => {
      const qAlterar = getRadio("etapa_alterar");
      const qCadastrar = getRadio("etapa_cadastrar");
      if (cadastrarQuestion) cadastrarQuestion.style.display = qAlterar === "nao" ? "" : "none";
      if (excluirQuestion) excluirQuestion.style.display = qAlterar === "nao" && qCadastrar === "nao" ? "" : "none";
    };
    const syncMode = (showForm = false) => {
      const m = mode();
      const isAlterar = m === "alterar";
      const isExcluir = m === "excluir";
      syncQuestions();
      if (selectWrap) selectWrap.style.display = isAlterar || isExcluir ? "" : "none";
      if (consultarWrap) consultarWrap.style.display = m ? "" : "none";
      if (consultarBtn) consultarBtn.textContent = m === "cadastrar" ? "Cadastrar" : "Consultar";
      if (formSection) formSection.style.display = showForm && m ? "" : "none";
      if (origemGrid) origemGrid.style.display = showForm && (isAlterar || isExcluir) ? "" : "none";
      if (formTitle) {
        formTitle.textContent = isExcluir
          ? "Excluir Etapa"
          : isAlterar
            ? "Alterar Etapa - antes e depois"
            : "Cadastrar Nova Etapa";
      }
      const novoFields = [
        fields.etapaNova,
        fields.responsavelNovo,
        fields.cpfNovo,
        fields.emailNovo,
        fields.inicioNovo,
        fields.fimNovo,
      ];
      novoFields.forEach((el) => {
        if (el) el.disabled = isExcluir;
      });
      novoFieldWraps.forEach((wrap) => {
        wrap.style.display = isExcluir ? "none" : "";
      });
      if (municipioWrap && !isExcluir) {
        municipioWrap.style.display = (fields.municipio?.options?.length || 0) > 2 ? "" : "none";
      }
      if (!isAlterar && !isExcluir && fields.plan21Id) fields.plan21Id.value = "";
      if (!m) hideEtapaForm();
    };
    const clearForm = () => {
      setEtapaApprovalProtected(false);
      approvalMode = false;
      form.reset();
      resetEtapaFormFields();
      if (approvalFields) approvalFields.style.display = "none";
      syncMode(false);
      setMsg("");
      loadOptions().catch((err) => setMsg(err.message, true));
    };
    const loadSelectedEtapa = async () => {
      if (!fields.etapaSelect?.value) return;
      const currentMode = mode();
      let row = currentMode === "excluir"
        ? null
        : catalogRowsFor("").find((item) => String(item.etapa || "") === fields.etapaSelect.value);
      if (!row) {
        const params = new URLSearchParams();
        if (fields.exercicio?.value) params.set("exercicio", fields.exercicio.value);
        if (fields.uo?.value) params.set("unidade_orcamentaria", fields.uo.value);
        if (fields.programa?.value) params.set("programa", fields.programa.value);
        if (fields.produto?.value) params.set("produto_acao", fields.produto.value);
        if (fields.acao?.value) params.set("acao_paoe", fields.acao.value);
        if (fields.subacao?.value) params.set("subacao_entrega", fields.subacao.value);
        params.set("etapa", fields.etapaSelect.value);
        if (currentMode === "excluir") params.set("validar_exclusao", "1");
        const res = await fetch(`/api/etapa/plan21-rows?${params.toString()}`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar etapa.");
        row = (data.rows || [])[0];
      }
      if (!row) {
        setMsg("Etapa selecionada nao encontrada.", true);
        return;
      }
      fields.plan21Id.value = row.id || "";
      fields.origem.value = row.etapa || "";
      fields.responsavelOrigem.value = row.responsavel_etapa || "";
      fields.prazoOrigem.value = row.prazo_etapa || "";
      if (mode() !== "excluir") {
        etapaNumberPrefix = mode() === "alterar" ? extractEtapaLeadingNumberPrefix(row.etapa || "") : "";
        fields.etapaNova.value = `${etapaNumberPrefix}${stripEtapaLeadingNumber(row.etapa || "")}`.slice(0, 260);
        fields.responsavelNovo.value = row.responsavel_etapa || "";
        const municipioCount = Math.max(0, (fields.municipio?.options?.length || 0) - 1);
        if (fields.municipio && row.municipios_entrega && municipioCount <= 1) {
          setSelectValueFallback(fields.municipio, row.municipios_entrega || "");
          applyEtapaPrefix();
        } else if (fields.municipio && municipioCount > 1) {
          fields.municipio.value = "";
        }
      }
    };
    const payload = () => ({
      tipo_solicitacao: approvalMode ? selectedRow?.dataset.tipoSolicitacao || mode() : mode(),
      exercicio: fields.exercicio?.value || "",
      unidade_orcamentaria: fields.uo?.value || "",
      programa: fields.programa?.value || "",
      produto_acao: fields.produto?.value || "",
      acao_paoe: fields.acao?.value || "",
      subacao_entrega: fields.subacao?.value || "",
      adj_solicitante: fields.adj?.value || "",
      plan21_nger_id: fields.plan21Id?.value || "",
      municipios_entrega: fields.municipio?.value || "",
      etapa_nova: fields.etapaNova?.value || "",
      responsavel_etapa_novo: fields.responsavelNovo?.value || "",
      cpf_responsavel_etapa_novo: fields.cpfNovo?.value || "",
      email_responsavel_etapa_novo: fields.emailNovo?.value || "",
      data_inicio_novo: fields.inicioNovo?.value || "",
      data_fim_novo: fields.fimNovo?.value || "",
      justificativa: fields.justificativa?.value || "",
      responsavel_nger: fields.responsavelNger?.value || "",
    });
    const buildPrintDatasetFromPayload = (registro, payloadData) => ({
      controleEtapa: registro?.controle_etapa || `ETAPA.${payloadData.exercicio || ""}.${registro?.id || ""}`,
      statusAprovacao: registro?.status_aprovacao || "Aguardando",
      tipoSolicitacao: registro?.tipo_solicitacao || payloadData.tipo_solicitacao || "",
      criadoEm: registro?.criado_em || "",
      usuarioNome: String(page.dataset.userNome || "").trim(),
      usuarioPerfil: "",
      aprovadoPorNome: "",
      aprovadoPorPerfil: "",
      dataAprovacao: "",
      motivoRejeicao: registro?.motivo_rejeicao || "",
      exercicio: registro?.exercicio || payloadData.exercicio || "",
      acaoPaoe: registro?.acao_paoe || payloadData.acao_paoe || "",
      produtoAcao: registro?.produto_acao || payloadData.produto_acao || "",
      subacaoEntrega: registro?.subacao_entrega || payloadData.subacao_entrega || "",
      etapaOrigem: registro?.etapa_origem || fields.origem?.value || "",
      responsavelEtapaOrigem: registro?.responsavel_etapa_origem || fields.responsavelOrigem?.value || "",
      etapaNova: registro?.etapa_nova || payloadData.etapa_nova || "",
      responsavelEtapaNovo: registro?.responsavel_etapa_novo || payloadData.responsavel_etapa_novo || "",
      cpfResponsavelEtapaNovo: registro?.cpf_responsavel_etapa_novo || payloadData.cpf_responsavel_etapa_novo || "",
      dataInicioNovo: registro?.data_inicio_novo || payloadData.data_inicio_novo || "",
      dataFimNovo: registro?.data_fim_novo || payloadData.data_fim_novo || "",
      justificativa: registro?.justificativa || payloadData.justificativa || "",
      responsavelNger: registro?.responsavel_nger || payloadData.responsavel_nger || "",
    });
    const etapaPrintTitle = (tipo) => {
      const key = String(tipo || "").toLowerCase();
      if (key === "alterar") return "ALTERAR ETAPA";
      if (key === "excluir") return "EXCLUIR ETAPA";
      return "CADASTRAR ETAPA";
    };
    const formatEtapaPrintDate = (value) => {
      const raw = String(value || "").trim();
      if (!raw) return "";
      const parsed = new Date(raw);
      if (Number.isNaN(parsed.getTime())) return raw;
      return parsed.toLocaleString("pt-BR", {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      });
    };
    const etapaPrintRows = (d) => {
      const formatDateOnly = (value) => {
        const raw = String(value || "").trim();
        if (!raw) return "";
        const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
        if (match) return `${match[3]}/${match[2]}/${match[1]}`;
        const parsed = new Date(raw);
        if (Number.isNaN(parsed.getTime())) return raw;
        return parsed.toLocaleDateString("pt-BR");
      };
      const prazo = [formatDateOnly(d.dataInicioNovo), formatDateOnly(d.dataFimNovo)].filter(Boolean).join(" à ");
      const controlePrefix = d.controleEtapa ? `${d.controleEtapa}* ` : "";
      const stripControleJustificativaPrefix = (value) =>
        String(value || "").replace(/^ETAPA\.[^*]+\*\s*/i, "").trimStart();
      const justificativaPrint = controlePrefix
        ? `${controlePrefix}${stripControleJustificativaPrefix(d.justificativa)}`
        : d.justificativa;
      const motivoRejeicao = String(d.motivoRejeicao || d.motivo_rejeicao || "").trim();
      const statusAprovacao = String(d.statusAprovacao || d.status_aprovacao || "").trim().toLowerCase();
      const rejectionRows = statusAprovacao === "rejeitado" && motivoRejeicao
        ? [["Motivo da Rejeição", motivoRejeicao, true]]
        : [];
      const formatCpf = (value) => {
        const digits = String(value || "").replace(/\D/g, "").slice(0, 11);
        if (digits.length !== 11) return String(value || "").trim();
        return digits.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/, "$1.$2.$3-$4");
      };
      const cpfNovo = formatCpf(d.cpfResponsavelEtapaNovo);
      const responsavelAlterado = cpfNovo
        ? `${d.responsavelEtapaNovo || ""} - CPF: ${cpfNovo}`.trim()
        : d.responsavelEtapaNovo;
      const contextRows = [
        ["Exercício", d.exercicio],
        ["Ação/PAOE", d.acaoPaoe],
        ["Produto da Ação", d.produtoAcao],
        ["Subação/Entrega", d.subacaoEntrega],
      ];
      const tipoSolicitacao = String(d.tipoSolicitacao || "").trim().toLowerCase();
      let rows;
      if (tipoSolicitacao === "cadastrar") {
        rows = [
          ...contextRows,
          ["Nome da Etapa", d.etapaNova],
          ["Responsável", responsavelAlterado],
          ["Prazo", prazo],
          ["Justificativa", justificativaPrint],
          ["Responsável NGER", d.responsavelNger],
          ...rejectionRows,
        ];
      } else if (tipoSolicitacao === "excluir") {
        rows = [
          ...contextRows,
          ["Nome da Etapa", d.etapaOrigem],
          ["Responsável", d.responsavelEtapaOrigem],
          ["Justificativa", justificativaPrint],
          ["Responsável NGER", d.responsavelNger],
          ...rejectionRows,
        ];
      } else {
        rows = [
          ...contextRows,
          ["Nome da Etapa", d.etapaOrigem],
          ["Responsável", d.responsavelEtapaOrigem],
          ["Nome da Etapa Alterada", d.etapaNova, true],
          ["Responsável Alterado", responsavelAlterado, true],
          ["Novo Prazo", prazo, true],
          ["Justificativa", justificativaPrint],
          ["Responsável NGER", d.responsavelNger],
          ...rejectionRows,
        ];
      }
      rows = rows.filter((row) => String(row[1] || "").trim());
      return `
        <table class="print-table">
          <tbody>
            ${rows
              .map(([label, value, highlight]) => `<tr${highlight ? ' class="print-row-highlight"' : ""}><th>${esc(label)}</th><td>${esc(value)}</td></tr>`)
              .join("")}
          </tbody>
        </table>
      `;
    };
    const prepareEtapaPrintWindow = () => {
      const win = window.open("", "_blank");
      if (!win) return null;
      try {
        win.document.open();
        win.document.write("<!doctype html><html><head><meta charset=\"utf-8\" /><title>Preparando impressão...</title></head><body>Preparando impressão...</body></html>");
        win.document.close();
      } catch (err) {
        // noop
      }
      return win;
    };
    const printEtapaRecord = (d, targetWin = null) => {
      const titulo = etapaPrintTitle(d.tipoSolicitacao);
      const controle = d.controleEtapa || "";
      const criadoEm = formatEtapaPrintDate(d.criadoEm || "");
      const usuarioNome = d.usuarioNome || "";
      const usuarioPerfil = d.usuarioPerfil || "";
      const aprovadoNome = d.aprovadoPorNome || "";
      const aprovadoPerfil = d.aprovadoPorPerfil || "";
      const aprovadoEm = formatEtapaPrintDate(d.dataAprovacao || "");
      const status = String(d.statusAprovacao || "").trim().toLowerCase();
      const joinNonEmptyLocal = (parts, sep = " - ") =>
        (parts || []).map((part) => String(part || "").trim()).filter(Boolean).join(sep);
      const footerLine2 = joinNonEmptyLocal([
        joinNonEmptyLocal([usuarioNome, usuarioPerfil]),
        criadoEm ? `cadastrado em ${criadoEm}` : "",
        controle,
      ]);
      const statusEventoLabel =
        status === "rejeitado" ? "rejeitado em" : status === "aprovado" ? "aprovado em" : "aprovado em";
      const footerLine3 = joinNonEmptyLocal([
        joinNonEmptyLocal([aprovadoNome, aprovadoPerfil]),
        aprovadoEm ? `${statusEventoLabel} ${aprovadoEm}` : "",
      ]);
      let watermarkText = "";
      if (status === "aguardando") watermarkText = "AGUARDANDO";
      if (status === "rejeitado") watermarkText = "REJEITADO";
      const win = targetWin && !targetWin.closed ? targetWin : window.open("", "_blank");
      if (!win) return;
      win.document.open();
      win.document.write(`<!doctype html><html><head><meta charset="utf-8"><title>Etapa</title>
        <style>
          body { font-family: Arial, sans-serif; color: #000; margin: 12px 20px 24px; padding-bottom: 80px; }
          .print-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; padding-bottom: 6px; border-bottom: 1px dashed #000; }
          .print-brand { display: flex; align-items: center; gap: 12px; }
          .print-brand img { height: 48px; }
          .print-brand-title { font-weight: 700; font-size: 16px; }
          .print-brand-subtitle { font-size: 12px; color: #333; }
          .print-title-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin: 8px 0 18px; }
          .print-title { text-align: center; font-weight: 700; flex: 1; text-transform: uppercase; }
          .print-title-key { min-width: 200px; font-size: 12px; }
          .print-title-date { min-width: 200px; text-align: right; font-size: 12px; }
          .print-footer { position: fixed; left: 20px; right: 20px; bottom: 12px; border-top: 1px dashed #000; font-size: 11px; padding-top: 6px; display: flex; align-items: center; justify-content: space-between; gap: 12px; }
          .print-footer img { height: 36px; }
          .print-footer-text { flex: 1; text-align: center; line-height: 1.35; }
          .print-body { margin-top: 4em; }
          .print-table { width: 100%; border-collapse: collapse; margin-bottom: 12px; table-layout: fixed; }
          .print-table th, .print-table td { border: 1px solid #000; padding: 6px 8px; text-align: left; font-size: 10px; vertical-align: top; word-break: break-word; }
          .print-table th { width: 180px; background: #dddddd; box-shadow: inset 0 0 0 9999px #dddddd; -webkit-print-color-adjust: exact; print-color-adjust: exact; color-adjust: exact; }
          .print-row-highlight td { background: #eeeeee; box-shadow: inset 0 0 0 9999px #eeeeee; -webkit-print-color-adjust: exact; print-color-adjust: exact; color-adjust: exact; }
          .print-watermark { position: fixed; top: 45%; left: 50%; transform: translate(-50%, -50%) rotate(-30deg); font-size: 60px; color: rgba(0,0,0,0.12); font-family: "Arial Black", Arial, sans-serif; text-transform: uppercase; white-space: pre-line; text-align: center; pointer-events: none; }
        </style>
        </head><body>
        ${watermarkText ? `<div class="print-watermark">${esc(watermarkText)}</div>` : ""}
        <div class="print-header">
          <div class="print-brand">
            <img src="/static/img/logo.jpg" alt="Logo" />
            <div class="print-brand-text">
              <div class="print-brand-title">Sistema de Planejamento e Orçamento</div>
              <div class="print-brand-subtitle">SPO-NGER-SEDUCMT</div>
            </div>
          </div>
        </div>
        <div class="print-title-row">
          <div class="print-title-key">${esc(controle)}</div>
          <div class="print-title">${esc(titulo)}</div>
          <div class="print-title-date">${esc(criadoEm)}</div>
        </div>
        <div class="print-body">${etapaPrintRows(d)}</div>
        <div class="print-footer">
          <img src="/static/img/logo.jpg" alt="Logo" />
          <div class="print-footer-text">
            ${footerLine2 ? `<div>${esc(footerLine2)}</div>` : ""}
            ${footerLine3 ? `<div>${esc(footerLine3)}</div>` : ""}
          </div>
          <img src="/static/img/logoseduc.jpg" alt="Logo Seduc" />
        </div>
        </body></html>`);
      win.document.close();
      win.focus();
      win.print();
    };

    const allRows = () => Array.from(tbody ? tbody.querySelectorAll("tr") : []);
    const datasetKey = (field) => String(field || "").replace(/_([a-z])/g, (_, ch) => ch.toUpperCase());
    const parseFilterValue = (value) => {
      const raw = String(value ?? "").trim();
      if (!raw) return { raw, num: null };
      const num = Number(raw.replace(/\./g, "").replace(",", "."));
      return Number.isNaN(num) ? { raw, num: null } : { raw, num };
    };
    const compareFilterValues = (left, right, op) => {
      const l = parseFilterValue(left);
      const r = parseFilterValue(right);
      if (l.num !== null && r.num !== null) {
        if (op === "eq") return l.num === r.num;
        if (op === "gt") return l.num > r.num;
        if (op === "lt") return l.num < r.num;
        if (op === "gte") return l.num >= r.num;
        if (op === "lte") return l.num <= r.num;
      }
      const lraw = l.raw.toLowerCase();
      const rraw = r.raw.toLowerCase();
      const cmp = lraw.localeCompare(rraw, "pt-BR", { sensitivity: "base" });
      if (op === "eq") return cmp === 0;
      if (op === "contains") return lraw.includes(rraw);
      if (op === "gt") return cmp > 0;
      if (op === "lt") return cmp < 0;
      if (op === "gte") return cmp >= 0;
      if (op === "lte") return cmp <= 0;
      return false;
    };
    const rowMatchesFilters = (row) => {
      if (!filters.length) return true;
      return filters.every((f) => {
        const raw = String(row.dataset[datasetKey(f.field)] || "");
        const val = String(f.value || "");
        return compareFilterValues(raw, val, f.op);
      });
    };
    const visibleRows = () => allRows().filter(rowMatchesFilters);
    const renderPagination = () => {
      const rows = visibleRows();
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      currentPage = Math.min(currentPage, totalPages);
      allRows().forEach((row) => (row.style.display = "none"));
      rows.forEach((row, idx) => {
        const show = idx >= (currentPage - 1) * pageSize && idx < currentPage * pageSize;
        row.style.display = show ? "" : "none";
      });
      if (!paginationEl) return;
      paginationEl.innerHTML = "";
      const addBtn = (label, page, disabled = false, active = false) => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "page-btn";
        btn.textContent = label;
        if (disabled) btn.disabled = true;
        if (active) btn.classList.add("active");
        btn.addEventListener("click", () => {
          if (disabled || page === currentPage) return;
          currentPage = page;
          renderPagination();
        });
        paginationEl.appendChild(btn);
      };
      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);
      const maxButtons = 5;
      let start = Math.max(1, currentPage - Math.floor(maxButtons / 2));
      let end = Math.min(totalPages, start + maxButtons - 1);
      if (end - start + 1 < maxButtons) {
        start = Math.max(1, end - maxButtons + 1);
      }
      if (start > 1) {
        addBtn("1", 1, false, currentPage === 1);
        if (start > 2) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          paginationEl.appendChild(ellipsis);
        }
      }
      for (let p = 1; p <= totalPages; p += 1) {
        if (p < start || p > end) continue;
        addBtn(String(p), p, false, p === currentPage);
      }
      if (end < totalPages) {
        const ellipsis = document.createElement("span");
        ellipsis.textContent = "...";
        paginationEl.appendChild(ellipsis);
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }
      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };
    const renderFilterList = () => {
      if (!filterList) return;
      filterList.innerHTML = "";
      const opLabels = {
        eq: "Igual a",
        contains: "Contém",
        gt: "Maior que",
        lt: "Menor que",
        gte: "Maior igual a",
        lte: "Menor igual a",
      };
      filters.forEach((f, idx) => {
        const li = document.createElement("li");
        li.className = "pill";
        li.dataset.idx = String(idx);
        li.textContent = `${f.label} ${opLabels[f.op] || f.op} ${f.value}`;
        if (idx === filterSelected) li.classList.add("selected");
        li.addEventListener("click", () => {
          filterSelected = idx;
          renderFilterList();
        });
        filterList.appendChild(li);
      });
    };
    const fillFromRow = (row, forApproval = false) => {
      if (!row) return;
      setEtapaApprovalProtected(false);
      approvalMode = Boolean(forApproval);
      if (fields.id) fields.id.value = row.dataset.id || "";
      setRadio("etapa_alterar", row.dataset.tipoSolicitacao === "alterar" ? "sim" : "nao");
      setRadio("etapa_cadastrar", row.dataset.tipoSolicitacao === "cadastrar" ? "sim" : "nao");
      setSelectValueFallback(fields.exercicio, row.dataset.exercicio || "");
      setSelectValueFallback(fields.uo, row.dataset.unidadeOrcamentaria || "");
      setSelectValueFallback(fields.programa, row.dataset.programa || "");
      setSelectValueFallback(fields.produto, row.dataset.produtoAcao || "");
      setSelectValueFallback(fields.acao, row.dataset.acaoPaoe || "");
      setSelectValueFallback(fields.subacao, row.dataset.subacaoEntrega || "");
      setSelectValueFallback(fields.adj, row.dataset.adjSolicitante || "");
      fields.plan21Id.value = row.dataset.plan21NgerId || "";
      fields.origem.value = row.dataset.etapaOrigem || "";
      fields.responsavelOrigem.value = row.dataset.responsavelEtapaOrigem || "";
      fields.prazoOrigem.value = [row.dataset.dataInicioOrigem, row.dataset.dataFimOrigem].filter(Boolean).join(" à ");
      fields.etapaNova.value = row.dataset.etapaNova || "";
      fields.responsavelNovo.value = row.dataset.responsavelEtapaNovo || "";
      fields.cpfNovo.value = row.dataset.cpfResponsavelEtapaNovo || "";
      fields.emailNovo.value = row.dataset.emailResponsavelEtapaNovo || "";
      fields.inicioNovo.value = row.dataset.dataInicioNovo || "";
      fields.fimNovo.value = row.dataset.dataFimNovo || "";
      fields.justificativa.value = row.dataset.justificativa || "";
      fields.responsavelNger.value = row.dataset.responsavelNger || "";
      syncMode(true);
      if (approvalFields) approvalFields.style.display = forApproval ? "" : "none";
      if (saveBtn) saveBtn.textContent = forApproval ? "Confirmar" : "Salvar";
      setEtapaApprovalProtected(Boolean(forApproval));
      if (editBadge) {
        const controle = row.dataset.controleEtapa || row.dataset.id || "";
        editBadge.textContent = forApproval
          ? `- Aprovação do registro ${controle}`
          : `- Edição do registro ${controle}`;
        editBadge.style.display = "inline";
      }
      window.scrollTo({ top: 0, behavior: "smooth" });
    };

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      setMsg("");
      try {
        if (approvalMode) {
          const id = selectedRow?.dataset.id || fields.id?.value;
          const aprovado = form.querySelector('input[name="etapa-aprovada"]:checked')?.value || "sim";
          const motivo = fields.justificativaAprovacao?.value || "";
          const pendingPrintWin = prepareEtapaPrintWindow();
          const res = await fetch(`/api/etapa/${encodeURIComponent(id)}/aprovar`, {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify({ etapa_aprovada: aprovado, motivo_rejeicao: motivo }),
          });
          const data = await res.json();
          if (!res.ok) {
            if (pendingPrintWin && !pendingPrintWin.closed) pendingPrintWin.close();
            throw new Error(data.error || "Falha ao aprovar.");
          }
          const printData = {
            ...(selectedRow?.dataset || {}),
            ...(data.etapa ? buildPrintDatasetFromPayload(data.etapa, payload()) : {}),
            controleEtapa: selectedRow?.dataset.controleEtapa || data.etapa?.controle_etapa || "",
            statusAprovacao: aprovado === "sim" ? "Aprovado" : "Rejeitado",
            motivoRejeicao: aprovado === "nao" ? motivo : "",
            motivo_rejeicao: aprovado === "nao" ? motivo : "",
            dataAprovacao: data.etapa?.data_aprovacao || "",
            usuarioNome: selectedRow?.dataset.usuarioNome || "",
            usuarioPerfil: selectedRow?.dataset.usuarioPerfil || "",
            aprovadoPorNome: String(page.dataset.userNome || "").trim(),
          };
          printEtapaRecord(printData, pendingPrintWin || null);
          await loadPage("cadastrar/plan_21-nger/etapa/consultar");
          return;
        }
        const m = mode();
        if (!m) throw new Error("Selecione se deseja alterar, cadastrar ou excluir uma etapa.");
        if ((m === "alterar" || m === "excluir") && !fields.plan21Id?.value) throw new Error("Selecione uma etapa cadastrada.");
        const editId = fields.id?.value || "";
        const payloadData = payload();
        const res = await fetch(editId ? `/api/etapa/${encodeURIComponent(editId)}` : "/api/etapa", {
          method: editId ? "PUT" : "POST",
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payloadData),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao salvar.");
        printEtapaRecord(buildPrintDatasetFromPayload(data.etapa || {}, payloadData));
        await loadPage("cadastrar/plan_21-nger/etapa/consultar");
      } catch (err) {
        setMsg(err.message || "Falha ao salvar.", true);
      }
    });

    [fields.uo, fields.programa, fields.produto, fields.acao, fields.subacao].forEach((select) => {
      if (!select) return;
      select.addEventListener("change", () => {
        refreshCascadeOptionsFromCatalog();
      });
    });
    const requiredEtapaFilters = [
      { el: fields.exercicio, label: "Exercício" },
      { el: fields.uo, label: "UO" },
      { el: fields.programa, label: "Programa de Governo" },
      { el: fields.produto, label: "Produto da Ação" },
      { el: fields.acao, label: "Ação/PAOE" },
      { el: fields.subacao, label: "Subação/Entrega" },
      { el: fields.adj, label: "Adjunta Solicitante" },
    ];
    const validateEtapaRequiredFilters = () => {
      const missing = requiredEtapaFilters.find((item) => !String(item.el?.value || "").trim());
      if (!missing) return true;
      hideEtapaForm();
      resetEtapaFormFields();
      syncMode(false);
      setMsg(`Preencha todos os filtros obrigatórios antes de consultar. Campo pendente: ${missing.label}.`, true);
      if (missing.el && !missing.el.disabled) {
        try {
          missing.el.focus();
        } catch (err) {
          // noop
        }
      }
      return false;
    };
    form.querySelectorAll('input[name="etapa_alterar"], input[name="etapa_cadastrar"], input[name="etapa_excluir"]').forEach((input) => {
      input.addEventListener("change", () => {
        if (input.name === "etapa_alterar") {
          clearRadio("etapa_cadastrar");
          clearRadio("etapa_excluir");
        }
        if (input.name === "etapa_cadastrar") {
          clearRadio("etapa_excluir");
        }
        if (input.value === "sim" && input.checked) {
          ["etapa_alterar", "etapa_cadastrar", "etapa_excluir"].forEach((name) => {
            if (name === input.name) return;
            if (name === "etapa_excluir") {
              const excluirSim = form.querySelector('input[name="etapa_excluir"][value="sim"]');
              if (excluirSim) excluirSim.checked = false;
            } else {
              setRadio(name, "nao");
            }
          });
        }
        hideEtapaForm();
        resetEtapaFormFields();
        syncMode(false);
      });
    });
    consultarBtn?.addEventListener("click", async () => {
      try {
        setMsg("");
        const m = mode();
        resetEtapaFormFields({ keepEtapaSelect: true });
        if (!m) {
          setMsg("Responda Sim para uma das opções antes de consultar.", true);
          return;
        }
        if (!validateEtapaRequiredFilters()) {
          return;
        }
        if ((m === "alterar" || m === "excluir") && !fields.etapaSelect?.value) {
          setMsg("Selecione uma etapa cadastrada antes de consultar.", true);
          return;
        }
        if (m === "alterar" || m === "excluir") {
          await loadSelectedEtapa();
        }
        syncMode(true);
      } catch (err) {
        setMsg(err.message || "Falha ao consultar etapa.", true);
      }
    });
    form.addEventListener("input", () => setMsg(""));
    form.addEventListener("change", () => setMsg(""));
    fields.etapaSelect?.addEventListener("change", () => {
      setMsg("");
      hideEtapaForm();
      resetEtapaFormFields({ keepEtapaSelect: true });
      syncMode(false);
    });
    fields.municipio?.addEventListener("change", applyEtapaPrefix);
    fields.etapaNova?.addEventListener("input", protectEtapaPrefix);
    fields.etapaNova?.addEventListener("blur", protectEtapaPrefix);
    fields.cpfNovo?.addEventListener("input", () => {
      const digits = fields.cpfNovo.value.replace(/\D/g, "").slice(0, 11);
      fields.cpfNovo.value = digits
        .replace(/(\d{3})(\d)/, "$1.$2")
        .replace(/(\d{3})(\d)/, "$1.$2")
        .replace(/(\d{3})(\d{1,2})$/, "$1-$2");
    });
    clearBtn?.addEventListener("click", clearForm);

    tbody?.addEventListener("click", (ev) => {
      const row = ev.target.closest("tr");
      if (!row) return;
      allRows().forEach((r) => r.classList.remove("selected"));
      row.classList.add("selected");
      selectedRow = row;
    });
    approveBtn?.addEventListener("click", () => {
      setMsg("");
      if (!selectedRow) return setFilterMsg("Selecione um registro para aprovar.", true);
      const status = String(selectedRow.dataset.statusAprovacao || "").toLowerCase();
      if (status && status !== "aguardando") return setFilterMsg("Somente registros aguardando podem ser aprovados.", true);
      if (![1, 2].includes(nivelAtual)) return setFilterMsg("Usuario sem permissao para aprovar.", true);
      if (etapaIsConsultaView) {
        sessionStorage.setItem(
          etapaPendingActionKey,
          JSON.stringify({ action: "approve", id: selectedRow.dataset.id || "" })
        );
        loadPage("cadastrar/plan_21-nger/etapa/formulario");
        return;
      }
      setFilterMsg("");
      fillFromRow(selectedRow, true);
    });
    editBtn?.addEventListener("click", () => {
      setMsg("");
      if (!selectedRow) return setFilterMsg("Selecione um registro para editar.", true);
      const status = String(selectedRow.dataset.statusAprovacao || "").toLowerCase();
      if (status && status !== "aguardando") return setFilterMsg("Somente registros aguardando podem ser editados.", true);
      if (selectedRow.dataset.criadorPerfilId && selectedRow.dataset.criadorPerfilId !== currentUserPerfilId) {
        return setFilterMsg("Usuario sem permissao para editar este registro.", true);
      }
      if (etapaIsConsultaView) {
        sessionStorage.setItem(
          etapaPendingActionKey,
          JSON.stringify({ action: "edit", id: selectedRow.dataset.id || "" })
        );
        loadPage("cadastrar/plan_21-nger/etapa/formulario");
        return;
      }
      setFilterMsg("");
      fillFromRow(selectedRow, false);
    });
    deleteBtn?.addEventListener("click", async () => {
      setMsg("");
      if (!selectedRow) return setFilterMsg("Selecione um registro para excluir.", true);
      const controle = String(selectedRow.dataset.controleEtapa || "").trim() || "(sem controle)";
      const status = String(selectedRow.dataset.statusAprovacao || "").trim().toLowerCase();
      if (status !== "aguardando") {
        setFilterMsg(`Somente registros com status Aguardando podem ser excluídos (${controle}).`, true);
        return;
      }
      const criadorPerfilId = String(selectedRow.dataset.criadorPerfilId || "").trim();
      if (!currentUserPerfilId || currentUserPerfilId !== criadorPerfilId) {
        setFilterMsg(`Usuário sem permissão para excluir registro ${controle}.`, true);
        return;
      }
      const etapaId = String(selectedRow.dataset.id || "").trim();
      if (!etapaId) {
        setFilterMsg(`Registro inválido para exclusão (${controle}).`, true);
        return;
      }
      if (!confirm("Confirma a exclusao do registro de etapa selecionado?")) return;
      try {
        const res = await fetch(`/api/etapa/${encodeURIComponent(etapaId)}`, {
          method: "DELETE",
          headers: { "X-Requested-With": "fetch" },
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || `Falha ao excluir registro ${controle}.`);
        showToast(data.message || `Registro ${controle} excluído com sucesso.`, "success");
        await loadPage("cadastrar/plan_21-nger/etapa/consultar");
      } catch (err) {
        setFilterMsg(err.message || `Falha ao excluir registro ${controle}.`, true);
      }
    });
    printBtn?.addEventListener("click", () => {
      setMsg("");
      if (!selectedRow) return setFilterMsg("Selecione um registro para imprimir.", true);
      setFilterMsg("");
      printEtapaRecord(selectedRow.dataset);
    });
    const setFilterMsg = (text, isError = false) => {
      if (!filterMsg) return;
      filterMsg.textContent = text || "";
      filterMsg.classList.toggle("text-error", Boolean(isError));
    };
    const clearFilterMsgOnUserAction = () => {
      if (!filterMsg?.textContent) return;
      setFilterMsg("");
    };
    [filterForm, summary].forEach((el) => {
      if (!el) return;
      el.addEventListener("input", clearFilterMsgOnUserAction, true);
      el.addEventListener("change", clearFilterMsgOnUserAction, true);
      el.addEventListener("click", clearFilterMsgOnUserAction, true);
    });
    const setResultsVisible = (visible) => {
      if (summary) summary.style.display = "";
      if (resultsPlaceholder) resultsPlaceholder.style.display = visible ? "none" : "";
      if (resultsTableWrap) resultsTableWrap.style.display = visible ? "" : "none";
      if (resultsFooter) resultsFooter.style.display = visible ? "" : "none";
      if (!visible) {
        selectedRow = null;
        allRows().forEach((row) => {
          row.style.display = "none";
          row.classList.remove("selected");
        });
      }
    };
    filterAdd?.addEventListener("click", () => {
      const field = filterField?.value || "";
      const value = filterValue?.value || "";
      if (!field || !value) {
        setFilterMsg("Informe campo e valor.", true);
        return;
      }
      if (field !== "exercicio" && !filters.some((f) => f.field === "exercicio")) {
        setFilterMsg("Informe o critério de Exercício antes dos demais.", true);
        return;
      }
      filters.push({
        field,
        op: filterOp?.value || "eq",
        value,
        label: filterField.options[filterField.selectedIndex]?.textContent || field,
      });
      filterSelected = filters.length - 1;
      if (filterValue) filterValue.value = "";
      renderFilterList();
      setResultsVisible(false);
      setFilterMsg("");
      currentPage = 1;
    });
    filterRemove?.addEventListener("click", () => {
      if (filterSelected < 0 || filterSelected >= filters.length) {
        setFilterMsg("Selecione um critério para remover.", true);
        return;
      }
      filters.splice(filterSelected, 1);
      filterSelected = -1;
      renderFilterList();
      setResultsVisible(false);
      setFilterMsg("");
    });
    filterClear?.addEventListener("click", () => {
      filters.length = 0;
      filterSelected = -1;
      renderFilterList();
      setResultsVisible(false);
      setFilterMsg("");
    });
    filterCancel?.addEventListener("click", () => {
      filters.length = 0;
      filterSelected = -1;
      renderFilterList();
      setResultsVisible(false);
      allRows().forEach((row) => {
        row.style.display = "none";
        row.classList.remove("selected");
      });
      if (filterField) filterField.value = "";
      if (filterOp) filterOp.value = "eq";
      if (filterValue) filterValue.value = "";
      setFilterMsg("");
    });
    filterApply?.addEventListener("click", () => {
      if (!filters.some((f) => f.field === "exercicio")) {
        setFilterMsg("Informe o critério de Exercício antes de consultar.", true);
        return;
      }
      setResultsVisible(true);
      renderPagination();
      if (visibleRows().length) {
        setFilterMsg("");
      } else {
        setFilterMsg("Nenhum registro encontrado para os critérios informados.", true);
      }
    });
    pageSizeSelect?.addEventListener("change", () => {
      pageSize = parseInt(pageSizeSelect.value || "5", 10) || 5;
      renderPagination();
    });

    loadOptions().catch((err) => setMsg(err.message, true));
    syncMode(false);
    setResultsVisible(false);
    renderPagination();
    if (!etapaIsConsultaView) {
      const pendingRaw = sessionStorage.getItem(etapaPendingActionKey);
      if (pendingRaw) {
        sessionStorage.removeItem(etapaPendingActionKey);
        try {
          const pending = JSON.parse(pendingRaw);
          const pendingId = String(pending?.id || "").trim();
          const row = pendingId
            ? tbody?.querySelector(`.etapa-summary-row[data-id="${CSS.escape(pendingId)}"]`)
            : null;
          if (row) {
            row.click();
            if (pending.action === "approve") approveBtn?.click();
            else editBtn?.click();
          } else {
            setMsg("Registro selecionado na consulta não foi encontrado.", true);
          }
        } catch (err) {
          console.error(err);
        }
      }
    }
  }

  function initSubacaoPlan21() {
    const form = document.getElementById("form-subacao");
    const msg = document.getElementById("subacao-msg");
    if (!form || !msg) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";
    const page = document.getElementById("subacao-page");
    const subacaoViewMode = String(page?.dataset.viewMode || "formulario");
    const subacaoIsConsultaView = subacaoViewMode === "consultar";
    const subacaoPendingActionKey = "spo.subacao.pendingAction";
    let subacaoPendingAction = null;
    if (!subacaoIsConsultaView) {
      try {
        subacaoPendingAction = JSON.parse(sessionStorage.getItem(subacaoPendingActionKey) || "null");
        sessionStorage.removeItem(subacaoPendingActionKey);
      } catch (err) {
        subacaoPendingAction = null;
      }
    }
    const setSubacaoPendingLoading = (enabled) => {
      if (enabled) {
        showAppLoading("Carregando registro...", "Aguarde enquanto os dados da Subação são preparados.");
      } else {
        hideAppLoading();
      }
    };
    if (subacaoPendingAction) {
      setSubacaoPendingLoading(true);
    }
    const subacaoRowSnapshot = (row) => (row ? Object.fromEntries(Object.entries(row.dataset || {})) : {});
    const ensureSubacaoPendingRow = (pending) => {
      const pendingId = String(pending?.id || "").trim();
      let row = pendingId
        ? summaryBody?.querySelector(`.dotacao-summary-row[data-id="${CSS.escape(pendingId)}"]`)
        : null;
      if (!row && summaryBody && pending?.dataset) {
        row = document.createElement("tr");
        row.className = "dotacao-summary-row selected";
        row.style.display = "none";
        Object.entries(pending.dataset || {}).forEach(([key, value]) => {
          row.dataset[key] = value == null ? "" : String(value);
        });
        summaryBody.appendChild(row);
      }
      return row;
    };

      const stepButtons = Array.from(form.querySelectorAll(".wizard-step-btn"));
      const stepsWrap = document.getElementById("subacao-steps");
    const stepPanels = Array.from(form.querySelectorAll(".wizard-step"));
    const prevBtn = document.getElementById("subacao-prev");
    const nextBtn = document.getElementById("subacao-next");
    const saveBtn = document.getElementById("subacao-save");
    let totalSteps = stepPanels.length || 1;
    let currentStep = 1;

    const modeSelect = document.getElementById("subacao-formulario");
    const normalizeSolicitacao = (value) => {
      const v = String(value || "").toLowerCase();
      return v === "alterar" ? "editar" : v;
    };
    const getModeValue = () => normalizeSolicitacao(modeSelect?.value || "");
    const getRawModeValue = () => String(modeSelect?.value || "").toLowerCase();
    const idInput = document.getElementById("subacao-id");
    const clearBtn = document.getElementById("subacao-clear");

    const planSelects = {
      exercicio: document.getElementById("subacao-exercicio"),
      uo: document.getElementById("subacao-uo"),
      programa: document.getElementById("subacao-programa"),
      acao_paoe: document.getElementById("subacao-acao"),
      responsavel_acao: document.getElementById("subacao-responsavel-acao"),
      produto_acao: document.getElementById("subacao-produto-acao"),
    };

    const chaveSelects = {
      regiao: document.getElementById("subacao-regiao"),
      subfuncao: document.getElementById("subacao-subfuncao"),
      ug: document.getElementById("subacao-ug"),
      adj: document.getElementById("subacao-adj"),
      macropolitica: document.getElementById("subacao-macropolitica"),
      pilar: document.getElementById("subacao-pilar"),
      eixo: document.getElementById("subacao-eixo"),
      politica_decr: document.getElementById("subacao-politica"),
      publico_transversal: document.getElementById("subacao-publico"),
    };

    const chaveInput = document.getElementById("subacao-chave");
    const chaveTitle = document.getElementById("subacao-chave-title");
    const chaveSection = document.getElementById("subacao-chave-section");
    const cadastrarGrid = document.getElementById("subacao-cadastrar-grid");
    const editarGrid = document.getElementById("subacao-editar-grid");
    const subacaoEntregaInput = document.getElementById("subacao-entrega");
    const responsavelInput = document.getElementById("subacao-responsavel");
    const cpfInput = document.getElementById("subacao-cpf");
    const dataInicioInput = document.getElementById("subacao-data-inicio");
    const dataFimInput = document.getElementById("subacao-data-fim");
    const unidGestoraSelect = document.getElementById("subacao-unid-gestora");
    const unidadeSetorialSelect = document.getElementById("subacao-unidade-setorial");
    const produtoSubacaoSelect = document.getElementById("subacao-produto");
    const unidadeMedidaSelect = document.getElementById("subacao-unidade-medida");
    const regiaoEntregaSelect = document.getElementById("subacao-regiao-entrega");
    const codigoSelect = document.getElementById("subacao-codigo");
    const municipioSelect = document.getElementById("subacao-municipio");
    const metaInput = document.getElementById("subacao-meta");
    const detalhamentoInput = document.getElementById("subacao-detalhamento");
    const etapaInput = document.getElementById("subacao-etapa");
    const etapaMunicipioSelect = document.getElementById("subacao-etapa-municipio");
    const etapaCounter = document.getElementById("subacao-etapa-counter");
    const addEtapaBtn = document.getElementById("subacao-add-etapa");
    const etapaListWrap = document.getElementById("subacao-etapa-list-wrap");
    const etapaListEl = document.getElementById("subacao-etapa-list");
    const responsavelEtapaInput = document.getElementById("subacao-responsavel-etapa");
    const cpfEtapaInput = document.getElementById("subacao-cpf-etapa");
    const justificativaInput = document.getElementById("subacao-justificativa");
    const responsavelNgerInput = document.getElementById("subacao-responsavel-nger");
    const municipioAddBtn = document.getElementById("subacao-municipio-add");
    const municipioListWrap = document.getElementById("subacao-municipio-list-wrap");
    const municipioListEl = document.getElementById("subacao-municipio-list");
    const editChaveSelect = document.getElementById("subacao-edit-chave");
    const editSubacaoSelect = document.getElementById("subacao-edit-subacao-select");
    const editSubacaoInput = document.getElementById("subacao-edit-subacao-input");
    const editResponsavelSelect = document.getElementById("subacao-edit-responsavel-select");
    const editResponsavelInput = document.getElementById("subacao-edit-responsavel-input");
    const editCpfInput = document.getElementById("subacao-edit-cpf");
    const editPrazoSelect = document.getElementById("subacao-edit-prazo");
    const editUnidGestoraSelect = document.getElementById("subacao-edit-unid-gestora");
    const editUnidadeSetorialSelect = document.getElementById("subacao-edit-unidade-setorial");
    const editProdutoSelect = document.getElementById("subacao-edit-produto-select");
    const editProdutoInput = document.getElementById("subacao-edit-produto-input");
    const editRegiaoSelect = document.getElementById("subacao-edit-regiao");
    const editMunicipioAddBtn = document.getElementById("subacao-edit-municipio-add");
    const editMunicipioListWrap = document.getElementById("subacao-edit-municipio-list-wrap");
    const editMunicipioListEl = document.getElementById("subacao-edit-municipio-list");
    const editCodigoNovoSelect = document.getElementById("subacao-edit-codigo-novo");
    const editMunicipioNovoSelect = document.getElementById("subacao-edit-municipio-novo");
    const editMetaNovoInput = document.getElementById("subacao-edit-meta-novo");
      const editQuestions = document.getElementById("subacao-edit-questions");
      const editJustificativaInput = document.getElementById("subacao-edit-justificativa");
      const editResponsavelNgerInput = document.getElementById("subacao-edit-responsavel-nger");
      let pendingEditPref = null;
      let editMunicipioLocked = false;

    const summaryBody = document.querySelector("#subacao-summary-table tbody");
    const pageSizeSelect = document.getElementById("subacao-page-size");
    const paginationEl = document.getElementById("subacao-pagination");
    const filterForm = document.getElementById("subacao-filtro-form");
    const filterField = document.getElementById("subacao-filtro-campo");
    const filterOp = document.getElementById("subacao-filtro-operador");
    const filterValue = document.getElementById("subacao-filtro-valor");
    const filterAdd = document.getElementById("subacao-filtro-add");
    const filterList = document.getElementById("subacao-filtro-list");
    const filterRemove = document.getElementById("subacao-filtro-remove");
    const filterClear = document.getElementById("subacao-filtro-clear");
    const filterCancel = document.getElementById("subacao-filtro-cancel");
    const filterApply = document.getElementById("subacao-filtro-apply");
    const filterMsg = document.getElementById("subacao-filtro-msg");
    const approveBtn = document.getElementById("subacao-approve");
    const editBtn = document.getElementById("subacao-edit");
    const deleteBtn = document.getElementById("subacao-delete");
    const printBtn = document.getElementById("subacao-print");
    const subacaoPage = document.getElementById("subacao-page");
    const currentUserPerfilId = String(subacaoPage?.dataset?.userPerfilId || userPerfilId || "").trim();
    const subacaoSummary = document.getElementById("subacao-summary");
    const approvalFields = document.getElementById("subacao-aprovacao-fields");
    const approvalQuestionLabel = document.getElementById("subacao-aprovacao-pergunta");
    const approvalJustificativa = document.getElementById("subacao-justificativa-aprovacao");
    const editBadge = document.getElementById("subacao-editing-badge");
    const approvalRadios = form
      ? Array.from(form.querySelectorAll('input[name="subacao-aprovada"]'))
      : [];
    let selectedPlan21Ids = { id: "", ids: "" };

    const hasAllPlanSelects = Object.values(planSelects).every((el) => el);
    const hasAllKeySelects = Object.values(chaveSelects).every((el) => el);
    if (!hasAllPlanSelects || !hasAllKeySelects) return;

    if (unidadeSetorialSelect) {
      unidadeSetorialSelect.disabled = true;
      unidadeSetorialSelect.dataset.readonly = "1";
    }
    if (editUnidadeSetorialSelect) {
      editUnidadeSetorialSelect.disabled = true;
      editUnidadeSetorialSelect.dataset.readonly = "1";
    }

    let lastRegiaoValue = planSelects.regiao?.value || "";

      const setMsg = (text, isError = false) => {
        msg.textContent = text || "";
        msg.classList.toggle("text-error", isError);
        if (text && msg.scrollIntoView) {
          msg.scrollIntoView({ behavior: "smooth", block: "center" });
        }
      };

      const syncRequiredByVisibility = () => {
        const fields = Array.from(form.querySelectorAll("input, select, textarea"));
        fields.forEach((field) => {
          if (field.type === "hidden") return;
          if (field.dataset.required === "1") {
            field.required = field.offsetParent !== null;
            return;
          }
          if (field.required && field.offsetParent === null) {
            field.dataset.required = "1";
            field.required = false;
          }
        });
      };

    const setFilterMsg = (text, isError = false) => {
      if (!filterMsg) return;
      filterMsg.textContent = text || "";
      filterMsg.classList.toggle("text-error", isError);
    };

    let approvalMode = false;
    const approvalPlaceholder = document.createComment("subacao-approval-placeholder");
    if (approvalFields?.parentNode) {
      approvalFields.parentNode.insertBefore(approvalPlaceholder, approvalFields);
    }
    const approvalStep2Anchor = document.createElement("div");
    approvalStep2Anchor.id = "subacao-aprovacao-anchor";
    let approvalDisabledSnapshot = null;
    const syncApprovalFieldsVisibility = () => {
      if (!approvalFields) return;
      const modeValue = getModeValue();
      const approvalStep = modeValue === "excluir" ? 1 : modeValue === "editar" ? 2 : 3;
      const shouldShow = approvalMode && Number(currentStep) === approvalStep;
      approvalFields.style.display = shouldShow ? "" : "none";
    };
    const setFormDisabled = (disabled) => {
      const fields = Array.from(form.querySelectorAll("button, input, select, textarea"));
      if (disabled && !approvalDisabledSnapshot) {
        approvalDisabledSnapshot = new Map();
      }
      fields.forEach((el) => {
        if (approvalFields && approvalFields.contains(el)) {
          el.disabled = false;
          return;
        }
        if (disabled && approvalDisabledSnapshot && !approvalDisabledSnapshot.has(el)) {
          approvalDisabledSnapshot.set(el, el.disabled);
        }
        el.disabled = disabled;
      });
      if (!disabled && approvalDisabledSnapshot) {
        approvalDisabledSnapshot.forEach((wasDisabled, el) => {
          el.disabled = wasDisabled;
        });
        approvalDisabledSnapshot = null;
      }
      // Keep step navigation enabled so approver can review all wizard parts.
      stepButtons.forEach((btn) => {
        btn.disabled = false;
      });
      if (nextBtn) nextBtn.disabled = false;
      if (prevBtn) prevBtn.disabled = false;
      if (saveBtn) saveBtn.disabled = false;
      if (clearBtn) clearBtn.disabled = disabled;
    };
      const updateEditingBanner = () => {
        if (!editBadge) return;
        const registroId = String(idInput?.value || "").trim();
        const controle = String(currentControleSubacao || "").trim();
        if (controle || registroId) {
          const ref = controle || registroId;
          editBadge.textContent = approvalMode
            ? `- Aprovação do registro ${ref}`
            : `- Edição do registro ${ref}`;
          editBadge.style.display = "inline";
          return;
        }
        editBadge.textContent = "";
        editBadge.style.display = "none";
      };
      const updateApprovalQuestion = () => {
        if (!approvalQuestionLabel) return;
        const controle = String(currentControleSubacao || "").trim();
        approvalQuestionLabel.textContent = controle
          ? `*Deseja aprovar o registro (${controle})?`
          : "*Deseja aprovar o registro (Controle de Subação)?";
      };

      const setApprovalMode = (enabled) => {
        approvalMode = enabled;
        if (approvalFields) {
          if (enabled) {
            const modeValue = getModeValue();
            if (modeValue === "excluir") {
              const actionsWrap = saveBtn?.closest(".actions");
              if (actionsWrap?.parentNode) {
                actionsWrap.parentNode.insertBefore(approvalFields, actionsWrap);
              }
            } else {
              const approvalStep = modeValue === "editar" ? 2 : 3;
              const approvalPanel = stepPanels.find(
                (panel) => Number(panel.dataset.step) === approvalStep
              );
              if (approvalPanel) {
                if (approvalStep2Anchor.parentNode !== approvalPanel) {
                  approvalPanel.appendChild(approvalStep2Anchor);
                }
                approvalStep2Anchor.parentNode.insertBefore(approvalFields, approvalStep2Anchor);
              }
            }
          } else if (approvalPlaceholder.parentNode) {
            approvalPlaceholder.parentNode.insertBefore(approvalFields, approvalPlaceholder.nextSibling);
          }
        }
        syncApprovalFieldsVisibility();
        if (approvalJustificativa) approvalJustificativa.required = enabled;
        if (enabled) {
          setFormDisabled(true);
        if (approvalJustificativa) approvalJustificativa.value = "";
        approvalRadios.forEach((r) => {
          r.checked = r.value === "sim";
        });
        } else {
          setFormDisabled(false);
          if (approvalJustificativa) approvalJustificativa.value = "";
          approvalRadios.forEach((r) => {
            r.checked = r.value === "sim";
          });
        }
        if (municipioAddBtn) municipioAddBtn.disabled = enabled;
        renderMunicipioList();
        updateEditingBanner();
        updateApprovalQuestion();
      };

    const nivelAtual = parseInt(String(userNivel || "").trim(), 10);
    const canApprove = nivelAtual === 1 || nivelAtual === 2;
    if (approveBtn) approveBtn.style.display = canApprove ? "" : "none";

    const criteria = [];
    let criteriaSelected = -1;
    const fieldLabels = {
      controleSubacao: "Controle de Subação",
      exercicio: "Exercício",
      statusAprovacao: "Status",
      acaoPaoe: "Ação/PAOE",
      programa: "Programa",
      produtoAcao: "Produto",
      tipoSolicitacao: "Tipo de Solicitação",
    };
    const opLabels = {
      eq: "Igual a",
      contains: "Contém",
      gt: "Maior que",
      lt: "Menor que",
      gte: "Maior igual a",
      lte: "Menor igual a",
    };

    const parseMaybeNumber = (value) => {
      if (value === null || value === undefined) return { raw: "", num: null };
      const raw = String(value).trim();
      if (!raw) return { raw, num: null };
      const num = Number(raw.replace(",", "."));
      return Number.isNaN(num) ? { raw, num: null } : { raw, num };
    };

    const compareValues = (left, right, op) => {
      const l = parseMaybeNumber(left);
      const r = parseMaybeNumber(right);
      if (l.num !== null && r.num !== null) {
        if (op === "eq") return l.num === r.num;
        if (op === "gt") return l.num > r.num;
        if (op === "lt") return l.num < r.num;
        if (op === "gte") return l.num >= r.num;
        if (op === "lte") return l.num <= r.num;
      }
      const lraw = l.raw.toLowerCase();
      const rraw = r.raw.toLowerCase();
      const cmp = lraw.localeCompare(rraw, "pt-BR", { sensitivity: "base" });
      if (op === "eq") return cmp === 0;
      if (op === "contains") return lraw.includes(rraw);
      if (op === "gt") return cmp > 0;
      if (op === "lt") return cmp < 0;
      if (op === "gte") return cmp >= 0;
      if (op === "lte") return cmp <= 0;
      return false;
    };

    const renderCriteria = () => {
      if (!filterList) return;
      filterList.innerHTML = "";
      criteria.forEach((c, idx) => {
        const li = document.createElement("li");
        const label = fieldLabels[c.field] || c.field;
        const op = opLabels[c.op] || c.op;
        li.textContent = `${label} ${op} ${c.value}`;
        li.dataset.index = String(idx);
        if (idx === criteriaSelected) {
          li.style.borderColor = "var(--primary)";
        }
        li.addEventListener("click", () => {
          criteriaSelected = idx;
          renderCriteria();
        });
        filterList.appendChild(li);
      });
    };

    const normalizeText = (value) =>
      String(value || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "");

    const compareField = (field, rowVal, targetVal, op) => {
      if (field === "controleSubacao") {
        const left = normalizeText(rowVal);
        const right = normalizeText(targetVal);
        return compareValues(left, right, op);
      }
      if (field === "acaoPaoe" || field === "programa") {
        const left = normalizeDigits(rowVal);
        const right = normalizeDigits(targetVal);
        return compareValues(left, right, op);
      }
      return compareValues(rowVal, targetVal, op);
    };

    const subacaoEntregaCounter = document.getElementById("subacao-entrega-counter");

    const getStepPanel = (step) => stepPanels.find((panel) => Number(panel.dataset.step) === step);

    const toggleMode = () => {
      const modeValue = getModeValue();
      const isEdit = modeValue === "editar";
      const isExcluir = modeValue === "excluir";
      const isEditLike = isEdit || isExcluir;
        if (chaveTitle) chaveTitle.style.display = isEditLike ? "none" : "";
        if (chaveSection) chaveSection.style.display = isEditLike ? "none" : "";
        if (cadastrarGrid) cadastrarGrid.style.display = isEditLike ? "none" : "";
        if (editarGrid) editarGrid.style.display = isEditLike ? "" : "none";
        if (editQuestions) editQuestions.style.display = isEdit ? "" : "none";
        if (municipioListWrap) municipioListWrap.style.display = isEditLike ? "none" : "";
        if (municipioAddBtn) municipioAddBtn.style.display = isEditLike ? "none" : "";
        if (stepsWrap) stepsWrap.style.display = isExcluir ? "none" : "";

      const toggleFields = (root, enable) => {
        if (!root) return;
        root.querySelectorAll("input, select, textarea").forEach((el) => {
          if (enable) {
            if (el.dataset.readonly === "1") return;
            el.disabled = false;
          } else {
            el.disabled = true;
          }
        });
      };
        toggleFields(chaveSection, !isEditLike);
        toggleFields(cadastrarGrid, !isEditLike);
        toggleFields(editarGrid, isEditLike);
        updateStepVisibility();
        if (unidadeSetorialSelect) unidadeSetorialSelect.disabled = true;
        if (editUnidadeSetorialSelect) editUnidadeSetorialSelect.disabled = true;
        syncRequiredByVisibility();
        if (isEdit) syncEditMode();
        if (approvalMode) {
          setFormDisabled(true);
          syncApprovalFieldsVisibility();
        }
      };

    const editGroupEls = editarGrid ? Array.from(editarGrid.querySelectorAll("[data-edit-group]")) : [];
    const getEditQuestionValue = (name) =>
      form.querySelector(`input[name="${name}"]:checked`)?.value || "";
    const setEditQuestionValue = (name, value) => {
      const el = form.querySelector(`input[name="${name}"][value="${value}"]`);
      if (el) el.checked = true;
    };
      const getEditModeKey = () => {
        if (getModeValue() === "excluir") return "excluir";
        if (getEditQuestionValue("subacao_edit_q_subacao") === "sim") return "subacao_name";
        if (getEditQuestionValue("subacao_edit_q_responsavel") === "sim") return "responsavel_name";
        if (getEditQuestionValue("subacao_edit_q_produto") === "sim") return "produto_subacao";
        if (getEditQuestionValue("subacao_edit_q_remove_municipio") === "sim")
          return "remover_municipio";
        if (getEditQuestionValue("subacao_edit_q_municipio") === "sim") return "novo_municipio";
        return "";
      };
    const setEditGroupVisibility = (modeKey) => {
      if (!editarGrid) return;
      const active = new Set();
      if (modeKey) {
        active.add("core");
        active.add("all");
        active.add(modeKey);
      }
      editGroupEls.forEach((el) => {
        const groups = String(el.dataset.editGroup || "").split(/\s+/).filter(Boolean);
        const show = groups.some((g) => active.has(g));
        el.style.display = show ? "" : "none";
        el.querySelectorAll("input, select, textarea").forEach((field) => {
          field.disabled = !show;
        });
      });
        if (modeKey === "produto_subacao" || modeKey === "novo_municipio" || modeKey === "remover_municipio") {
          const lockTargets = [
            editResponsavelSelect,
            editPrazoSelect,
            editUnidGestoraSelect,
            editUnidadeSetorialSelect,
          editProdutoSelect,
          editRegiaoSelect,
        ];
        lockTargets.forEach((el) => {
          if (!el) return;
          el.disabled = true;
        });
        const responsavelEditField = editResponsavelInput?.closest(".field");
        if (responsavelEditField) {
          responsavelEditField.style.display = "none";
          editResponsavelInput.disabled = true;
        }
      } else {
        const lockTargets = [
          editResponsavelSelect,
          editPrazoSelect,
          editUnidGestoraSelect,
          editUnidadeSetorialSelect,
          editProdutoSelect,
          editRegiaoSelect,
        ];
        lockTargets.forEach((el) => {
          if (!el) return;
          const wrap = el.closest(".field");
          if (wrap && wrap.style.display === "none") return;
          el.disabled = false;
        });
      }
    };
    const setEditRequiredFields = (modeKey) => {
      const allFields = [
        editSubacaoInput,
        editResponsavelInput,
        editCpfInput,
        editPrazoSelect,
        editUnidGestoraSelect,
        editUnidadeSetorialSelect,
        editProdutoSelect,
        editProdutoInput,
        editRegiaoSelect,
        editCodigoNovoSelect,
        editMunicipioNovoSelect,
        editMetaNovoInput,
        editJustificativaInput,
        editResponsavelNgerInput,
      ];
      allFields.forEach((el) => {
        if (el) el.required = false;
      });
        const requiredMap = {
          subacao_name: [
            editSubacaoInput,
            editResponsavelInput,
            editCpfInput,
            editJustificativaInput,
            editResponsavelNgerInput,
          ],
          responsavel_name: [
            editResponsavelInput,
            editCpfInput,
            editJustificativaInput,
            editResponsavelNgerInput,
          ],
          produto_subacao: [
            editProdutoSelect,
            editProdutoInput,
            editJustificativaInput,
            editResponsavelNgerInput,
          ],
          remover_municipio: [
            editResponsavelInput,
            editPrazoSelect,
            editUnidGestoraSelect,
            editUnidadeSetorialSelect,
            editProdutoSelect,
            editRegiaoSelect,
            editCodigoNovoSelect,
            editMunicipioNovoSelect,
            editMetaNovoInput,
            editJustificativaInput,
            editResponsavelNgerInput,
          ],
          novo_municipio: [
            editResponsavelInput,
            editPrazoSelect,
            editUnidGestoraSelect,
            editUnidadeSetorialSelect,
          editProdutoSelect,
          editRegiaoSelect,
          editCodigoNovoSelect,
          editMunicipioNovoSelect,
          editMetaNovoInput,
          editJustificativaInput,
          editResponsavelNgerInput,
        ],
        excluir: [editJustificativaInput, editResponsavelNgerInput],
      };
      const required = requiredMap[modeKey] || [];
      required.forEach((el) => {
        if (el) el.required = true;
      });
    };
      const syncEditMode = () => {
        const modeKey = getEditModeKey();
        updateQuestionVisibility();
        setEditGroupVisibility(modeKey);
        setEditRequiredFields(modeKey);
        updateEditMunicipioRequired();
        if (["novo_municipio", "remover_municipio"].includes(modeKey)) {
          loadPlan21Municipios();
        } else {
          editMunicipioLocked = false;
        }
        if (!["novo_municipio", "remover_municipio"].includes(modeKey)) {
          editMunicipioItems.length = 0;
          renderEditMunicipioList();
        }
        return modeKey;
      };

    if (modeSelect) {
      modeSelect.addEventListener("change", () => {
        const modeValue = getModeValue();
        toggleMode();
        if (modeValue === "editar") {
          resetEditQuestions();
          updateQuestionVisibility();
        }
        if (modeValue === "editar" || modeValue === "excluir") {
          loadEditOptions();
        }
        syncEditMode();
      });
    }

      const editQuestionNames = [
        "subacao_edit_q_subacao",
        "subacao_edit_q_responsavel",
        "subacao_edit_q_produto",
        "subacao_edit_q_remove_municipio",
        "subacao_edit_q_municipio",
      ];
      const editQuestionFields = {
        subacao: form.querySelector('input[name="subacao_edit_q_subacao"]')?.closest(".field"),
        responsavel: form
          .querySelector('input[name="subacao_edit_q_responsavel"]')
          ?.closest(".field"),
        produto: form.querySelector('input[name="subacao_edit_q_produto"]')?.closest(".field"),
        removerMunicipio: form
          .querySelector('input[name="subacao_edit_q_remove_municipio"]')
          ?.closest(".field"),
        municipio: form
          .querySelector('input[name="subacao_edit_q_municipio"]')
          ?.closest(".field"),
      };
      const updateQuestionVisibility = () => {
        const q1 = getEditQuestionValue("subacao_edit_q_subacao");
        const q2 = getEditQuestionValue("subacao_edit_q_responsavel");
        const q3 = getEditQuestionValue("subacao_edit_q_produto");
        const q4 = getEditQuestionValue("subacao_edit_q_remove_municipio");
        if (editQuestionFields.subacao) {
          editQuestionFields.subacao.style.display = "";
        }
        if (editQuestionFields.responsavel) {
          editQuestionFields.responsavel.style.display = q1 === "nao" ? "" : "none";
        }
        if (editQuestionFields.produto) {
          editQuestionFields.produto.style.display = q1 === "nao" && q2 === "nao" ? "" : "none";
        }
        if (editQuestionFields.removerMunicipio) {
          editQuestionFields.removerMunicipio.style.display =
            q1 === "nao" && q2 === "nao" && q3 === "nao" ? "" : "none";
        }
        if (editQuestionFields.municipio) {
          editQuestionFields.municipio.style.display =
            q1 === "nao" && q2 === "nao" && q3 === "nao" && q4 === "nao" ? "" : "none";
        }
      };
    const resetEditQuestions = () => {
      editQuestionNames.forEach((name) => {
        form.querySelectorAll(`input[name="${name}"]`).forEach((input) => {
          input.checked = false;
        });
      });
    };
    editQuestionNames.forEach((name) => {
      form.querySelectorAll(`input[name="${name}"]`).forEach((input) => {
        input.addEventListener("change", () => {
          if (input.value === "sim") {
            editQuestionNames.forEach((other) => {
              if (other !== name) setEditQuestionValue(other, "nao");
            });
          }
          updateQuestionVisibility();
          syncEditMode();
        });
      });
    });

    const validateStep = (step) => {
      const panel = getStepPanel(step);
      if (!panel) return true;
      const isEditMode = ["editar", "excluir"].includes(getModeValue());
      const etapaMunicipios = new Set(
        etapaItems
          .map((item) => String(item?.municipio || "").trim())
          .filter((value) => value)
      );
      const municipiosPendentes =
        step === 3
          ? municipioItems.filter(
              (item) => !etapaMunicipios.has(String(item?.municipios_entrega || "").trim())
            )
          : [];
      const etapaListaCompleta = step === 3 && municipioItems.length > 0 && municipiosPendentes.length === 0;
      const fields = panel.querySelectorAll("input, select, textarea");
        for (const field of fields) {
            if (field.disabled) continue;
            if (field.type === "hidden") continue;
            if (field.offsetParent === null) continue;
            if (municipioItems.length) {
              if (field === codigoSelect || field === municipioSelect || field === metaInput) {
                continue;
              }
            }
          if (editMunicipioItems.length) {
            if (
              field === editCodigoNovoSelect ||
              field === editMunicipioNovoSelect ||
              field === editMetaNovoInput
            ) {
              continue;
            }
          }
          if (etapaListaCompleta) {
            if (
              field === etapaMunicipioSelect ||
              field === etapaInput ||
              field === responsavelEtapaInput ||
              field === cpfEtapaInput
            ) {
              continue;
            }
          }
          if (!field.checkValidity()) {
            if (field === metaInput && municipioItems.length) {
              continue;
            }
            field.reportValidity();
            field.focus();
            return false;
          }
        }
      if (step === 2 && !isEditMode && cpfInput && cpfInput.value && !isValidCpf(cpfInput.value)) {
        setMsg("CPF do responsável inválido.", true);
        cpfInput.focus();
        return false;
      }
      if (
        step === 2 &&
        isEditMode &&
        editCpfInput &&
        editCpfInput.required &&
        editCpfInput.value &&
        !isValidCpf(editCpfInput.value)
      ) {
        setMsg("CPF do responsável inválido.", true);
        editCpfInput.focus();
        return false;
      }
      if (step === 2 && !isEditMode) {
        if (dataInicioInput && !isValidDateBR(dataInicioInput.value)) {
          setMsg("Data início inválida.", true);
          dataInicioInput.focus();
          return false;
        }
        if (dataFimInput && !isValidDateBR(dataFimInput.value)) {
          setMsg("Data final inválida.", true);
          dataFimInput.focus();
          return false;
        }
        if (dataInicioInput && dataFimInput) {
          const di = parseDateBR(dataInicioInput.value);
          const df = parseDateBR(dataFimInput.value);
          if (di && df && df < di) {
            setMsg("Data final deve ser maior ou igual à data início.", true);
            dataFimInput.focus();
            return false;
          }
        }
      }
      return true;
    };

      const setStep = (step) => {
        if (!stepPanels.length) return;
      const modeValue = getModeValue();
        if (modeValue === "excluir") {
          stepPanels.forEach((panel) => {
            const stepNum = Number(panel.dataset.step);
            if (stepNum === 3) {
              panel.classList.remove("active");
              panel.style.display = "none";
              return;
            }
            panel.style.display = "";
            panel.classList.add("active");
          });
          stepButtons.forEach((btn) => {
            btn.classList.remove("active");
            btn.style.display = "none";
          });
          if (prevBtn) prevBtn.style.display = "none";
          if (nextBtn) nextBtn.style.display = "none";
          if (saveBtn) saveBtn.style.display = "inline-flex";
          syncRequiredByVisibility();
          return;
        }
        const visiblePanels = stepPanels.filter((panel) => panel.style.display !== "none");
        const visibleSteps = visiblePanels.map((panel) => Number(panel.dataset.step));
        if (!visibleSteps.length) return;
        let target = step;
        if (!visibleSteps.includes(step)) {
          target = visibleSteps[0];
        }
        currentStep = target;
        totalSteps = visibleSteps.length || 1;
        stepPanels.forEach((panel) => {
          const stepNum = Number(panel.dataset.step);
          panel.classList.toggle("active", stepNum === target);
        });
        stepButtons.forEach((btn) => {
          const stepNum = Number(btn.dataset.step);
          const isVisible = visibleSteps.includes(stepNum);
          btn.style.display = isVisible ? "inline-flex" : "none";
          if (!isVisible) {
            btn.classList.remove("active");
            return;
          }
          btn.classList.toggle("active", stepNum === target);
        });
        const currentIndex = visibleSteps.indexOf(target);
        if (prevBtn) {
          prevBtn.disabled = currentIndex <= 0;
          prevBtn.style.display = currentIndex <= 0 ? "none" : "inline-flex";
        }
        if (nextBtn) nextBtn.style.display = currentIndex >= visibleSteps.length - 1 ? "none" : "inline-flex";
        if (saveBtn) saveBtn.style.display = currentIndex >= visibleSteps.length - 1 ? "inline-flex" : "none";
        syncApprovalFieldsVisibility();
        setMsg("");
        syncRequiredByVisibility();
      };

      const updateStepVisibility = () => {
        const modeValue = getModeValue();
        const isEdit = modeValue === "editar" || modeValue === "excluir";
        stepPanels.forEach((panel) => {
          if (panel.dataset.step === "3") {
            panel.style.display = isEdit ? "none" : "";
            panel.querySelectorAll("input, select, textarea").forEach((field) => {
              if (!isEdit && field.dataset.readonly === "1") {
                field.disabled = true;
                return;
              }
              field.disabled = isEdit;
            });
          }
        });
        if (isEdit && currentStep === 3) {
          currentStep = 2;
        }
        syncRequiredByVisibility();
        setStep(currentStep);
      };

    const normalizeDigits = (value) => {
      const digits = String(value || "").replace(/\D/g, "");
      return digits ? String(parseInt(digits, 10)) : "";
    };

    const syncSubacaoEntregaCounter = () => {
      if (!subacaoEntregaCounter || !subacaoEntregaInput) return;
      subacaoEntregaCounter.textContent = `${(subacaoEntregaInput.value || "").length}/260`;
    };

    const formatKey = () => {
      if (municipioItems.length && chaveSelects.regiao) {
        const currentRegiao = chaveSelects.regiao.value || "";
        if (currentRegiao && lastRegiaoValue && currentRegiao !== lastRegiaoValue) {
          setMsg("Remova todos os municípios antes de trocar a região.", true);
          chaveSelects.regiao.value = lastRegiaoValue;
        }
      }
      const regiao = normalizeDigits(chaveSelects.regiao.value);
      const subfuncao = normalizeDigits(chaveSelects.subfuncao.value);
      const ugRaw = normalizeDigits(chaveSelects.ug.value);
      const ug = ugRaw ? String(parseInt(ugRaw, 10)) : "";
      const adj = chaveSelects.adj.value || "";
      const macro = chaveSelects.macropolitica.value || "";
      const pilar = chaveSelects.pilar.value || "";
      const eixo = chaveSelects.eixo.value || "";
      const politica = chaveSelects.politica_decr.value || "";
      const publico = chaveSelects.publico_transversal.value || "";
      if (!regiao || !subfuncao || !ug || !adj || !macro || !pilar || !eixo || !politica || !publico) {
        if (chaveInput) chaveInput.value = "";
        return;
      }
      const subfuncaoUg = `${subfuncao}.${ug}`;
      const parts = [
        `R${regiao}`,
        subfuncaoUg,
        adj,
        macro,
        pilar,
        eixo,
        politica,
        publico,
      ];
      if (chaveInput) {
        chaveInput.value = `* ${parts.join(" * ")} *`;
      }
      syncSubacaoEntregaCounter();
    };

    const syncSelectByCodigo = (select, codigo) => {
      if (!select || !codigo) return;
      const codigoNorm = normalizeDigits(codigo);
      if (!codigoNorm) return;
      const options = Array.from(select.options || []);
      const match = options.find((opt) => {
        const val = normalizeDigits(opt.value || opt.textContent || "");
        return val === codigoNorm;
      });
      if (match) select.value = match.value;
    };

      const syncBridges = () => {
        if (chaveSelects.ug && unidGestoraSelect) {
          syncSelectByCodigo(unidGestoraSelect, chaveSelects.ug.value || "");
          unidGestoraSelect.disabled = true;
        }
        if (chaveSelects.regiao && regiaoEntregaSelect) {
          syncSelectByCodigo(regiaoEntregaSelect, chaveSelects.regiao.value || "");
          regiaoEntregaSelect.disabled = true;
        }
      if (regiaoEntregaSelect) {
        const newRegiao = regiaoEntregaSelect.value || "";
        const lastRegiao = regiaoEntregaSelect.dataset.bridgeValue || "";
        if (newRegiao && newRegiao !== lastRegiao) {
          regiaoEntregaSelect.dataset.bridgeValue = newRegiao;
          if (codigoSelect) codigoSelect.value = "";
          if (municipioSelect) municipioSelect.value = "";
          loadOptions();
        }
      }
    };

    const maskDate = (input) => {
      if (!input) return;
      const raw = String(input.value || "").replace(/\D/g, "");
        const d = raw.slice(0, 2);
        const m = raw.slice(2, 4);
        const y = raw.slice(4, 8);
        let out = d;
        if (m) out += `/${m}`;
        if (y) out += `/${y}`;
        input.value = out;
      };

      const maskCpf = (input) => {
        if (!input) return;
        const digits = String(input.value || "").replace(/\D/g, "").slice(0, 11);
        const part1 = digits.slice(0, 3);
        const part2 = digits.slice(3, 6);
        const part3 = digits.slice(6, 9);
        const part4 = digits.slice(9, 11);
        let out = part1;
        if (part2) out += `.${part2}`;
        if (part3) out += `.${part3}`;
        if (part4) out += `-${part4}`;
        input.value = out;
      };

      const isValidCpf = (value) => {
        const cpf = String(value || "").replace(/\D/g, "");
        if (cpf.length !== 11) return false;
        if (/^(\d)\1{10}$/.test(cpf)) return false;
        const calc = (base) => {
          let sum = 0;
          for (let i = 0; i < base.length; i += 1) {
            sum += Number(base[i]) * (base.length + 1 - i);
          }
          const mod = sum % 11;
          return mod < 2 ? 0 : 11 - mod;
        };
        const d1 = calc(cpf.slice(0, 9));
        const d2 = calc(cpf.slice(0, 9) + d1);
        return cpf === cpf.slice(0, 9) + String(d1) + String(d2);
      };

    const formatPtBr = (value) => {
      const n = Number(value || 0);
      if (Number.isNaN(n)) return "";
      return new Intl.NumberFormat("pt-BR", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }).format(n);
    };

    const formatMetaInput = () => {
      if (!metaInput) return;
      const digits = String(metaInput.value || "").replace(/\D/g, "");
      if (!digits) {
        metaInput.value = "";
        return;
      }
      const num = Number(digits) / 100;
      metaInput.value = formatPtBr(num);
    };
    const normalizeSubacaoMetaDisplay = (value) => {
      let raw = String(value || "").trim();
      if (!raw) return "";
      if (raw.startsWith("[") && raw.endsWith("]")) {
        try {
          const parsed = JSON.parse(raw);
          if (Array.isArray(parsed) && parsed.length) {
            raw = String(parsed[0] ?? "").trim();
          }
        } catch (err) {
          const inner = raw.slice(1, -1);
          const first = inner.split(",")[0] || "";
          raw = first.replace(/^["']|["']$/g, "").trim();
        }
      } else if (raw.includes("*")) {
        raw = raw.split("*")[0].trim();
      }
      if (!raw) return "";
      let cleaned = raw;
      const hasDot = cleaned.includes(".");
      const hasComma = cleaned.includes(",");
      if (hasDot && hasComma) {
        cleaned = cleaned.replace(/\./g, "").replace(",", ".");
      } else if (hasComma) {
        cleaned = cleaned.replace(",", ".");
      } else if (hasDot) {
        if (/^\d+\.\d{1,2}$/.test(cleaned)) {
          // "10.00" => decimal with dot
          cleaned = cleaned;
        } else if (/^\d{1,3}(\.\d{3})+$/.test(cleaned)) {
          // "1.000" => thousand separators
          cleaned = cleaned.replace(/\./g, "");
        } else {
          cleaned = cleaned.replace(/\./g, "");
        }
      }
      let num = Number(cleaned);
      if (Number.isNaN(num)) {
        const digits = raw.replace(/\D/g, "");
        if (!digits) return raw;
        num = Number(digits) / 100;
      }
      return formatPtBr(num);
    };
    const formatEditMetaNovoInput = () => {
      if (!editMetaNovoInput) return;
      const digits = String(editMetaNovoInput.value || "").replace(/\D/g, "");
      if (!digits) {
        editMetaNovoInput.value = "";
        return;
      }
      const num = Number(digits) / 100;
      editMetaNovoInput.value = formatPtBr(num);
    };
    const isValidDateBR = (value) => {
      const raw = String(value || "").trim();
      if (!raw) return false;
      const m = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
      if (!m) return false;
      const day = Number(m[1]);
      const month = Number(m[2]);
      const year = Number(m[3]);
      if (month < 1 || month > 12) return false;
      if (day < 1 || day > 31) return false;
      const dt = new Date(year, month - 1, day);
      return (
        dt.getFullYear() === year &&
        dt.getMonth() === month - 1 &&
        dt.getDate() === day
      );
    };

    const parseDateBR = (value) => {
      const raw = String(value || "").trim();
      const m = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
      if (!m) return null;
      const day = Number(m[1]);
      const month = Number(m[2]);
      const year = Number(m[3]);
      if (month < 1 || month > 12) return null;
      if (day < 1 || day > 31) return null;
      const dt = new Date(year, month - 1, day);
      if (dt.getFullYear() !== year || dt.getMonth() !== month - 1 || dt.getDate() !== day) {
        return null;
      }
      return dt;
    };

    const formatDecimalInput = (input) => {
      if (!input) return;
      const digits = String(input.value || "").replace(/\D/g, "");
      if (!digits) {
        input.value = "";
        return;
      }
      const num = Number(digits) / 100;
      input.value = formatPtBr(num);
    };

    const municipioItems = [];
    const etapaItems = [];

    function getSelectLabel(select) {
      if (!select) return "";
      const opt = select.options?.[select.selectedIndex];
      return (opt?.textContent || "").trim();
    }

    const extractMunicipioName = (label, value) => {
      const raw = String(label || "").trim();
      if (!raw) return "";
      if (String(value || "").trim().startsWith("5100000")) {
        return "";
      }
      const parts = raw.split(" - ");
      if (parts.length >= 2 && /^\d+/.test(parts[0].trim())) {
        return parts.slice(1).join(" - ").trim();
      }
      return raw;
    };

    const syncEtapaMunicipioOptions = () => {
      if (!etapaMunicipioSelect) return;
      const usados = new Set(
        etapaItems
          .map((item) => String(item?.municipio || "").trim())
          .filter((value) => value)
      );
      etapaMunicipioSelect.innerHTML = "<option value=\"\">Selecione...</option>";
      municipioItems.forEach((item) => {
        if (usados.has(String(item.municipios_entrega || "").trim())) return;
        const opt = document.createElement("option");
        opt.value = item.municipios_entrega;
        opt.textContent = item.municipioLabel || item.municipios_entrega;
        etapaMunicipioSelect.appendChild(opt);
      });
      if (etapaMunicipioSelect.options.length > 1) {
        etapaMunicipioSelect.selectedIndex = 1;
      } else {
        etapaMunicipioSelect.value = "";
      }
    };

    const renderMunicipioList = () => {
      if (!municipioListEl) return;
      municipioListEl.innerHTML = "";
      if (!municipioItems.length) {
        const empty = document.createElement("div");
        empty.className = "municipio-empty";
        empty.textContent = "Nenhum município adicionado.";
        municipioListEl.appendChild(empty);
        return;
      }
      municipioItems.forEach((item, idx) => {
        const row = document.createElement("div");
        row.className = "municipio-item";
        row.innerHTML = `
          <div>
            <small>Código</small>
            <div>${item.codigoLabel || item.codigo}</div>
          </div>
          <div>
            <small>Município</small>
            <div>${item.municipioLabel || item.municipios_entrega}</div>
          </div>
          <div>
            <small>Meta</small>
            <div>${normalizeSubacaoMetaDisplay(item.meta_subacao)}</div>
          </div>
          <div>
            <button class="btn btn-danger sm" type="button" data-remove-index="${idx}" ${
              approvalMode ? "disabled" : ""
            }>Remover</button>
          </div>
        `;
        municipioListEl.appendChild(row);
      });
      if (addEtapaBtn) {
        addEtapaBtn.style.display = municipioItems.length > 1 ? "inline-flex" : "none";
      }
      const hasItems = municipioItems.length > 0;
      if (codigoSelect) codigoSelect.required = !hasItems;
      if (municipioSelect) municipioSelect.required = !hasItems;
      if (metaInput) metaInput.required = !hasItems;
      syncEtapaMunicipioOptions();
    };

    if (municipioListEl) {
      municipioListEl.addEventListener("click", (ev) => {
        if (approvalMode) return;
        const btn = ev.target.closest("[data-remove-index]");
        if (!btn) return;
        const idx = Number(btn.getAttribute("data-remove-index"));
        if (Number.isNaN(idx)) return;
        const removed = municipioItems[idx];
        municipioItems.splice(idx, 1);
        if (removed?.municipios_entrega) {
          removeEtapasByMunicipio(removed.municipios_entrega);
        }
        renderMunicipioList();
        renderEtapaList();
        updateEtapaStepLabel();
      });
    }

    const handleRegiaoChange = () => {
      if (!planSelects.regiao) return;
      if (municipioItems.length) {
        setMsg("Remova todos os municípios antes de trocar a região.", true);
        planSelects.regiao.value = lastRegiaoValue || "";
        planSelects.regiao.blur();
        return;
      }
      lastRegiaoValue = planSelects.regiao.value;
    };

    if (planSelects.regiao) {
      planSelects.regiao.addEventListener("change", handleRegiaoChange);
      planSelects.regiao.addEventListener("input", handleRegiaoChange);
      form.addEventListener("change", (ev) => {
        if (ev.target === planSelects.regiao) handleRegiaoChange();
      });
    }

    const addMunicipioItem = (opts = {}) => {
      const codigoValue = codigoSelect?.value || "";
      const municipioValue = municipioSelect?.value || "";
      const codigoLabel = getSelectLabel(codigoSelect);
      const municipioLabel = getSelectLabel(municipioSelect);
      const codigoFromLabel = codigoLabel.includes(" - ")
        ? codigoLabel.split(" - ")[0].trim()
        : normalizeDigits(codigoLabel || "");
      const municipioFromLabel = municipioLabel.includes(" - ")
        ? municipioLabel.split(" - ").slice(1).join(" - ").trim()
        : String(municipioLabel || "").trim();
      const codigo = codigoValue || codigoFromLabel;
      const municipio = municipioValue || municipioFromLabel;
      const meta = metaInput?.value || "";
      if (!codigo || !municipio || !meta) {
        if (!opts.silent) setMsg("Informe código, município e meta para adicionar.", true);
        return false;
      }
      const regiaoAtual = regiaoEntregaSelect?.value || "";
      const regiaoChave = chaveSelects.regiao?.value || "";
      const regiaoAtualNorm = normalizeDigits(regiaoAtual);
      const regiaoChaveNorm = normalizeDigits(regiaoChave);
      if (regiaoChaveNorm && regiaoAtualNorm && regiaoChaveNorm !== regiaoAtualNorm) {
        if (!opts.silent) {
          setMsg("A região da Subação/Entrega não confere com a região da chave.", true);
        }
        return false;
      }
      if (municipioItems.length) {
        const baseRegiao = municipioItems[0].regiao_subacao || "";
        const baseRegiaoNorm = normalizeDigits(baseRegiao);
        if (baseRegiaoNorm && regiaoAtualNorm && baseRegiaoNorm !== regiaoAtualNorm) {
          if (!opts.silent) {
            setMsg("Remova todos os municípios antes de trocar a região.", true);
          }
          return false;
        }
      }
      const key = `${codigo}::${municipio}`;
      const exists = municipioItems.some((item) => `${item.codigo}::${item.municipios_entrega}` === key);
      const municipioExists = municipioItems.some((item) => item.municipios_entrega === municipio);
      if (exists) {
        if (!opts.silent) setMsg("Este município já foi adicionado.", true);
        return false;
      }
      if (municipioExists) {
        if (!opts.silent) setMsg("Este município já foi adicionado.", true);
        return false;
      }
      municipioItems.push({
        codigo,
        municipios_entrega: municipio,
        meta_subacao: normalizeSubacaoMetaDisplay(meta),
        regiao_subacao: regiaoAtual,
        codigoLabel: codigoLabel || codigo,
        municipioLabel: municipioLabel || municipio,
      });
      if (!opts.silent) setMsg("");
      if (codigoSelect) codigoSelect.value = "";
      if (municipioSelect) municipioSelect.value = "";
      if (metaInput) metaInput.value = "";
      setMsg("");
      renderMunicipioList();
      updateEtapaStepLabel();
      fillEtapaNomePrefix();
      return true;
    };

    const buildEtapaLabelPrefix = (municipioLabel, municipioValue) => {
      if (!String(municipioValue || "").trim()) return "";
      const rawLabel = String(municipioLabel || "").trim().toLowerCase();
      if (rawLabel.startsWith("selecione")) return "";
      const label = extractMunicipioName(municipioLabel, municipioValue);
      return label ? `${label} * ` : "";
    };

    const editMunicipioItems = [];
      const updateEditMunicipioRequired = () => {
        const isMunicipioEdit = ["novo_municipio", "remover_municipio"].includes(getEditModeKey());
        const shouldRequire = isMunicipioEdit && editMunicipioItems.length === 0;
        [editCodigoNovoSelect, editMunicipioNovoSelect, editMetaNovoInput].forEach((el) => {
          if (el) el.required = shouldRequire;
        });
      };

      const renderEditMunicipioList = () => {
        if (!editMunicipioListEl) return;
        editMunicipioListEl.innerHTML = "";
        if (!editMunicipioItems.length) {
          const empty = document.createElement("div");
          empty.className = "municipio-empty";
          empty.textContent = "Nenhum município adicionado.";
          editMunicipioListEl.appendChild(empty);
          if (editMunicipioListWrap) editMunicipioListWrap.style.display = "none";
          updateEditMunicipioRequired();
          return;
        }
        if (editMunicipioListWrap) editMunicipioListWrap.style.display = "";
        const modeKey = getEditModeKey();
        editMunicipioItems.forEach((item, idx) => {
          const row = document.createElement("div");
          row.className = "municipio-item";
          const allowRemove = modeKey === "remover_municipio";
          const removeBtn = `
            <button class="btn btn-danger sm" type="button" data-edit-remove-index="${idx}" ${
              approvalMode || editMunicipioLocked ? "disabled" : ""
            }>Remover</button>
          `;
          row.innerHTML = `
            <div>
              <small>Código</small>
              <div>${item.codigoLabel || item.codigo}</div>
            </div>
            <div>
              <small>Município</small>
              <div>${item.municipioLabel || item.municipios_entrega}</div>
            </div>
            <div>
              <small>Meta</small>
              <div>${normalizeSubacaoMetaDisplay(item.meta_subacao)}</div>
            </div>
            <div>${removeBtn}</div>
          `;
          editMunicipioListEl.appendChild(row);
        });
        if (editMunicipioAddBtn) editMunicipioAddBtn.disabled = approvalMode || editMunicipioLocked;
        updateEditMunicipioRequired();
      };

      const addEditMunicipioItem = () => {
        if (editMunicipioLocked && getEditModeKey() === "remover_municipio") {
          setMsg(
            "Antes de remover um município da Subação, por favor, exclua as etapas vinculadas.",
            true
          );
          return false;
        }
        const codigo = editCodigoNovoSelect?.value || "";
        const municipio = editMunicipioNovoSelect?.value || "";
        const meta = editMetaNovoInput?.value || "";
      if (!codigo || !municipio || !meta) {
        setMsg("Informe código, município e meta para adicionar.", true);
        return false;
      }
      const key = `${codigo}::${municipio}`;
      const exists = editMunicipioItems.some(
        (item) => `${item.codigo}::${item.municipios_entrega}` === key
      );
      if (exists) {
        setMsg("Este município já foi adicionado.", true);
        return false;
      }
      editMunicipioItems.push({
        codigo,
        municipios_entrega: municipio,
        meta_subacao: meta,
        codigoLabel: getSelectLabel(editCodigoNovoSelect),
        municipioLabel: getSelectLabel(editMunicipioNovoSelect),
      });
      if (editCodigoNovoSelect) editCodigoNovoSelect.value = "";
      if (editMunicipioNovoSelect) editMunicipioNovoSelect.value = "";
      if (editMetaNovoInput) editMetaNovoInput.value = "";
      setMsg("");
      renderEditMunicipioList();
      return true;
    };

    const syncEtapaCounter = () => {
      if (!etapaCounter || !etapaInput) return;
      etapaCounter.textContent = `${(etapaInput.value || "").length}/260`;
    };

    const parseDecimalInput = (value) => {
      const digits = String(value || "").replace(/\D/g, "");
      if (!digits) return 0;
      return Number(digits) / 100;
    };

    const fillEtapaNomePrefix = () => {
      if (!etapaInput || !etapaMunicipioSelect) return;
      const label = getSelectLabel(etapaMunicipioSelect);
      const value = etapaMunicipioSelect.value;
      if (!String(value || "").trim()) {
        const current = String(etapaInput.value || "");
        if (current.toLowerCase().startsWith("selecione")) {
          etapaInput.value = "";
        }
        syncEtapaCounter();
        return;
      }
      const valueRaw = String(value || "").trim();
      const labelRaw = String(label || "").toLowerCase();
      const isEstadoMt =
        valueRaw.startsWith("5100000") ||
        labelRaw.includes("estado mato grosso");
      if (isEstadoMt) {
        const raw = String(label || "").trim();
        const parts = raw.split(" - ");
        const name = parts.length >= 2 ? parts.slice(1).join(" - ").trim() : raw;
        const current = String(etapaInput.value || "");
        if (current.toLowerCase().startsWith("selecione")) {
          etapaInput.value = "";
          syncEtapaCounter();
          return;
        }
        const needle = name ? `${name} * ` : "";
        if (needle && current.startsWith(needle)) {
          etapaInput.value = current.slice(needle.length).trim();
        }
        syncEtapaCounter();
        return;
      }
      const prefix = buildEtapaLabelPrefix(label, value);
      if (!prefix) {
        const raw = String(label || "").trim();
        const parts = raw.split(" - ");
        const name = parts.length >= 2 ? parts.slice(1).join(" - ").trim() : raw;
        const current = String(etapaInput.value || "");
        const needle = name ? `${name} * ` : "";
        if (needle && current.startsWith(needle)) {
          etapaInput.value = current.slice(needle.length).trim();
        }
        syncEtapaCounter();
        return;
      }
      if (!etapaInput.value || !etapaInput.value.startsWith(prefix)) {
        etapaInput.value = prefix;
      }
      syncEtapaCounter();
    };

    const clearEtapaFields = () => {
      if (etapaInput) etapaInput.value = "";
      if (responsavelEtapaInput) responsavelEtapaInput.value = "";
      if (cpfEtapaInput) cpfEtapaInput.value = "";
      syncEtapaCounter();
    };

    const captureEtapaPayload = () => ({
      municipio: etapaMunicipioSelect?.value || "",
      municipio_label: getSelectLabel(etapaMunicipioSelect),
      nome_etapa: etapaInput?.value || "",
      responsavel_etapa: responsavelEtapaInput?.value || "",
      cpf_responsavel_etapa: cpfEtapaInput?.value || "",
    });

    const renderEtapaList = () => {
      if (!etapaListEl) return;
      etapaListEl.innerHTML = "";
      if (!etapaItems.length) {
        const empty = document.createElement("div");
        empty.className = "etapa-empty";
        empty.textContent = "Nenhuma etapa adicionada.";
        etapaListEl.appendChild(empty);
        if (etapaListWrap) etapaListWrap.style.display = "none";
        return;
      }
      if (etapaListWrap) etapaListWrap.style.display = "";
      etapaItems.forEach((item, idx) => {
        const row = document.createElement("div");
        row.className = "etapa-item";
        row.innerHTML = `
          <div>
            <small>Município</small>
            <div>${extractMunicipioName(item.municipio_label || item.municipio)}</div>
          </div>
          <div>
            <small>Nome da Etapa</small>
            <div>${item.nome_etapa}</div>
          </div>
          <div>
            <small>Responsável</small>
            <div>${item.responsavel_etapa || "-"}</div>
          </div>
          <div>
            <small>CPF do Responsável</small>
            <div>${formatCpf(item.cpf_responsavel_etapa || "") || "-"}</div>
          </div>
          <div>
            <button class="btn btn-danger sm" type="button" data-remove-etapa="${idx}">Remover</button>
          </div>
        `;
        etapaListEl.appendChild(row);
      });
    };

    const removeEtapasByMunicipio = (municipioValue) => {
      if (!municipioValue) return;
      const before = etapaItems.length;
      for (let i = etapaItems.length - 1; i >= 0; i -= 1) {
        if (etapaItems[i].municipio === municipioValue) {
          etapaItems.splice(i, 1);
        }
      }
      if (before !== etapaItems.length) {
        renderEtapaList();
        syncEtapaMunicipioOptions();
        updateEtapaStepLabel();
      }
    };

    const validateEtapaBlock = () => {
      if (!etapaMunicipioSelect || !etapaMunicipioSelect.value) {
        setMsg("Selecione o município da etapa.", true);
        etapaMunicipioSelect?.focus();
        return false;
      }
      if (!etapaInput?.value || etapaInput.value.length > 260) {
        setMsg("Informe o nome da etapa (até 260 caracteres).", true);
        etapaInput?.focus();
        return false;
      }
      if (cpfEtapaInput && !cpfEtapaInput.value) {
        setMsg("Informe o CPF do responsável da etapa.", true);
        cpfEtapaInput.focus();
        return false;
      }
      if (cpfEtapaInput && cpfEtapaInput.value && !isValidCpf(cpfEtapaInput.value)) {
        setMsg("CPF do responsável da etapa inválido.", true);
        cpfEtapaInput.focus();
        return false;
      }
      return true;
    };

    const setOptions = (select, items, placeholder, valueKey = "value", labelKey = "label", preserveMissing = false) => {
      if (!select) return;
      const current = select.value;
      select.innerHTML = "";
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = placeholder || "Selecione...";
      select.appendChild(opt);
      items.forEach((item) => {
        const isPrimitive = typeof item === "string" || typeof item === "number";
        const o = document.createElement("option");
        if (isPrimitive) {
          o.value = String(item);
          o.textContent = String(item);
        } else {
          o.value = item?.[valueKey] ?? "";
          o.textContent = item?.[labelKey] ?? item?.[valueKey] ?? "";
        }
        select.appendChild(o);
      });
      if (current) {
        select.value = current;
      }
      if (current && select.value !== current) {
        const alt = Array.from(select.options).find(
          (o) => o.value === current || o.textContent === current
        );
        if (alt) select.value = alt.value;
      }
      if (preserveMissing && current && select.value !== current) {
        const preserved = document.createElement("option");
        preserved.value = current;
        preserved.textContent = current;
        preserved.dataset.preserved = "1";
        select.appendChild(preserved);
        select.value = current;
      }
    };

    const setOptionsFromLabel = (select, items, placeholder, preserveMissing = false) => {
      if (!select) return;
      const current = select.value;
      const seen = new Set();
      select.innerHTML = "";
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = placeholder || "Selecione...";
      select.appendChild(opt);
      items.forEach((item) => {
        const raw = item.label ?? "";
        const label = String(raw);
        const key = label.replace(/\s+/g, " ").trim();
        if (seen.has(key)) return;
        seen.add(key);
        const o = document.createElement("option");
        o.value = label;
        o.textContent = label;
        select.appendChild(o);
      });
      if (current) select.value = current;
      if (preserveMissing && current && select.value !== current) {
        const preserved = document.createElement("option");
        preserved.value = current;
        preserved.textContent = current;
        preserved.dataset.preserved = "1";
        select.appendChild(preserved);
        select.value = current;
      }
    };
    const setSelectValueFallback = (select, value) => {
      if (!select) return;
      const text = String(value || "");
      if (text && !Array.from(select.options || []).some((opt) => opt.value === text)) {
        const opt = document.createElement("option");
        opt.value = text;
        opt.textContent = text;
        opt.dataset.preserved = "1";
        select.appendChild(opt);
      }
      select.value = text;
    };

    let planCatalogRows = [];
    const subacaoPlanOptionKeys = [
      "exercicio",
      "uo",
      "programa",
      "acao_paoe",
      "responsavel_acao",
      "produto_acao",
    ];
    const catalogRowsForPlanKey = (targetKey) => {
      if (!planCatalogRows.length) return [];
      return planCatalogRows.filter((row) => {
        for (const key of subacaoPlanOptionKeys) {
          if (key === targetKey) continue;
          const selectedValue = String(planSelects[key]?.value || "").trim();
          if (selectedValue && String(row[key] || "").trim() !== selectedValue) {
            return false;
          }
        }
        return true;
      });
    };
    const refreshPlanCascadeOptionsFromCatalog = () => {
      if (!planCatalogRows.length) return false;
      subacaoPlanOptionKeys.forEach((targetKey) => {
        const select = planSelects[targetKey];
        if (!select) return;
        const values = [];
        const seen = new Set();
        catalogRowsForPlanKey(targetKey).forEach((row) => {
          const value = String(row[targetKey] || "").trim();
          if (!value || seen.has(value)) return;
          seen.add(value);
          values.push(value);
        });
        values.sort((a, b) => a.localeCompare(b, "pt-BR", { sensitivity: "base" }));
        setOptions(select, values, "Selecione...", "value", "label", true);
      });
      return true;
    };

    let pendingOptionsAbort = null;
    let optionsSeq = 0;
    const loadOptions = async () => {
      const seq = ++optionsSeq;
      if (pendingOptionsAbort) {
        pendingOptionsAbort.abort();
      }
      pendingOptionsAbort = new AbortController();
      const url = new URL("/api/subacao/options", window.location.origin);
      Object.entries(planSelects).forEach(([key, el]) => {
        if (el?.value) url.searchParams.set(key, el.value);
      });
      Object.entries(chaveSelects).forEach(([key, el]) => {
        if (el?.value) url.searchParams.set(key, el.value);
      });
      if (regiaoEntregaSelect?.value) {
        url.searchParams.set("regiao_subacao", regiaoEntregaSelect.value);
      }
      if (codigoSelect?.value) {
        url.searchParams.set("codigo", codigoSelect.value);
      }
      try {
        const res = await fetch(url, {
          headers: { "X-Requested-With": "fetch" },
          signal: pendingOptionsAbort.signal,
        });
        if (!res.ok) return;
        const data = await res.json();
        if (seq !== optionsSeq) return;
        if (Array.isArray(data.option_rows) && data.option_rows.length) {
          planCatalogRows = data.option_rows;
        }
        const plan = data.plan21 || {};
        if (!refreshPlanCascadeOptionsFromCatalog()) {
          setOptions(planSelects.exercicio, plan.exercicio || [], "Selecione...", "value", "label", true);
          setOptions(planSelects.uo, plan.uo || [], "Selecione...", "value", "label", true);
          setOptions(planSelects.programa, plan.programa || [], "Selecione...", "value", "label", true);
          setOptions(planSelects.acao_paoe, plan.acao_paoe || [], "Selecione...", "value", "label", true);
          setOptions(planSelects.responsavel_acao, plan.responsavel_acao || [], "Selecione...", "value", "label", true);
          setOptions(planSelects.produto_acao, plan.produto_acao || [], "Selecione...", "value", "label", true);
        }

        setOptions(chaveSelects.regiao, data.regioes || [], "Selecione...", "value", "label", true);
        if (chaveSelects.regiao) {
          lastRegiaoValue = chaveSelects.regiao.value || lastRegiaoValue;
        }
        setOptions(chaveSelects.subfuncao, data.subfuncoes || [], "Selecione...", "value", "label", true);
        setOptions(chaveSelects.ug, data.ugs || [], "Selecione...", "value", "label", true);
        setOptions(chaveSelects.adj, data.adjs || [], "Selecione...", "value", "label", true);
        setOptions(chaveSelects.macropolitica, data.macropoliticas || [], "Selecione...", "value", "label", true);
        if (chaveSelects.pilar) {
          const pilares = Array.isArray(data.pilares) ? data.pilares : [];
          if (pilares.length) {
            setOptions(chaveSelects.pilar, pilares, "Selecione...", "value", "label", true);
          } else if (!chaveSelects.pilar.value) {
            setOptions(chaveSelects.pilar, [], "Selecione...");
          }
        }
        setOptions(chaveSelects.eixo, data.eixos || [], "Selecione...", "value", "label", true);
        setOptions(chaveSelects.politica_decr, data.politicas || [], "Selecione...", "value", "label", true);
        setOptions(chaveSelects.publico_transversal, data.publicos || [], "Selecione...", "value", "label", true);

        setOptionsFromLabel(regiaoEntregaSelect, data.regioes || [], "Selecione...");
        setOptions(codigoSelect, data.municipios || [], "Selecione...", "codigo", "label");
        setOptions(municipioSelect, data.municipios || [], "Selecione...", "nome", "label");
        if (editCodigoNovoSelect) {
          setOptions(editCodigoNovoSelect, data.municipios || [], "Selecione...", "codigo", "label");
        }
        if (editMunicipioNovoSelect) {
          setOptions(editMunicipioNovoSelect, data.municipios || [], "Selecione...", "nome", "label");
        }
        setOptions(responsavelNgerInput, data.responsaveis_nger || [], "Selecione...");
        setOptions(editResponsavelNgerInput, data.responsaveis_nger || [], "Selecione...");
        formatKey();
        syncBridges();
      } catch (err) {
        if (err.name !== "AbortError") console.error(err);
      }
    };

    let pendingEditAbort = null;
    let editOptionsSeq = 0;
    let editCatalogRows = [];
    const getEditSelectMap = () => ({
      chave_planejamento: editChaveSelect,
      subacao_entrega: editSubacaoSelect,
      responsavel: editResponsavelSelect,
      prazo: editPrazoSelect,
      unid_gestora: editUnidGestoraSelect,
      unidade_setorial_planejamento: editUnidadeSetorialSelect,
      produto_subacao: editProdutoSelect,
      regiao_subacao: editRegiaoSelect,
    });
    const editOptionKeys = Object.keys(getEditSelectMap());
    const catalogRowsForEditKey = (targetKey) => {
      if (!editCatalogRows.length) return [];
      const editSelectMap = getEditSelectMap();
      return editCatalogRows.filter((row) => {
        for (const key of subacaoPlanOptionKeys) {
          const selectedValue = String(planSelects[key]?.value || "").trim();
          if (selectedValue && String(row[key] || "").trim() !== selectedValue) {
            return false;
          }
        }
        for (const key of editOptionKeys) {
          if (key === targetKey) continue;
          const selectedValue = String(editSelectMap[key]?.value || "").trim();
          if (selectedValue && String(row[key] || "").trim() !== selectedValue) {
            return false;
          }
        }
        return true;
      });
    };
    const refreshEditOptionsFromCatalog = () => {
      if (!editCatalogRows.length) return false;
      const editSelectMap = getEditSelectMap();
      editOptionKeys.forEach((targetKey) => {
        const select = editSelectMap[targetKey];
        if (!select) return;
        const values = [];
        const seen = new Set();
        catalogRowsForEditKey(targetKey).forEach((row) => {
          const value = String(row[targetKey] || "").trim();
          if (!value || seen.has(value)) return;
          seen.add(value);
          values.push({ label: value });
        });
        values.sort((a, b) => a.label.localeCompare(b.label, "pt-BR", { sensitivity: "base" }));
        setOptionsFromLabel(select, values, "Selecione...", true);
      });
      return true;
    };

      const loadEditOptions = async () => {
        if (!editarGrid) return;
        const seq = ++editOptionsSeq;
      if (pendingEditAbort) {
        pendingEditAbort.abort();
      }
      pendingEditAbort = new AbortController();
      const url = new URL("/api/subacao/plan21-edit-options", window.location.origin);
      Object.entries(planSelects).forEach(([key, el]) => {
        if (el?.value) url.searchParams.set(key, el.value);
      });
      const editSelectMap = getEditSelectMap();
      Object.entries(editSelectMap).forEach(([key, el]) => {
        if (!el?.value) return;
        url.searchParams.set(key, el.value);
      });
      try {
        const res = await fetch(url, {
          headers: { "X-Requested-With": "fetch" },
          signal: pendingEditAbort.signal,
        });
        if (!res.ok) return;
        const data = await res.json();
        if (seq !== editOptionsSeq) return;
        if (Array.isArray(data.option_rows) && data.option_rows.length) {
          editCatalogRows = data.option_rows;
        }
        const options = data.options || {};
          if (!refreshEditOptionsFromCatalog()) {
            Object.entries(editSelectMap).forEach(([key, el]) => {
              if (!el) return;
              setOptionsFromLabel(el, (options[key] || []).map((v) => ({ label: v })), "Selecione...", true);
            });
          }
          if (pendingEditPref) {
            const applyIfAvailable = (el, value) => {
              if (!el || !value) return;
              const exists = Array.from(el.options || []).some((opt) => opt.value === value);
              if (exists) el.value = value;
            };
            if (editChaveSelect && pendingEditPref.chave_planejamento) {
              const exists = Array.from(editChaveSelect.options || []).some(
                (opt) => opt.value === pendingEditPref.chave_planejamento
              );
              if (!exists) {
                const opt = document.createElement("option");
                opt.value = pendingEditPref.chave_planejamento;
                opt.textContent = pendingEditPref.chave_planejamento;
                editChaveSelect.appendChild(opt);
              }
              editChaveSelect.value = pendingEditPref.chave_planejamento;
            }
            if (editSubacaoSelect && pendingEditPref.subacao_entrega) {
              const exists = Array.from(editSubacaoSelect.options || []).some(
                (opt) => opt.value === pendingEditPref.subacao_entrega
              );
              if (!exists) {
                const opt = document.createElement("option");
                opt.value = pendingEditPref.subacao_entrega;
                opt.textContent = pendingEditPref.subacao_entrega;
                editSubacaoSelect.appendChild(opt);
              }
              editSubacaoSelect.value = pendingEditPref.subacao_entrega;
            }
            if (editResponsavelSelect && pendingEditPref.responsavel) {
              const exists = Array.from(editResponsavelSelect.options || []).some(
                (opt) => opt.value === pendingEditPref.responsavel
              );
              if (!exists) {
                const opt = document.createElement("option");
                opt.value = pendingEditPref.responsavel;
                opt.textContent = pendingEditPref.responsavel;
                editResponsavelSelect.appendChild(opt);
              }
              editResponsavelSelect.value = pendingEditPref.responsavel;
            }
            applyIfAvailable(editPrazoSelect, pendingEditPref.prazo);
            applyIfAvailable(editUnidGestoraSelect, pendingEditPref.unid_gestora);
            applyIfAvailable(editUnidadeSetorialSelect, pendingEditPref.unidade_setorial_planejamento);
            if (editProdutoSelect && pendingEditPref.produto_subacao) {
              const exists = Array.from(editProdutoSelect.options || []).some(
                (opt) => opt.value === pendingEditPref.produto_subacao
              );
              if (!exists) {
                const opt = document.createElement("option");
                opt.value = pendingEditPref.produto_subacao;
                opt.textContent = pendingEditPref.produto_subacao;
                editProdutoSelect.appendChild(opt);
              }
              editProdutoSelect.value = pendingEditPref.produto_subacao;
            }
            applyIfAvailable(editRegiaoSelect, pendingEditPref.regiao_subacao);
            pendingEditPref = null;
          }
            const softSelects = [
              editResponsavelSelect,
              editPrazoSelect,
              editUnidGestoraSelect,
              editUnidadeSetorialSelect,
              editProdutoSelect,
              editRegiaoSelect,
            ];
        softSelects.forEach((el) => {
          if (!el) return;
          const count = el.options.length;
          if (!el.value && count === 2) {
            el.value = el.options[1].value;
          }
          });
          const modeKey = getEditModeKey();
          if (["novo_municipio", "remover_municipio"].includes(modeKey)) {
            await loadPlan21Municipios();
          }
          await loadEditMunicipios();
        } catch (err) {
          if (err.name !== "AbortError") console.error(err);
        }
      };

      const loadPlan21Municipios = async () => {
        if (!editChaveSelect || !editSubacaoSelect) return;
        const chave = editChaveSelect.value || "";
        const subacao = editSubacaoSelect.value || "";
        if (!chave || !subacao) return;
        const url = new URL("/api/subacao/plan21-municipios", window.location.origin);
        Object.entries(planSelects).forEach(([key, el]) => {
          if (el?.value) url.searchParams.set(key, el.value);
        });
        url.searchParams.set("chave_planejamento", chave);
        url.searchParams.set("subacao_entrega", subacao);
        try {
          const res = await fetch(url, { headers: { "X-Requested-With": "fetch" } });
          if (!res.ok) return;
          const data = await res.json();
          const items = Array.isArray(data.municipios) ? data.municipios : [];
          editMunicipioLocked =
            Boolean(data.has_etapas) && getEditModeKey() === "remover_municipio";
          if (editMunicipioLocked) {
            setMsg(
              "Antes de remover um município da Subação, por favor, exclua as etapas vinculadas.",
              true
            );
          }
          editMunicipioItems.length = 0;
          items.forEach((item) => {
            const codigo = String(item.codigo || "").trim();
            const municipio = String(item.municipio || "").trim();
            const meta = String(item.meta || "").trim();
            if (!codigo || !municipio || !meta) return;
            editMunicipioItems.push({
              codigo,
              municipios_entrega: municipio,
              meta_subacao: meta,
              codigoLabel: item.codigo_label || codigo,
              municipioLabel: item.municipio_label || municipio,
            });
          });
          renderEditMunicipioList();
          updateEditMunicipioRequired();
        } catch (err) {
          console.error(err);
        }
      };

    const loadEditMunicipios = async () => {
      if (!editCodigoNovoSelect || !editMunicipioNovoSelect) return;
      const url = new URL("/api/subacao/options", window.location.origin);
      if (editRegiaoSelect?.value) {
        url.searchParams.set("regiao_subacao", editRegiaoSelect.value);
      }
      try {
        const res = await fetch(url, { headers: { "X-Requested-With": "fetch" } });
        if (!res.ok) return;
        const data = await res.json();
        const municipios = Array.isArray(data.municipios) ? data.municipios : [];
        setOptions(editCodigoNovoSelect, municipios, "Selecione...", "codigo", "label");
        setOptions(editMunicipioNovoSelect, municipios, "Selecione...", "nome", "label");
      } catch (err) {
        console.error(err);
      }
    };

    const clearStepOne = () => {
      Object.values(planSelects).forEach((el) => {
        if (el) el.value = "";
      });
      Object.values(chaveSelects).forEach((el) => {
        if (el) el.value = "";
      });
      if (chaveInput) chaveInput.value = "";
      setMsg("");
      loadOptions();
    };

      const clearStepTwo = () => {
      if (subacaoEntregaInput) subacaoEntregaInput.value = "";
      if (responsavelInput) responsavelInput.value = "";
      if (cpfInput) cpfInput.value = "";
      if (dataInicioInput) dataInicioInput.value = "";
      if (dataFimInput) dataFimInput.value = "";
      if (unidGestoraSelect) unidGestoraSelect.value = "";
      if (unidadeSetorialSelect) unidadeSetorialSelect.value = "";
      if (produtoSubacaoSelect) produtoSubacaoSelect.value = "";
      if (unidadeMedidaSelect) unidadeMedidaSelect.value = "";
      if (regiaoEntregaSelect) regiaoEntregaSelect.value = "";
      if (codigoSelect) codigoSelect.value = "";
      if (municipioSelect) municipioSelect.value = "";
      if (metaInput) metaInput.value = "";
      if (detalhamentoInput) detalhamentoInput.value = "";
      if (editChaveSelect) editChaveSelect.value = "";
      if (editSubacaoSelect) editSubacaoSelect.value = "";
      if (editSubacaoInput) editSubacaoInput.value = "";
      if (editResponsavelSelect) editResponsavelSelect.value = "";
      if (editResponsavelInput) editResponsavelInput.value = "";
      if (editCpfInput) editCpfInput.value = "";
      if (editPrazoSelect) editPrazoSelect.value = "";
      if (editUnidGestoraSelect) editUnidGestoraSelect.value = "";
      if (editUnidadeSetorialSelect) editUnidadeSetorialSelect.value = "";
      if (editProdutoSelect) editProdutoSelect.value = "";
      if (editProdutoInput) editProdutoInput.value = "";
      if (editRegiaoSelect) editRegiaoSelect.value = "";
      if (editCodigoNovoSelect) editCodigoNovoSelect.value = "";
      if (editMunicipioNovoSelect) editMunicipioNovoSelect.value = "";
      if (editMetaNovoInput) editMetaNovoInput.value = "";
      if (editJustificativaInput) editJustificativaInput.value = "";
      if (editResponsavelNgerInput) editResponsavelNgerInput.value = "";
      editQuestionNames.forEach((name) => setEditQuestionValue(name, "nao"));
      updateQuestionVisibility();
      municipioItems.length = 0;
      renderMunicipioList();
      editMunicipioItems.length = 0;
      renderEditMunicipioList();
      if (etapaMunicipioSelect) {
        etapaMunicipioSelect.innerHTML = "<option value=\"\">Selecione...</option>";
      }
        setMsg("");
        syncSubacaoEntregaCounter();
        const isEditMode = getModeValue() === "editar";
        if (isEditMode) {
          loadEditOptions();
        } else {
          loadOptions();
        }
      };

    const clearStepThree = () => {
      clearEtapaFields();
      etapaItems.length = 0;
      renderEtapaList();
      updateEtapaStepLabel();
      setMsg("");
    };

      const clearForm = () => {
        if (currentStep === 1) {
          clearStepOne();
          clearStepTwo();
          clearStepThree();
          syncRequiredByVisibility();
          return;
        }
        if (currentStep === 2) {
          clearStepTwo();
          clearStepThree();
          syncRequiredByVisibility();
          return;
        }
        if (currentStep === 3) {
          clearStepThree();
          syncRequiredByVisibility();
          return;
      }
    };

    const getRows = () => {
      if (!summaryBody) return [];
      return Array.from(summaryBody.querySelectorAll(".dotacao-summary-row"));
    };

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;

    const clearPagination = () => {
      if (paginationEl) paginationEl.innerHTML = "";
    };

    const setResultsVisible = (show) => {
      if (!subacaoSummary) return;
      subacaoSummary.classList.toggle("dotacao-summary-hidden", !show);
      subacaoSummary.classList.toggle("consulta-summary-hidden", !show);
      if (!show) {
        getRows().forEach((row) => row.classList.remove("selected"));
        clearPagination();
      }
    };

    const getFilteredRows = () => {
      const rows = getRows();
      if (!criteria.length) return rows;
      return rows.filter((row) =>
        criteria.every((c) => {
          const rowVal = row.dataset[c.field] || "";
          return compareField(c.field, rowVal, c.value, c.op);
        })
      );
    };

    const renderPagination = (totalPages) => {
      if (!paginationEl) return;
      paginationEl.innerHTML = "";
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
        b.type = "button";
        b.className = "page-btn";
        b.textContent = label;
        if (disabled) b.disabled = true;
        if (active) b.classList.add("active");
        b.addEventListener("click", () => {
          if (disabled || page === currentPage) return;
          currentPage = page;
          renderSummaryPage();
        });
        paginationEl.appendChild(b);
      };
      addBtn("<<", 1, currentPage === 1);
      addBtn("<", Math.max(1, currentPage - 1), currentPage === 1);
      const maxButtons = 5;
      let start = Math.max(1, currentPage - Math.floor(maxButtons / 2));
      let end = Math.min(totalPages, start + maxButtons - 1);
      if (end - start + 1 < maxButtons) {
        start = Math.max(1, end - maxButtons + 1);
      }
      if (start > 1) {
        addBtn("1", 1, false, currentPage === 1);
        if (start > 2) {
          const ellipsis = document.createElement("span");
          ellipsis.textContent = "...";
          paginationEl.appendChild(ellipsis);
        }
      }
      for (let p = start; p <= end; p += 1) {
        addBtn(String(p), p, false, p === currentPage);
      }
      if (end < totalPages) {
        const ellipsis = document.createElement("span");
        ellipsis.textContent = "...";
        paginationEl.appendChild(ellipsis);
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }
      addBtn(">", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn(">>", totalPages, currentPage === totalPages);
    };

    const renderSummaryPage = () => {
      const rows = getRows();
      const filtered = getFilteredRows();
      const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const pageRows = filtered.slice(startIdx, startIdx + pageSize);
      rows.forEach((row) => {
        row.style.display = "none";
        row.classList.remove("selected");
      });
      pageRows.forEach((row) => {
        row.style.display = "";
      });
      renderPagination(totalPages);
    };

    const applyCriteriaToResults = (resetPage = true) => {
      if (resetPage) currentPage = 1;
      renderSummaryPage();
    };

    const flashSummaryWarning = () => {
      if (!subacaoSummary) return;
      subacaoSummary.classList.add("dotacao-summary-warn");
      setTimeout(() => {
        subacaoSummary.classList.remove("dotacao-summary-warn");
      }, 1200);
    };

    const bindRowSelection = () => {
      getRows().forEach((row) => {
        row.addEventListener("click", () => {
          setSelectedRow(row);
          setFilterMsg("");
        });
      });
    };

    const parseChaveParts = (chave) => {
      const raw = String(chave || "").trim();
      if (!raw) return {};
      const parts = raw
        .split("*")
        .map((p) => p.trim())
        .filter((p) => p);
      if (parts.length < 8) return {};
      const regiao = parts[0].replace(/^R/i, "").trim();
      const subfuncaoUg = parts[1] || "";
      const [subfuncao, ug] = subfuncaoUg.split(".").map((p) => p.trim());
      return {
        regiao,
        subfuncao,
        ug,
        adj: parts[2] || "",
        macropolitica: parts[3] || "",
        pilar: parts[4] || "",
        eixo: parts[5] || "",
        politica: parts[6] || "",
        publico: parts[7] || "",
      };
    };

    const setSelectedRow = (row) => {
      getRows().forEach((r) => r.classList.remove("selected"));
      if (row) row.classList.add("selected");
    };

    const decodeHtmlEntities = (value) => {
      if (value === null || value === undefined) return "";
      const textarea = document.createElement("textarea");
      textarea.innerHTML = String(value);
      return textarea.value;
    };

    const parseJsonArray = (value) => {
      if (!value) return [];
      if (Array.isArray(value)) {
        return value.filter((item) => String(item ?? "").trim());
      }
      let raw = decodeHtmlEntities(String(value)).trim();
      if (!raw) return [];
      const normalizeJsonLike = (str) => {
        let s = decodeHtmlEntities(str).trim();
        if (
          (s.startsWith('"') && s.endsWith('"')) ||
          (s.startsWith("'") && s.endsWith("'"))
        ) {
          s = s.slice(1, -1);
        }
        s = s.replace(/\\"/g, '"').replace(/\\'/g, "'");
        return s;
      };
      raw = normalizeJsonLike(raw);
      if (raw.startsWith("[") && raw.endsWith("]")) {
        const tryParse = (text) => {
          try {
            const parsed = JSON.parse(text);
            if (!Array.isArray(parsed)) return [];
            return parsed.filter((item) => String(item ?? "").trim());
          } catch (err) {
            return null;
          }
        };
        const parsed = tryParse(raw);
        if (parsed) return parsed;
        const alt = raw.includes('"') ? null : tryParse(raw.replace(/'/g, '"'));
        if (alt) return alt;
        const inner = raw.slice(1, -1);
        return inner
          .split(",")
          .map((part) => part.replace(/^["']|["']$/g, "").trim())
          .filter((part) => part);
      }
      if (raw.includes("*")) {
        return raw
          .split("*")
          .map((part) => String(part || "").trim())
          .filter((part) => part);
      }
      return [raw];
    };

      const escapeHtml = (value) => {
        return String(value || "")
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/"/g, "&quot;")
          .replace(/'/g, "&#39;");
      };

      const joinNonEmpty = (parts, sep = " - ") => {
        return parts.map((p) => String(p || "").trim()).filter(Boolean).join(sep);
      };

      const formatCpf = (value) => {
        const digits = String(value || "").replace(/\D/g, "").slice(0, 11);
        if (digits.length !== 11) return String(value || "").trim();
        return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6, 9)}-${digits.slice(9)}`;
      };

      const formatPrintDate = (value) => {
        if (!value) return "";
        const d = new Date(value);
        if (Number.isNaN(d.getTime())) return String(value);
        return d.toLocaleString("pt-BR");
      };

      const buildSubacaoPrintTable = (row) => {
        const tipoSolicitacao = String(row.dataset.tipoSolicitacao || "").trim().toLowerCase();
        const tipoEdicao = String(row.dataset.tipoEdicao || "").trim().toLowerCase();
        const showEtapas = tipoSolicitacao === "cadastrar";
        const showResponsavelSubacao = tipoSolicitacao !== "alterar" && tipoSolicitacao !== "excluir";
        const showProdutoSubacao = tipoSolicitacao !== "excluir";
        const showRegiaoPlanejamento = tipoSolicitacao !== "excluir";
        const showDetalhamento = tipoSolicitacao !== "excluir";
        const exercicio = row.dataset.exercicio || "";
        const programa = row.dataset.programa || "";
        const acaoPaoe = row.dataset.acaoPaoe || "";
        const uo = row.dataset.uo || "";
        const produtoAcao = row.dataset.produtoAcao || "";
        const subacaoEntrega = row.dataset.subacaoEntrega || "";
        const subacaoOrigem = row.dataset.subacaoOrigem || "";
        const responsavel = row.dataset.responsavel || "";
        const cpfResponsavel = formatCpf(row.dataset.cpf || "");
        const produtoSubacao = row.dataset.produtoSubacao || "";
        const unidadeMedida = row.dataset.unidadeMedida || "";
        const regiaoSubacaoRaw = row.dataset.regiaoSubacao || "";
        const detalhamento = row.dataset.detalhamento || "";
        const justificativa = row.dataset.justificativa || "";
        const responsavelNger = row.dataset.responsavelNger || "";

        const regioes = parseJsonArray(regiaoSubacaoRaw);
        const codigos = parseJsonArray(row.dataset.codigo);
        const municipios = parseJsonArray(row.dataset.municipio);
        const metas = parseJsonArray(row.dataset.meta);
        const regiaoRowsCount = Math.max(
          regioes.length,
          codigos.length,
          municipios.length,
          metas.length,
          1
        );
        const regiaoRows = [];
        for (let i = 0; i < regiaoRowsCount; i += 1) {
          const regiao = regioes[i] ?? regioes[0] ?? "";
          const codigo = codigos[i] ?? "";
          const municipio = municipios[i] ?? "";
          const meta = metas[i] ?? "";
          const municipioLabel = joinNonEmpty([codigo, municipio], " - ");
          regiaoRows.push(
            `<tr><td>${escapeHtml(regiao)}</td><td>${escapeHtml(municipioLabel)}</td><td>${escapeHtml(meta)}</td></tr>`
          );
        }

        const etapas = parseJsonArray(row.dataset.etapa);
        const responsaveis = parseJsonArray(row.dataset.responsavelEtapa);
        const cpfs = parseJsonArray(row.dataset.cpfResponsavelEtapa).map(formatCpf);
        const cpfFallback = formatCpf(row.dataset.cpfResponsavelEtapa || "");
        const etapaCount = Math.max(etapas.length, responsaveis.length, cpfs.length, 1);
        const etapaRows = [];
        if (showEtapas) {
          for (let i = 0; i < etapaCount; i += 1) {
            const etapa = etapas[i] ?? "";
            const resp = responsaveis[i] ?? "";
            const cpf = cpfs[i] ?? (cpfs.length ? "" : cpfFallback);
            etapaRows.push(`
              <tr><th>Relação das etapas</th><td>${escapeHtml(etapa)}</td></tr>
              <tr><th>Responsável da etapa</th><td>${escapeHtml(joinNonEmpty([resp, cpf]))}</td></tr>
            `);
          }
        }

        const subacaoNameRows =
          tipoSolicitacao === "alterar" && tipoEdicao === "subacao_name"
            ? `
              <tr><th>Nome da Subação de Origem</th><td>${escapeHtml(subacaoOrigem || "-")}</td></tr>
              <tr><th>Novo nome de Subação</th><td>${escapeHtml(subacaoEntrega)}</td></tr>
            `
            : `<tr><th>Nome da Subação</th><td>${escapeHtml(subacaoEntrega)}</td></tr>`;

        return `
          <table class="print-table subacao-print">
            <tbody>
              <tr><th>Exercício</th><td>${escapeHtml(exercicio)}</td></tr>
              <tr><th>Código e nome do Programa de governo</th><td>${escapeHtml(programa)}</td></tr>
              <tr><th>Código da Ação</th><td>${escapeHtml(acaoPaoe)}</td></tr>
              <tr><th>U.O. Responsável pela Ação</th><td>${escapeHtml(uo)}</td></tr>
              <tr><th>Produto da Ação</th><td>${escapeHtml(produtoAcao)}</td></tr>
              ${subacaoNameRows}
              ${showResponsavelSubacao
                ? `<tr><th>Nome e CPF do Responsável pela Subação *</th><td>${escapeHtml(
                    joinNonEmpty([responsavel, cpfResponsavel])
                  )}</td></tr>`
                : ""}
              ${
                showProdutoSubacao
                  ? `<tr>
                <th>Produto da subação e Unidade de Medida</th>
                <td>
                  <table class="print-inner">
                    <thead>
                      <tr><th>Produto da subação</th><th>Unidade de Medida</th></tr>
                    </thead>
                    <tbody>
                      <tr><td>${escapeHtml(produtoSubacao)}</td><td>${escapeHtml(unidadeMedida)}</td></tr>
                    </tbody>
                  </table>
                </td>
              </tr>`
                  : ""
              }
              ${
                showRegiaoPlanejamento
                  ? `<tr>
                <th>Região (es) de Planejamento / Município / quantidade</th>
                <td>
                  <table class="print-inner">
                    <thead>
                      <tr><th>Região</th><th>Município</th><th>Quantidade</th></tr>
                    </thead>
                    <tbody>
                      ${regiaoRows.join("")}
                    </tbody>
                  </table>
                </td>
              </tr>`
                  : ""
              }
              ${showDetalhamento
                ? `<tr><th>Detalhamento e qualificação do produto da subação</th><td>${escapeHtml(
                    detalhamento
                  )}</td></tr>`
                : ""}
              ${etapaRows.join("")}
              <tr><th>${
                tipoSolicitacao === "excluir"
                  ? "Justificativa para cancelamento"
                  : tipoSolicitacao === "alterar"
                    ? "Justificativa para alteração"
                    : "Justificativa para Inclusão"
              }</th><td>${escapeHtml(justificativa)}</td></tr>
              <tr><th>Nome do Responsável pelo NGER</th><td>${escapeHtml(responsavelNger)}</td></tr>
            </tbody>
          </table>
        `;
      };

      const openSubacaoPrintPopup = (row, targetWin = null) => {
        const tipoSolicitacao = String(row.dataset.tipoSolicitacao || "").trim().toLowerCase();
        const tipoEdicao = String(row.dataset.tipoEdicao || "").trim().toLowerCase();
        const titleMap = {
          cadastrar: "Cadastrar Subação",
          editar: "Alterar Subação",
          alterar: "Alterar Subação",
          excluir: "Excluir Subação",
        };
        const alteracaoTitleMap = {
          subacao_name: "ALTERAR SUBAÇÃO: NOME DA SUBAÇÃO",
          responsavel_name: "ALTERAR SUBAÇÃO: NOME DO RESPONSÁVEL",
          produto_subacao: "ALTERAR SUBAÇÃO: PRODUTO DA SUBAÇÃO",
          remover_municipio: "ALTERAR SUBAÇÃO: TROCAR MUNICÍPIO DA REGIÃO",
          novo_municipio: "ALTERAR SUBAÇÃO: ACRESCENTAR MUNICÍPIO NA REGIÃO",
        };
        const titulo =
          tipoSolicitacao === "alterar" || tipoSolicitacao === "editar"
            ? alteracaoTitleMap[tipoEdicao] || "ALTERAR SUBAÇÃO"
            : titleMap[tipoSolicitacao] || "Cadastrar Subação";
        const controle = row.dataset.controleSubacao || "";
        const criadoEm = formatPrintDate(row.dataset.criadoEm || "");
        const usuarioNome = row.dataset.usuarioNome || "";
        const usuarioPerfil = row.dataset.usuarioPerfil || "";
        const aprovadoNome = row.dataset.aprovadoPorNome || "";
        const aprovadoPerfil = row.dataset.aprovadoPorPerfil || "";
        const aprovadoEm = formatPrintDate(row.dataset.dataAprovacao || "");
        const status = String(row?.dataset?.statusAprovacao || "").trim().toLowerCase();
        const footerLine2 = joinNonEmpty(
          [
            joinNonEmpty([usuarioNome, usuarioPerfil]),
            criadoEm ? `cadastrado em ${criadoEm}` : "",
            controle,
          ],
          " - "
        );
        const statusEventoLabel =
          status === "rejeitado"
            ? "rejeitado em"
            : status === "aprovado"
              ? "aprovado em"
              : "aprovado em";
        const footerLine3 = joinNonEmpty(
          [
            joinNonEmpty([aprovadoNome, aprovadoPerfil]),
            aprovadoEm ? `${statusEventoLabel} ${aprovadoEm}` : "",
          ],
          " - "
        );
        let watermarkText = "";
        if (status === "aguardando") watermarkText = "AGUARDANDO";
        if (status === "rejeitado") watermarkText = "REJEITADO";
        const html = `<!doctype html>
  <html>
  <head>
    <meta charset="utf-8" />
    <title>Cadastrar Subação</title>
    <style>
      body { font-family: Arial, sans-serif; color: #000; margin: 12px 20px 24px; padding-bottom: 80px; }
      .print-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; padding-bottom: 6px; border-bottom: 1px dashed #000; }
      .print-brand { display: flex; align-items: center; gap: 12px; }
      .print-brand img { height: 48px; }
      .print-brand-title { font-weight: 700; font-size: 16px; }
      .print-brand-subtitle { font-size: 12px; color: #333; }
      .print-title-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin: 8px 0 18px; }
      .print-title { text-align: center; font-weight: 700; flex: 1; text-transform: uppercase; }
      .print-title-key { min-width: 200px; font-size: 12px; }
      .print-title-date { min-width: 200px; text-align: right; font-size: 12px; }
      .print-footer { position: fixed; left: 20px; right: 20px; bottom: 12px; border-top: 1px dashed #000; font-size: 11px; padding-top: 6px; display: flex; align-items: center; justify-content: space-between; gap: 12px; }
      .print-footer img { height: 36px; }
      .print-footer-text { flex: 1; text-align: center; line-height: 1.35; }
      .print-body { margin-top: 4em; }
      .print-table { width: 100%; border-collapse: collapse; margin-bottom: 12px; table-layout: fixed; }
      .print-table th, .print-table td { border: 1px solid #000; padding: 6px 8px; text-align: left; font-size: 10px; vertical-align: top; word-break: break-word; }
      .print-table th { width: 35%; background: #dddddd; box-shadow: inset 0 0 0 9999px #dddddd; -webkit-print-color-adjust: exact; print-color-adjust: exact; color-adjust: exact; }
      .print-inner { width: 100%; border-collapse: collapse; table-layout: fixed; }
      .print-inner th, .print-inner td { border: 1px solid #000; padding: 4px 6px; font-size: 9px; vertical-align: top; }
      .print-inner th { background: #e5e5e5; box-shadow: inset 0 0 0 9999px #e5e5e5; -webkit-print-color-adjust: exact; print-color-adjust: exact; color-adjust: exact; }
      .print-watermark { position: fixed; top: 45%; left: 50%; transform: translate(-50%, -50%) rotate(-30deg); font-size: 60px; color: rgba(0,0,0,0.12); font-family: \"Arial Black\", Arial, sans-serif; text-transform: uppercase; white-space: pre-line; text-align: center; pointer-events: none; }
    </style>
  </head>
  <body>
    ${watermarkText ? `<div class="print-watermark">${watermarkText}</div>` : ""}
    <div class="print-header">
      <div class="print-brand">
        <img src="/static/img/logo.jpg" alt="Logo" />
        <div class="print-brand-text">
          <div class="print-brand-title">Sistema de Planejamento e Orçamento</div>
          <div class="print-brand-subtitle">SPO-NGER-SEDUCMT</div>
        </div>
      </div>
    </div>
    <div class="print-title-row">
      <div class="print-title-key">${escapeHtml(controle)}</div>
      <div class="print-title">${escapeHtml(titulo)}</div>
      <div class="print-title-date">${escapeHtml(criadoEm)}</div>
    </div>
    <div class="print-body">
      ${buildSubacaoPrintTable(row)}
    </div>
    <div class="print-footer">
      <img src="/static/img/logo.jpg" alt="Logo" />
        <div class="print-footer-text">
        ${footerLine2 ? `<div>${escapeHtml(footerLine2)}</div>` : ""}
        ${footerLine3 ? `<div>${escapeHtml(footerLine3)}</div>` : ""}
      </div>
      <img src="/static/img/logoseduc.jpg" alt="Logo Seduc" />
    </div>
  </body>
  </html>`;
        const win =
          targetWin && !targetWin.closed ? targetWin : window.open("", "_blank");
        if (!win) {
          setFilterMsg("Popup bloqueado. Libere o navegador para imprimir.", true);
          return;
        }
        win.document.open();
        win.document.write(html);
        win.document.close();
        win.focus();
        setTimeout(() => {
          win.print();
        }, 300);
      };

    const prepareAutoPrintWindow = () => {
      const existing = window.__subacaoAutoPrintWin;
      if (existing && !existing.closed) return existing;
      const win = window.open("", "_blank");
      if (!win) return null;
      try {
        win.document.open();
        win.document.write("<!doctype html><html><head><meta charset=\"utf-8\" /><title>Preparando impressão...</title></head><body>Preparando impressão...</body></html>");
        win.document.close();
      } catch (err) {
        console.error(err);
      }
      window.__subacaoAutoPrintWin = win;
      return win;
    };

    let isPrefill = false;
    let currentControleSubacao = "";

    const fillFormFromRow = async (row) => {
      if (!row) return;
      const initialStep = currentStep;
      let prefilledMunicipios = false;
        const chave = row.dataset.chave || "";
        currentControleSubacao = row.dataset.controleSubacao || "";
        updateApprovalQuestion();
        const chaveParts = parseChaveParts(chave);
        const subacaoEntregaFull = row.dataset.subacaoEntrega || "";
      let subacaoEntregaRaw = subacaoEntregaFull;
      if (chave && subacaoEntregaFull.startsWith(chave)) {
        subacaoEntregaRaw = subacaoEntregaFull.slice(chave.length).trim();
      }
      const prazo = row.dataset.prazo || "";
      const prazoParts = prazo.split(" a ");
      if (idInput) idInput.value = row.dataset.id || "";
      const tipoSolic = String(row.dataset.tipoSolicitacao || "").trim().toLowerCase();
      if (modeSelect && row.dataset.tipoSolicitacao) {
        isPrefill = true;
        modeSelect.value = tipoSolic === "editar" ? "alterar" : tipoSolic;
        modeSelect.dispatchEvent(new Event("change", { bubbles: true }));
        isPrefill = false;
      }
      if (["editar", "alterar"].includes(String(row.dataset.tipoSolicitacao || "").toLowerCase()) && row.dataset.tipoEdicao) {
        const tipo = row.dataset.tipoEdicao;
        editQuestionNames.forEach((q) => {
          form.querySelectorAll(`input[name="${q}"]`).forEach((el) => {
            el.checked = false;
          });
        });
        if (tipo === "subacao_name") {
          setEditQuestionValue("subacao_edit_q_subacao", "sim");
        } else if (tipo === "responsavel_name") {
          setEditQuestionValue("subacao_edit_q_subacao", "nao");
          setEditQuestionValue("subacao_edit_q_responsavel", "sim");
        } else if (tipo === "produto_subacao") {
          setEditQuestionValue("subacao_edit_q_subacao", "nao");
          setEditQuestionValue("subacao_edit_q_responsavel", "nao");
          setEditQuestionValue("subacao_edit_q_produto", "sim");
        } else if (tipo === "novo_municipio") {
          setEditQuestionValue("subacao_edit_q_subacao", "nao");
          setEditQuestionValue("subacao_edit_q_responsavel", "nao");
          setEditQuestionValue("subacao_edit_q_produto", "nao");
          setEditQuestionValue("subacao_edit_q_remove_municipio", "nao");
          setEditQuestionValue("subacao_edit_q_municipio", "sim");
        } else if (tipo === "remover_municipio") {
          setEditQuestionValue("subacao_edit_q_subacao", "nao");
          setEditQuestionValue("subacao_edit_q_responsavel", "nao");
          setEditQuestionValue("subacao_edit_q_produto", "nao");
          setEditQuestionValue("subacao_edit_q_remove_municipio", "sim");
          setEditQuestionValue("subacao_edit_q_municipio", "nao");
        }
        updateQuestionVisibility();
        syncEditMode();
      }
      if (planSelects.exercicio) setSelectValueFallback(planSelects.exercicio, row.dataset.exercicio || "");
      if (planSelects.uo) setSelectValueFallback(planSelects.uo, row.dataset.uo || "");
      if (planSelects.programa) setSelectValueFallback(planSelects.programa, row.dataset.programa || "");
      if (planSelects.acao_paoe) setSelectValueFallback(planSelects.acao_paoe, row.dataset.acaoPaoe || "");
      if (planSelects.responsavel_acao) setSelectValueFallback(planSelects.responsavel_acao, row.dataset.responsavelAcao || "");
      if (planSelects.produto_acao) setSelectValueFallback(planSelects.produto_acao, row.dataset.produtoAcao || "");

      setSelectValueFallback(chaveSelects.regiao, chaveParts.regiao || "");
      setSelectValueFallback(chaveSelects.subfuncao, chaveParts.subfuncao || "");
      if (chaveParts.ug) {
        setSelectValueFallback(chaveSelects.ug, String(chaveParts.ug).padStart(4, "0"));
      }
      setSelectValueFallback(chaveSelects.adj, chaveParts.adj || "");
      setSelectValueFallback(chaveSelects.macropolitica, chaveParts.macropolitica || "");
      setSelectValueFallback(chaveSelects.pilar, chaveParts.pilar || "");
      setSelectValueFallback(chaveSelects.eixo, chaveParts.eixo || "");
      setSelectValueFallback(chaveSelects.politica_decr, chaveParts.politica || "");
      setSelectValueFallback(chaveSelects.publico_transversal, chaveParts.publico || "");
      if (chaveInput) chaveInput.value = chave;

      if (subacaoEntregaInput) subacaoEntregaInput.value = subacaoEntregaRaw || "";
      syncSubacaoEntregaCounter();
      if (responsavelInput) responsavelInput.value = row.dataset.responsavel || "";
      if (cpfInput) cpfInput.value = row.dataset.cpf || "";
      if (dataInicioInput) dataInicioInput.value = prazoParts[0] || "";
      if (dataFimInput) dataFimInput.value = prazoParts[1] || "";
      if (unidGestoraSelect) setSelectValueFallback(unidGestoraSelect, row.dataset.unidGestora || "");
      if (unidadeSetorialSelect) setSelectValueFallback(unidadeSetorialSelect, row.dataset.unidadeSetorial || "");
      if (produtoSubacaoSelect) setSelectValueFallback(produtoSubacaoSelect, row.dataset.produtoSubacao || "");
      if (unidadeMedidaSelect) setSelectValueFallback(unidadeMedidaSelect, row.dataset.unidadeMedida || "");
      if (regiaoEntregaSelect) setSelectValueFallback(regiaoEntregaSelect, row.dataset.regiaoSubacao || "");
      const codigoPref = parseJsonArray(row.dataset.codigo)[0] || "";
      const municipioPref = parseJsonArray(row.dataset.municipio)[0] || "";
      const metaPref = parseJsonArray(row.dataset.meta)[0] || "";
      if (codigoSelect) setSelectValueFallback(codigoSelect, codigoPref);
      if (municipioSelect) setSelectValueFallback(municipioSelect, municipioPref);
      if (metaInput) metaInput.value = normalizeSubacaoMetaDisplay(metaPref);
      if (detalhamentoInput) detalhamentoInput.value = decodeHtmlEntities(row.dataset.detalhamento || "");
      if (etapaInput) etapaInput.value = parseJsonArray(row.dataset.etapa)[0] || "";
      if (responsavelEtapaInput) {
        responsavelEtapaInput.value = parseJsonArray(row.dataset.responsavelEtapa)[0] || "";
      }
      if (cpfEtapaInput) cpfEtapaInput.value = parseJsonArray(row.dataset.cpfResponsavelEtapa)[0] || "";
      if (justificativaInput) justificativaInput.value = row.dataset.justificativa || "";
      if (responsavelNgerInput) responsavelNgerInput.value = row.dataset.responsavelNger || "";

      if (tipoSolic === "cadastrar") {
        municipioItems.length = 0;
        const codigos = parseJsonArray(row.dataset.codigo);
        const municipios = parseJsonArray(row.dataset.municipio);
        const metas = parseJsonArray(row.dataset.meta);
        const regiaoAtual = row.dataset.regiaoSubacao || "";
        const total = Math.max(codigos.length, municipios.length, metas.length);
        for (let i = 0; i < total; i += 1) {
          const codigo = codigos[i] ?? "";
          const municipio = municipios[i] ?? "";
          const meta = metas[i] ?? "";
          if (!codigo && !municipio && !meta) continue;
          municipioItems.push({
            codigo,
            codigoLabel: codigo,
            municipios_entrega: municipio,
            municipioLabel: municipio,
            meta_subacao: normalizeSubacaoMetaDisplay(meta),
            regiao_subacao: regiaoAtual,
          });
        }
        renderMunicipioList();
        prefilledMunicipios = municipioItems.length > 0;
        if (prefilledMunicipios) {
          if (codigoSelect) codigoSelect.value = "";
          if (municipioSelect) municipioSelect.value = "";
          if (metaInput) metaInput.value = "";
        }

        etapaItems.length = 0;
        const etapas = parseJsonArray(row.dataset.etapa);
        const responsaveis = parseJsonArray(row.dataset.responsavelEtapa);
        const cpfs = parseJsonArray(row.dataset.cpfResponsavelEtapa);
        const cpfFallback = row.dataset.cpfResponsavelEtapa || "";
        const etapaTotal = Math.max(municipios.length, etapas.length, responsaveis.length, cpfs.length);
        for (let i = 0; i < etapaTotal; i += 1) {
          const municipio = municipios[i] ?? "";
          const nomeEtapa = etapas[i] ?? "";
          const resp = responsaveis[i] ?? "";
          const cpf = cpfs[i] ?? cpfs[0] ?? cpfFallback ?? "";
          if (!municipio && !nomeEtapa && !resp && !cpf) continue;
          etapaItems.push({
            municipio,
            municipio_label: municipio,
            nome_etapa: nomeEtapa,
            responsavel_etapa: resp,
            cpf_responsavel_etapa: cpf,
          });
        }
        renderEtapaList();
        syncEtapaMunicipioOptions();
        updateEtapaStepLabel();
      }
        if (editChaveSelect) setSelectValueFallback(editChaveSelect, row.dataset.chave || "");
        if (editSubacaoSelect) setSelectValueFallback(editSubacaoSelect, row.dataset.subacaoEntrega || "");
        if (editSubacaoInput) editSubacaoInput.value = subacaoEntregaRaw || "";
        if (editResponsavelInput) editResponsavelInput.value = row.dataset.responsavel || "";
        if (editCpfInput) editCpfInput.value = row.dataset.cpf || "";
        if (editProdutoInput) editProdutoInput.value = row.dataset.produtoSubacao || "";
        if (editJustificativaInput) editJustificativaInput.value = row.dataset.justificativa || "";
        if (editResponsavelNgerInput) editResponsavelNgerInput.value = row.dataset.responsavelNger || "";
        selectedPlan21Ids = {
          id: row.dataset.plan21NgerId || "",
          ids: row.dataset.plan21NgerIds || "",
        };
        pendingEditPref = {
          chave_planejamento: row.dataset.chave || "",
          subacao_entrega: row.dataset.subacaoEntrega || "",
          responsavel: row.dataset.responsavel || "",
          prazo: row.dataset.prazo || "",
          unid_gestora: row.dataset.unidGestora || "",
          unidade_setorial_planejamento: row.dataset.unidadeSetorial || "",
          produto_subacao: row.dataset.produtoSubacao || "",
          regiao_subacao: row.dataset.regiaoSubacao || "",
        };
        syncBridges();
        if (["editar", "alterar", "excluir"].includes(tipoSolic)) {
          await loadEditOptions();
        }
      formatMetaInput();
      await loadOptions();
      if (planSelects.exercicio) setSelectValueFallback(planSelects.exercicio, row.dataset.exercicio || "");
      if (planSelects.uo) setSelectValueFallback(planSelects.uo, row.dataset.uo || "");
      if (planSelects.programa) setSelectValueFallback(planSelects.programa, row.dataset.programa || "");
      if (planSelects.acao_paoe) setSelectValueFallback(planSelects.acao_paoe, row.dataset.acaoPaoe || "");
      if (planSelects.responsavel_acao) setSelectValueFallback(planSelects.responsavel_acao, row.dataset.responsavelAcao || "");
      if (planSelects.produto_acao) setSelectValueFallback(planSelects.produto_acao, row.dataset.produtoAcao || "");
      setSelectValueFallback(chaveSelects.regiao, chaveParts.regiao || "");
      setSelectValueFallback(chaveSelects.subfuncao, chaveParts.subfuncao || "");
      if (chaveParts.ug) setSelectValueFallback(chaveSelects.ug, String(chaveParts.ug).padStart(4, "0"));
      setSelectValueFallback(chaveSelects.adj, chaveParts.adj || "");
      setSelectValueFallback(chaveSelects.macropolitica, chaveParts.macropolitica || "");
      setSelectValueFallback(chaveSelects.pilar, chaveParts.pilar || "");
      setSelectValueFallback(chaveSelects.eixo, chaveParts.eixo || "");
      setSelectValueFallback(chaveSelects.politica_decr, chaveParts.politica || "");
      setSelectValueFallback(chaveSelects.publico_transversal, chaveParts.publico || "");
      if (unidGestoraSelect) setSelectValueFallback(unidGestoraSelect, row.dataset.unidGestora || "");
      if (unidadeSetorialSelect) setSelectValueFallback(unidadeSetorialSelect, row.dataset.unidadeSetorial || "");
      if (produtoSubacaoSelect) setSelectValueFallback(produtoSubacaoSelect, row.dataset.produtoSubacao || "");
      if (unidadeMedidaSelect) setSelectValueFallback(unidadeMedidaSelect, row.dataset.unidadeMedida || "");
      if (regiaoEntregaSelect) setSelectValueFallback(regiaoEntregaSelect, row.dataset.regiaoSubacao || "");
      if (codigoSelect) setSelectValueFallback(codigoSelect, codigoPref);
      if (municipioSelect) setSelectValueFallback(municipioSelect, municipioPref);
      if (editChaveSelect) setSelectValueFallback(editChaveSelect, row.dataset.chave || "");
      if (editSubacaoSelect) setSelectValueFallback(editSubacaoSelect, row.dataset.subacaoEntrega || "");
      if (["editar", "alterar", "excluir"].includes(tipoSolic)) {
        await loadEditOptions();
        if (editChaveSelect) setSelectValueFallback(editChaveSelect, row.dataset.chave || "");
        if (editSubacaoSelect) setSelectValueFallback(editSubacaoSelect, row.dataset.subacaoEntrega || "");
      }
      if (currentStep === initialStep) {
        setStep(1);
      }
      if (!prefilledMunicipios) {
        municipioItems.length = 0;
        etapaItems.length = 0;
        renderMunicipioList();
        renderEtapaList();
      }
      if (etapaMunicipioSelect) {
        etapaMunicipioSelect.innerHTML = "<option value=\"\">Selecione...</option>";
      }
      clearEtapaFields();
      syncSubacaoEntregaCounter();
      updateEditingBanner();
    };

    const getSelectedRow = () => summaryBody?.querySelector(".dotacao-summary-row.selected");

    const loadSubacaoActionFromRow = async (row, action) => {
      if (!row) return;
      setSelectedRow(row);
      currentControleSubacao = row.dataset.controleSubacao || "";
      if (idInput && row.dataset.id) idInput.value = row.dataset.id;
      if (action === "approve") {
        setApprovalMode(true);
        updateEditingBanner();
        await fillFormFromRow(row);
        setApprovalMode(true);
        updateEditingBanner();
        return;
      }
      setApprovalMode(false);
      updateEditingBanner();
      await fillFormFromRow(row);
      updateEditingBanner();
    };

      Object.values(planSelects).forEach((el) => {
        if (!el) return;
        el.addEventListener("change", () => {
          el.dataset.touched = "1";
          refreshPlanCascadeOptionsFromCatalog();
          loadOptions();
          const modeValue = getModeValue();
          if (modeValue === "editar" || modeValue === "excluir") {
            loadEditOptions();
          }
        });
      });
    Object.values(chaveSelects).forEach((el) => {
      if (!el) return;
      el.addEventListener("change", () => {
        el.dataset.touched = "1";
        loadOptions();
        formatKey();
        syncBridges();
      });
    });
    if (dataInicioInput) dataInicioInput.addEventListener("input", () => maskDate(dataInicioInput));
    if (dataFimInput) dataFimInput.addEventListener("input", () => maskDate(dataFimInput));
    if (cpfInput) cpfInput.addEventListener("input", () => maskCpf(cpfInput));
    if (editCpfInput) editCpfInput.addEventListener("input", () => maskCpf(editCpfInput));
    if (cpfEtapaInput) cpfEtapaInput.addEventListener("input", () => maskCpf(cpfEtapaInput));
    if (subacaoEntregaInput) {
      subacaoEntregaInput.setAttribute("maxlength", "260");
      subacaoEntregaInput.addEventListener("input", () => {
        syncSubacaoEntregaCounter();
      });
    }
    if (regiaoEntregaSelect) {
      regiaoEntregaSelect.disabled = true;
      regiaoEntregaSelect.addEventListener("change", () => {
        loadOptions();
      });
    }
    if (codigoSelect) {
      codigoSelect.addEventListener("change", () => {
        const selectedCodigo = codigoSelect.value || "";
        const match = Array.from(municipioSelect?.options || []).find((opt) => {
          const optCodigo = normalizeDigits(opt.textContent || "");
          return optCodigo === normalizeDigits(selectedCodigo);
        });
        if (match && municipioSelect) municipioSelect.value = match.value;
      });
    }
    if (municipioSelect) {
      municipioSelect.addEventListener("change", () => {
        const selectedNome = municipioSelect.value || "";
        const match = Array.from(codigoSelect?.options || []).find((opt) => {
          const label = opt.textContent || "";
          return label.endsWith(selectedNome) || label.includes(`- ${selectedNome}`);
        });
        if (match && codigoSelect) codigoSelect.value = match.value;
      });
    }
      if (metaInput) metaInput.addEventListener("input", formatMetaInput);
      if (editMetaNovoInput) editMetaNovoInput.addEventListener("input", formatEditMetaNovoInput);
    if (etapaInput) {
      etapaInput.setAttribute("maxlength", "260");
      etapaInput.addEventListener("input", syncEtapaCounter);
    }
    if (etapaMunicipioSelect) {
      etapaMunicipioSelect.addEventListener("change", () => {
        fillEtapaNomePrefix();
      });
    }
      const editReloadSelects = [
        editChaveSelect,
        editSubacaoSelect,
        editResponsavelSelect,
        editPrazoSelect,
        editUnidGestoraSelect,
        editUnidadeSetorialSelect,
        editProdutoSelect,
        editRegiaoSelect,
      ].filter(Boolean);
        editReloadSelects.forEach((sel) => {
          sel.addEventListener("change", () => {
            refreshEditOptionsFromCatalog();
            loadEditOptions();
          });
        });
          if (editSubacaoSelect) {
            editSubacaoSelect.addEventListener("change", () => {
              if (editResponsavelSelect) editResponsavelSelect.value = "";
              if (editPrazoSelect) editPrazoSelect.value = "";
              if (editUnidGestoraSelect) editUnidGestoraSelect.value = "";
              if (editUnidadeSetorialSelect) editUnidadeSetorialSelect.value = "";
              if (editProdutoSelect) editProdutoSelect.value = "";
              if (editRegiaoSelect) editRegiaoSelect.value = "";
              editMunicipioLocked = false;
              editMunicipioItems.length = 0;
              renderEditMunicipioList();
              pendingEditPref = null;
              refreshEditOptionsFromCatalog();
              loadEditOptions();
            });
          }
    if (editRegiaoSelect) {
      editRegiaoSelect.addEventListener("change", () => {
        loadEditMunicipios();
      });
    }
    if (editCodigoNovoSelect) {
      editCodigoNovoSelect.addEventListener("change", () => {
        const selectedCodigo = editCodigoNovoSelect.value || "";
        const match = Array.from(editMunicipioNovoSelect?.options || []).find((opt) => {
          const optCodigo = normalizeDigits(opt.textContent || "");
          return optCodigo === normalizeDigits(selectedCodigo);
        });
        if (match && editMunicipioNovoSelect) editMunicipioNovoSelect.value = match.value;
      });
    }
    if (editMunicipioNovoSelect) {
      editMunicipioNovoSelect.addEventListener("change", () => {
        const selectedNome = editMunicipioNovoSelect.value || "";
        const match = Array.from(editCodigoNovoSelect?.options || []).find((opt) => {
          const label = opt.textContent || "";
          return label.endsWith(selectedNome) || label.includes(`- ${selectedNome}`);
        });
        if (match && editCodigoNovoSelect) editCodigoNovoSelect.value = match.value;
      });
    }
    if (editMunicipioAddBtn) {
      editMunicipioAddBtn.addEventListener("click", () => {
        if (approvalMode) return;
        addEditMunicipioItem();
      });
    }
    if (editMunicipioListEl) {
      editMunicipioListEl.addEventListener("click", (ev) => {
        if (approvalMode) return;
        const btn = ev.target.closest("[data-edit-remove-index]");
        if (!btn) return;
        if (editMunicipioLocked) {
          setMsg(
            "Antes de remover um município da Subação, por favor, exclua as etapas vinculadas.",
            true
          );
          return;
        }
        const idx = Number(btn.getAttribute("data-edit-remove-index"));
        if (Number.isNaN(idx)) return;
        editMunicipioItems.splice(idx, 1);
        renderEditMunicipioList();
      });
    }

    if (filterForm) {
      renderCriteria();
      if (filterAdd) {
        filterAdd.addEventListener("click", () => {
          const field = String(filterField?.value || "");
          const op = String(filterOp?.value || "eq");
          const value = String(filterValue?.value || "").trim();
          if (!field) {
            setFilterMsg("Selecione um campo.", true);
            return;
          }
          if (!value) {
            setFilterMsg("Informe um valor.", true);
            return;
          }
          if (field !== "exercicio" && !criteria.some((c) => c.field === "exercicio")) {
            setFilterMsg("Informe um critério de Exercício antes dos demais.", true);
            return;
          }
          criteria.push({ field, op, value });
          criteriaSelected = criteria.length - 1;
          renderCriteria();
          setFilterMsg("");
          if (filterValue) filterValue.value = "";
        });
      }
      if (filterRemove) {
        filterRemove.addEventListener("click", () => {
          if (criteriaSelected < 0 || criteriaSelected >= criteria.length) {
            setFilterMsg("Selecione um criterio para remover.", true);
            return;
          }
          criteria.splice(criteriaSelected, 1);
          criteriaSelected = -1;
          renderCriteria();
          setResultsVisible(false);
          setFilterMsg("");
        });
      }
      if (filterClear) {
        filterClear.addEventListener("click", () => {
          criteria.length = 0;
          criteriaSelected = -1;
          renderCriteria();
          setResultsVisible(false);
          setFilterMsg("");
        });
      }
      if (filterCancel) {
        filterCancel.addEventListener("click", () => {
          criteria.length = 0;
          criteriaSelected = -1;
          renderCriteria();
          setResultsVisible(false);
          if (filterField) filterField.value = "";
          if (filterOp) filterOp.value = "eq";
          if (filterValue) filterValue.value = "";
          setFilterMsg("");
        });
      }
      if (filterApply) {
        filterApply.addEventListener("click", () => {
          if (!criteria.some((c) => c.field === "exercicio")) {
            setFilterMsg("Informe o critério de Exercício antes de consultar.", true);
            return;
          }
          setResultsVisible(true);
          applyCriteriaToResults(true);
          setFilterMsg("");
        });
      }
    }

    if (approveBtn) {
      approveBtn.addEventListener("click", async () => {
        if (!canApprove) {
          setFilterMsg("Aprovação disponível apenas para nível 1 ou 2.", true);
          return;
        }
        if (subacaoSummary && subacaoSummary.style.display === "none") {
          setFilterMsg("Consulte antes de aprovar.", true);
          return;
        }
        const row = getSelectedRow();
        if (!row) {
          setFilterMsg("Selecione um registro para aprovar.", true);
          flashSummaryWarning();
          return;
        }
        const status = String(row.dataset.statusAprovacao || "").trim().toLowerCase();
        if (status && status !== "aguardando") {
          setFilterMsg("Somente registros com status Aguardando podem ser aprovados.", true);
          return;
        }
        if (subacaoIsConsultaView) {
          sessionStorage.setItem(
            subacaoPendingActionKey,
            JSON.stringify({
              action: "approve",
              id: row.dataset.id || "",
              dataset: subacaoRowSnapshot(row),
            })
          );
          await loadPage("cadastrar/plan_21-nger/subacao/formulario");
          return;
        }
        await loadSubacaoActionFromRow(row, "approve");
      });
    }

    if (editBtn) {
      editBtn.addEventListener("click", async () => {
        setApprovalMode(false);
        if (subacaoSummary && subacaoSummary.style.display === "none") {
          setFilterMsg("Consulte antes de editar.", true);
          return;
        }
        const row = getSelectedRow();
        if (!row) {
          setFilterMsg("Selecione um registro para editar.", true);
          flashSummaryWarning();
          return;
        }
        const status = String(row.dataset.statusAprovacao || "").trim().toLowerCase();
        if (status && status !== "aguardando") {
          setFilterMsg("Somente registros com status Aguardando podem ser editados.", true);
          return;
        }
        const criadorPerfilId = String(row.dataset.criadorPerfilId || "").trim();
        if (!criadorPerfilId || !currentUserPerfilId || currentUserPerfilId !== criadorPerfilId) {
          setFilterMsg("Usuário sem permissão para editar o registro de controle de subação.", true);
          return;
        }
        if (subacaoIsConsultaView) {
          sessionStorage.setItem(
            subacaoPendingActionKey,
            JSON.stringify({
              action: "edit",
              id: row.dataset.id || "",
              dataset: subacaoRowSnapshot(row),
            })
          );
          await loadPage("cadastrar/plan_21-nger/subacao/formulario");
          return;
        }
        await loadSubacaoActionFromRow(row, "edit");
      });
    }

    if (deleteBtn) {
      deleteBtn.addEventListener("click", async () => {
        setApprovalMode(false);
        if (subacaoSummary && subacaoSummary.style.display === "none") {
          setFilterMsg("Consulte antes de excluir.", true);
          return;
        }
        const row = getSelectedRow();
        if (!row) {
          setFilterMsg("Selecione um registro para excluir.", true);
          flashSummaryWarning();
          return;
        }
        const status = String(row.dataset.statusAprovacao || "").trim().toLowerCase();
        if (status && status !== "aguardando") {
          setFilterMsg("Somente registros com status Aguardando podem ser excluídos.", true);
          return;
        }
        const criadorPerfilId = String(row.dataset.criadorPerfilId || "").trim();
        if (!criadorPerfilId || !currentUserPerfilId || currentUserPerfilId !== criadorPerfilId) {
          setFilterMsg("Usuário sem permissão para excluir o registro de controle de subação.", true);
          return;
        }
        const id = row.dataset.id || "";
        if (!id) {
          setFilterMsg("Registro inválido para exclusão.", true);
          return;
        }
        try {
          const res = await fetch(`/api/subacao/${encodeURIComponent(id)}`, {
            method: "DELETE",
            headers: { "X-Requested-With": "fetch" },
          });
          const data = await res.json();
          if (!res.ok) {
            setFilterMsg(data.error || "Falha ao excluir.", true);
            return;
          }
          row.remove();
          renderSummaryPage();
          setFilterMsg(data.message || "Registro excluído.", false);
        } catch (err) {
          console.error(err);
          setFilterMsg("Falha ao excluir.", true);
        }
      });
    }

    if (printBtn) {
      printBtn.addEventListener("click", () => {
        setApprovalMode(false);
        if (subacaoSummary && subacaoSummary.style.display === "none") {
          setFilterMsg("Consulte antes de imprimir.", true);
          return;
        }
          const row = getSelectedRow();
          if (!row) {
            setFilterMsg("Selecione um registro para imprimir.", true);
            flashSummaryWarning();
            return;
          }
          openSubacaoPrintPopup(row);
        });
      }

    if (modeSelect) {
      modeSelect.addEventListener("change", () => {
        setApprovalMode(false);
        const modeValue = getModeValue();
        if (modeValue === "cadastrar") {
          if (!isPrefill && idInput) idInput.value = "";
          if (subacaoSummary) subacaoSummary.classList.remove("dotacao-summary-warn");
          if (municipioAddBtn) municipioAddBtn.disabled = false;
          if (municipioListWrap) municipioListWrap.style.display = "";
          updateEditingBanner();
        }
        if (modeValue === "editar") {
          municipioItems.length = 0;
          etapaItems.length = 0;
          renderMunicipioList();
          if (municipioAddBtn) municipioAddBtn.disabled = true;
          if (municipioListWrap) municipioListWrap.style.display = "none";
          if (addEtapaBtn) addEtapaBtn.style.display = "none";
          resetEditQuestions();
          updateQuestionVisibility();
          syncEditMode();
        }
      });
    }

    const updateEtapaStepLabel = () => {
      if (!stepButtons.length) return;
      stepButtons.forEach((btn) => {
        const stepNum = Number(btn.dataset.step);
        if (stepNum === 1) btn.textContent = "1. Chave de Planejamento";
        if (stepNum === 2) btn.textContent = "2. Subação/Entrega";
        if (stepNum === 3) btn.textContent = "3. Etapa";
      });
    };

    if (addEtapaBtn) {
      addEtapaBtn.addEventListener("click", () => {
        if (approvalMode) return;
        if (!validateEtapaBlock()) return;
        const payload = captureEtapaPayload();
        etapaItems.push(payload);
        renderEtapaList();
        syncEtapaMunicipioOptions();
        clearEtapaFields();
        fillEtapaNomePrefix();
        const remaining = (etapaMunicipioSelect?.options?.length || 0) - 1;
        if (remaining <= 1 && addEtapaBtn) {
          addEtapaBtn.style.display = "none";
        }
        updateEtapaStepLabel();
      });
    }

    if (prevBtn) {
      prevBtn.addEventListener("click", () => {
        setStep(currentStep - 1);
      });
    }

    if (nextBtn) {
      nextBtn.addEventListener("click", () => {
        if (!validateStep(currentStep)) return;
        setStep(currentStep + 1);
      });
    }

    if (etapaListEl) {
      etapaListEl.addEventListener("click", (ev) => {
        if (approvalMode) return;
        const btn = ev.target.closest("[data-remove-etapa]");
        if (!btn) return;
        const idx = Number(btn.getAttribute("data-remove-etapa"));
        if (Number.isNaN(idx) || idx < 0 || idx >= etapaItems.length) return;
        const [removed] = etapaItems.splice(idx, 1);
        renderEtapaList();
        syncEtapaMunicipioOptions();
        updateEtapaStepLabel();
        clearEtapaFields();
        if (removed?.municipio && etapaMunicipioSelect) {
          etapaMunicipioSelect.value = removed.municipio;
          fillEtapaNomePrefix();
        } else {
          fillEtapaNomePrefix();
        }
      });
    }
    if (clearBtn) {
      clearBtn.addEventListener("click", clearForm);
    }
    if (municipioAddBtn) {
      municipioAddBtn.addEventListener("click", () => {
        if (approvalMode) return;
        addMunicipioItem();
      });
    }

    if (stepButtons.length) {
      stepButtons.forEach((btn) => {
        btn.addEventListener("click", () => {
          const target = Number(btn.dataset.step || "1");
          if (target > currentStep && !validateStep(currentStep)) return;
          setStep(target);
        });
      });
    }

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      if (approvalMode) {
        const registroId = String(idInput?.value || "").trim();
        if (!registroId) {
          setMsg("Registro inválido para aprovação.", true);
          return;
        }
        const aprovado =
          approvalRadios.find((r) => r.checked)?.value || approvalRadios[0]?.value || "sim";
        const justificativa = String(approvalJustificativa?.value || "").trim();
        if (!justificativa) {
          setMsg("Justificativa obrigatória.", true);
          return;
        }
        const printWin = prepareAutoPrintWindow();
        if (!printWin) {
          setMsg("Popup bloqueado. Libere o navegador para imprimir.", true);
          return;
        }
        setMsg("Aprovando...");
        try {
          const res = await fetch(`/api/subacao/${encodeURIComponent(registroId)}/aprovar`, {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify({
              subacao_aprovada: aprovado,
              motivo_rejeicao: justificativa,
            }),
          });
          const data = await res.json();
          if (!res.ok) {
            setMsg(data.error || "Falha ao aprovar.", true);
            return;
          }
          setMsg(data.message || "Subação atualizada.");
          if (data.subacao?.id) {
            window.__subacaoAutoPrintId = String(data.subacao.id);
          } else if (registroId) {
            window.__subacaoAutoPrintId = String(registroId);
          }
          setApprovalMode(false);
          await loadPage("cadastrar/plan_21-nger/subacao/consultar");
        } catch (err) {
          console.error(err);
          try {
            if (printWin && !printWin.closed) printWin.close();
          } catch (_) {}
          window.__subacaoAutoPrintWin = null;
          setMsg("Falha ao aprovar.", true);
        }
        return;
      }
      const modeRaw = getRawModeValue() || "cadastrar";
      const mode = normalizeSolicitacao(modeRaw);
      const isExcluir = mode === "excluir";
      if (!isExcluir) {
        if (totalSteps > 1) {
          if (currentStep < totalSteps) {
            if (!validateStep(currentStep)) return;
            setStep(currentStep + 1);
            return;
          }
          if (!validateStep(1)) {
            setStep(1);
            return;
          }
          if (!validateStep(2)) {
            setStep(2);
            return;
          }
          if (!validateStep(totalSteps)) return;
        } else if (!form.checkValidity()) {
          form.reportValidity();
          return;
        }
      } else {
        if (!validateStep(1)) {
          setStep(1);
          return;
        }
        if (!validateStep(2)) {
          setStep(2);
          return;
        }
      }
      const etapaMunicipios = new Set(
        etapaItems
          .map((item) => String(item?.municipio || "").trim())
          .filter((value) => value)
      );
      const municipiosPendentes = municipioItems.filter(
        (m) => !etapaMunicipios.has(String(m?.municipios_entrega || "").trim())
      );
        if (mode === "cadastrar") {
          if (cpfInput && cpfInput.value && !isValidCpf(cpfInput.value)) {
            setMsg("CPF do responsável inválido.", true);
            cpfInput.focus();
            return;
        }
        if (cpfEtapaInput && cpfEtapaInput.value && !isValidCpf(cpfEtapaInput.value)) {
          setMsg("CPF do responsável da etapa inválido.", true);
          cpfEtapaInput.focus();
          return;
        }
      }
      setMsg("");
      const isEditMode = mode === "editar" || mode === "excluir";
      if (isEditMode) {
        const editModeKey = syncEditMode();
        if (!editModeKey) {
          setMsg("Selecione uma opção de edição.", true);
          return;
        }
        if (!validateStep(1)) {
          setStep(1);
          return;
        }
        if (!validateStep(2)) {
          setStep(2);
          return;
        }
        const payload = {
          edit_mode: editModeKey,
          registro_id: idInput?.value || "",
          tipo_solicitacao: modeRaw,
          tipo_edicao: editModeKey,
          plan21_nger_id: selectedPlan21Ids.id,
          plan21_nger_ids: selectedPlan21Ids.ids,
          exercicio: planSelects.exercicio?.value || "",
          uo: planSelects.uo?.value || "",
          programa: planSelects.programa?.value || "",
          acao_paoe: planSelects.acao_paoe?.value || "",
          responsavel_acao: planSelects.responsavel_acao?.value || "",
          produto_acao: planSelects.produto_acao?.value || "",
          chave_planejamento: editChaveSelect?.value || "",
          subacao_entrega: editSubacaoSelect?.value || "",
          subacao_entrega_edit: editSubacaoInput?.value || "",
          responsavel: editResponsavelSelect?.value || "",
            responsavel_edit:
              editResponsavelInput?.value || editResponsavelSelect?.value || "",
          cpf_responsavel: editCpfInput?.value || "",
          prazo: editPrazoSelect?.value || "",
          unid_gestora: editUnidGestoraSelect?.value || "",
          unidade_setorial_planejamento: editUnidadeSetorialSelect?.value || "",
          produto_subacao: editProdutoSelect?.value || "",
          produto_subacao_edit: editProdutoInput?.value || "",
          regiao_subacao: editRegiaoSelect?.value || "",
          justificativa: editJustificativaInput?.value || "",
          responsavel_nger: editResponsavelNgerInput?.value || "",
        };
          if (editModeKey === "novo_municipio" || editModeKey === "remover_municipio") {
            if (editModeKey === "remover_municipio" && editMunicipioLocked) {
              setMsg(
                "Antes de remover um município da Subação, por favor, exclua as etapas vinculadas.",
                true
              );
              return;
            }
            const hasPending = Boolean(
              editCodigoNovoSelect?.value || editMunicipioNovoSelect?.value || editMetaNovoInput?.value
            );
            if (hasPending) addEditMunicipioItem();
            if (!editMunicipioItems.length) {
              setMsg("Informe código, município e meta para adicionar.", true);
              return;
            }
            payload.municipios_items = editMunicipioItems.map((item) => ({
              codigo: item.codigo,
              municipios_entrega: item.municipios_entrega,
              meta_subacao: item.meta_subacao,
            }));
          }
        const printWin = prepareAutoPrintWindow();
        if (!printWin) {
          setMsg("Popup bloqueado. Libere o navegador para imprimir.", true);
          return;
        }
        try {
          const res = await fetch("/api/subacao/editar", {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
            body: JSON.stringify(payload),
          });
          const data = await res.json();
            if (!res.ok) {
              try {
                if (printWin && !printWin.closed) printWin.close();
              } catch (_) {}
              window.__subacaoAutoPrintWin = null;
              setMsg(data.error || "Falha ao salvar.", true);
              return;
            }
            const count = data.count || (Array.isArray(data.subacoes) ? data.subacoes.length : 1);
            showToast(`Subação salva com sucesso. Registros: ${count}.`, "success");
            if (data.warning) {
              setMsg(data.warning);
            }
            if (Array.isArray(data.subacoes) && data.subacoes.length) {
              const last = data.subacoes[data.subacoes.length - 1];
              if (last && last.id) {
                window.__subacaoAutoPrintId = String(last.id);
              }
            }
            await loadPage("cadastrar/plan_21-nger/subacao/consultar");
          } catch (err) {
            console.error(err);
            try {
              if (printWin && !printWin.closed) printWin.close();
            } catch (_) {}
            window.__subacaoAutoPrintWin = null;
            setMsg("Falha ao salvar.", true);
        }
        return;
      }
      const chavePlanejamento = chaveInput?.value || "";
      const prazoInicio = dataInicioInput?.value || "";
      const prazoFim = dataFimInput?.value || "";
      if (mode === "cadastrar") {
        const hasPending = Boolean(codigoSelect?.value || municipioSelect?.value || metaInput?.value);
        if (hasPending) addMunicipioItem({ silent: true });
      }
      const payload = {
        tipo_solicitacao: modeRaw,
        exercicio: planSelects.exercicio?.value || "",
        unidade_orcamentaria: planSelects.uo?.value || "",
        programa: planSelects.programa?.value || "",
        acao_paoe: planSelects.acao_paoe?.value || "",
        responsavel_acao: planSelects.responsavel_acao?.value || "",
        produto_acao: planSelects.produto_acao?.value || "",
        chave_planejamento: chavePlanejamento,
        subacao_entrega: subacaoEntregaInput?.value || "",
        responsavel: responsavelInput?.value || "",
        cpf_responsavel: cpfInput?.value || "",
        prazo_inicio: prazoInicio,
        prazo_fim: prazoFim,
        unid_gestora: unidGestoraSelect?.value || "",
        unidade_setorial_planejamento: unidadeSetorialSelect?.value || "",
        produto_subacao: produtoSubacaoSelect?.value || "",
        unidade_medida: unidadeMedidaSelect?.value || "",
        regiao_subacao: regiaoEntregaSelect?.value || "",
        codigo: codigoSelect?.value || "",
        municipios_entrega: municipioSelect?.value || "",
        meta_subacao: metaInput?.value || "",
        detalhamento_produto: detalhamentoInput?.value || "",
        etapa: etapaInput?.value || "",
        responsavel_etapa: responsavelEtapaInput?.value || "",
        cpf_responsavel_etapa: cpfEtapaInput?.value || "",
        justificativa: justificativaInput?.value || "",
        responsavel_nger: responsavelNgerInput?.value || "",
      };
        if (municipioItems.length) {
          const etapaMunicipios = new Set(
            etapaItems
              .map((item) => String(item?.municipio || "").trim())
              .filter((value) => value)
          );
          const municipiosPendentes = municipioItems.filter(
            (m) => !etapaMunicipios.has(String(m?.municipios_entrega || "").trim())
          );
          const hasEtapaInput = Boolean(
            etapaMunicipioSelect?.value ||
              etapaInput?.value ||
              responsavelEtapaInput?.value ||
              cpfEtapaInput?.value
          );
          if (!municipiosPendentes.length && !hasEtapaInput) {
            payload.etapas_items = etapaItems;
          } else {
            if (!validateEtapaBlock()) return;
            const last = captureEtapaPayload();
            const all = [...etapaItems, last];
            if (!last.municipio) {
              setMsg("Selecione um município para a etapa antes de salvar.", true);
              return;
            }
            if (all.length < municipioItems.length) {
              const faltando = municipioItems.length - all.length;
              setMsg(`Faltam ${faltando} etapa(s) para concluir os municípios.`, true);
              return;
            }
            const missing = municipioItems.some(
              (m) => !all.find((e) => e.municipio === m.municipios_entrega)
            );
            if (missing) {
              setMsg("Existe município sem etapa vinculada.", true);
              return;
            }
            payload.etapas_items = all;
          }
        }
        if (mode === "cadastrar" && municipioItems.length) {
          payload.municipios_items = municipioItems.map((item) => ({
            codigo: item.codigo,
            municipios_entrega: item.municipios_entrega,
            meta_subacao: item.meta_subacao,
          }));
        }
        const registroId = idInput?.value || "";
        const url = registroId ? `/api/subacao/${encodeURIComponent(registroId)}` : "/api/subacao";
      const method = registroId ? "PUT" : "POST";
      const printWin = prepareAutoPrintWindow();
      if (!printWin) {
        setMsg("Popup bloqueado. Libere o navegador para imprimir.", true);
        return;
      }
      try {
        const res = await fetch(url, {
          method,
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) {
          try {
            if (printWin && !printWin.closed) printWin.close();
          } catch (_) {}
          window.__subacaoAutoPrintWin = null;
          setMsg(data.error || "Falha ao salvar.", true);
          return;
        }
          const count = Array.isArray(data.subacoes) ? data.subacoes.length : 1;
          showToast(`Subação salva com sucesso. Registros: ${count}.`, "success");
          if (Array.isArray(data.subacoes) && data.subacoes.length) {
            const last = data.subacoes[data.subacoes.length - 1];
            if (last && last.id) {
              window.__subacaoAutoPrintId = String(last.id);
            }
          } else if (data.subacao?.id) {
            window.__subacaoAutoPrintId = String(data.subacao.id);
          }
          await loadPage("cadastrar/plan_21-nger/subacao/consultar");
        } catch (err) {
          console.error(err);
          try {
            if (printWin && !printWin.closed) printWin.close();
          } catch (_) {}
          window.__subacaoAutoPrintWin = null;
          setMsg("Falha ao salvar.", true);
      }
    });

    bindRowSelection();
    setStep(1);
    syncRequiredByVisibility();
    renderMunicipioList();
    updateEtapaStepLabel();
    renderEtapaList();
    if (modeSelect && getModeValue() === "editar") {
      if (municipioAddBtn) municipioAddBtn.disabled = true;
      if (municipioListWrap) municipioListWrap.style.display = "none";
    }
    loadOptions();
    renderSummaryPage();
    const autoPrintId = window.__subacaoAutoPrintId;
    if (autoPrintId) {
      const row = summaryBody?.querySelector(
        `.dotacao-summary-row[data-id="${CSS.escape(String(autoPrintId))}"]`
      );
      const pendingPrintWin = window.__subacaoAutoPrintWin || null;
      if (row) {
        setSelectedRow(row);
        openSubacaoPrintPopup(row, pendingPrintWin);
      } else if (pendingPrintWin && !pendingPrintWin.closed) {
        pendingPrintWin.close();
      }
      window.__subacaoAutoPrintId = null;
      window.__subacaoAutoPrintWin = null;
    }
    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        pageSize = parseInt(pageSizeSelect.value || "20", 10) || 20;
        if (subacaoSummary && subacaoSummary.style.display !== "none") {
          renderSummaryPage();
        }
      });
    }
    setResultsVisible(false);
    syncEtapaCounter();
    fillEtapaNomePrefix();
    syncSubacaoEntregaCounter();
    toggleMode();
    if (getModeValue() === "editar") {
      loadEditOptions();
    }
    if (!subacaoIsConsultaView && subacaoPendingAction) {
        try {
          const pending = subacaoPendingAction;
          const row = ensureSubacaoPendingRow(pending);
          if (row) {
            loadSubacaoActionFromRow(row, pending.action === "approve" ? "approve" : "edit")
              .catch((err) => {
                console.error(err);
                setMsg("Falha ao carregar o registro selecionado.", true);
              })
              .finally(() => {
                setSubacaoPendingLoading(false);
              });
          } else {
            setMsg("Registro selecionado na consulta não foi encontrado.", true);
            setSubacaoPendingLoading(false);
          }
        } catch (err) {
          console.error(err);
          setSubacaoPendingLoading(false);
        }
    }
  }

  if (menu) {
    menu.addEventListener("click", (ev) => {
      const parentToggle = ev.target.closest(".menu-parent[data-submenu]");
      if (parentToggle) {
        const targetId = parentToggle.getAttribute("data-submenu");
        const group = parentToggle.closest(".menu-group");
        const activeRouteItem = menu.querySelector(".menu-item.active[data-route]");
        if (activeRouteItem?.getAttribute("data-route") === "dashboard") {
          activeRouteItem.classList.remove("active");
        } else if (group && activeRouteItem && !group.contains(activeRouteItem)) {
          clearMenuRouteActiveState();
        }
        if (sidebar?.classList.contains("collapsed")) {
          sidebar.classList.remove("collapsed");
          sidebar.classList.add("open");
          if (group && targetId && document.getElementById(targetId)) {
            group.classList.add("open");
          }
          updateToggleIcon();
          resizeTetoDashboardCharts();
          return;
        }
        const isOpen = group?.classList.contains("open");
        if (group) {
          if (isOpen) {
            group.classList.remove("open");
          } else if (targetId) {
            const submenu = document.getElementById(targetId);
            if (submenu) group.classList.add("open");
          }
        }
        return;
      }

      const link = ev.target.closest("[data-route]");
      if (!link) return;
      ev.preventDefault();
      const route = link.getAttribute("data-route");
      setActive(route);
      closeNarrowSidebar();
      loadPage(route).then(() => {
        if (route === "dashboard") {
          initDashboard({ forceShow: true });
        }
      });
    });
  }

  setUserMeta();
  syncTopbarHeight();
  window.setInterval(() => {
    setUserMeta();
    syncTopbarHeight();
  }, 30000);
  fetchCurrentPermissions();

  if (content) {
    const initial = content.dataset.initial || "dashboard";
    setActive(initial);
    loadPage(initial);
  }
})();
    const negateCols = new Set([
      "reducao",
      "bloqueado_conting",
      "reserva_empenho",
      "empenhado",
    ]);
    const adjustVal = (k, v) => (negateCols.has(k) ? Number(v || 0) * -1 : Number(v || 0));












