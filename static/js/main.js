(function () {
  const content = document.getElementById("content-area");
  const sidebar = document.getElementById("sidebar");
  const toggle = document.getElementById("sidebar-toggle");
  const logoutBtn = document.getElementById("logout-btn");
  const menu = document.getElementById("menu");
  const userMeta = document.getElementById("user-meta");
  const userPerfilId = userMeta ? userMeta.dataset.perfilId : "";
  const userNivel = userMeta ? userMeta.dataset.nivel : "";
  const themeLightBtn = document.getElementById("theme-light");
  const themeDarkBtn = document.getElementById("theme-dark");
  let multiFilterClickBound = false;

  function applyTheme(theme) {
    const body = document.body;
    const isDark = theme === "dark";
    body.classList.toggle("theme-dark", isDark);
    if (themeLightBtn && themeDarkBtn) {
      themeLightBtn.classList.toggle("active", !isDark);
      themeDarkBtn.classList.toggle("active", isDark);
    }
    localStorage.setItem("app-theme", isDark ? "dark" : "light");
  }

  function initTheme() {
    const saved = localStorage.getItem("app-theme") || "light";
    applyTheme(saved);
    if (themeLightBtn) {
      themeLightBtn.addEventListener("click", () => applyTheme("light"));
    }
    if (themeDarkBtn) {
      themeDarkBtn.addEventListener("click", () => applyTheme("dark"));
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

  async function loadPage(route) {
    let url = "/partial/" + route;
    if (route === "logout") {
      await logout();
      return;
    }
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
      initRoute(route);
    } catch (err) {
      content.innerHTML = '<div class="card"><div class="card-title">Erro</div><p>Falha ao carregar.</p></div>';
      console.error(err);
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
    document.querySelectorAll(".menu-group").forEach((group) => group.classList.remove("open"));
    const activeLink = document.querySelector(`.menu-item[data-route="${route}"]`);
    if (activeLink) {
      let parentGroup = activeLink.closest(".menu-group");
      while (parentGroup) {
        parentGroup.classList.add("open");
        parentGroup = parentGroup.parentElement?.closest(".menu-group");
      }
    }
  }

  async function logout() {
    try {
      await fetch("/logout", { method: "POST" });
    } finally {
      window.location.href = "/login";
    }
  }

  function updateToggleIcon() {
    if (!toggle || !sidebar) return;
    const icon = toggle.querySelector("i");
    if (!icon) return;
    const collapsed = sidebar.classList.contains("collapsed");
    icon.classList.toggle("bi-chevron-right", collapsed);
    icon.classList.toggle("bi-chevron-left", !collapsed);
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
  }

  if (toggle) {
    toggle.addEventListener("click", () => {
      sidebar.classList.toggle("collapsed");
      sidebar.classList.toggle("open");
      updateToggleIcon();
    });
    updateToggleIcon();
  }

  if (logoutBtn) {
    logoutBtn.addEventListener("click", () => logout());
  }

  initTheme();
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
        msg.textContent = "UsuÃ¡rio criado.";
        form.reset();
        await loadPage("usuarios");
      } catch (err) {
        console.error(err);
        msg.textContent = "Falha na requisiÃ§Ã£o.";
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
        msg.textContent = "Selecione um usuÃ¡rio na lista.";
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
          // se nÃ£o for JSON, usa texto bruto na mensagem de erro
        }
        if (!res.ok) throw new Error(data.error || raw || `Falha ao salvar. Status ${res.status}`);
        msg.textContent = data.message || "UsuÃ¡rio atualizado.";
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
            cells[3].textContent = payload.ativo ? "Sim" : "NÃ£o";
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

  function applyMenuPermissions(features = []) {
    if (!menu) return;
    const allowed = new Set(["dashboard", "logout", ...features]);

    // Children: show only allowed
    menu.querySelectorAll(".submenu [data-route]").forEach((link) => {
      const route = link.getAttribute("data-route");
      if (!route) return;
      link.style.display = allowed.has(route) ? "" : "none";
    });

    // Parents: show if any allowed child
    menu.querySelectorAll(".menu-group").forEach((group) => {
      const submenu = group.querySelector(".submenu");
      if (!submenu) return;
      const parentId = group.id?.replace("menu-", "") || "";
      const hasAllowedChild = Array.from(submenu.querySelectorAll("[data-route]")).some((item) =>
        allowed.has(item.getAttribute("data-route"))
      );
      const parentAllowed = parentId && allowed.has(parentId);
      group.style.display = hasAllowedChild || parentAllowed ? "" : "none";
    });

    // Top-level items without submenu
    menu.querySelectorAll(".menu > .menu-item[data-route]").forEach((item) => {
      const route = item.getAttribute("data-route");
      if (!route) return;
      if (route === "logout") return;
      item.style.display = allowed.has(route) ? "" : "none";
    });
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
      // admin: libera tudo visÃ­vel no menu
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
        target.textContent = "Nenhuma atualizaÃ§Ã£o encontrada.";
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
        <div><strong>SaÃ­da gerada:</strong> ${last.output_filename || "-"}</div>
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
        target.textContent = "Nenhuma atualizaÃ§Ã£o encontrada.";
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
        <div><strong>SaÃ­da gerada:</strong> ${last.output_filename || "-"}</div>
      `;
      if (submitBtn && last.output_filename) {
        submitBtn.dataset.mode = "view";
        submitBtn.dataset.output = last.output_filename;
        submitBtn.textContent = viewLabel || "Ver relatÃ³rio";
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
        target.textContent = "Nenhuma atualizaÃ§Ã£o encontrada.";
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
        <div><strong>SaÃ­da gerada:</strong> ${last.output_filename || "-"}</div>
      `;
      if (submitBtn && last.output_filename) {
        submitBtn.dataset.mode = "view";
        submitBtn.dataset.output = last.output_filename;
        submitBtn.textContent = viewLabel || "Ver relatÃ³rio";
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
        target.textContent = "Nenhuma atualizaÃ§Ã£o encontrada.";
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
        <div><strong>SaÃ­da gerada:</strong> ${last.output_filename || "-"}</div>
      `;
      if (submitBtn && last.output_filename) {
        submitBtn.dataset.mode = "view";
        submitBtn.dataset.output = last.output_filename;
        submitBtn.textContent = viewLabel || "Ver relatÃ³rio";
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
        target.textContent = "Nenhuma atualizaÃ§Ã£o encontrada.";
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
        <div><strong>SaÃ­da gerada:</strong> ${last.output_filename || "-"}</div>
      `;
      if (submitBtn && last.output_filename) {
        submitBtn.dataset.mode = "view";
        submitBtn.dataset.output = last.output_filename;
        submitBtn.textContent = viewLabel || "Ver relatÃ³rio";
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
  const viewLabel = "Ver RelatÃ³rio";

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
          msg.textContent = data.message || "Upload concluÃ­do.";
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
    const viewLabel = "Ver relatÃ³rio";
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
          msg.textContent = data.message || "Upload concluÃ­do.";
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
    const viewLabel = "Ver relatÃ³rio";
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
    const viewLabel = "Ver relatÃ³rio";
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
    const viewLabel = "Ver relatÃ³rio";
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
  const viewLabel = "Ver RelatÃ³rio";
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
          statusBox.textContent = "Nenhuma atualizaÃ§Ã£o encontrada.";
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
          <div><strong>SaÃ­da gerada:</strong> ${last.output_filename || "-"}</div>
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
          msg.textContent = data.message || "Upload concluÃ­do.";
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
    const currentUserPerfilId = String(dotacaoPage?.dataset?.userPerfilId || userPerfilId || "").trim();
    const currentUserId = dotacaoPage?.dataset?.userId || "";
    const currentUserNome = dotacaoPage?.dataset?.userNome || "";

    const hasAllSelects = Object.values(selects).every((el) => el);
    if (!hasAllSelects || !adjSelect) return;

    let updating = false;
    const baseSaldoKeys = new Set(["exercicio", "chave_planejamento"]);
    let approvalMode = false;

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
      exercicio: "Exerc\u00edcio",
      statusAprovacao: "Status da Dota\u00e7\u00e3o",
      chaveDotacao: "Controle de Dota\u00e7\u00e3o",
      adjunta: "Adjunta Solicitante",
      programa: "Programa",
      paoe: "A\u00e7\u00e3o/PAOE",
    };
    const opLabels = {
      eq: "Igual a",
      contains: "Cont\u00e9m",
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

    const clearPagination = () => {
      if (paginationEl) paginationEl.innerHTML = "";
    };

    const setResultsVisible = (show) => {
      if (!dotacaoSummary) return;
      dotacaoSummary.classList.toggle("dotacao-summary-hidden", !show);
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
        ["ExercÃ­cio", row.dataset.exercicio],
        ["Adjunta Solicitante", row.dataset.adjunta],
        ...(isEmp ? [["Adjunta Concedente", adjConc]] : []),
        ["Chave do Planejamento", row.dataset.chave],
        ["UO", row.dataset.uo],
        ["Programa", row.dataset.programaRaw],
        ["AÃ§Ã£o/PAOE", row.dataset.acaoPaoe],
        ["Produto", row.dataset.produto],
        ["UG", row.dataset.ug],
        ["RegiÃ£o", row.dataset.regiao],
        ["SubaÃ§Ã£o/Entrega", row.dataset.subacao],
        ["Etapa", row.dataset.etapa],
        ["Natureza de Despesa", row.dataset.natureza],
        ["Elemento de Despesa", row.dataset.elemento],
        ["Subelemento", row.dataset.subelemento],
        ["Fonte", row.dataset.fonte],
        ["Iduso", row.dataset.iduso],
        ["Justificativa/HistÃ³rico", row.dataset.justificativa],
        ["Valor da DotaÃ§Ã£o", formatPtBr(parsePtBr(row.dataset.valor) || 0)],
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
      if (status === "aguardando") watermarkText = "Aguardando Aprova\u00e7\u00e3o";
      if (status === "rejeitado") watermarkText = "Sem Validade";
      const showRegularizacao = isEmprestada && status !== "rejeitado";
      const html = `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>DotaÃ§Ã£o Cadastrada</title>
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
    .print-table th { width: auto; white-space: nowrap; background: #f1f1f1; text-transform: uppercase; }
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
        <div class="print-brand-title">Sistema de Planejamento e OrÃ§amento</div>
        <div class="print-brand-subtitle">SPO-NGER-SEDUCMT</div>
      </div>
    </div>
  </div>
  <div class="print-title-row">
    <div class="print-title-key">${escapeHtml(rows[0]?.dataset?.chaveDotacao || "")}</div>
    <div class="print-title">DOTAÃ‡ÃƒO CADASTRADA</div>
    <div class="print-title-date">${formatPrintDate((rows[0]?.dataset?.alteradoEm && rows[0]?.dataset?.alteradoEm !== rows[0]?.dataset?.criadoEm) ? rows[0]?.dataset?.alteradoEm : rows[0]?.dataset?.criadoEm)}</div>
  </div>
  <div style="height: 36px;"></div>
  ${content}
  <div class="print-footer">
    <img src="/static/img/logo.jpg" alt="Logo" />
    <div class="print-footer-text">
      <div>${footerText}</div>
      ${approvalText ? `<div class="print-footer-approval">${approvalText}</div>` : ""}
      ${showRegularizacao ? `<div class="print-footer-note">Dota\u00e7\u00e3o estar\u00e1 sujeita a regulariza\u00e7\u00e3o</div>` : ""}
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
      const keep = options.includes(current) ? current : "";
      select.innerHTML = '<option value="">Selecione...</option>';
      options.forEach((opt) => {
        const o = document.createElement("option");
        o.value = opt;
        o.textContent = opt;
        select.appendChild(o);
      });
      if (keep) select.value = keep;
    };

    const setAdjOptions = (options, current) => {
      const keep = options.some((o) => String(o.id) === current) ? current : "";
      adjSelect.innerHTML = '<option value="">Selecione...</option>';
      options.forEach((opt) => {
        const o = document.createElement("option");
        o.value = String(opt.id);
        o.textContent = opt.label || "";
        adjSelect.appendChild(o);
      });
      if (keep) adjSelect.value = keep;
    };

    const setPerfilOptions = (select, options, current) => {
      if (!select) return;
      const keep = options.includes(current) ? current : "";
      select.innerHTML = '<option value="">Selecione...</option>';
      options.forEach((opt) => {
        const o = document.createElement("option");
        o.value = opt;
        o.textContent = opt;
        select.appendChild(o);
      });
      if (keep) select.value = keep;
    };

    const getCurrentYear = () => {
      return new Intl.DateTimeFormat("pt-BR", { timeZone: "America/Manaus", year: "numeric" }).format(
        new Date()
      );
    };

    const loadOptions = async () => {
      const params = currentSaldoFilters();
      const url = new URL("/api/dotacao/options", window.location.origin);
      Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
      try {
        const res = await fetch(url, { headers: { "X-Requested-With": "fetch" } });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar opcoes.");
        updating = true;
        Object.entries(selects).forEach(([key, el]) => {
          let opts = (data.options && data.options[key]) || [];
          if (key === "exercicio") {
            opts = [getCurrentYear()];
          }
          setSelectOptions(el, opts, el.value);
        });
        if (Array.isArray(data.adj)) {
          setAdjOptions(data.adj, adjSelect.value);
        }
        if (Array.isArray(data.perfis) && adjConcedenteSelect) {
          setPerfilOptions(adjConcedenteSelect, data.perfis, adjConcedenteSelect.value);
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
            setFilterMsg("Informe um crit\u00e9rio de Exerc\u00edcio antes dos demais.", true);
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
            setFilterMsg("Informe o crit\u00e9rio de Exerc\u00edcio antes de consultar.", true);
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
        const adjConcedenteId = String(selected.dataset.adjConcedenteId || "").trim();
        if (!adjConcedenteId) {
          setFilterMsg("Adjunta Concedente nÃ£o definida.", true);
          return;
        }
        if (!currentUserPerfilId || currentUserPerfilId !== String(selected.dataset.adjConcedenteId || "").trim()) {
          setFilterMsg("UsuÃ¡rio sem permissÃ£o para editar a dotaÃ§Ã£o atual.", true);
          return;
        }
        const status = String(selected.dataset.statusAprovacao || "").trim().toLowerCase();
        if (status && status !== "aguardando") {
          setFilterMsg("Somente dotaÃ§Ãµes com status Aguardando podem ser editadas.", true);
          return;
        }
        setApprovalMode(false);
        await fillFormFromRow(selected);
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
        const adjConcedenteId = String(selected.dataset.adjConcedenteId || "").trim();
        if (!adjConcedenteId) {
          setFilterMsg("Adjunta Concedente nÃ£o definida.", true);
          return;
        }
        if (!currentUserPerfilId || currentUserPerfilId !== String(selected.dataset.adjConcedenteId || "").trim()) {
          setFilterMsg("UsuÃ¡rio sem permissÃ£o para excluir a dotaÃ§Ã£o atual.", true);
          return;
        }
        const status = String(selected.dataset.statusAprovacao || "").trim().toLowerCase();
        if (status && status !== "aguardando") {
          setFilterMsg("Somente dotaÃ§Ãµes com status Aguardando podem ser excluÃ­das.", true);
          return;
        }
        const dotacaoId = selected.dataset.id;
        if (!dotacaoId) {
          setFilterMsg("Registro inv\u00e1lido para exclus\u00e3o.", true);
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
          setFilterMsg(data.message || "DotaÃ§Ã£o excluÃ­da.", false);
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
          msg.textContent = "Registro invÃ¡lido para aprovaÃ§Ã£o.";
          msg.classList.add("text-error");
          return;
        }
        const aprovado = Array.from(approvalRadios).find((r) => r.checked)?.value || "sim";
        const justificativa = String(approvalJustificativa?.value || "").trim();
        if (!justificativa) {
          msg.textContent = "Justificativa obrigatÃ³ria.";
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
          msg.textContent = data.message || "DotaÃ§Ã£o atualizada.";
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
          await loadPage("cadastrar/dotacao");
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
        await loadPage("cadastrar/dotacao");
      } catch (err) {
        console.error(err);
        msg.textContent = err.message || "Falha ao salvar.";
        msg.classList.add("text-error");
      }
    });

    if (clearBtn) {
      clearBtn.addEventListener("click", () => {
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
          setFilterMsg("Somente dotaÃ§Ãµes com status Aguardando podem ser aprovadas.", true);
          return;
        }
        const adjConcedenteId = String(selected.dataset.adjConcedenteId || "").trim();
        if (!adjConcedenteId) {
          setFilterMsg("Adjunta Concedente nÃ£o definida.", true);
          return;
        }
        if (!currentUserPerfilId || currentUserPerfilId !== String(selected.dataset.adjConcedenteId || "").trim()) {
          setFilterMsg("UsuÃ¡rio sem permissÃ£o para aprovar a dotaÃ§Ã£o atual.", true);
          return;
        }
        setApprovalMode(true);
        await fillFormFromRow(selected);
        if (approvalJustificativa) approvalJustificativa.value = "";
      });
    }

    loadOptions();
    loadSaldo();
    updateJustificativaPrefix();
    toggleAdjConcedente();
    setApprovalMode(false);
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

  function initRelatorioFip() {
    const table = document.getElementById("fip613-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
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
          paoeEl.textContent = "VÃ¡rios PAOEs";
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
            <div><strong>Ãšltima atualizaÃ§Ã£o</strong></div>
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
            <div><strong>Ãšltima atualizaÃ§Ã£o</strong></div>
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

  function initRelatorioEmp() {
    const table = document.getElementById("emp-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
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
            <div><strong>Ãšltima atualizaÃ§Ã£o</strong></div>
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
            <div><strong>Ãšltima atualizaÃ§Ã£o</strong></div>
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
            <div><strong>Ãšltima atualizaÃ§Ã£o</strong></div>
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
            <div><strong>Ãšltima atualizaÃ§Ã£o</strong></div>
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

  function initRoute(route) {
    if (route === "dashboard") {
      initDashboard();
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
    if (route === "cadastrar/dotacao") {
      initDotacao();
    }
    if (route === "cadastrar/est-dotacao") {
      initEstDotacao();
    }
    if (route === "cadastrar/plan_21-nger/subacao") {
      initSubacaoPlan21();
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
  }

  function initRelatorioDotacao() {
    const table = document.getElementById("dotacao-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
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
    const currentUserPerfilId = String(estPage?.dataset?.userPerfilId || userPerfilId || "").trim();
    const currentUserId = estPage?.dataset?.userId || "";
    const pageSizeSelect = document.getElementById("est-dotacao-page-size");
    const paginationEl = document.getElementById("est-dotacao-pagination");
    const summaryBox = document.getElementById("est-dotacao-summary");
    if (!summaryBody || !dotacaoTable || !estornoTable) return;

    const criteria = [];
    let criteriaSelected = -1;
    const fieldLabels = {
      exercicio: "Exerc\u00edcio",
      chaveDotacao: "Controle de Dota\u00e7\u00e3o",
      adjunta: "Adjunta Solicitante",
      programa: "Programa",
      paoe: "A\u00e7\u00e3o/PAOE",
    };
    const opLabels = {
      eq: "Igual a",
      contains: "Cont\u00e9m",
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

    const getActiveTable = () => (tipoSelect?.value === "estorno" ? estornoTable : dotacaoTable);
    const getRows = () => Array.from(getActiveTable().querySelectorAll(".dotacao-summary-row"));
    const getAllRows = () => Array.from(document.querySelectorAll("#est-dotacao-summary .dotacao-summary-row"));

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
          setFilterMsg("Informe um crit\u00e9rio de Exerc\u00edcio antes dos demais.", true);
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
          setFilterMsg("Selecione um crit\u00e9rio para remover.", true);
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
          setFilterMsg("Informe o crit\u00e9rio de Exerc\u00edcio antes de consultar.", true);
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
        const adjunta = String(selected.dataset.adjunta || "").trim();
        if (!currentUserPerfilId || currentUserPerfilId !== String(selected.dataset.perfilId || "").trim()) {
          setFilterMsg("Usu\u00e1rio sem permiss\u00e3o de cadastrar estorno.", true);
          return;
        }
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
        if (msg) msg.textContent = "";
        setFilterMsg("");
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

    const canEditOrDeleteEstorno = (row) => {
      if (!row) return false;
      const status = String(row.dataset.status || "").trim().toLowerCase();
      if (status && status !== "aguardando") {
        setFilterMsg("Somente estornos com status Aguardando podem ser alterados.", true);
        return false;
      }
      const adjunta = String(row.dataset.adjunta || "").trim();
      if (!currentUserPerfilId || currentUserPerfilId !== String(row.dataset.perfilId || "").trim()) {
        setFilterMsg("UsuÃ¡rio sem permissÃ£o para alterar o estorno atual.", true);
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
        fillFormFromEstorno(selected);
        setEditFieldsEnabled(true);
      });
    }

    if (deleteBtn) {
      deleteBtn.addEventListener("click", async () => {
        const selected = estornoTable?.querySelector(".dotacao-summary-row.selected");
        if (!selected) {
          setFilterMsg("Selecione um registro para excluir.", true);
          return;
        }
        if (!canEditOrDeleteEstorno(selected)) return;
        const estId = selected.dataset.id;
        if (!estId) {
          setFilterMsg("Registro invÃ¡lido para exclusÃ£o.", true);
          return;
        }
        try {
          const res = await fetch(`/api/est-dotacao/${encodeURIComponent(estId)}`, {
            method: "DELETE",
            headers: { "X-Requested-With": "fetch" },
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao excluir.");
          await loadPage("cadastrar/est-dotacao");
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
          setFilterMsg("UsuÃ¡rio sem permissÃ£o para aprovar o estorno atual.", true);
          return;
        }
        fillFormFromEstorno(selected);
        setApprovalMode(true);
      });
    }

    if (form) {
      form.addEventListener("submit", async (ev) => {
        ev.preventDefault();
        const selected = summaryBody?.querySelector(".dotacao-summary-row.selected");
        const editId = String(estIdInput?.value || "").trim();
        const useEdit = isEditMode && editId;
        if (isApprovalMode) {
          if (!editId) {
            if (msg) {
              msg.textContent = "Registro invÃ¡lido para aprovaÃ§Ã£o.";
              msg.classList.add("text-error");
            }
            return;
          }
          const aprovado = approvalRadios.find((r) => r.checked)?.value || "sim";
          const justificativa = String(approvalJustificativa?.value || "").trim();
          if (!justificativa) {
            if (msg) {
              msg.textContent = "Justificativa obrigatÃ³ria.";
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
            await loadPage("cadastrar/est-dotacao");
          } catch (err) {
            if (msg) {
              msg.textContent = err.message || "Falha ao aprovar.";
              msg.classList.add("text-error");
            }
          }
          return;
        }
        if (!useEdit && (!selected || selected.dataset.kind !== "dotacao")) {
          if (msg) {
            msg.textContent = "Carregue uma dotaÃ§Ã£o antes de salvar o estorno.";
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
            : summaryBody?.querySelector(".dotacao-summary-row.selected")?.dataset.chaveDotacao || "",
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
          await loadPage("cadastrar/est-dotacao");
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
        return `${dia} Ã s ${hora}`;
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

  function initSubacaoPlan21() {
    const form = document.getElementById("form-subacao");
    const msg = document.getElementById("subacao-msg");
    if (!form || !msg) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const stepButtons = Array.from(form.querySelectorAll(".wizard-step-btn"));
    const stepPanels = Array.from(form.querySelectorAll(".wizard-step"));
    const prevBtn = document.getElementById("subacao-prev");
    const nextBtn = document.getElementById("subacao-next");
    const saveBtn = document.getElementById("subacao-save");
    const totalSteps = stepPanels.length || 1;
    let currentStep = 1;

    const modeSelect = document.getElementById("subacao-formulario");
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
    const regiaoMemoriaInput = document.getElementById("subacao-regiao-memoria");
    const naturezaInput = document.getElementById("subacao-natureza");
    const fonteSuperavitInputs = Array.from(
      form.querySelectorAll("input[name='fonte_superavit']")
    );
    const fonteSelect = document.getElementById("subacao-fonte");
    const idusoSelect = document.getElementById("subacao-iduso");
    const descricaoInput = document.getElementById("subacao-descricao");
    const unidadeEtapaSelect = document.getElementById("subacao-unidade-etapa");
    const quantidadeInput = document.getElementById("subacao-quantidade");
    const valorUnitarioInput = document.getElementById("subacao-valor-unitario");
    const valorTotalInput = document.getElementById("subacao-valor-total");
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
    const editUnidadeMedidaSelect = document.getElementById("subacao-edit-unidade-medida");
    const editRegiaoSelect = document.getElementById("subacao-edit-regiao");
    const editCodigoSelect = document.getElementById("subacao-edit-codigo");
    const editMunicipioSelect = document.getElementById("subacao-edit-municipio");
    const editMetaSelect = document.getElementById("subacao-edit-meta");
    const editDetalhamentoSelect = document.getElementById("subacao-edit-detalhamento-select");
    const editDetalhamentoInput = document.getElementById("subacao-edit-detalhamento-input");
    const editMunicipioToggle = document.getElementById("subacao-edit-municipio-toggle");
    const editMunicipioWrap = document.getElementById("subacao-edit-municipio-wrap");
    const editMunicipioAddBtn = document.getElementById("subacao-edit-municipio-add");
    const editMunicipioListWrap = document.getElementById("subacao-edit-municipio-list-wrap");
    const editMunicipioListEl = document.getElementById("subacao-edit-municipio-list");
    const editCodigoNovoSelect = document.getElementById("subacao-edit-codigo-novo");
    const editMunicipioNovoSelect = document.getElementById("subacao-edit-municipio-novo");
    const editMetaNovoInput = document.getElementById("subacao-edit-meta-novo");

    const summaryBody = document.querySelector("#subacao-summary-table tbody");
    const pageSizeSelect = document.getElementById("subacao-page-size");
    const paginationEl = document.getElementById("subacao-pagination");
    const editBtn = document.getElementById("subacao-edit");
    const deleteBtn = document.getElementById("subacao-delete");
    const printBtn = document.getElementById("subacao-print");
    const subacaoSummary = document.getElementById("subacao-summary");

    const hasAllPlanSelects = Object.values(planSelects).every((el) => el);
    const hasAllKeySelects = Object.values(chaveSelects).every((el) => el);
    if (!hasAllPlanSelects || !hasAllKeySelects) return;

    let lastRegiaoValue = planSelects.regiao?.value || "";

    const setMsg = (text, isError = false) => {
      msg.textContent = text || "";
      msg.classList.toggle("text-error", isError);
      if (text && msg.scrollIntoView) {
        msg.scrollIntoView({ behavior: "smooth", block: "center" });
      }
    };

    const subacaoEntregaCounter = document.getElementById("subacao-entrega-counter");

    const getStepPanel = (step) => stepPanels.find((panel) => Number(panel.dataset.step) === step);

    const toggleMode = () => {
      const isEdit = (modeSelect?.value || "").toLowerCase() === "editar";
      if (chaveTitle) chaveTitle.style.display = isEdit ? "none" : "";
      if (chaveSection) chaveSection.style.display = isEdit ? "none" : "";
      if (cadastrarGrid) cadastrarGrid.style.display = isEdit ? "none" : "";
      if (editarGrid) editarGrid.style.display = isEdit ? "" : "none";
      if (municipioListWrap) municipioListWrap.style.display = isEdit ? "none" : "";
      if (municipioAddBtn) municipioAddBtn.style.display = isEdit ? "none" : "";
      if (editMunicipioToggle) editMunicipioToggle.style.display = isEdit ? "" : "none";
      if (editMunicipioWrap) editMunicipioWrap.style.display = "none";

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
      toggleFields(chaveSection, !isEdit);
      toggleFields(cadastrarGrid, !isEdit);
      toggleFields(editarGrid, isEdit);
    };

    if (modeSelect) {
      modeSelect.addEventListener("change", () => {
        toggleMode();
        if ((modeSelect.value || "").toLowerCase() === "editar") {
          loadEditOptions();
        }
      });
    }

    const validateStep = (step) => {
      const panel = getStepPanel(step);
      if (!panel) return true;
      const fields = panel.querySelectorAll("input, select, textarea");
      for (const field of fields) {
        if (field.disabled) continue;
        if (field.type === "hidden") continue;
        if (municipioItems.length) {
          if (field === codigoSelect || field === municipioSelect || field === metaInput) {
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
      const isEditMode = (modeSelect?.value || "").toLowerCase() === "editar";
      if (step === 2 && !isEditMode && cpfInput && cpfInput.value && !isValidCpf(cpfInput.value)) {
        setMsg("CPF do responsável inválido.", true);
        cpfInput.focus();
        return false;
      }
      if (step === 2 && isEditMode && editCpfInput && editCpfInput.value && !isValidCpf(editCpfInput.value)) {
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
            setMsg("Data final deve ser maior ou igual Ã  data inÃ­cio.", true);
            dataFimInput.focus();
            return false;
          }
        }
      }
      return true;
    };

    const setStep = (step) => {
      if (!stepPanels.length) return;
      const clamped = Math.min(Math.max(step, 1), totalSteps);
      currentStep = clamped;
      stepPanels.forEach((panel) => {
        panel.classList.toggle("active", Number(panel.dataset.step) === clamped);
      });
      stepButtons.forEach((btn) => {
        btn.classList.toggle("active", Number(btn.dataset.step) === clamped);
      });
      if (prevBtn) prevBtn.disabled = clamped === 1;
      if (nextBtn) nextBtn.style.display = clamped === totalSteps ? "none" : "inline-flex";
      if (saveBtn) saveBtn.style.display = clamped === totalSteps ? "inline-flex" : "none";
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
          setMsg("Remova todos os munic\u00edpios antes de trocar a regi\u00e3o.", true);
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
      }
      if (chaveSelects.regiao && regiaoEntregaSelect) {
        syncSelectByCodigo(regiaoEntregaSelect, chaveSelects.regiao.value || "");
      }
      if (chaveSelects.regiao && regiaoMemoriaInput) {
        regiaoMemoriaInput.value = getSelectLabel(chaveSelects.regiao);
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

    const fontesNao = [
      "15000000 - Recursos nÃ£o vinculados de Impostos",
      "15001001 - Recursos destinados Ã  ManutenÃ§Ã£o e Desenvolvimento do Ensino",
      "15010000 - Outros Recursos nÃ£o Vinculados",
      "15010100 - Outros Recursos nÃ£o vinculados destinados ao Tesouro",
      "15021001 - Recursos nÃ£o vinculados da compensaÃ§Ã£o de impostos - Destinado ao Ensino",
      "15400000 - TransferÃªncia de recursos do FUNDEB desenvolvimento do Ensino",
      "15401070 - TransferÃªncia de recursos do FUNDEB RemuneraÃ§Ã£o EducaÃ§Ã£o BÃ¡sica",
      "15401071 - TransferÃªncia de recursos do FUNDEB - Ensino em Tempo Integral (ETI)",
      "15401072 - TransferÃªncia de recursos do FUNDEB - RemuneraÃ§Ã£o Profissionais ETI",
      "15460000 - TransferÃªncias do FUNDEB - ComplementaÃ§Ã£o da UniÃ£o - ETI",
      "15500000 - Recursos da ContribuiÃ§Ã£o ao SalÃ¡rio EducaÃ§Ã£o",
      "15510000 - TransferÃªncias de Recursos do FNDE referente ao Programa Dinheiro Direto na Escola (PDDE)",
      "15520000 - TransferÃªncias de Recursos do FNDE referente ao Programa Nacional de AlimentaÃ§Ã£o Escolar (PNAE)",
      "15530000 - TransferÃªncias de Recursos do FNDE referente ao P. N. de Apoio ao Transporte Escolar (PNATE)",
      "15690000 - Outras TransferÃªncias de Recursos do FNDE",
      "15700000 - TransferÃªncias do Governo Federal ref. a ConvÃªnios e outros Repasses vinculados Ã  EducaÃ§Ã£o",
      "15703110 - Transf. Gov. Federal ref. ConvÃªnios e outros Repasses vinculados Ã  EducaÃ§Ã£o - Emenda Individual",
      "15703120 - Transf. Gov. Federal ref. ConvÃªnios e outros Repasses vinculados Ã  EducaÃ§Ã£o - Emenda Bancada",
      "15740000 - Recursos de OperaÃ§Ãµes de CrÃ©dito EducaÃ§Ã£o",
    ];

    const fontesSim = [
      "25000000 - Recursos nÃ£o vinculados de Impostos",
      "25001001 - Recursos destinados Ã  ManutenÃ§Ã£o e Desenvolvimento do Ensino",
      "25010000 - Outros Recursos nÃ£o Vinculados",
      "25010100 - Outros Recursos nÃ£o vinculados destinados ao Tesouro",
      "25021001 - Recursos nÃ£o vinculados da compensaÃ§Ã£o de impostos - Destinado ao Ensino",
      "25400000 - TransferÃªncia de recursos do FUNDEB desenvolvimento do Ensino",
      "25401070 - TransferÃªncia de recursos do FUNDEB RemuneraÃ§Ã£o EducaÃ§Ã£o BÃ¡sica",
      "25401071 - TransferÃªncia de recursos do FUNDEB - Ensino em Tempo Integral (ETI)",
      "25401072 - TransferÃªncia de recursos do FUNDEB - RemuneraÃ§Ã£o Profissionais ETI",
      "25460000 - TransferÃªncias do FUNDEB - ComplementaÃ§Ã£o da UniÃ£o - ETI",
      "25500000 - Recursos da ContribuiÃ§Ã£o ao SalÃ¡rio EducaÃ§Ã£o",
      "25510000 - TransferÃªncias de Recursos do FNDE referente ao Programa Dinheiro Direto na Escola (PDDE)",
      "25520000 - TransferÃªncias de Recursos do FNDE referente ao Programa Nacional de AlimentaÃ§Ã£o Escolar (PNAE)",
      "25530000 - TransferÃªncias de Recursos do FNDE referente ao P. N. de Apoio ao Transporte Escolar (PNATE)",
      "25690000 - Outras TransferÃªncias de Recursos do FNDE",
      "25700000 - TransferÃªncias do Governo Federal ref. a ConvÃªnios e outros Repasses vinculados Ã  EducaÃ§Ã£o",
      "25703110 - Transf. Gov. Federal ref. ConvÃªnios e outros Repasses vinculados Ã  EducaÃ§Ã£o - Emenda Individual",
      "25703120 - Transf. Gov. Federal ref. ConvÃªnios e outros Repasses vinculados Ã  EducaÃ§Ã£o - Emenda Bancada",
      "25740000 - Recursos de OperaÃ§Ãµes de CrÃ©dito EducaÃ§Ã£o",
    ];

    const setFonteOptions = (items) => {
      if (!fonteSelect) return;
      const current = fonteSelect.value || "";
      fonteSelect.innerHTML = "";
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = "Selecione";
      fonteSelect.appendChild(opt);
      items.forEach((item) => {
        const o = document.createElement("option");
        o.value = item;
        o.textContent = item;
        fonteSelect.appendChild(o);
      });
      if (current) fonteSelect.value = current;
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
      etapaMunicipioSelect.innerHTML = "<option value=\"\">Selecione...</option>";
      municipioItems.forEach((item) => {
        const opt = document.createElement("option");
        opt.value = item.municipios_entrega;
        opt.textContent = item.municipioLabel || item.municipios_entrega;
        etapaMunicipioSelect.appendChild(opt);
      });
      if (etapaMunicipioSelect.options.length > 1) {
        etapaMunicipioSelect.selectedIndex = 1;
      }
    };

    const renderMunicipioList = () => {
      if (!municipioListEl) return;
      municipioListEl.innerHTML = "";
      if (!municipioItems.length) {
        const empty = document.createElement("div");
        empty.className = "municipio-empty";
        empty.textContent = "Nenhum munic\u00edpio adicionado.";
        municipioListEl.appendChild(empty);
        return;
      }
      municipioItems.forEach((item, idx) => {
        const row = document.createElement("div");
        row.className = "municipio-item";
        row.innerHTML = `
          <div>
            <small>C\u00f3digo</small>
            <div>${item.codigoLabel || item.codigo}</div>
          </div>
          <div>
            <small>Munic\u00edpio</small>
            <div>${item.municipioLabel || item.municipios_entrega}</div>
          </div>
          <div>
            <small>Meta</small>
            <div>${item.meta_subacao}</div>
          </div>
          <div>
            <button class="btn btn-danger sm" type="button" data-remove-index="${idx}">Remover</button>
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

    const handleRegiaoChange = () => {
      if (!planSelects.regiao) return;
      if (municipioItems.length) {
        setMsg("Remova todos os municÃ­pios antes de trocar a regiÃ£o.", true);
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
      const codigo = codigoSelect?.value || "";
      const municipio = municipioSelect?.value || "";
      const meta = metaInput?.value || "";
      if (!codigo || !municipio || !meta) {
        if (!opts.silent) setMsg("Informe c\u00f3digo, munic\u00edpio e meta para adicionar.", true);
        return false;
      }
      const regiaoAtual = regiaoEntregaSelect?.value || "";
      const regiaoChave = chaveSelects.regiao?.value || "";
      const regiaoAtualNorm = normalizeDigits(regiaoAtual);
      const regiaoChaveNorm = normalizeDigits(regiaoChave);
      if (regiaoChaveNorm && regiaoAtualNorm && regiaoChaveNorm !== regiaoAtualNorm) {
        if (!opts.silent) {
          setMsg("A regi\u00e3o da Suba\u00e7\u00e3o/Entrega n\u00e3o confere com a regi\u00e3o da chave.", true);
        }
        return false;
      }
      if (municipioItems.length) {
        const baseRegiao = municipioItems[0].regiao_subacao || "";
        const baseRegiaoNorm = normalizeDigits(baseRegiao);
        if (baseRegiaoNorm && regiaoAtualNorm && baseRegiaoNorm !== regiaoAtualNorm) {
          if (!opts.silent) {
            setMsg("Remova todos os munic\u00edpios antes de trocar a regi\u00e3o.", true);
          }
          return false;
        }
      }
      const key = `${codigo}::${municipio}`;
      const exists = municipioItems.some((item) => `${item.codigo}::${item.municipios_entrega}` === key);
      const municipioExists = municipioItems.some((item) => item.municipios_entrega === municipio);
      if (exists) {
        if (!opts.silent) setMsg("Este munic\u00edpio j\u00e1 foi adicionado.", true);
        return false;
      }
      if (municipioExists) {
        if (!opts.silent) setMsg("Este munic\u00edpio j\u00e1 foi adicionado.", true);
        return false;
      }
      municipioItems.push({
        codigo,
        municipios_entrega: municipio,
        meta_subacao: meta,
        regiao_subacao: regiaoAtual,
        codigoLabel: getSelectLabel(codigoSelect),
        municipioLabel: getSelectLabel(municipioSelect),
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

    const renderEditMunicipioList = () => {
      if (!editMunicipioListEl) return;
      editMunicipioListEl.innerHTML = "";
      if (!editMunicipioItems.length) {
        const empty = document.createElement("div");
        empty.className = "municipio-empty";
        empty.textContent = "Nenhum municÃ­pio adicionado.";
        editMunicipioListEl.appendChild(empty);
        if (editMunicipioListWrap) editMunicipioListWrap.style.display = "none";
        return;
      }
      if (editMunicipioListWrap) editMunicipioListWrap.style.display = "";
      editMunicipioItems.forEach((item, idx) => {
        const row = document.createElement("div");
        row.className = "municipio-item";
        row.innerHTML = `
          <div>
            <small>CÃ³digo</small>
            <div>${item.codigoLabel || item.codigo}</div>
          </div>
          <div>
            <small>MunicÃ­pio</small>
            <div>${item.municipioLabel || item.municipios_entrega}</div>
          </div>
          <div>
            <small>Meta</small>
            <div>${item.meta_subacao}</div>
          </div>
          <div>
            <button class="btn btn-danger sm" type="button" data-edit-remove-index="${idx}">Remover</button>
          </div>
        `;
        editMunicipioListEl.appendChild(row);
      });
    };

    const addEditMunicipioItem = () => {
      const codigo = editCodigoNovoSelect?.value || "";
      const municipio = editMunicipioNovoSelect?.value || "";
      const meta = editMetaNovoInput?.value || "";
      if (!codigo || !municipio || !meta) {
        setMsg("Informe cÃ³digo, municÃ­pio e meta para adicionar.", true);
        return false;
      }
      const key = `${codigo}::${municipio}`;
      const exists = editMunicipioItems.some(
        (item) => `${item.codigo}::${item.municipios_entrega}` === key
      );
      if (exists) {
        setMsg("Este municÃ­pio jÃ¡ foi adicionado.", true);
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

    const updateValorTotal = () => {
      if (!quantidadeInput || !valorUnitarioInput || !valorTotalInput) return;
      if (!quantidadeInput.value || !valorUnitarioInput.value) {
        valorTotalInput.value = "";
        return;
      }
      const total = parseDecimalInput(quantidadeInput.value) * parseDecimalInput(valorUnitarioInput.value);
      valorTotalInput.value = formatPtBr(total);
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
      if (naturezaInput) naturezaInput.value = "";
      if (fonteSelect) fonteSelect.value = "";
      if (idusoSelect) idusoSelect.value = "";
      if (descricaoInput) descricaoInput.value = "";
      if (unidadeEtapaSelect) unidadeEtapaSelect.value = unidadeEtapaSelect.options?.[0]?.value || "Real (R$)";
      if (quantidadeInput) quantidadeInput.value = "";
      if (valorUnitarioInput) valorUnitarioInput.value = "";
      if (valorTotalInput) valorTotalInput.value = "";
      if (justificativaInput) justificativaInput.value = "";
      if (responsavelNgerInput) responsavelNgerInput.value = "";
      syncEtapaCounter();
    };

    const captureEtapaPayload = () => ({
      municipio: etapaMunicipioSelect?.value || "",
      municipio_label: getSelectLabel(etapaMunicipioSelect),
      nome_etapa: etapaInput?.value || "",
      responsavel_etapa: responsavelEtapaInput?.value || "",
      cpf_responsavel_etapa: cpfEtapaInput?.value || "",
      regiao_memoria: regiaoMemoriaInput?.value || "",
      natureza: naturezaInput?.value || "",
      fonte: fonteSelect?.value || "",
      idu: idusoSelect?.value || "",
      descricao: descricaoInput?.value || "",
      unidade_etapa: unidadeEtapaSelect?.value || "",
      quantidade: quantidadeInput?.value || "",
      valor_unitario: valorUnitarioInput?.value || "",
      valor_total: valorTotalInput?.value || "",
      justificativa: justificativaInput?.value || "",
      responsavel_nger: responsavelNgerInput?.value || "",
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
            <small>Munic\u00edpio</small>
            <div>${extractMunicipioName(item.municipio_label || item.municipio)}</div>
          </div>
          <div>
            <small>Nome da Etapa</small>
            <div>${item.nome_etapa}</div>
          </div>
          <div>
            <small>Respons\u00e1vel</small>
            <div>${item.responsavel_etapa || "-"}</div>
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
        updateEtapaStepLabel();
      }
    };

    const validateEtapaBlock = () => {
      if (!etapaMunicipioSelect || !etapaMunicipioSelect.value) {
        setMsg("Selecione o munic\u00edpio da etapa.", true);
        etapaMunicipioSelect?.focus();
        return false;
      }
      if (!etapaInput?.value || etapaInput.value.length > 260) {
        setMsg("Informe o nome da etapa (at\u00e9 260 caracteres).", true);
        etapaInput?.focus();
        return false;
      }
      if (cpfEtapaInput && cpfEtapaInput.value && !isValidCpf(cpfEtapaInput.value)) {
        setMsg("CPF do respons\u00e1vel da etapa inv\u00e1lido.", true);
        cpfEtapaInput.focus();
        return false;
      }
      return true;
    };

    const setOptions = (select, items, placeholder, valueKey = "value", labelKey = "label") => {
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
      if (current) select.value = current;
    };

    const setOptionsFromLabel = (select, items, placeholder) => {
      if (!select) return;
      const current = select.value;
      select.innerHTML = "";
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = placeholder || "Selecione...";
      select.appendChild(opt);
      items.forEach((item) => {
        const o = document.createElement("option");
        o.value = item.label ?? "";
        o.textContent = item.label ?? "";
        select.appendChild(o);
      });
      if (current) select.value = current;
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
        const plan = data.plan21 || {};
        setOptions(planSelects.exercicio, plan.exercicio || [], "Selecione...");
        setOptions(planSelects.uo, plan.uo || [], "Selecione...");
        setOptions(planSelects.programa, plan.programa || [], "Selecione...");
        setOptions(planSelects.acao_paoe, plan.acao_paoe || [], "Selecione...");
        setOptions(planSelects.responsavel_acao, plan.responsavel_acao || [], "Selecione...");
        setOptions(planSelects.produto_acao, plan.produto_acao || [], "Selecione...");

        setOptions(chaveSelects.regiao, data.regioes || [], "Selecione...");
        if (chaveSelects.regiao) {
          lastRegiaoValue = chaveSelects.regiao.value || lastRegiaoValue;
        }
        setOptions(chaveSelects.subfuncao, data.subfuncoes || [], "Selecione...");
        setOptions(chaveSelects.ug, data.ugs || [], "Selecione...");
        setOptions(chaveSelects.adj, data.adjs || [], "Selecione...");
        setOptions(chaveSelects.macropolitica, data.macropoliticas || [], "Selecione...");
        setOptions(chaveSelects.pilar, data.pilares || [], "Selecione...");
        setOptions(chaveSelects.eixo, data.eixos || [], "Selecione...");
        setOptions(chaveSelects.politica_decr, data.politicas || [], "Selecione...");
        setOptions(chaveSelects.publico_transversal, data.publicos || [], "Selecione...");

        setOptionsFromLabel(regiaoEntregaSelect, data.regioes || [], "Selecione...");
        setOptions(codigoSelect, data.municipios || [], "Selecione...", "codigo", "label");
        setOptions(municipioSelect, data.municipios || [], "Selecione...", "nome", "label");
        if (editCodigoNovoSelect) {
          setOptions(editCodigoNovoSelect, data.municipios || [], "Selecione...", "codigo", "label");
        }
        if (editMunicipioNovoSelect) {
          setOptions(editMunicipioNovoSelect, data.municipios || [], "Selecione...", "nome", "label");
        }
        formatKey();
        syncBridges();
      } catch (err) {
        if (err.name !== "AbortError") console.error(err);
      }
    };

    let pendingEditAbort = null;
    let editOptionsSeq = 0;
    let editMunicipioPairs = [];
    const syncEditMunicipioPair = (source) => {
      if (!editCodigoSelect || !editMunicipioSelect) return;
      if (!editMunicipioPairs.length) return;
      const byCodigo = new Map();
      const byMunicipio = new Map();
      editMunicipioPairs.forEach((pair) => {
        if (pair?.codigo && !byCodigo.has(pair.codigo)) {
          byCodigo.set(pair.codigo, { municipio: pair.municipio, meta: pair.meta || "" });
        }
        if (pair?.municipio && !byMunicipio.has(pair.municipio)) {
          byMunicipio.set(pair.municipio, { codigo: pair.codigo, meta: pair.meta || "" });
        }
      });
      const setMetaValue = (metaValue) => {
        if (!editMetaSelect || !metaValue) return;
        const existing = [...editMetaSelect.options].some((opt) => opt.value === metaValue);
        if (!existing) {
          const opt = document.createElement("option");
          opt.value = metaValue;
          opt.textContent = metaValue;
          editMetaSelect.appendChild(opt);
        }
        editMetaSelect.value = metaValue;
      };
      if (source === "codigo") {
        const match = byCodigo.get(editCodigoSelect.value);
        if (match?.municipio) editMunicipioSelect.value = match.municipio;
        if (match?.meta) setMetaValue(match.meta);
      } else if (source === "municipio") {
        const match = byMunicipio.get(editMunicipioSelect.value);
        if (match?.codigo) editCodigoSelect.value = match.codigo;
        if (match?.meta) setMetaValue(match.meta);
      } else if (source === "meta") {
        const metaValue = editMetaSelect?.value || "";
        if (!metaValue) return;
        let match = editMunicipioPairs.find((pair) => pair.meta === metaValue && pair.codigo === editCodigoSelect.value);
        if (!match) {
          match = editMunicipioPairs.find((pair) => pair.meta === metaValue && pair.municipio === editMunicipioSelect.value);
        }
        if (!match) {
          match = editMunicipioPairs.find((pair) => pair.meta === metaValue);
        }
        if (match?.codigo) editCodigoSelect.value = match.codigo;
        if (match?.municipio) editMunicipioSelect.value = match.municipio;
      }
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
      const editSelectMap = {
        chave_planejamento: editChaveSelect,
        subacao_entrega: editSubacaoSelect,
        responsavel: editResponsavelSelect,
        prazo: editPrazoSelect,
        unid_gestora: editUnidGestoraSelect,
        unidade_setorial_planejamento: editUnidadeSetorialSelect,
        produto_subacao: editProdutoSelect,
        unidade_medida: editUnidadeMedidaSelect,
        regiao_subacao: editRegiaoSelect,
        codigo: editCodigoSelect,
        municipios_entrega: editMunicipioSelect,
        meta_subacao: editMetaSelect,
        detalhamento_produto: editDetalhamentoSelect,
      };
        Object.entries(editSelectMap).forEach(([key, el]) => {
          if (!el?.value) return;
          if (el.dataset.readonly === "soft") return;
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
        const options = data.options || {};
        editMunicipioPairs = Array.isArray(data.pairs) ? data.pairs : [];
        Object.entries(editSelectMap).forEach(([key, el]) => {
          if (!el) return;
          setOptionsFromLabel(el, (options[key] || []).map((v) => ({ label: v })), "Selecione...");
        });
        const softSelects = [
          editPrazoSelect,
          editUnidGestoraSelect,
          editUnidadeSetorialSelect,
          editUnidadeMedidaSelect,
          editRegiaoSelect,
          editCodigoSelect,
          editMunicipioSelect,
          editMetaSelect,
        ];
        softSelects.forEach((el) => {
          if (!el) return;
          const count = el.options.length;
          if (!el.value && count === 2) {
            el.value = el.options[1].value;
          }
          el.disabled = count <= 2;
        });
        if (editSubacaoSelect && editSubacaoInput) {
          editSubacaoInput.value = editSubacaoSelect.value || "";
        }
        if (editResponsavelSelect && editResponsavelInput) {
          editResponsavelInput.value = editResponsavelSelect.value || "";
        }
        if (editDetalhamentoSelect && editDetalhamentoInput) {
          editDetalhamentoInput.value = editDetalhamentoSelect.value || "";
        }
        const muniCount = editMunicipioSelect?.options?.length || 0;
        if (editMunicipioToggle) {
          editMunicipioToggle.style.display = muniCount > 2 ? "" : "none";
        }
        if (editCodigoSelect?.value) {
          syncEditMunicipioPair("codigo");
        } else if (editMunicipioSelect?.value) {
          syncEditMunicipioPair("municipio");
        }
      } catch (err) {
        if (err.name !== "AbortError") console.error(err);
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
      if (editUnidadeMedidaSelect) editUnidadeMedidaSelect.value = "";
      if (editRegiaoSelect) editRegiaoSelect.value = "";
      if (editCodigoSelect) editCodigoSelect.value = "";
      if (editMunicipioSelect) editMunicipioSelect.value = "";
      if (editMetaSelect) editMetaSelect.value = "";
      if (editDetalhamentoSelect) editDetalhamentoSelect.value = "";
      if (editDetalhamentoInput) editDetalhamentoInput.value = "";
      if (editCodigoNovoSelect) editCodigoNovoSelect.value = "";
      if (editMunicipioNovoSelect) editMunicipioNovoSelect.value = "";
      if (editMetaNovoInput) editMetaNovoInput.value = "";
      municipioItems.length = 0;
      renderMunicipioList();
      editMunicipioItems.length = 0;
      renderEditMunicipioList();
      if (etapaMunicipioSelect) {
        etapaMunicipioSelect.innerHTML = "<option value=\"\">Selecione...</option>";
      }
        setMsg("");
        syncSubacaoEntregaCounter();
        const isEditMode = (modeSelect?.value || "").toLowerCase() === "editar";
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
        return;
      }
      if (currentStep === 2) {
        clearStepTwo();
        clearStepThree();
        return;
      }
      if (currentStep === 3) {
        clearStepThree();
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
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const pageRows = rows.slice(startIdx, startIdx + pageSize);
      rows.forEach((row) => {
        row.style.display = "none";
        row.classList.remove("selected");
      });
      pageRows.forEach((row) => {
        row.style.display = "";
      });
      renderPagination(totalPages);
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

    const fillFormFromRow = (row) => {
      if (!row) return;
      const chave = row.dataset.chave || "";
      const chaveParts = parseChaveParts(chave);
      const subacaoEntregaFull = row.dataset.subacaoEntrega || "";
      let subacaoEntregaRaw = subacaoEntregaFull;
      if (chave && subacaoEntregaFull.startsWith(chave)) {
        subacaoEntregaRaw = subacaoEntregaFull.slice(chave.length).trim();
      }
      const prazo = row.dataset.prazo || "";
      const prazoParts = prazo.split(" a ");
      if (planSelects.exercicio) planSelects.exercicio.value = row.dataset.exercicio || "";
      if (planSelects.uo) planSelects.uo.value = row.dataset.uo || "";
      if (planSelects.programa) planSelects.programa.value = row.dataset.programa || "";
      if (planSelects.acao_paoe) planSelects.acao_paoe.value = row.dataset.acaoPaoe || "";
      if (planSelects.responsavel_acao) planSelects.responsavel_acao.value = row.dataset.responsavelAcao || "";
      if (planSelects.produto_acao) planSelects.produto_acao.value = row.dataset.produtoAcao || "";

      chaveSelects.regiao.value = chaveParts.regiao || "";
      chaveSelects.subfuncao.value = chaveParts.subfuncao || "";
      if (chaveParts.ug) {
        chaveSelects.ug.value = String(chaveParts.ug).padStart(4, "0");
      }
      chaveSelects.adj.value = chaveParts.adj || "";
      chaveSelects.macropolitica.value = chaveParts.macropolitica || "";
      chaveSelects.pilar.value = chaveParts.pilar || "";
      chaveSelects.eixo.value = chaveParts.eixo || "";
      chaveSelects.politica_decr.value = chaveParts.politica || "";
      chaveSelects.publico_transversal.value = chaveParts.publico || "";
      if (chaveInput) chaveInput.value = chave;

      if (subacaoEntregaInput) subacaoEntregaInput.value = subacaoEntregaRaw || "";
      syncSubacaoEntregaCounter();
      if (responsavelInput) responsavelInput.value = row.dataset.responsavel || "";
      if (cpfInput) cpfInput.value = row.dataset.cpf || "";
      if (dataInicioInput) dataInicioInput.value = prazoParts[0] || "";
      if (dataFimInput) dataFimInput.value = prazoParts[1] || "";
      if (unidGestoraSelect) unidGestoraSelect.value = row.dataset.unidGestora || "";
      if (unidadeSetorialSelect) unidadeSetorialSelect.value = row.dataset.unidadeSetorial || "";
      if (produtoSubacaoSelect) produtoSubacaoSelect.value = row.dataset.produtoSubacao || "";
      if (unidadeMedidaSelect) unidadeMedidaSelect.value = row.dataset.unidadeMedida || "";
      if (regiaoEntregaSelect) regiaoEntregaSelect.value = row.dataset.regiaoSubacao || "";
      if (codigoSelect) codigoSelect.value = row.dataset.codigo || "";
      if (municipioSelect) municipioSelect.value = row.dataset.municipio || "";
      if (metaInput) metaInput.value = row.dataset.meta || "";
      if (detalhamentoInput) detalhamentoInput.value = row.dataset.detalhamento || "";
      if (etapaInput) etapaInput.value = row.dataset.etapa || "";
      if (responsavelEtapaInput) responsavelEtapaInput.value = row.dataset.responsavelEtapa || "";
      if (cpfEtapaInput) cpfEtapaInput.value = row.dataset.cpfResponsavelEtapa || "";
      if (regiaoMemoriaInput) regiaoMemoriaInput.value = row.dataset.regiaoMemoria || "";
      if (naturezaInput) naturezaInput.value = row.dataset.natureza || "";
      if (fonteSelect) fonteSelect.value = row.dataset.fonte || "";
      if (idusoSelect) idusoSelect.value = row.dataset.idu || "";
      if (descricaoInput) descricaoInput.value = row.dataset.descricao || "";
      if (unidadeEtapaSelect) unidadeEtapaSelect.value = row.dataset.unidadeEtapa || "";
      if (quantidadeInput) quantidadeInput.value = row.dataset.quantidade || "";
      if (valorUnitarioInput) valorUnitarioInput.value = row.dataset.valorUnitario || "";
      if (valorTotalInput) valorTotalInput.value = row.dataset.valorTotal || "";
      if (justificativaInput) justificativaInput.value = row.dataset.justificativa || "";
      if (responsavelNgerInput) responsavelNgerInput.value = row.dataset.responsavelNger || "";
      syncBridges();
      formatMetaInput();
      formatDecimalInput(quantidadeInput);
      formatDecimalInput(valorUnitarioInput);
      formatDecimalInput(valorTotalInput);
      loadOptions();
      setStep(1);
      municipioItems.length = 0;
      etapaItems.length = 0;
      renderMunicipioList();
      renderEtapaList();
      if (etapaMunicipioSelect) {
        etapaMunicipioSelect.innerHTML = "<option value=\"\">Selecione...</option>";
      }
      clearEtapaFields();
      syncSubacaoEntregaCounter();
    };

    const getSelectedRow = () => summaryBody?.querySelector(".dotacao-summary-row.selected");

    Object.values(planSelects).forEach((el) => {
      if (!el) return;
      el.addEventListener("change", () => {
        el.dataset.touched = "1";
        loadOptions();
        if ((modeSelect?.value || "").toLowerCase() === "editar") {
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
    if (etapaInput) {
      etapaInput.setAttribute("maxlength", "260");
      etapaInput.addEventListener("input", syncEtapaCounter);
    }
    if (etapaMunicipioSelect) {
      etapaMunicipioSelect.addEventListener("change", () => {
        fillEtapaNomePrefix();
      });
    }
    if (quantidadeInput) {
      quantidadeInput.addEventListener("input", () => {
        formatDecimalInput(quantidadeInput);
        updateValorTotal();
      });
    }
      if (valorUnitarioInput) {
        valorUnitarioInput.addEventListener("input", () => {
          formatDecimalInput(valorUnitarioInput);
          updateValorTotal();
        });
      }
      if (valorTotalInput) valorTotalInput.addEventListener("input", () => formatDecimalInput(valorTotalInput));
      const editReloadSelects = [
        editChaveSelect,
        editSubacaoSelect,
        editResponsavelSelect,
        editPrazoSelect,
        editUnidGestoraSelect,
        editUnidadeSetorialSelect,
        editProdutoSelect,
        editUnidadeMedidaSelect,
        editRegiaoSelect,
        editCodigoSelect,
        editMunicipioSelect,
        editMetaSelect,
        editDetalhamentoSelect,
      ].filter(Boolean);
      editReloadSelects.forEach((sel) => {
        sel.addEventListener("change", loadEditOptions);
      });
      if (editCodigoSelect) {
        editCodigoSelect.addEventListener("change", () => {
          syncEditMunicipioPair("codigo");
        });
      }
      if (editMunicipioSelect) {
        editMunicipioSelect.addEventListener("change", () => {
          syncEditMunicipioPair("municipio");
        });
      }
      if (editMetaSelect) {
        editMetaSelect.addEventListener("change", () => {
          syncEditMunicipioPair("meta");
        });
      }
      if (editSubacaoSelect && editSubacaoInput) {
        editSubacaoSelect.addEventListener("change", () => {
          editSubacaoInput.value = editSubacaoSelect.value || "";
        });
      }
      if (editResponsavelSelect && editResponsavelInput) {
        editResponsavelSelect.addEventListener("change", () => {
          editResponsavelInput.value = editResponsavelSelect.value || "";
        });
      }
      if (editDetalhamentoSelect && editDetalhamentoInput) {
        editDetalhamentoSelect.addEventListener("change", () => {
          editDetalhamentoInput.value = editDetalhamentoSelect.value || "";
        });
      }
      if (editMunicipioToggle) {
        editMunicipioToggle.addEventListener("change", (ev) => {
          if (!editMunicipioWrap) return;
          const target = ev.target;
          if (target && target.name === "edit_municipio_change") {
            editMunicipioWrap.style.display = target.value === "sim" ? "" : "none";
          }
        });
      }
      if (editMunicipioAddBtn) {
        editMunicipioAddBtn.addEventListener("click", () => {
          addEditMunicipioItem();
        });
      }
      if (editMunicipioListEl) {
        editMunicipioListEl.addEventListener("click", (ev) => {
          const btn = ev.target.closest("[data-edit-remove-index]");
          if (!btn) return;
          const idx = Number(btn.getAttribute("data-edit-remove-index"));
          if (Number.isNaN(idx)) return;
          editMunicipioItems.splice(idx, 1);
          renderEditMunicipioList();
        });
      }
      if (naturezaInput) {
        naturezaInput.addEventListener("input", () => {
        const digits = String(naturezaInput.value || "").replace(/\D/g, "").slice(0, 9);
        if (!digits) {
          naturezaInput.value = "";
          return;
        }
        const groups = [1, 1, 2, 2, 3];
        const parts = [];
        let idx = 0;
        for (const size of groups) {
          if (idx >= digits.length) break;
          parts.push(digits.slice(idx, Math.min(idx + size, digits.length)));
          idx += size;
        }
        naturezaInput.value = parts.join(".");
      });
    }
    const getFonteSuperavitValue = () => {
      const selected = fonteSuperavitInputs.find((input) => input.checked);
      return selected?.value || "nao";
    };

    if (fonteSuperavitInputs.length) {
      const updateFonteList = () => {
        const mode = getFonteSuperavitValue();
        setFonteOptions(mode === "sim" ? fontesSim : fontesNao);
      };
      fonteSuperavitInputs.forEach((input) => {
        input.addEventListener("change", updateFonteList);
        input.addEventListener("input", updateFonteList);
      });
      form.addEventListener("change", (ev) => {
        if (ev.target && ev.target.name === "fonte_superavit") {
          updateFonteList();
        }
      });
    }
    if (clearBtn) clearBtn.addEventListener("click", clearForm);
    if (municipioAddBtn) {
      municipioAddBtn.addEventListener("click", () => {
        addMunicipioItem();
      });
    }
    if (municipioListEl) {
      municipioListEl.addEventListener("click", (ev) => {
        const btn = ev.target.closest("[data-remove-index]");
        if (!btn) return;
        const idx = Number(btn.getAttribute("data-remove-index"));
        if (Number.isNaN(idx)) return;
        const removed = municipioItems.splice(idx, 1)[0];
        renderMunicipioList();
        removeEtapasByMunicipio(removed?.municipios_entrega || "");
      });
    }
    if (etapaListEl) {
      etapaListEl.addEventListener("click", (ev) => {
        const btn = ev.target.closest("[data-remove-etapa]");
        if (!btn) return;
        const idx = Number(btn.getAttribute("data-remove-etapa"));
        if (Number.isNaN(idx)) return;
        etapaItems.splice(idx, 1);
        renderEtapaList();
        updateEtapaStepLabel();
      });
    }
    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        pageSize = parseInt(pageSizeSelect.value || "20", 10) || 20;
        currentPage = 1;
        renderSummaryPage();
      });
    }

    if (summaryBody) {
      summaryBody.addEventListener("click", (ev) => {
        const row = ev.target.closest(".dotacao-summary-row");
        if (!row) return;
        setSelectedRow(row);
        setMsg("");
      });
    }

    if (editBtn) {
      editBtn.addEventListener("click", () => {
        const row = getSelectedRow();
        if (!row) {
          setMsg("Selecione um registro para editar.", true);
          return;
        }
        if (modeSelect) modeSelect.value = "editar";
        toggleMode();
        loadEditOptions();
        if (idInput) idInput.value = row.dataset.id || "";
        fillFormFromRow(row);
        setMsg("");
      });
    }

    if (deleteBtn) {
      deleteBtn.addEventListener("click", async () => {
        const row = getSelectedRow();
        if (!row) {
          setMsg("Selecione um registro para excluir.", true);
          return;
        }
        const id = row.dataset.id || "";
        if (!id) return;
        try {
          const res = await fetch(`/api/subacao/${encodeURIComponent(id)}`, {
            method: "DELETE",
            headers: { "X-Requested-With": "fetch" },
          });
          const data = await res.json();
          if (!res.ok) {
            setMsg(data.error || "Falha ao excluir.", true);
            return;
          }
          await loadPage("cadastrar/plan_21-nger/subacao");
        } catch (err) {
          console.error(err);
          setMsg("Falha ao excluir.", true);
        }
      });
    }

    if (printBtn) {
      printBtn.addEventListener("click", () => window.print());
    }

    if (modeSelect) {
      modeSelect.addEventListener("change", () => {
        if (modeSelect.value === "cadastrar") {
          if (idInput) idInput.value = "";
          if (subacaoSummary) subacaoSummary.classList.remove("dotacao-summary-warn");
          if (municipioAddBtn) municipioAddBtn.disabled = false;
          if (municipioListWrap) municipioListWrap.style.display = "";
        }
        if (modeSelect.value === "editar") {
          municipioItems.length = 0;
          etapaItems.length = 0;
          renderMunicipioList();
          if (municipioAddBtn) municipioAddBtn.disabled = true;
          if (municipioListWrap) municipioListWrap.style.display = "none";
          if (addEtapaBtn) addEtapaBtn.style.display = "none";
        }
      });
    }

    const updateEtapaStepLabel = () => {
      if (!stepButtons.length) return;
      const idx = etapaItems.length + 1;
      stepButtons.forEach((btn) => {
        if (Number(btn.dataset.step) === 3) {
          btn.textContent = idx > 1 ? `3.${idx} Etapa` : "3. Etapa";
        }
      });
    };

    if (addEtapaBtn) {
      addEtapaBtn.addEventListener("click", () => {
        if (!validateEtapaBlock()) return;
        const payload = captureEtapaPayload();
        etapaItems.push(payload);
        renderEtapaList();
        const usedMunicipio = payload.municipio;
        if (etapaMunicipioSelect) {
          const options = Array.from(etapaMunicipioSelect.options || []);
          const opt = options.find((o) => o.value === usedMunicipio);
          if (opt) opt.remove();
          if (etapaMunicipioSelect.options.length > 1) {
            etapaMunicipioSelect.selectedIndex = 1;
          }
        }
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
      if (municipioItems.length) {
        if (codigoSelect) codigoSelect.required = false;
        if (municipioSelect) municipioSelect.required = false;
        if (metaInput) metaInput.required = false;
      }
      if (cpfInput && !isValidCpf(cpfInput.value)) {
        setMsg("CPF do responsável inválido.", true);
        cpfInput.focus();
        return;
      }
      if (cpfEtapaInput && cpfEtapaInput.value && !isValidCpf(cpfEtapaInput.value)) {
        setMsg("CPF do responsÃ¡vel da etapa invÃ¡lido.", true);
        cpfEtapaInput.focus();
        return;
      }
      setMsg("");
      const mode = modeSelect?.value || "cadastrar";
      const subacaoId = idInput?.value || "";
      if (mode === "editar" && !subacaoId) {
        setMsg("Selecione um registro para editar.", true);
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
        regiao_memoria: regiaoMemoriaInput?.value || "",
        natureza: naturezaInput?.value || "",
        fonte: fonteSelect?.value || "",
        idu: idusoSelect?.value || "",
        descricao: descricaoInput?.value || "",
        unidade_etapa: unidadeEtapaSelect?.value || "",
        quantidade: quantidadeInput?.value || "",
        valor_unitario: valorUnitarioInput?.value || "",
        valor_total: valorTotalInput?.value || "",
        justificativa: justificativaInput?.value || "",
        responsavel_nger: responsavelNgerInput?.value || "",
      };
      if (municipioItems.length) {
        if (!validateEtapaBlock()) return;
        const last = captureEtapaPayload();
        const all = [...etapaItems, last];
        if (!last.municipio) {
          setMsg("Selecione um munic\u00edpio para a etapa antes de salvar.", true);
          return;
        }
        if (all.length < municipioItems.length) {
          const faltando = municipioItems.length - all.length;
          setMsg(`Faltam ${faltando} etapa(s) para concluir os munic\u00edpios.`, true);
          return;
        }
        const missing = municipioItems.some(
          (m) => !all.find((e) => e.municipio === m.municipios_entrega)
        );
        if (missing) {
          setMsg("Existe munic\u00edpio sem etapa vinculada.", true);
          return;
        }
        payload.etapas_items = all;
      }
      if (mode === "cadastrar" && municipioItems.length) {
        payload.municipios_items = municipioItems.map((item) => ({
          codigo: item.codigo,
          municipios_entrega: item.municipios_entrega,
          meta_subacao: item.meta_subacao,
        }));
      }
      const url = mode === "editar" ? `/api/subacao/${encodeURIComponent(subacaoId)}` : "/api/subacao";
      const method = mode === "editar" ? "PUT" : "POST";
      try {
        const res = await fetch(url, {
          method,
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) {
          setMsg(data.error || "Falha ao salvar.", true);
          return;
        }
        const count = Array.isArray(data.subacoes) ? data.subacoes.length : 1;
        showToast(`Suba\u00e7\u00e3o salva com sucesso. Registros: ${count}.`, "success");
        await loadPage("cadastrar/plan_21-nger/subacao");
      } catch (err) {
        console.error(err);
        setMsg("Falha ao salvar.", true);
      }
    });

    if (subacaoSummary) {
      subacaoSummary.classList.toggle("dotacao-summary-hidden", getRows().length === 0);
    }
    setStep(1);
    renderMunicipioList();
    updateEtapaStepLabel();
    renderEtapaList();
    if (modeSelect && modeSelect.value === "editar") {
      if (municipioAddBtn) municipioAddBtn.disabled = true;
      if (municipioListWrap) municipioListWrap.style.display = "none";
    }
    loadOptions();
    renderSummaryPage();
    syncEtapaCounter();
    fillEtapaNomePrefix();
    syncSubacaoEntregaCounter();
    setFonteOptions(getFonteSuperavitValue() === "sim" ? fontesSim : fontesNao);
    toggleMode();
    if ((modeSelect?.value || "").toLowerCase() === "editar") {
      loadEditOptions();
    }
  }

  if (menu) {
    menu.addEventListener("click", (ev) => {
      const parentToggle = ev.target.closest(".menu-parent[data-submenu]");
      if (parentToggle) {
        const targetId = parentToggle.getAttribute("data-submenu");
        const group = parentToggle.closest(".menu-group");
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
      loadPage(route).then(() => {
        if (route === "dashboard") {
          initDashboard({ forceShow: true });
        }
      });
    });
  }

  setUserMeta();
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






