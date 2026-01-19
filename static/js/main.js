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

    // expand parent submenu for active route
    document.querySelectorAll(".menu-group").forEach((group) => {
      const submenu = group.querySelector(".submenu");
      if (!submenu) return;
      const hasActive = Array.from(submenu.querySelectorAll("[data-route]")).some(
        (item) => item.getAttribute("data-route") === route
      );
      group.classList.toggle("open", hasActive);
    });
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
      const email = row.dataset.email || "";
      document.getElementById("edit-email").value = email;
      document.getElementById("edit-email-display").value = email;
      document.getElementById("edit-nome").value = row.dataset.nome || "";
      document.getElementById("edit-perfil").value = row.dataset.perfil || "";
      document.getElementById("edit-senha").value = "";
      document.getElementById("edit-ativo").checked = row.dataset.ativo === "1";
    };
    if (!form || !msg) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    bindToggleVisibility(form);

    document.querySelectorAll(".select-usuario").forEach((btn) => {
      btn.addEventListener("click", () => {
        const row = btn.closest("tr[data-email]");
        if (row) fillFromRow(row);
      });
    });

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      msg.textContent = "Salvando...";
      msg.classList.remove("text-error");
      const email = document.getElementById("edit-email").value;
      if (!email) {
        msg.textContent = "Selecione um usuário na lista.";
        msg.classList.add("text-error");
        return;
      }
      const payload = {
        nome: document.getElementById("edit-nome").value,
        perfil: document.getElementById("edit-perfil").value,
        senha: document.getElementById("edit-senha").value,
        ativo: document.getElementById("edit-ativo").checked,
      };
      try {
        const res = await fetch(`/api/usuarios/${encodeURIComponent(email)}`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payload),
        });
        const raw = await res.text();
        let data = {};
        try {
          data = JSON.parse(raw || "{}");
        } catch {
          // se n+úo for JSON, usa texto bruto na mensagem de erro
        }
        if (!res.ok) throw new Error(data.error || raw || `Falha ao salvar. Status ${res.status}`);
        msg.textContent = data.message || "Usuário atualizado.";
        document.getElementById("edit-senha").value = "";
        const row = document.querySelector(`tr[data-email="${email}"]`);
        if (row) {
          row.dataset.nome = payload.nome || row.dataset.nome || "";
          row.dataset.perfil = payload.perfil || row.dataset.perfil || "";
          row.dataset.ativo = payload.ativo ? "1" : "0";
          const cells = row.querySelectorAll("td");
          if (cells.length >= 4) {
            cells[1].textContent = payload.nome || cells[1].textContent;
            cells[2].textContent = payload.perfil || cells[2].textContent;
            cells[3].textContent = payload.ativo ? "Sim" : "N+úo";
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

  async function fetchCurrentPermissions() {
    if (userNivel === "1") {
      // admin: libera tudo vis+¡vel no menu
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
        <div><strong>Sa+¡da gerada:</strong> ${last.output_filename || "-"}</div>
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
        <div><strong>Sa+¡da gerada:</strong> ${last.output_filename || "-"}</div>
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
        <div><strong>Saida gerada:</strong> ${last.output_filename || "-"}</div>
      `;
      if (submitBtn && last.output_filename) {
        submitBtn.dataset.mode = "view";
        submitBtn.dataset.output = last.output_filename;
        submitBtn.textContent = viewLabel || "Ver relatório";
      }
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
        <div><strong>Saida gerada:</strong> ${last.output_filename || "-"}</div>
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
        <div><strong>Saida gerada:</strong> ${last.output_filename || "-"}</div>
      `;
      if (submitBtn && last.output_filename) {
        submitBtn.dataset.mode = "view";
        submitBtn.dataset.output = last.output_filename;
        submitBtn.textContent = viewLabel || "Ver relatório";
      }
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
  const viewLabel = "Ver Relat+¦rio";

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
          msg.textContent = data.message || "Upload conclu+¡do.";
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
          msg.textContent = data.message || "Upload conclu+¡do.";
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
  const viewLabel = "Ver Relat+¦rio";
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
          <div><strong>Sa+¡da gerada:</strong> ${last.output_filename || "-"}</div>
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
          msg.textContent = data.message || "Upload conclu+¡do.";
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
    const editBtn = document.getElementById("dotacao-edit");
    const deleteBtn = document.getElementById("dotacao-delete");
    const printBtn = document.getElementById("dotacao-print");

    const hasAllSelects = Object.values(selects).every((el) => el);
    if (!hasAllSelects || !adjSelect) return;

    let updating = false;
    const baseSaldoKeys = new Set(["exercicio", "chave_planejamento"]);

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
        params[key] = val;
      });
      return params;
    };

    const getAdjLabel = () => {
      const opt = adjSelect.options[adjSelect.selectedIndex];
      return opt ? String(opt.textContent || "").trim() : "";
    };

    const buildDotacaoPrefix = () => {
      const exercicio = selects.exercicio.value || "";
      const adjLabel = getAdjLabel();
      return `DOT.${exercicio}.${adjLabel}.`;
    };

    const updateJustificativaPrefix = () => {
      if (prefixInput) prefixInput.value = `${buildDotacaoPrefix()}*`;
    };

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
      if (totalPages <= 1) return;
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
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
      const start = Math.max(1, Math.min(currentPage - 2, totalPages - maxButtons + 1));
      const end = Math.min(totalPages, start + maxButtons - 1);
      for (let p = start; p <= end; p++) {
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
      if (idInput) idInput.value = row.dataset.id || "";
      selects.exercicio.value = row.dataset.exercicio || "";
      adjSelect.value = row.dataset.adjId || "";
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
      if (valorInput) valorInput.value = formatPtBr(parsePtBr(row.dataset.valor) || 0);
      if (justificativaInput) {
        justificativaInput.value = extractJustificativaOnly(row.dataset.justificativa || "");
      }
      updateJustificativaPrefix();
      await loadOptions();
      selects.exercicio.value = row.dataset.exercicio || "";
      adjSelect.value = row.dataset.adjId || "";
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
          chaveDotacao: data.chave_dotacao || "",
          usuarioNome: data.usuario_nome || "",
          criadoEm: data.criado_em || "",
          alteradoEm: data.alterado_em || "",
        },
      };
    };

    const buildPrintTable = (row) => {
      const fields = [
        ["Exerc&#237;cio", row.dataset.exercicio],
        ["Adjunta Solicitante", row.dataset.adjunta],
        ["Chave de Planejamento", row.dataset.chave],
        ["UO", row.dataset.uo],
        ["Programa", row.dataset.programaRaw],
        ["A&#231;&#227;o/PAOE", row.dataset.acaoPaoe],
        ["Produto", row.dataset.produto],
        ["UG", row.dataset.ug],
        ["Regi&#227;o", row.dataset.regiao],
        ["Suba&#231;&#227;o/Entrega", row.dataset.subacao],
        ["Etapa", row.dataset.etapa],
        ["Natureza de Despesa", row.dataset.natureza],
        ["Elemento de Despesa", row.dataset.elemento],
        ["Subelemento", row.dataset.subelemento],
        ["Fonte", row.dataset.fonte],
        ["Iduso", row.dataset.iduso],
        ["Justificativa/Hist&#243;rico", row.dataset.justificativa],
        ["Valor da Dota&#231;&#227;o", formatPtBr(parsePtBr(row.dataset.valor) || 0)],
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
      if (nome) parts.push(nome);
      if (dataFmt) parts.push(`${label} ${dataFmt}`);
      if (chave) parts.push(chave);
      return parts.join(" - ");
    };

    const openPrintPopup = (rows) => {
      const content = rows.map((row) => buildPrintTable(row)).join('<div class="print-gap"></div>');
      const footerText = buildFooterText(rows[0]);
      const html = `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Dota&#231;&#227;o Cadastrada</title>
  <style>
    body { font-family: Arial, sans-serif; color: #000; margin: 24px; }
    .print-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px dashed #000; }
    .print-brand { display: flex; align-items: center; gap: 12px; }
    .print-brand img { height: 48px; }
    .print-brand-title { font-weight: 700; font-size: 16px; }
    .print-brand-subtitle { font-size: 12px; color: #333; }
    .print-title-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin: 0 0 12px; }
    .print-title { text-align: center; font-weight: 700; flex: 1; text-transform: uppercase; }
    .print-title-key { min-width: 200px; font-size: 12px; }
    .print-title-date { min-width: 200px; text-align: right; font-size: 12px; }
    .print-footer { margin-top: 16px; border-top: 1px dashed #000; font-size: 12px; padding-top: 6px; display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .print-footer img { height: 36px; }
    .print-footer-text { flex: 1; text-align: center; }
    .print-table { width: 100%; border-collapse: collapse; margin-bottom: 12px; table-layout: auto; }
    .print-table th, .print-table td { border: 1px solid #000; padding: 6px 8px; text-align: left; font-size: 8px; vertical-align: top; word-break: break-word; }
    .print-table th { width: auto; white-space: nowrap; background: #f1f1f1; text-transform: uppercase; }
    .print-gap { height: 10px; }
  </style>
</head>
<body>
  <div class="print-header">
    <div class="print-brand">
      <img src="/static/img/logo.jpg" alt="Logo" />
      <div class="print-brand-text">
        <div class="print-brand-title">Sistema de Planejamento e Or&#231;amento</div>
        <div class="print-brand-subtitle">SPO-NGER-SEDUCMT</div>
      </div>
    </div>
  </div>
  <div class="print-title-row">
    <div class="print-title-key">${escapeHtml(rows[0]?.dataset?.chaveDotacao || "")}</div>
    <div class="print-title">DOTA&#199;&#195;O CADASTRADA</div>
    <div class="print-title-date">${formatPrintDate((rows[0]?.dataset?.alteradoEm && rows[0]?.dataset?.alteradoEm !== rows[0]?.dataset?.criadoEm) ? rows[0]?.dataset?.alteradoEm : rows[0]?.dataset?.criadoEm)}</div>
  </div>
  ${content}
  <div class="print-footer">
    <img src="/static/img/logo.jpg" alt="Logo" />
    <div class="print-footer-text">${footerText}</div>
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
        if (saldoDebug) saldoDebug.textContent = "";
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
            `Plan21: ${planSum} | Dotacao: ${dotSum} | PED: ${pedSum} (${pedCount}) | EMP: ${empSum} (${empCount})`;
        }
      } catch (err) {
        console.error(err);
        saldoInput.value = "";
        if (saldoInfo) saldoInfo.textContent = "";
        if (saldoDebug) saldoDebug.textContent = "";
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
          if (criteria.length) {
            applyCriteriaToResults(false);
          }
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
        const dotacaoId = selected.dataset.id;
        if (!dotacaoId) {
          setFilterMsg("Registro inv?lido para exclus?o.", true);
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
      msg.textContent = dotacaoId ? "Atualizando..." : "Salvando...";
      msg.classList.remove("text-error");
      const payload = {
        exercicio: selects.exercicio.value,
        adj_id: adjSelect.value,
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
        loadOptions();
        updateJustificativaPrefix();
      });
    }

    loadOptions();
    loadSaldo();
    updateJustificativaPrefix();
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
      if (totalPages <= 1) return;
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
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
      if (totalPages <= 1) return;
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
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
      if (totalPages <= 1) return;
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
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

  function initRoute(route) {
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
    if (route === "relatorios/fip613") {
      initRelatorioFip();
    }
    if (route === "relatorios/emp") {
      initRelatorioEmp();
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

  if (menu) {
    menu.addEventListener("click", (ev) => {
      const parentToggle = ev.target.closest(".menu-parent[data-submenu]");
      if (parentToggle) {
        const targetId = parentToggle.getAttribute("data-submenu");
        const group = parentToggle.closest(".menu-group");
        const isOpen = group?.classList.contains("open");
        document.querySelectorAll(".menu-group").forEach((g) => {
          if (g !== group) g.classList.remove("open");
        });
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
      loadPage(route);
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





