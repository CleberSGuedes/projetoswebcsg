(function () {
  const palettes = {
    classic: { light: ["#0f7c68", "#1f9b7e", "15, 124, 104", "127, 207, 187", "#0d6154"], dark: ["#3dd6b7", "#22b58f", "61, 214, 183", "15, 124, 104", "#1f9b7e"] },
    blue: { light: ["#1c71d8", "#3584e4", "53, 132, 228", "153, 193, 241", "#1a5fb4"], dark: ["#78aeed", "#99c1f1", "120, 174, 237", "53, 132, 228", "#3584e4"] },
    teal: { light: ["#0e8fb3", "#19a7cc", "25, 167, 204", "143, 220, 238", "#0b7494"], dark: ["#5fd0ec", "#8fdcee", "95, 208, 236", "25, 167, 204", "#19a7cc"] },
    green: { light: ["#26a255", "#33b865", "51, 184, 101", "163, 232, 189", "#1e8545"], dark: ["#5fd48b", "#8fe3ac", "95, 212, 139", "51, 184, 101", "#33b865"] },
    yellow: { light: ["#e5a50a", "#f6d32d", "229, 165, 10", "249, 240, 107", "#c88800"], dark: ["#f8e45c", "#f9f06b", "248, 228, 92", "229, 165, 10", "#e5a50a"] },
    orange: { light: ["#e66100", "#ff7800", "230, 97, 0", "255, 190, 111", "#c64600"], dark: ["#ffa348", "#ffbe6f", "255, 163, 72", "230, 97, 0", "#e66100"] },
    red: { light: ["#e01b24", "#ed333b", "224, 27, 36", "255, 123, 123", "#c01c28"], dark: ["#ff7b7b", "#f66151", "255, 123, 123", "224, 27, 36", "#e01b24"] },
    purple: { light: ["#813d9c", "#9141ac", "129, 61, 156", "220, 138, 221", "#613583"], dark: ["#c061cb", "#dc8add", "192, 97, 203", "129, 61, 156", "#813d9c"] },
  };

  const body = document.body;
  const systemDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  const themePreference = localStorage.getItem("app-theme") || "auto";
  const dark = themePreference === "dark" || (themePreference === "auto" && systemDark);
  const storedAccent = localStorage.getItem("app-accent") || "classic";
  const accent = storedAccent === "spo" ? "green" : storedAccent;
  const palette = palettes[accent] || palettes.classic;
  const values = dark ? palette.dark : palette.light;

  body.classList.toggle("theme-dark", dark);
  body.dataset.themePreference = themePreference;
  body.dataset.accentPreference = palettes[accent] ? accent : "classic";
  body.style.setProperty("--accent", values[0]);
  body.style.setProperty("--accent-strong", values[1]);
  body.style.setProperty("--lab-accent", values[1]);
  body.style.setProperty("--accent-rgb", values[2]);
  body.style.setProperty("--accent-soft-rgb", values[3]);
  body.style.setProperty("--color-600", values[4]);
  body.style.setProperty("--color-500", values[0]);
  body.style.setProperty("--color-400", values[1]);

  try {
    const visual = JSON.parse(localStorage.getItem("spo-common-visual-preferences") || "{}");
    body.dataset.visualDensity = visual.density || "comfortable";
    body.dataset.visualContrast = visual.contrast || "standard";
    body.dataset.visualCards = visual.cards || "soft";
  } catch (_) {
    body.dataset.visualDensity = "comfortable";
    body.dataset.visualContrast = "standard";
    body.dataset.visualCards = "soft";
  }

  // [jean] Fontes e tamanho do texto escolhidos no Personalizar (feat/login-fontes, 2026-11-14).
  // Mesma chave e mesmos valores do initFontesPersonalizar (main.js); valor desconhecido fica no padrão SPO.
  const fontOptions = {
    spoFontUi: ["inter", ["classic", "inter", "atkinson", "source"], "ui"],
    spoFontData: ["intel-mono", ["classic", "inter", "plex-sans", "intel-mono"], "data"],
    spoTextSize: ["default", ["default", "large", "larger"], "size"],
    spoTableText: ["legible", ["compact", "medium", "legible"], "table"],
    spoLineHeight: ["default", ["default", "ample", "larger"], "line"],
  };
  let fontPrefs = {};
  try {
    fontPrefs = JSON.parse(localStorage.getItem("spo-font-preferences") || "{}") || {};
  } catch (_) {
    fontPrefs = {};
  }
  Object.keys(fontOptions).forEach((attr) => {
    const [fallback, allowed, key] = fontOptions[attr];
    body.dataset[attr] = allowed.includes(fontPrefs[key]) ? fontPrefs[key] : fallback;
  });
})();
