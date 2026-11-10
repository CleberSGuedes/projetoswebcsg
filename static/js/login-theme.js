(function () {
  const palettes = {
    blue: { light: ["#1c71d8", "#3584e4", "53, 132, 228", "153, 193, 241", "#1a5fb4"], dark: ["#78aeed", "#99c1f1", "120, 174, 237", "53, 132, 228", "#3584e4"] },
    teal: { light: ["#15828e", "#2190a4", "33, 144, 164", "147, 221, 223", "#0e6f7d"], dark: ["#6ed4de", "#93dddf", "110, 212, 222", "33, 144, 164", "#2190a4"] },
    green: { light: ["#009879", "#25a98e", "0, 152, 121", "150, 221, 203", "#007f68"], dark: ["#42dfc0", "#6ee7cf", "66, 223, 192", "0, 152, 121", "#25a98e"] },
    yellow: { light: ["#e5a50a", "#f6d32d", "229, 165, 10", "249, 240, 107", "#c88800"], dark: ["#f8e45c", "#f9f06b", "248, 228, 92", "229, 165, 10", "#e5a50a"] },
    orange: { light: ["#e66100", "#ff7800", "230, 97, 0", "255, 190, 111", "#c64600"], dark: ["#ffa348", "#ffbe6f", "255, 163, 72", "230, 97, 0", "#e66100"] },
    red: { light: ["#e01b24", "#ed333b", "224, 27, 36", "255, 123, 123", "#c01c28"], dark: ["#ff7b7b", "#f66151", "255, 123, 123", "224, 27, 36", "#e01b24"] },
    purple: { light: ["#813d9c", "#9141ac", "129, 61, 156", "220, 138, 221", "#613583"], dark: ["#c061cb", "#dc8add", "192, 97, 203", "129, 61, 156", "#813d9c"] },
  };

  const body = document.body;
  const systemDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  const themePreference = localStorage.getItem("app-theme") || "auto";
  const dark = themePreference === "dark" || (themePreference === "auto" && systemDark);
  const storedAccent = localStorage.getItem("app-accent") || "blue";
  const accent = storedAccent === "spo" ? "green" : storedAccent;
  const palette = palettes[accent] || palettes.blue;
  const values = dark ? palette.dark : palette.light;

  body.classList.toggle("theme-dark", dark);
  body.dataset.themePreference = themePreference;
  body.dataset.accentPreference = palettes[accent] ? accent : "blue";
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
})();
