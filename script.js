// ==========================================
// DOM Elements Configuration
// ==========================================
const DOM = {
  nav: {
    putaway: document.getElementById('nav-putaway'),
    report: document.getElementById('nav-report'),
    instructions: document.getElementById('nav-instructions'),
  },
  subNav: {
    lookup: document.getElementById('nav-search'),
  },
  sections: {
    lookupForm: document.getElementById('lookup-form'),
    lookupResults: document.getElementById('results'),
    lookupLoading: document.getElementById('search-loading'),
    lookupActions: document.getElementById('lookup-actions'),
    report: document.getElementById('report-section'),
    instructions: document.getElementById('instructions-section'),
  },
  inputs: {
    lookup: document.getElementById("lookup-input"),
    qty: document.getElementById("qty-input"),
    mode: document.getElementById("mode-select"),
    dock: document.getElementById("dock-select"),
  },
  fields: {
    loc1: document.getElementById("loc-1"),
    loc2: document.getElementById("loc-2"),
    loc3: document.getElementById("loc-3"),
  },
  buttons: {
    print: document.getElementById('btn-print'),
  }
};

const REQUEST_TIMEOUT_MS = 25_000;

let activeLookupController = null;
let activeLookupSeq = 0;

function cancelActiveLookup() {
  activeLookupSeq += 1;
  if (activeLookupController) {
    try { activeLookupController.abort(); } catch (_) { }
  }
  activeLookupController = null;
}

// ==========================================
// State Management
// ==========================================
let state = {
  pendingLookupKey: null,
  pendingQuantity: null,
};

// ==========================================
// 1. INPUT HANDLING & MODES
// ==========================================

function enterQtyMode(key) {
  state.pendingLookupKey = key;
  state.pendingQuantity = null;

  DOM.sections.lookupForm.classList.add("qty-mode");

  DOM.inputs.lookup.disabled = true;
  DOM.inputs.qty.disabled = false;
  DOM.inputs.qty.setAttribute("aria-hidden", "false");
  DOM.inputs.qty.value = "";
  DOM.inputs.qty.focus();
}

function exitQtyMode() {
  DOM.sections.lookupForm.classList.remove("qty-mode");

  DOM.inputs.lookup.disabled = false;
  DOM.inputs.qty.disabled = true;
  DOM.inputs.qty.setAttribute("aria-hidden", "true");
  DOM.inputs.qty.value = "";

  state.pendingLookupKey = null;
  state.pendingQuantity = null;
}

[DOM.inputs.lookup, DOM.inputs.qty].forEach(el => {
  if (!el) return;

  el.addEventListener("input", () => {
    if (/[^\d]/.test(el.value)) {
      el.value = el.value.replace(/[^\d]/g, "");
    }
  });

  el.addEventListener("paste", (e) => {
    e.preventDefault();
    const cleanData = (e.clipboardData || window.clipboardData)
      .getData("text")
      .replace(/[^\d]/g, "");
    if (cleanData) document.execCommand("insertText", false, cleanData);
  });
});

// ==========================================
// 2. NAVIGATION LOGIC
// ==========================================

function activateTab(tabName) {
  exitQtyMode();

  const navMap = {
    'search': DOM.nav.putaway,
    'report': DOM.nav.report,
    'instructions': DOM.nav.instructions
  };

  Object.values(DOM.nav).forEach(el => {
    if (el) el.classList.remove('active');
  });
  if (navMap[tabName]) navMap[tabName].classList.add('active');

  Object.values(DOM.sections).forEach(el => {
    if (el) el.classList.add('hidden');
  });

  if (tabName === 'search') {
    DOM.sections.lookupForm.classList.remove('hidden');
    if (DOM.sections.lookupActions) DOM.sections.lookupActions.classList.add('hidden');
  } else if (tabName === 'report') {
    DOM.sections.report.classList.remove('hidden');
  } else if (tabName === 'instructions') {
    DOM.sections.instructions.classList.remove('hidden');
  }

  DOM.subNav.lookup.classList.toggle('active', tabName === 'search');

  const showPrint = (tabName === 'search' && DOM.sections.lookupResults.classList.contains('is-visible'));
  if (DOM.sections.lookupActions) DOM.sections.lookupActions.classList.toggle('hidden', !showPrint);
}

DOM.nav.putaway.addEventListener('click', (e) => { e.preventDefault(); activateTab('search'); });
DOM.nav.report.addEventListener('click', (e) => { e.preventDefault(); activateTab('report'); });
DOM.nav.instructions.addEventListener('click', (e) => { e.preventDefault(); activateTab('instructions'); });

DOM.subNav.lookup.addEventListener('click', (e) => { e.preventDefault(); activateTab('search'); });

// ==========================================
// 3. SEARCH & API LOGIC
// ==========================================

function startLoading() {
  DOM.sections.lookupLoading.classList.remove('hidden');
  DOM.sections.lookupResults.classList.add('hidden');
  DOM.sections.lookupResults.classList.remove('is-visible');
  DOM.sections.lookupActions.classList.add('hidden');
}

function stopLoading() {
  DOM.sections.lookupLoading.classList.add('hidden');
}

function showResult(locations) {
  stopLoading();

  const safeText = (val) => (val !== null && val !== undefined && val !== "") ? val : "—";

  const [l1, l2, l3] = Array.isArray(locations) ? locations : [];

  DOM.fields.loc1.textContent = safeText(l1);
  DOM.fields.loc2.textContent = safeText(l2);
  DOM.fields.loc3.textContent = safeText(l3);

  const resEl = DOM.sections.lookupResults;
  resEl.classList.remove("hidden");
  void resEl.offsetWidth; 
  resEl.classList.add("is-visible");

  if (DOM.sections.lookupActions) DOM.sections.lookupActions.classList.remove('hidden');
}

let OWNER_ID = null;

function makeOwnerId() {
  try {
    if (crypto?.randomUUID) return crypto.randomUUID();
  } catch (_) { }

  try {
    const buf = new Uint8Array(16);
    (crypto || window.crypto).getRandomValues(buf);
    buf[6] = (buf[6] & 0x0f) | 0x40;
    buf[8] = (buf[8] & 0x3f) | 0x80;
    const hex = Array.from(buf, b => b.toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  } catch (_) { }

  return `anon-${Date.now().toString(16)}-${Math.random().toString(16).slice(2)}`;
}

function getOwnerId() {
  if (OWNER_ID) return OWNER_ID;
  const KEY = "owner_id";

  try {
    if (typeof localStorage !== "undefined" && localStorage) {
      let id = localStorage.getItem(KEY);
      if (!id) {
        id = makeOwnerId();
        try { localStorage.setItem(KEY, id); } catch (_) { }
      }
      if (id) {
        OWNER_ID = id;
        return OWNER_ID;
      }
    }
  } catch (_) { }

  try {
    if (typeof sessionStorage !== "undefined" && sessionStorage) {
      let id = sessionStorage.getItem(KEY);
      if (!id) {
        id = makeOwnerId();
        try { sessionStorage.setItem(KEY, id); } catch (_) { }
      }
      if (id) {
        OWNER_ID = id;
        return OWNER_ID;
      }
    }
  } catch (_) { }

  OWNER_ID = makeOwnerId();
  return OWNER_ID;
}

async function performLookup(key, qty) {
  cancelActiveLookup();

  activeLookupController = new AbortController();
  const mySeq = activeLookupSeq;

  const timer = setTimeout(() => {
    if (mySeq === activeLookupSeq && activeLookupController) {
      activeLookupController.abort();
    }
  }, REQUEST_TIMEOUT_MS);

  const ownerId = getOwnerId();
  const mode = String(DOM.inputs.mode?.value || "OP").toUpperCase();
  const dock = String(DOM.inputs.dock?.value || "");

  const url = `/api/storage?hu_id=${encodeURIComponent(key)}&qty=${encodeURIComponent(qty)}&owner_id=${encodeURIComponent(ownerId)}&mode=${encodeURIComponent(mode)}&dock=${encodeURIComponent(dock)}`;

  try {
    const res = await fetch(url, { signal: activeLookupController.signal });
    if (mySeq !== activeLookupSeq) return;

    if (!res.ok) {
      console.error("API error:", res.status);
      if (mySeq !== activeLookupSeq) return;
      showResult(null); 
      return;
    }

    const data = await res.json();
    if (mySeq !== activeLookupSeq) return;

    if (!Array.isArray(data) || data.length === 0) {
      showResult(null);
      return;
    }

    const row = data[0];

    const allocations = Array.isArray(row.allocations) ? row.allocations : [];
    const locationsFromAlloc = allocations.map(a => {
      const loc = a.location_id || "";
      const units = Number(a.units || 0);
      return units > 0 ? `${loc} — ${units} cartons` : `${loc} — backup`;
    });

    const fallbackLocs = (row.location_suggestions || "")
      .split(",")
      .map(s => s.trim())
      .filter(Boolean);

    const locations = locationsFromAlloc.length ? locationsFromAlloc : fallbackLocs;

    showResult(locations);

  } catch (err) {
    if (mySeq !== activeLookupSeq) return;
    if (err.name !== "AbortError") {
      console.error("Network/JS error:", err);
    }
    showResult(null);
  } finally {
    clearTimeout(timer);
    if (mySeq === activeLookupSeq) {
      stopLoading();
      activeLookupController = null;
    }
  }
}

DOM.sections.lookupForm.addEventListener("submit", async (e) => {
  e.preventDefault();

  if (DOM.sections.lookupForm.classList.contains("qty-mode")) {
    const qtyRaw = String(DOM.inputs.qty.value || "").replace(/[^\d]/g, "");

    if (!qtyRaw) {
      DOM.inputs.qty.focus();
      return;
    }

    const qty = parseInt(qtyRaw, 10);
    if (!Number.isFinite(qty) || qty <= 0) {
      DOM.inputs.qty.value = "";
      DOM.inputs.qty.focus();
      return;
    }

    const key = state.pendingLookupKey;
    if (!key) {
      exitQtyMode();
      return;
    }

    exitQtyMode();
    activateTab("search");
    startLoading();
    await performLookup(key, qty);
    return;
  }

  const key = String(DOM.inputs.lookup.value || "").replace(/[^\d]/g, "");

  if (!key) {
    DOM.inputs.lookup.value = "";
    return;
  }

  DOM.inputs.lookup.value = key;
  activateTab("search");
  enterQtyMode(key);
});

try {
  const modeSaved = localStorage.getItem("mode_select");
  if (modeSaved && DOM.inputs.mode) DOM.inputs.mode.value = modeSaved;

  const dockSaved = localStorage.getItem("dock_select");
  if (dockSaved !== null && DOM.inputs.dock) DOM.inputs.dock.value = dockSaved;

  DOM.inputs.mode?.addEventListener("change", () => localStorage.setItem("mode_select", DOM.inputs.mode.value));
  DOM.inputs.dock?.addEventListener("change", () => localStorage.setItem("dock_select", DOM.inputs.dock.value));
} catch (_) { }

// ==========================================
// 4. PRINTING LOGIC
// ==========================================

function esc(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function parseLocation(code) {
  if (!code || code === '—') return null;
  const raw = String(code).trim();
  const [basePart] = raw.split('—');
  const base = (basePart || '').trim();

  const m = base.match(/^([A-Za-z])(\d{2})(\d{2})(\d{2})(\d{2})$/);

  if (!m) {
    return { goto: raw, zone: '', aisle: '', bay: '', level: '', position: '' };
  }

  return {
    goto: base,
    zone: m[1].toUpperCase(),
    aisle: m[2],
    bay: m[3],
    level: m[4],
    position: m[5]
  };
}

function buildLabelHTML(values) {
  const w = 8, h = 4;
  let html = `<!DOCTYPE html>
  <html>
  <head>
    <meta charset="utf-8"><title>Labels</title>
    <style>
      @page { size: ${w}in ${h}in; margin: 0; }
      body { margin: 0; width: ${w}in; height: ${h}in; font-family: sans-serif; }
      table { width: 100%; height: 100%; border-collapse: collapse; text-align: center; table-layout: fixed; }
      td, th { border: 2px solid black; font-size: 14pt; padding: 5px; word-wrap: break-word;}
      th { background: #eee; height: 30px; }
    </style>
  </head>
  <body>
    <table>
      <tr><th>GO TO</th><th>Zone</th><th>Aisle</th><th>Bay</th><th>Level</th><th>Pos</th></tr>`;

  values.forEach(row => {
    html += `<tr>${row.map(cell => `<td>${esc(cell)}</td>`).join('')}</tr>`;
  });

  html += `</table></body></html>`;
  return html;
}

if (DOM.buttons.print) {
  DOM.buttons.print.addEventListener('click', () => {
    const locs = [
      DOM.fields.loc1.textContent,
      DOM.fields.loc2.textContent,
      DOM.fields.loc3.textContent
    ].filter(s => s && s !== '—');

    if (!locs.length) {
      alert('No locations to print.');
      return;
    }

    const values = locs.map(code => {
      const p = parseLocation(code) || { goto: code, zone: '', aisle: '', bay: '', level: '', position: '' };
      return [p.goto, p.zone, p.aisle, p.bay, p.level, p.position];
    });

    printHTML(buildLabelHTML(values));
  });
}

function printHTML(html) {
  const iframe = document.createElement('iframe');
  Object.assign(iframe.style, {
    position: 'fixed', right: '0', bottom: '0', width: '0', height: '0', border: '0'
  });

  document.body.appendChild(iframe);

  const doc = iframe.contentWindow.document;
  doc.open();
  doc.write(html);
  doc.close();

  setTimeout(() => {
    try {
      iframe.contentWindow.focus();
      iframe.contentWindow.print();
    } finally {
      setTimeout(() => iframe.remove(), 2000);
    }
  }, 0);
}

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    DOM.inputs.lookup.value = "";
    exitQtyMode();
    DOM.sections.lookupResults.classList.add("hidden");
    DOM.sections.lookupResults.classList.remove("is-visible");
    DOM.sections.lookupActions.classList.add("hidden");
    cancelActiveLookup();
    stopLoading();
  }
});
