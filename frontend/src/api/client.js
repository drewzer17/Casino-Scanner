const BASE = import.meta.env.VITE_API_BASE || "";

async function get(path) {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  return res.json();
}

async function post(path) {
  const res = await fetch(`${BASE}${path}`, { method: "POST" });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  return res.json();
}

async function postJson(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return res.json();
}

async function putJson(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return res.json();
}

async function del(path) {
  const res = await fetch(`${BASE}${path}`, { method: "DELETE" });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return res.json();
}

export const api = {
  scanLatest: () => get("/api/scan/latest"),
  ticker: (symbol) => get(`/api/ticker/${encodeURIComponent(symbol)}`),
  movers: (days = 7, limit = 5) => get(`/api/movers?days=${days}&limit=${limit}`),
  triggerScan: () => get("/api/scan/run"),
  triggerScanExtensive: () => get("/api/scan/extensive"),
  scanStatus: () => get("/api/scan/status"),
  wheel: (ticker, support_1 = null, resistance_1 = null) => {
    const params = new URLSearchParams();
    if (support_1 != null) params.set("support_1", support_1);
    if (resistance_1 != null) params.set("resistance_1", resistance_1);
    const qs = params.toString();
    return get(`/api/ticker/${encodeURIComponent(ticker)}/wheel${qs ? "?" + qs : ""}`);
  },
  chains: (ticker) => get(`/api/ticker/${encodeURIComponent(ticker)}/chains`),
  sitrep: (ticker) => get(`/api/sitrep/${encodeURIComponent(ticker)}`),
  reloadUniverse: () => post("/api/universe/reload"),
  stopScan: () => post("/api/scan/stop"),
  resetScan: () => post("/api/scan/reset"),
  runEarnings: () => post("/api/refresh-earnings"),
  earningsStatus: () => get("/api/earnings-status"),

  // My Positions — CRUD
  getPositions: () => get("/api/positions"),
  addPosition: (ticker, opts = {}) => postJson("/api/positions", { ticker, ...opts }),
  quickAddPositions: (tickers) => postJson("/api/positions/quick-add", { tickers }),
  deletePosition: (id) => del(`/api/positions/${id}`),
  updatePosition: (id, fields) => putJson(`/api/positions/${id}`, fields),

  // My Positions — Quick Scan
  scanPositions: () => post("/api/scan/positions"),
  scanPositionsLatest: () => get("/api/scan/positions/latest"),

  // LEAPS — on-demand scan
  scanLeaps: () => post("/api/scan/leaps"),
  scanLeapsLatest: () => get("/api/scan/leaps/latest"),
};
