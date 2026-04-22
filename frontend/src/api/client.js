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
  reloadUniverse: () => post("/api/universe/reload"),
  stopScan: () => post("/api/scan/stop"),
};
