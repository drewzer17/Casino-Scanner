const BASE = import.meta.env.VITE_API_BASE || "";

async function get(path) {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  return res.json();
}

export const api = {
  scanLatest: () => get("/api/scan/latest"),
  ticker: (symbol) => get(`/api/ticker/${encodeURIComponent(symbol)}`),
  movers: () => get("/api/movers"),
};
