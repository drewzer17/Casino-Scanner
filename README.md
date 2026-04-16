# Casino Scanner

Daily scanner for ~500 mid/large-cap US stocks (S&P 500 + Nasdaq 100 union).
Scores each name 0–100 based on covered-call / cash-secured-put wheel-strategy
readiness and sorts them into three buckets: **Sell Now**, **Buy Now Sell
Later**, and **Watchlist**.

## Stack

- **Backend**: Python 3.12, FastAPI, SQLAlchemy 2, PostgreSQL (Railway) / SQLite locally
- **Data**: yfinance for equity history + options chains
- **Frontend**: React 18 + Vite
- **Deploy**: Railway (one service per app)

## Scoring

Five factors, 20 points each, total 0–100. See `backend/app/scanner/scoring.py`.

| Factor          | Signal                                         |
| --------------- | ---------------------------------------------- |
| IV Rank         | ATM IV percentile vs 1-year realized-vol range |
| Premium Quality | ATM call mid price / stock price               |
| IV vs HV        | Current ATM IV / 30-day realized vol           |
| Catalyst        | Earnings proximity + IV ramp + unusual volume  |
| Chain Quality   | Open interest + bid/ask spread on ATM option   |

### Buckets

- **sell_now**: `score ≥ 55` AND `iv_rank ≥ 45` AND `premium_pct ≥ 1.5%`
- **buy_sell_later**: `score ≥ 25` AND earnings within 30 days, OR `score ≥ 30` AND low IV rank (<30)
- **watchlist**: everything else

## Layout

```
backend/
  app/
    api/routes.py         # /api/scan/latest, /api/ticker/{t}, /api/movers
    scanner/
      scoring.py          # 5-factor scoring
      buckets.py          # bucket assignment
      engine.py           # yfinance pulls + persistence
    universe.py           # load static ticker list
    config.py             # env var parsing (Railway-safe)
    database.py, models.py, schemas.py, main.py
  data/universe.json      # S&P 500 + Nasdaq 100 union
  requirements.txt, Procfile, runtime.txt
frontend/
  src/
    components/           # Dashboard, BucketTabs, ScoreCard
    api/client.js
    App.jsx, main.jsx, index.css
  index.html, package.json, vite.config.js
railway.json, nixpacks.toml
```

## Local development

### Backend

```bash
cd backend
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env       # edit DATABASE_URL if using Postgres locally
uvicorn app.main:app --reload
```

Run a one-off scan against the configured DB:

```bash
python -m app.scanner.engine
```

### Frontend

```bash
cd frontend
npm install
npm run dev
```

The Vite dev server proxies `/api/*` to `http://localhost:8000` by default.

## Environment variables

| Name            | Purpose                                                   |
| --------------- | --------------------------------------------------------- |
| `DATABASE_URL`  | Postgres URL. `postgres://` is rewritten to `postgresql://`. |
| `ALERT_EMAIL`   | Email address for future alert hooks.                     |
| `ALERT_SMS`     | SMS-via-email gateway address.                            |
| `CORS_ORIGINS`  | Comma-separated origins allowed by the API.               |
| `ENVIRONMENT`   | Free-form environment label.                              |

All values are stripped of a leading `=` and surrounding whitespace
(`value.lstrip('=').strip()`) before use — Railway sometimes injects those.

## Deploy notes

- Pin `httpx==0.27.2` (already in `requirements.txt`). 0.28+ breaks some
  transitive clients on Railway.
- Backend and frontend deploy as separate Railway services pointed at the same repo.
- The daily scanner runs via Railway cron invoking `python -m app.scanner.engine`.
