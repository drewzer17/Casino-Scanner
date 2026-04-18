FROM python:3.12-slim

# Install curl (for Node.js setup script) — everything else comes with python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Frontend: install deps first (layer-cached until package.json changes) ──
COPY frontend/package*.json frontend/
RUN cd frontend && npm ci

# Copy all frontend source — any change here invalidates the build layer
COPY frontend/src frontend/src
COPY frontend/index.html frontend/index.html
COPY frontend/vite.config.js frontend/vite.config.js

# ARG busts the build cache when passed at deploy time (e.g. --build-arg CACHEBUST=$(date +%s))
ARG CACHEBUST=1
RUN cd frontend && npm run build
# Produces /app/frontend/dist — served by FastAPI at runtime

# ── Python: install deps before copying source (same caching trick) ──────────
COPY backend/requirements.txt backend/
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install -r backend/requirements.txt

# ── Backend source ────────────────────────────────────────────────────────────
COPY backend/ backend/

EXPOSE 8000

# Railway injects $PORT; default to 8000 for local docker run
CMD ["sh", "-c", "cd /app/backend && /opt/venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
