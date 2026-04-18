FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY frontend/package*.json frontend/
RUN cd frontend && npm ci
COPY frontend/src frontend/src
COPY frontend/index.html frontend/index.html
COPY frontend/vite.config.js frontend/vite.config.js
ARG CACHEBUST=1
RUN cd frontend && npm run build
COPY backend/requirements.txt backend/
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install -r backend/requirements.txt
COPY backend/ backend/
EXPOSE 8000
CMD ["sh", "-c", "cd /app/backend && /opt/venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
