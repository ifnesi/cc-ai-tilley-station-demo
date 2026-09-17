# Tilley Station demo — one image runs the emulator, the backend, or the mock
# feed (chosen by the command). Users only need Docker; no Python/venv.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install deps first for layer caching. confluent-kafka's wheels bundle
# librdkafka, so no apt packages are needed.
COPY emulator/requirements.txt emulator/requirements.txt
COPY backend/requirements.txt backend/requirements.txt
RUN pip install -r emulator/requirements.txt -r backend/requirements.txt

# Application code. All config comes from the runtime environment (docker-compose
# env_file: .env) — nothing is baked in, and .env is never copied.
COPY emulator/ emulator/
COPY backend/ backend/
COPY frontend/ frontend/
COPY tools/ tools/

EXPOSE 8080

# Default: the backend (serves the dashboard). Compose overrides the command
# for the emulator and mock-feed services.
CMD ["python", "-m", "backend.run"]
