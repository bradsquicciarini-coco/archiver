FROM ghcr.io/astral-sh/uv:python3.12-bookworm AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    UV_PYTHON=python3.12

WORKDIR /app

COPY pyproject.toml uv.lock README.md .python-version /app/
COPY src /app/src

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libxml2-dev \
        libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

RUN uv sync --frozen --no-dev

FROM ghcr.io/astral-sh/uv:python3.12-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PATH=/app/.venv/bin:$PATH \
    UV_PYTHON=python3.12 \
    UV_NO_SYNC=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        libxml2 \
        libxslt1.1 \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://github.com/foxglove/mcap/releases/download/releases%2Fmcap-cli%2Fv0.0.58/mcap-linux-arm64 -O mcap && \
    chmod +x mcap && \
    mv mcap /usr/local/bin/mcap

COPY --from=builder /app /app

ENTRYPOINT ["uv", "run", "--no-sync", "--no-dev", "python", "-m", "data_archiver.sqs_worker"]
