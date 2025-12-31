FROM ghcr.io/astral-sh/uv:python3.12-alpine AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    CC=clang \
    CXX=clang++

WORKDIR /app

COPY pyproject.toml uv.lock README.md /app/

RUN apk add --no-cache \
        libxml2-dev \
        libxslt-dev \
        clang \
        lld \
        compiler-rt \
        gcc \
        musl-dev

RUN uv sync --frozen --no-dev --no-install-project

COPY src /app/src

FROM ghcr.io/astral-sh/uv:python3.12-alpine AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    CC=clang \
    CXX=clang++

WORKDIR /app

RUN apk add --no-cache \
        libxml2 \
        libxslt

RUN wget https://github.com/foxglove/mcap/releases/download/releases%2Fmcap-cli%2Fv0.0.58/mcap-linux-arm64 -O mcap && \
    chmod +x mcap && \
    mv mcap /usr/local/bin/mcap

COPY --from=builder /app /app

ENTRYPOINT ["uv", "run", "python", "-m", "data_archiver.sqs_worker"]
