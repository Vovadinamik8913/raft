# =============================================================================
# Многоуровневая сборка Go-приложения raft-node
# =============================================================================
ARG GOLANG_VERSION=1.25-alpine3.21
ARG ALPINE_VERSION=3.21

# -----------------------------------------------------------------------------
# Этап 1: Загрузка зависимостей
# -----------------------------------------------------------------------------
FROM golang:${GOLANG_VERSION} AS deps

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

# -----------------------------------------------------------------------------
# Этап 2: Сборка приложения
# -----------------------------------------------------------------------------
FROM deps AS builder

RUN apk add --no-cache make git

COPY . .

ENV CGO_ENABLED=0
ARG ARTIFACT_VERSION=dev
ARG GOOS=linux
ARG GOARCH=amd64

RUN make build

# -----------------------------------------------------------------------------
# Этап 3: Runtime — минимальный образ
# -----------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION} AS runtime

LABEL org.opencontainers.image.title="raft-node"
LABEL org.opencontainers.image.description="Учебная реализация Raft"
LABEL org.opencontainers.image.source="https://github.com/Vovadinamik8913/raft"


RUN apk add --no-cache \
        bash \
        ca-certificates \
        curl \
        tzdata \
        libc6-compat \
    && update-ca-certificates \
    && rm -rf /var/cache/apk/*

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN addgroup -g 1001 -S appgroup \
    && adduser  -u 1001 -S appuser -G appgroup -h /app

WORKDIR /app

COPY --from=builder /app/bin/raft-node /app/raft-node
RUN chown -R appuser:appgroup /app

ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

USER appuser

EXPOSE 8000

ENTRYPOINT ["/app/raft-node"]
CMD ["--id=node1", "--port=8000", "--peers="]