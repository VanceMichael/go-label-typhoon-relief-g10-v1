FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/typhoon-relief ./cmd/server

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 app \
    && mkdir -p /data /app/migrations \
    && chown -R app:app /data /app
WORKDIR /app
COPY --from=build /out/typhoon-relief /app/typhoon-relief
COPY migrations /app/migrations
USER app
ENV HTTP_ADDR=:8080 DATABASE_PATH=/data/typhoon-relief.db MIGRATIONS_PATH=/app/migrations
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=2s --start-period=5s CMD ["/bin/sh", "-c", "wget -qO- http://127.0.0.1:8080/healthz >/dev/null"]
ENTRYPOINT ["/app/typhoon-relief"]
