FROM golang:1.26.1-bookworm AS builder

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ libc6-dev librdkafka-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -o /out/practical-work-2-app .

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends librdkafka1 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/practical-work-2-app /app/practical-work-2-app

CMD ["/app/practical-work-2-app"]
