FROM golang:1.26.1-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/pw5-cdc-consumer .

FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /out/pw5-cdc-consumer /app/pw5-cdc-consumer

CMD ["/app/pw5-cdc-consumer"]
