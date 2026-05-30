FROM golang:1.26.1-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/practical-work-3-app .

FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /out/practical-work-3-app /app/practical-work-3-app

CMD ["/app/practical-work-3-app"]
