FROM golang:1.25-alpine AS builder

ARG APP
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN test -n "${APP}" && CGO_ENABLED=0 go build -trimpath -o /out/app "./cmd/${APP}"

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /out/app /usr/local/bin/app

ENTRYPOINT ["/usr/local/bin/app"]
