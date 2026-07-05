# ─────────────────────────────────────────────────────────────────────────────
#  Multi-stage build: galactic-olcrtc Docker image
# ─────────────────────────────────────────────────────────────────────────────
#  Build stage
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary — no CGo dependency for the standard CLI build.
RUN CGO_ENABLED=0 go build \
    -o /usr/local/bin/olcrtc \
    -ldflags="-s -w" \
    ./cmd/olcrtc/

# ─────────────────────────────────────────────────────────────────────────────
#  Runtime stage
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /usr/local/bin/olcrtc /usr/local/bin/olcrtc

ENTRYPOINT ["olcrtc"]
