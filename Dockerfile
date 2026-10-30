# ─── Stage 1: frontend bundle ────────────────────────────────────────────────
# Builds the React portal. Vite is configured to emit into
# internal/ui/dist so the Go embed directive can pick it up.
FROM --platform=$BUILDPLATFORM node:22-alpine AS ui

RUN npm install -g pnpm@9.15.9

WORKDIR /src/ui

COPY ui/package.json ui/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

COPY ui/ ./
# `pnpm build` runs `tsc -b && vite build`; outDir is ../internal/ui/dist
RUN mkdir -p /src/internal/ui && pnpm run build

# ─── Stage 2: build ──────────────────────────────────────────────────────────
# --platform=$BUILDPLATFORM ensures the correct Go binary is used for the target architecture in multi-platform builds.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

# Build args injected by Docker Buildx for multi-platform builds
ARG TARGETOS
ARG TARGETARCH
ARG CLOUD=aws
# Version stamped into the binary (read by `jaiscloud-aws version` and
# `jaiscloud-gcp version`; the azure main declares a const and ignores -X).
# Passed by CI (docker-publish.yml) and `make docker-%`; the default keeps a
# bare `docker build` reporting "dev" instead of an empty string.
ARG VERSION=dev

# ca-certificates needed for TLS to external registries during `go mod download`
RUN apk add --no-cache ca-certificates git

WORKDIR /src

# Download dependencies first (cached layer)
COPY go.mod go.sum ./
# go.mod replaces the nested SDK module with ./sdk, so its go.mod must exist
# before `go mod download` can resolve the module graph.
COPY sdk/go.mod ./sdk/go.mod
RUN go mod download

# Copy source and build a fully static binary with the UI embedded
COPY . .
COPY --from=ui /src/internal/ui/dist /src/internal/ui/dist
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -tags ui -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /jaiscloud ./cmd/jaiscloud-${CLOUD}/

# ─── Stage 3: runtime ────────────────────────────────────────────────────────
FROM gcr.io/distroless/static:nonroot

# TLS roots so HTTPS calls (e.g. Prometheus scrape) work
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# The binary is the only thing in the image
COPY --from=builder /jaiscloud /jaiscloud

# Listen port of the cloud in this image: 4566 (aws/azure) or 8080 (gcp).
# Callers pass PORT explicitly (make docker-%, docker-publish.yml).
ARG PORT=4566
EXPOSE ${PORT}

# Prometheus metrics port (same port, /metrics path — no second port needed)

ENTRYPOINT ["/jaiscloud"]
CMD ["start"]
