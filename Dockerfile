# Builds the etcd-replicate binary.
# Run: docker build -t etcd-replicate .
# The builder runs natively on the build host and cross-compiles for the
# target platform, so multi-arch builds don't run the Go toolchain under QEMU.
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
      -trimpath \
      -ldflags="-s -w -X main.version=${VERSION}" \
      -o /etcd-replicate \
      ./cmd/etcd-replicate

# The binary is statically linked and needs no shell. The image carries CA
# certificates for TLS to etcd and runs as the nonroot user (UID 65532).
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /etcd-replicate /etcd-replicate

EXPOSE 9091

ENTRYPOINT ["/etcd-replicate"]
