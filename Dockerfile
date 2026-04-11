FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /firecracker-agent ./cmd/agent && \
    CGO_ENABLED=0 go build -o /nsexec ./cmd/nsexec

FROM alpine:3.22
RUN apk add --no-cache ca-certificates btrfs-progs coreutils
COPY --from=builder /firecracker-agent /usr/local/bin/firecracker-agent
COPY --from=builder /nsexec /usr/local/bin/nsexec

# Firecracker binary, kernel, and rootfs images are pre-built via `make all`
# and expected in out/ at docker build time.
COPY out/bin/firecracker /opt/firecracker/bin/firecracker
COPY out/bin/vmlinux /opt/firecracker/vmlinux
COPY out/images/ /opt/firecracker/images/

ENTRYPOINT ["firecracker-agent"]
