FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /firecracker-agent ./cmd/agent && \
    CGO_ENABLED=0 go build -o /nsexec ./cmd/nsexec && \
    CGO_ENABLED=0 go build -o /benchmark ./cmd/benchmark && \
    CGO_ENABLED=0 go build -o /serial-test ./cmd/serial-test

FROM alpine:3.22 AS c-builder
RUN apk add --no-cache gcc musl-dev linux-headers
COPY cmd/uffd-handler/uffd-handler.c /tmp/
RUN gcc -static -O2 -pthread -o /uffd-handler /tmp/uffd-handler.c

FROM alpine:3.22
RUN apk add --no-cache ca-certificates btrfs-progs coreutils
COPY --from=builder /firecracker-agent /usr/local/bin/firecracker-agent
COPY --from=builder /nsexec /usr/local/bin/nsexec
COPY --from=builder /benchmark /usr/local/bin/benchmark
COPY --from=builder /serial-test /usr/local/bin/serial-test
COPY --from=c-builder /uffd-handler /usr/local/bin/uffd-handler

# Firecracker binary, kernel, and rootfs images are pre-built via `make all`
# and expected in out/ at docker build time.
COPY out/bin/firecracker /opt/firecracker/bin/firecracker
COPY out/bin/vmlinux /opt/firecracker/vmlinux
COPY out/images/ /opt/firecracker/images/

ENTRYPOINT ["firecracker-agent"]
