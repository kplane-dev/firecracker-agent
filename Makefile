.PHONY: all images agent guest-agent docker download-firecracker download-kernel download clean

FC_VERSION ?= v1.15.1
FC_ARCH    ?= x86_64
IMAGE_TAG  ?= ghcr.io/kplane-dev/firecracker-agent:latest

all: download guest-agent images agent

# --- Downloads ---

download: download-firecracker download-kernel

download-firecracker:
	mkdir -p out/bin
	curl -fSL "https://github.com/firecracker-microvm/firecracker/releases/download/$(FC_VERSION)/firecracker-$(FC_VERSION)-$(FC_ARCH).tgz" | \
		tar xz -C /tmp
	mv "/tmp/release-$(FC_VERSION)-$(FC_ARCH)/firecracker-$(FC_VERSION)-$(FC_ARCH)" out/bin/firecracker
	chmod +x out/bin/firecracker
	@echo "-> out/bin/firecracker"

download-kernel:
	mkdir -p out/bin
	curl -fSL "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.15/$(FC_ARCH)/vmlinux-6.1.155" \
		-o out/bin/vmlinux
	@echo "-> out/bin/vmlinux"

# --- Guest agent (runs inside VM) ---

guest-agent:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o images/base/guest-agent ./cmd/guest-agent
	@echo "-> images/base/guest-agent"

# --- Rootfs images ---

images: guest-agent
	cp cmd/serial-agent/serial-agent.c images/base/serial-agent.c
	./images/build.sh out/images
	rm -f images/base/guest-agent images/base/serial-agent.c

# --- Go binary ---

agent:
	CGO_ENABLED=0 go build -o bin/firecracker-agent ./cmd/agent

# --- Docker ---

docker: all
	docker build -t $(IMAGE_TAG) .

# --- Clean ---

clean:
	rm -rf out/ bin/ images/base/guest-agent
