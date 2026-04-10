.PHONY: all images agent docker download-firecracker download-kernel download clean

FC_VERSION ?= v1.12.0
FC_ARCH    ?= x86_64
IMAGE_TAG  ?= ghcr.io/kplane-dev/firecracker-agent:latest

all: download images agent

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
	curl -fSL "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/$(FC_VERSION)/$(FC_ARCH)/vmlinux-5.10.233" \
		-o out/bin/vmlinux
	@echo "-> out/bin/vmlinux"

# --- Rootfs images ---

images:
	./images/build.sh out/images

# --- Go binary ---

agent:
	CGO_ENABLED=0 go build -o bin/firecracker-agent ./cmd/agent

# --- Docker ---

docker: all
	docker build -t $(IMAGE_TAG) .

# --- Clean ---

clean:
	rm -rf out/ bin/
