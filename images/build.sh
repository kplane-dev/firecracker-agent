#!/usr/bin/env bash
# Build rootfs ext4 images for Firecracker VMs.
# Usage: ./images/build.sh [output-dir]
#
# Produces: <output-dir>/python.ext4, <output-dir>/node.ext4
# Each is a read-only ext4 block device image. No initrd unpack at boot — demand-paged.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="${1:-${SCRIPT_DIR}/../out/images}"
mkdir -p "$OUT_DIR"

PLATFORM="${PLATFORM:-linux/amd64}"

echo "=== Building base rootfs (${PLATFORM}) ==="
docker build --platform "$PLATFORM" -t rootfs-base "$SCRIPT_DIR/base/"

build_ext4() {
    local name=$1
    echo "=== Building $name rootfs (${PLATFORM}) ==="

    docker build --platform "$PLATFORM" -t "rootfs-${name}" "$SCRIPT_DIR/${name}/"

    local cid
    cid=$(docker create --platform "$PLATFORM" "rootfs-${name}" /bin/true)

    # Export Docker filesystem and create ext4 image inside a container
    docker export "$cid" | docker run --rm --platform "$PLATFORM" -i alpine:3.22 sh -c '
        apk add --no-cache e2fsprogs >/dev/null 2>&1
        mkdir /rootfs && cd /rootfs
        tar xf -
        SIZE=$(du -sm /rootfs | cut -f1)
        SIZE=$((SIZE + 32))
        dd if=/dev/zero of=/rootfs.ext4 bs=1M count=$SIZE 2>/dev/null
        mkfs.ext4 -d /rootfs -L rootfs /rootfs.ext4 >/dev/null 2>&1
        cat /rootfs.ext4
    ' > "${OUT_DIR}/${name}.ext4"

    docker rm "$cid" > /dev/null

    local size
    size=$(du -h "${OUT_DIR}/${name}.ext4" | cut -f1)
    echo "  -> ${OUT_DIR}/${name}.ext4 ($size)"
}

build_ext4 python
build_ext4 node

echo ""
echo "=== Done ==="
ls -lh "$OUT_DIR/"
