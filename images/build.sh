#!/usr/bin/env bash
# Build rootfs initrd images for Firecracker VMs.
# Usage: ./images/build.sh [output-dir]
#
# Produces: <output-dir>/python.initrd, <output-dir>/node.initrd
# Each is a gzip-compressed cpio archive suitable for Firecracker's initrd_path.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="${1:-${SCRIPT_DIR}/../out/images}"
mkdir -p "$OUT_DIR"

PLATFORM="${PLATFORM:-linux/amd64}"

echo "=== Building base rootfs (${PLATFORM}) ==="
docker build --platform "$PLATFORM" -t rootfs-base "$SCRIPT_DIR/base/"

build_initrd() {
    local name=$1
    echo "=== Building $name rootfs (${PLATFORM}) ==="

    docker build --platform "$PLATFORM" -t "rootfs-${name}" "$SCRIPT_DIR/${name}/"

    local cid
    cid=$(docker create --platform "$PLATFORM" "rootfs-${name}" /bin/true)

    # Convert Docker filesystem to cpio initrd inside a container
    # (preserves permissions and device nodes without requiring host root)
    docker export "$cid" | docker run --rm --platform "$PLATFORM" -i alpine:3.22 sh -c '
        apk add --no-cache cpio >/dev/null 2>&1
        mkdir /rootfs && cd /rootfs
        tar xf -
        find . | cpio -o -H newc 2>/dev/null
    ' | gzip > "${OUT_DIR}/${name}.initrd"

    docker rm "$cid" > /dev/null

    local size
    size=$(du -h "${OUT_DIR}/${name}.initrd" | cut -f1)
    echo "  -> ${OUT_DIR}/${name}.initrd ($size)"
}

build_initrd python
build_initrd node

echo ""
echo "=== Done ==="
ls -lh "$OUT_DIR/"
