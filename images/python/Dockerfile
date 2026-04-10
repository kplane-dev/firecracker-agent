FROM rootfs-base

RUN apk add --no-cache python3 && \
    python3 -c "import sys; print(f'Python {sys.version}')" && \
    rm -rf /var/cache/apk/*
