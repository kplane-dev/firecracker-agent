FROM rootfs-base

RUN apk add --no-cache nodejs && \
    node -e "console.log('Node ' + process.version)" && \
    rm -rf /var/cache/apk/*
