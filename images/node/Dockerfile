FROM rootfs-base

RUN apk add --no-cache nodejs && \
    node -e "console.log('Node ' + process.version)" && \
    rm -rf /var/cache/apk/*

COPY eval-server.js /usr/local/lib/eval-server.js
