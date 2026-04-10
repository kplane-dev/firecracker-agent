FROM alpine:3.22

# Minimal base: busybox provides sh, mount, ip, hostname, etc.
RUN apk add --no-cache busybox-extras

COPY init /init
COPY guest-agent /usr/local/bin/guest-agent
RUN chmod +x /init /usr/local/bin/guest-agent
