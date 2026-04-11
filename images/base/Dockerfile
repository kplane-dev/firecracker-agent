FROM alpine:3.22

RUN apk add --no-cache busybox-extras dropbear iproute2 && \
    mkdir -p /etc/dropbear /root/.ssh

COPY init /init
COPY guest-agent /usr/local/bin/guest-agent
RUN chmod +x /init /usr/local/bin/guest-agent
