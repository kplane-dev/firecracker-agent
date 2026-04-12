FROM alpine:3.22 AS builder
RUN apk add --no-cache gcc musl-dev
COPY serial-agent.c /tmp/serial-agent.c
RUN gcc -static -O2 -o /serial-agent /tmp/serial-agent.c

FROM alpine:3.22
RUN apk add --no-cache busybox-extras dropbear iproute2 && \
    mkdir -p /etc/dropbear /root/.ssh

COPY init /init
COPY guest-agent /usr/local/bin/guest-agent
COPY --from=builder /serial-agent /usr/local/bin/serial-agent
RUN chmod +x /init /usr/local/bin/guest-agent /usr/local/bin/serial-agent
