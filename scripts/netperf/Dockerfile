# The container side of scripts/netperf.sh: curl, and the netperf tool as
# upload and latency client and as the server behind a published port.
FROM alpine:3.24.4
RUN apk add --no-cache curl iproute2-ss
COPY netperf-linux /usr/local/bin/netperf
