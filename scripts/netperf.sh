#!/usr/bin/env bash
# netperf.sh <docker context | unix:///path/docker.sock> [label]
#
# Network throughput and latency between the Mac and containers, run the same
# way against katamaran and against another engine. One result per line:
#   <label> <test> <run> <value> <unit>
#
# Tests (TESTS, default all): down up inet pull lat pub
#   down  container downloads from the Mac (host.docker.internal -> 127.0.0.1)
#   up    container uploads to the Mac
#   inet  container downloads from the internet (best effort, link-bound)
#   pull  docker pull of PULL_IMAGE (skipped if the image was already there)
#   lat   small HTTP requests from a container to the Mac (keep-alive and not), p50/p99 in us
#   pub   the Mac downloads from a container through a published port (-p)
# Env: RUNS (3), SIZE (bytes, 1 GiB), PULL_IMAGE (python:3.13-bookworm),
#      INET_URL, LAT_N (5000).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
TARGET=${1:?usage: netperf.sh <context|unix://socket> [label]}
LABEL=${2:-$TARGET}
TESTS=${TESTS:-down up inet pull lat pub}
RUNS=${RUNS:-3}
SIZE=${SIZE:-1073741824}
PULL_IMAGE=${PULL_IMAGE:-python:3.13-bookworm}
INET_URL=${INET_URL:-https://fsn1-speed.hetzner.com/1GB.bin}
LAT_N=${LAT_N:-5000}
IMAGE=
OUT=$ROOT/target/netperf

if [[ $TARGET == unix://* ]]; then
  docker() { command docker -H "$TARGET" "$@"; }
else
  docker() { command docker --context "$TARGET" "$@"; }
fi

build_tool() {
  mkdir -p "$OUT"
  local env=(GOCACHE="$ROOT/target/go/cache" GOMODCACHE="$ROOT/target/go/mod" GOTOOLCHAIN=local CGO_ENABLED=0)
  (cd "$ROOT/scripts/netperf" &&
    env "${env[@]}" go build -trimpath -o "$OUT/netperf" . &&
    env "${env[@]}" GOOS=linux GOARCH=arm64 go build -trimpath -o "$OUT/netperf-linux" .)
  cp "$ROOT/scripts/netperf/Dockerfile" "$OUT/Dockerfile"
  # The tag carries the tool's hash, so a changed tool rebuilds the image.
  IMAGE=katamaran-netperf:$(shasum -a 256 "$OUT/netperf-linux" "$OUT/Dockerfile" | shasum -a 256 | cut -c1-12)
  if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    docker build -q -t "$IMAGE" "$OUT" >/dev/null
  fi
}

SERVER_PID=
cleanup() {
  [[ -n $SERVER_PID ]] && kill "$SERVER_PID" 2>/dev/null || true
  docker rm -f netperf-pub >/dev/null 2>&1 || true
}
trap cleanup EXIT

start_server() {
  local pf
  pf=$(mktemp "$OUT/port.XXXXXX")
  "$OUT/netperf" -addr 127.0.0.1:0 -port-file "$pf" &
  SERVER_PID=$!
  for _ in $(seq 100); do [[ -s $pf ]] && break; sleep 0.05; done
  PORT=$(cat "$pf")
  rm -f "$pf"
}

emit() { printf '%s %s %s %s %s\n' "$LABEL" "$1" "$2" "$3" "$4"; }
mib() { awk -v b="$1" 'BEGIN { printf "%.0f", b / 1048576 }'; }
run_c() { docker run --rm "$IMAGE" "$@"; }

t_down() {
  local url="http://host.docker.internal:$PORT/bytes?n=$SIZE"
  for r in $(seq "$RUNS"); do
    emit down "$r" "$(mib "$(run_c curl -sS -o /dev/null -w '%{speed_download}' "$url")")" MiB/s
  done
}

t_up() {
  local url="http://host.docker.internal:$PORT/sink"
  for r in $(seq "$RUNS"); do
    emit up "$r" "$(mib "$(run_c netperf -put "$url" -n "$SIZE")")" MiB/s
  done
}

t_inet() {
  for r in $(seq "$RUNS"); do
    local bps
    bps=$(run_c curl -sS -o /dev/null --max-time 15 -w '%{speed_download}' "$INET_URL" || true)
    emit inet "$r" "$(mib "${bps:-0}")" MiB/s
  done
}

t_pull() {
  if docker image inspect "$PULL_IMAGE" >/dev/null 2>&1; then
    echo "# pull: $PULL_IMAGE already present on $TARGET; not touching it" >&2
    return
  fi
  for r in $(seq "$RUNS"); do
    local t0 t1
    t0=$(python3 -c 'import time; print(time.time())')
    docker pull -q "$PULL_IMAGE" >/dev/null
    t1=$(python3 -c 'import time; print(time.time())')
    emit pull "$r" "$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.2f", b - a }')" s
    docker rmi "$PULL_IMAGE" >/dev/null
  done
}

t_lat() {
  local url="http://host.docker.internal:$PORT/small"
  for r in $(seq "$RUNS"); do
    local mean p50 p99 max
    read -r mean p50 p99 max < <(run_c netperf -lat "$url" -n "$LAT_N" || echo err err err err)
    emit lat-keepalive-p50 "$r" "$p50" us
    emit lat-keepalive-p99 "$r" "$p99" us
    read -r mean p50 p99 max < <(run_c netperf -lat "$url" -n "$((LAT_N / 5))" -newconn || echo err err err err)
    emit lat-newconn-p50 "$r" "$p50" us
    emit lat-newconn-p99 "$r" "$p99" us
  done
}

t_pub() {
  local port
  port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
  docker rm -f netperf-pub >/dev/null 2>&1 || true
  docker run -d --name netperf-pub -p "$port:8080" "$IMAGE" netperf -addr 0.0.0.0:8080 >/dev/null
  local ok=
  for _ in $(seq 100); do
    curl -sf "http://127.0.0.1:$port/small" >/dev/null 2>&1 && { ok=1; break; }
    sleep 0.1
  done
  [[ -n $ok ]] || { echo "# pub: 127.0.0.1:$port never answered" >&2; return 1; }
  for r in $(seq "$RUNS"); do
    emit pub "$r" "$(mib "$(curl -sS -o /dev/null -w '%{speed_download}' "http://127.0.0.1:$port/bytes?n=$SIZE")")" MiB/s
  done
  docker rm -f netperf-pub >/dev/null
}

build_tool
start_server
for t in $TESTS; do "t_$t"; done
