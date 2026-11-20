#!/usr/bin/env bash
# modes.sh <mode>...: for each permission mode, boot katamaran, run fsprobe, shut down.
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd)
LOGS=${LOGS:-/tmp}
K=/Users/nkl/dev/personal/katamaran
for mode in "$@"; do
  log=$LOGS/katamaran-run-$mode.log
  "$K/target/release/katamaran" run --cpus 14 --memory-mib 16384 --permissions "$mode" >"$log" 2>&1 &
  pid=$!
  for _ in $(seq 1 120); do grep -q 'docker ready' "$log" && break; sleep 0.5; done
  if ! grep -q 'docker ready' "$log"; then echo "$mode: did not start"; cat "$log"; kill -INT $pid; wait $pid; continue; fi
  "$S/fsprobe.sh" katamaran "$mode"
  kill -INT $pid; wait $pid
done
