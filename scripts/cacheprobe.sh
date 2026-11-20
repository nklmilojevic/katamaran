#!/usr/bin/env bash
# cacheprobe.sh <label> <docker args...>: how long the guest keeps the /Users share cached.
#   stat_cold    first stat pass over the bench Linux tree (91k files) in a new container
#   stat_cached  the same pass again in that container
#   stat_rerun   a pass in a NEW container 10 s later (a long-lived cache still holds the tree)
#   git_status_1/2  git status in that container, 10 s apart (the first container
#                   runs one untimed git status, so the index holds this guest's stat data)
# Examples:
#   scripts/cacheprobe.sh katamaran -H unix://$HOME/.katamaran-cache/run/docker.sock
#   scripts/cacheprobe.sh other --context <another engine's context>
set -euo pipefail
label=$1; shift
W=${WORK:-$(cd "$(dirname "$0")/.." && pwd)/bench/work/ds.noindex}
IMAGE=${IMAGE:-katbench:f4787c2b1b1e}
N=$(wc -l <"$W/linux.files" | tr -d ' ')
run() { docker "$@" run --rm -v "$W:$W" -w "$W/linux" "$IMAGE" sh -c "$CMD"; }
STAT="r() { \"\$@\" | sed -E 's/.*ns_per_op=([0-9.]+).*/\1/'; }"
CMD="$STAT
echo \"stat_cold_ns    \$(r kb_stat $W/linux.files $N)\"
echo \"stat_cached_ns  \$(r kb_stat $W/linux.files $N)\"
git status >/dev/null 2>&1 || true"
run "$@" | sed "s/^/$label  /"
sleep 10
CMD="$STAT
echo \"stat_rerun_ns   \$(r kb_stat $W/linux.files $N)\"
for i in 1 2; do
  [ \$i = 2 ] && sleep 10
  s=\$(date +%s%N); git status >/dev/null 2>&1; e=\$(date +%s%N)
  echo \"git_status_\${i}_ms  \$(( (e-s)/1000000 ))\"
done"
run "$@" | sed "s/^/$label  /"
