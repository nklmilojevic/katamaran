#!/usr/bin/env bash
# fsprobe.sh <docker-context> <label>: stat/open microbench + git status on the bench Linux tree.
set -euo pipefail
ctx=$1; label=$2
W=/Users/nkl/dev/personal/katamaran/bench/work/ds.noindex
docker --context "$ctx" run --rm -v $W:$W -w $W/linux katbench:f4787c2b1b1e sh -c "
r() { \"\$@\" | sed -E 's/.*op=([a-z_]+).*ns_per_op=([0-9.]+).*/\1 \2/'; }
echo \"stat_pass1_ns  \$(r kb_stat $W/linux.files 91159 | cut -d' ' -f2)\"
echo \"stat_pass2_ns  \$(r kb_stat $W/linux.files 91159 | cut -d' ' -f2)\"
echo \"open_close_ns  \$(r kb_open $W/linux.files 30000 | cut -d' ' -f2)\"
for i in 1 2; do s=\$(date +%s%N); git status >/dev/null 2>&1; e=\$(date +%s%N); echo \"git_status_\${i}_ms \$(( (e-s)/1000000 ))\"; done
" | sed "s/^/$label  /"
