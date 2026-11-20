#!/bin/zsh
# Linux -> Mac file sharing benchmark: `ls -R` over a directory tree and a sequential read of a
# large file through a katamaran NFS socket, each run on a fresh mount so the Mac's NFS client
# starts with a cold cache. The guest-side page cache stays warm, as it would be in normal use.
#
# Usage: bench/nfs.sh <nfs.sock> <mount-point> <tree-dir> <big-file> [runs]
#   tree-dir and big-file are paths inside the mount, e.g. docker/volumes/bench/tree
#   OPTS overrides the NFS mount options (default: the ones `katamaran run` uses).
set -eu
zmodload zsh/datetime

sock=$1 mnt=$2 tree=$3 big=$4 runs=${5:-5}
OPTS=${OPTS:-vers=3,proto=ticotsord,port=$sock,mountport=$sock,soft,intr,timeo=10,retrans=3,deadtimeout=30,locallocks,nfc,actimeo=1,rsize=1048576,wsize=1048576,readahead=16,dsize=65536}

mount_it() { /sbin/mount_nfs -o "$OPTS" '<>:/' "$mnt"; }
unmount_it() { /sbin/umount "$mnt" 2>/dev/null || /sbin/umount -f "$mnt"; }

trap 'unmount_it 2>/dev/null || true' EXIT
mkdir -p "$mnt"

typeset -a ls_t read_t
for i in $(seq 1 "$runs"); do
    mount_it
    t0=$EPOCHREALTIME
    n=$(ls -R "$mnt/$tree" | wc -l)
    t1=$EPOCHREALTIME
    ls_t+=$(( (t1 - t0) * 1000 ))
    unmount_it

    mount_it
    t0=$EPOCHREALTIME
    bytes=$(cat "$mnt/$big" | wc -c)
    t1=$EPOCHREALTIME
    read_t+=$(( (t1 - t0) * 1000 ))
    unmount_it
done

median() { print -l -- "$@" | sort -n | sed -n "$(( ($# + 1) / 2 ))p"; }
printf 'options: %s\n' "$OPTS"
printf 'ls -R %s (%d lines): median %.1f ms  runs: %s\n' "$tree" "$n" "$(median $ls_t)" "${(j:, :)${(@)ls_t%%.*}}"
mb=$(( bytes / 1048576.0 ))
printf 'read %s (%.0f MiB): median %.1f ms = %.0f MiB/s  runs: %s\n' "$big" "$mb" "$(median $read_t)" \
    "$(( mb / $(median $read_t) * 1000 ))" "${(j:, :)${(@)read_t%%.*}}"
