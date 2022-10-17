#!/usr/bin/env bash

set -e

BASE=$(dirname "$0")
PKGS=/var/lib/cow/windows7/pkgs
PTR=/tmp/windows7

locked() {
  lockfile=$1; shift
  flock -n -E 10 -o "$lockfile" "$@"; rv="$?"
  if [[ "$rv" -eq 10 ]]; then
    echo "$lockfile is locked now, exiting"
  fi
  return "$rv"
}

silent() {
  local allfile="$(tempfile -p win7)"
  "$@" >"$allfile" 2>&1; rv="$?"
  [[ "$rv" -ne 0 ]] && cat "$allfile"
  rm "$allfile"
  return "$rv"
}

usage() {
  echo "usage: $1 {new,cleanup} <base config> <snapshot config>" >&2
  exit 1
}

new() {
  local base_config=$1 snapshot_config=$2
  silent locked "$base_config" make -C "$BASE/mp"
  silent locked "$base_config" python3 "$BASE/prepare.py" \
    -d 0 -S "$BASE/sysprep.xml" \
    -ss "$BASE/SetupComplete.cmd" -ss "$BASE/mp/set-mountpoint.exe" \
    -ss "$BASE/export.cmd" -ss "$BASE/hidden.vbs" \
    -l "$PKGS" -p "$PTR" "$base_config" "$snapshot_config"
}

if [[ "$#" -ne 3 ]]; then usage "$0"; fi

ACTION=$1
BASE_CONFIG=$2
SNAPSHOT_CONFIG=$3

if [[ ! -f "$BASE_CONFIG" ]] || [[ -f "$SNAPSHOT_CONFIG" ]]; then usage "$0"; fi

case "$ACTION" in
  new)
    new "$BASE_CONFIG" "$SNAPSHOT_CONFIG"
  ;;
  cleanup)
    silent locked "$BASE_CONFIG" "$BASE/clean.sh"
  ;;
  *)
    usage "$0"
  ;;
esac

# vim: ts=2 sw=2
