#!/usr/bin/env bash

BASE=$(dirname "$0")

no_lock() {
  config=$1; shift
  flock -n -E 10 -o "$config" "$@"; rv="$?"
  if [[ "$rv" -eq 10 ]]; then
    echo "$config is locked now, exiting"
  fi
  return "$rv"
}

locked() {
  config=$1; lockfile=$2; shift 2
  flock -n -E 10 -o "$config" \
    flock -s -n -E 11 -o "$lockfile" \
    "$@"; rv="$?"
  if [[ "$rv" -eq 10 ]]; then
    echo "$config is locked now, exiting"
  elif [[ "$rv" -eq 11 ]]; then
    echo "$lockfile is locked now, exiting"
  fi
  return "$rv"
}

double_locked() {
  config=$1; lockfile1=$2; lockfile2=$3; shift 3
  flock -n -E 10 -o "$config" \
    flock -s -n -E 11 -o "$lockfile1" \
    flock -s -n -E 12 -o "$lockfile2" \
    "$@"; rv="$?"
  if [[ "$rv" -eq 10 ]]; then
    echo "$config is locked now, exiting"
  elif [[ "$rv" -eq 11 ]]; then
    echo "$lockfile1 is locked now, exiting"
  elif [[ "$rv" -eq 12 ]]; then
    echo "$lockfile2 is locked now, exiting"
  fi
  return "$rv"
}

choose_locker() {
  case "$#" in
    0)  echo no_lock ;;
    1)  echo locked ;;
    2)  echo double_locked ;;
    *)
        echo "only single and double locking is supported" >&2
        exit 1
    ;;
  esac
}

usage() {
  echo "usage: $1 <config>" >&2
  exit 1
}

if [[ "$#" -ne 1 ]]; then usage "$0"; fi

config=$1
. "$config"

cmdline=("$(choose_locker "${LOCK[@]}")" "$config" "${LOCK[@]}"
         python "$BASE/main.py" -m "$METHOD" "${ARGS[@]}")

if ! [[ -z "$LOCAL_ADDRESS" ]]; then cmdline+=("-l" "$LOCAL_ADDRESS"); fi
if ! [[ -z "$AMTPASSWD" ]]; then cmdline+=("-p" "$AMTPASSWD"); fi

for group in "${DG_GROUPS[@]}"; do cmdline+=("-g" "$group"); done
for host in "${DG_HOSTS[@]}"; do cmdline+=("-H" "$host"); done

for image in "${NDD[@]}"; do cmdline+=("-n" "$image"); done
if ! [[ -z "$NDD_PORT" ]]; then cmdline+=("-np" "$NDD_PORT"); fi

for host in "${BAN[@]}"; do cmdline+=("-b" "$host"); done

if [[ "${#STAGES[@]}" > 0 ]]; then cmdline+=("-s" "${STAGES[@]}"); fi

for address in "${REPORT[@]}"; do cmdline+=("-r" "$address"); done

"${cmdline[@]}"
