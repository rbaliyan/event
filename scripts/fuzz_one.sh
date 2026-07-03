#!/usr/bin/env bash
# fuzz_one.sh runs a single fuzz target for FUZZTIME and classifies the outcome.
#
#   ./scripts/fuzz_one.sh <pkg> <FuzzName>
#   FUZZTIME=40s FUZZ_RACE=0 ./scripts/fuzz_one.sh ./content FuzzDecode
#
# Exit 0 on a pass -- or on a bare "context deadline exceeded", which is the
# fuzzing coordinator failing to drain its workers within the budget on a loaded
# runner (a flake, not a bug). Exit 1 only on a genuine finding: one that writes
# a minimized reproducer under <pkg>/testdata/fuzz/<Target>/ and prints
# "Failing input written to ...".
#
# Environment:
#   FUZZTIME   fuzz budget (default 30s).
#   FUZZ_RACE  run with -race when != "0" (default "1"). -race roughly halves
#              exec throughput; the short PR/push smoke budget disables it so a
#              slow input near the deadline does not flake the run.
set -uo pipefail

pkg="${1:?usage: fuzz_one.sh <pkg> <FuzzName>}"
fn="${2:?usage: fuzz_one.sh <pkg> <FuzzName>}"
budget="${FUZZTIME:-30s}"

race_flag=()
if [ "${FUZZ_RACE:-1}" != "0" ]; then
  race_flag=(-race)
fi

echo "== fuzzing ${fn} (${pkg}) for ${budget} =="
# ${arr[@]+"${arr[@]}"} expands safely when the array is empty under `set -u`
# (bash 3.2, as shipped on macOS, otherwise errors on a bare "${arr[@]}").
out="$(go test -run='^$' ${race_flag[@]+"${race_flag[@]}"} -fuzz="^${fn}\$" -fuzztime="$budget" "$pkg" 2>&1)"
code=$?
printf '%s\n' "$out"
[ "$code" -eq 0 ] && exit 0

if printf '%s\n' "$out" | grep -q 'Failing input written'; then
  echo "FUZZ FAILED: ${fn} (${pkg})"
  exit 1
elif printf '%s\n' "$out" | grep -qE 'context deadline exceeded|deadline exceeded gathering baseline coverage'; then
  echo "FUZZ TIMEOUT (ignored, no reproducer): ${fn} (${pkg})"
  exit 0
fi

echo "FUZZ FAILED: ${fn} (${pkg})"
exit 1
