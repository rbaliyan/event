#!/usr/bin/env bash
# fuzz_all.sh runs every discovered Go fuzz target for a short budget, in
# sequence (only one -fuzz target runs per `go test`). Targets are discovered by
# fuzz_targets.sh and each runs via fuzz_one.sh, so discovery and outcome
# classification live in one place and are shared with CI's per-target matrix.
#
#   FUZZTIME=30s ./scripts/fuzz_all.sh
#
# Exits non-zero when any target reports a genuine finding (a crash writes a
# reproducer under <pkg>/testdata/fuzz/<Target>/). See fuzz_one.sh for the
# FUZZTIME / FUZZ_RACE knobs and the deadline-flake handling.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
status=0

while read -r pkg fn; do
  [ -z "$fn" ] && continue
  "${here}/fuzz_one.sh" "$pkg" "$fn" || status=1
done < <("${here}/fuzz_targets.sh")

exit "$status"
