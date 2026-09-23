#!/usr/bin/env bash
# Builds the tests once per known bug and expects the named test to fail. A bug
# that a test lets through is a test that doesn't cover what it says it does.
#
#   scripts/known-bugs.sh [bug ...]
set -uo pipefail
cd "$(dirname "$0")/.."
declare -A test=(
  [log_position_guard]="./test/integration TestRevokeSurvivesChunk"
  [ack_before_commit]="./test/integration TestRestartLosesNothing"
  [position_first]="./test/integration TestRestartLosesNothing"
  [toast_as_null]="./test/integration TestCopyThenFollow"
  [unordered_commit]="./test/integration TestReadersSeeSourceOrder"
  [no_epoch_check]="./test/integration TestReplacedWorkerCantCommit"
  [merge_outside_order]="./test/integration TestChunkTakesItsPlaceInLine"
  [no_yield]="./internal/sched TestLaterWorkGivesUpItsLock"
  [no_start_barrier]="./test/integration TestCommitOlderThanSlot"
  [no_reread]="./test/integration TestUpdateBeforeItsRow"
  [subxact_abort_ignored]="./internal/txn TestRolledBackSavepointIsSkipped"
  [xid_not_widened]="./internal/xid TestWidenAcrossWrap"
)
caught=0
bugs=("$@")
[ $# = 0 ] && mapfile -t bugs < <(printf '%s\n' "${!test[@]}" | sort)
for bug in "${bugs[@]}"; do
  read -r pkg name <<<"${test[$bug]}"
  if ! go vet -tags "bug_$bug" "$pkg" >/dev/null 2>&1; then
    echo "$bug: doesn't build"; continue
  fi
  out=$(go test -tags "bug_$bug" "$pkg" -run "^$name\$" -count=1 -timeout 120s 2>&1)
  if grep -q -- "--- FAIL: $name\|panic: test timed out" <<<"$out"; then
    caught=$((caught + 1)); echo "$bug: caught by $name"
  else
    echo "$bug: NOT caught by $name"
  fi
done
echo "$caught of ${#bugs[@]} caught"
[ "$caught" = "${#bugs[@]}" ]
