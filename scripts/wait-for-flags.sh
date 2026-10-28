#!/bin/bash
#
# Wait for the slices of one phase to finish; fail on the first error.
#
#   scripts/wait-for-flags.sh <phase> <prefix> <count> [<stdout files glob>]
#   scripts/wait-for-flags.sh merge ../data/logs/merge-2026-10-28-101500 24 '../data/logs/merge_*.txt'
#
# or, sourced (run-all.sh does), the function `wait_for_phase` with the
# same arguments. Returns 0 when every slice is done, 1 as soon as one has
# failed (the other slices are left running, as before).
#
# With a prefix (every `lux ... --prefix <prefix>` slice), a slice is done
# when <prefix>-w<n>.status says "done": true, and has failed when its log
# <prefix>-w<n>.log holds a Traceback: the worker logs the exception there
# before it reports done. A `lux` that fails before its worker starts (no
# configuration, say) writes neither file, only a `lux <command>: ...` line
# on stderr, so the slices' stdout files are searched for that line too.
#
# With the prefix given as "-" this is the loop run-all.sh used to repeat
# for the legacy scripts: count <LOGS>/flags/<phase>_is_done-*.txt and
# search the stdout files for a Traceback.
#
# A slice killed outright (kill -9, the OOM killer) leaves no trace and is
# waited for forever, as it always was. `lux watch <prefix>` shows which.
#
# Environment: LOGS (default ../data/logs), WAIT_INTERVAL (seconds between
# checks, default 30).

wait_for_phase() {
    local phase=$1 prefix=$2 count=$3 outputs=$4
    local logs=${LOGS:-../data/logs}
    local interval=${WAIT_INTERVAL:-30}
    local done failed
    if [[ -z "$phase" || -z "$prefix" || -z "$count" ]]; then
        echo "Usage: wait_for_phase <phase> <prefix>|- <count> [<stdout files glob>]" >&2
        return 2
    fi
    while true; do
        # count first, then look for failures: a failing worker logs its
        # Traceback before it reports done, so a slice counted here as done
        # has already left its Traceback for the search below
        if [[ "$prefix" == "-" ]]; then
            done=$(ls -1 "$logs"/flags/"$phase"_is_done-*.txt 2> /dev/null | wc -l)
            failed=$([[ -n "$outputs" ]] && grep -sH Traceback $outputs)
        else
            done=$(grep -ls '"done": true' "$prefix"-w*.status | wc -l)
            failed=$(grep -sH Traceback "$prefix"-w*.log
                     [[ -n "$outputs" ]] && grep -sHE '^lux( [a-z-]+)?: ' $outputs)
        fi
        if [[ -n "$failed" ]]; then
            echo "$phase failed:"
            echo "$failed"
            return 1
        fi
        if [[ $done -ge $count ]]; then
            return 0
        fi
        sleep "$interval"
    done
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    wait_for_phase "$@"
    exit $?
fi
