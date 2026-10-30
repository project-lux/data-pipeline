#!/bin/bash
#
# A full build: reconcile, identify, metatypes, write-refs, merge,
# post-build-portal, export.
#
# run-all.sh on `lux`. The two sit side by side until builds with both have
# been compared on the build machine; then the old one is removed.
#
#   ./run-all2.sh --all              # or: ./run-all2.sh all
#   ./run-all2.sh --ycba             # one source (any --source value, with or without --)
#   ENGINE=mp ./run-all2.sh --all    # each phase as one lux command
#
# ENGINE=null (the default) is today's process model: one `lux` per slice,
# started with nohup, each writing ../data/logs/<phase>_<n>.txt, and the
# script waits for them all (scripts/wait-for-flags.sh). Every slice of a
# phase shares one --prefix, so from any terminal
#
#   lux watch ../data/logs/reconcile-<timestamp>      # or: lux watch --latest
#
# shows the whole phase. ENGINE=mp runs each phase as a single `lux`
# command whose workers are processes it starts itself; `lux reconcile`
# then runs identify, metatypes and write-refs at its end.
#
# Run from the repository: the metatypes-*.json slice files and
# reference_uris.txt are written in the working directory. On an error the
# script exits 1 and appends a line to $STATUS_FILE; slices still running
# are left to run.
#
# Environment (defaults in brackets): ENGINE [null], WORKERS [24],
# LOGS [../data/logs], STATUS_FILE [/data/logs/pipeline_process_status.txt],
# EXPORTS [/data-export/output/lux/latest], WAIT_INTERVAL [30].
#
# The old commands and their replacements:
#
#   manage-data.py --clear-all --new-token
#                         lux clear --source all --force && lux clear --refs --force && lux new-token
#   google-sames-diffs.py, load-csv-map2.py --same/--different
#                         lux google
#   run-reconcile.py i 24 --all
#                         lux reconcile --source all --my-worker i --max-workers 24 --no-ui --no-finish
#   run-identify.py       lux identify
#   merge-metatypes.py; mv metatypes.json ../data/files/
#                         lux metatypes
#   manage-data.py --write-refs
#                         lux write-refs
#   run-merge.py i 24 --all
#                         lux merge --source all --my-worker i --max-workers 24 --no-ui
#   post-build-portal.py --no-tqdm
#                         lux portal
#   run-export.py i 24    lux export --my-worker i --max-workers 24 --no-ui
#   manage-data.py i 24 --load --wikidata
#                         lux load --source wikidata --my-worker i --max-workers 24 --no-ui
#   run-harvest.py i 50 --pages --ycba
#                         lux harvest --source ycba --pages --my-worker i --max-workers 50 --no-ui
#   manage-data.py i 24 --nt
#                         lux export --type nt --my-worker i --max-workers 24 --no-ui

if [ ! -n "$1" ]
then
  echo "Usage: `basename $0` --all|--[source]"
  exit 0
fi

SOURCE=${1#--}
ENGINE=${ENGINE:-null}
WORKERS=${WORKERS:-24}
LOGS=${LOGS:-../data/logs}
STATUS_FILE=${STATUS_FILE:-/data/logs/pipeline_process_status.txt}
EXPORTS=${EXPORTS:-/data-export/output/lux/latest}
export LOGS

if [[ "$ENGINE" != "null" && "$ENGINE" != "mp" ]]
then
  echo "ENGINE must be null or mp, not $ENGINE"
  exit 1
fi

source "$(dirname "$0")/scripts/wait-for-flags.sh"

fail() {
    echo "Error in $1!"
    echo `date` [Error] Error in $1 >> $STATUS_FILE
    exit 1
}

stamp() {
    date +%Y-%m-%d-%H%M%S
}

# One phase as a `lux` command: on the null engine one process per slice
# and wait for them, on mp a single process. $1 is the phase (and the
# stdout file name), $2 the lux command, the rest its options.
run_phase() {
    local phase=$1 command=$2
    shift 2
    local prefix=$LOGS/$phase-$(stamp)
    echo "Watch with: lux watch $prefix"
    if [[ "$ENGINE" == "mp" ]]
    then
        lux $command "$@" --max-workers $WORKERS --no-ui --prefix $prefix \
            > $LOGS/$phase.txt 2>&1 || fail $phase
        return
    fi
    for count in `seq 0 $((WORKERS - 1))`;
    do
        echo $count
        nohup lux $command "$@" --my-worker $count --max-workers $WORKERS --no-ui --prefix $prefix \
            > $LOGS/${phase}_$count.txt 2>&1 &
    done
    wait_for_phase $phase $prefix $WORKERS "$LOGS/${phase}_*.txt" || fail $phase
    # every slice has reported done; let them exit (close their stores)
    # before the next step starts
    wait
}

echo "Did you clear and update the token?"
echo "    lux clear --source all --force && lux clear --refs --force && lux new-token"
echo "Did you check the flags for which units to run?"
echo ""

# Always update from google sheet
# was: google-sames-diffs.py and load-csv-map2.py --same / --different
lux google || fail google
echo "reloaded same/diffs from Google Sheet"

rm -f $LOGS/* 2> /dev/null

### Reconciliation Phase
#
echo "Starting Reconciliation Phase"

rm -f $LOGS/flags/reconcile_is_done*txt
rm -f metatypes-*.json

# was: nohup python ./run-reconcile.py $count 24 $1 > ../data/logs/reconcile_$count.txt 2>&1 &
if [[ "$ENGINE" == "mp" ]]
then
    # identify, metatypes and write-refs run at the end of this one
    run_phase reconcile reconcile --source $SOURCE
else
    run_phase reconcile reconcile --source $SOURCE --no-finish
    rm -f $LOGS/flags/reconcile_is_done*txt

    ### Resolve identity map deterministically from the logged assertions
    echo "Resolving identity map"
    # was: python ./run-identify.py > ../data/logs/identify.txt 2>&1
    lux identify > $LOGS/identify.txt 2>&1 || fail identify

    ### Merge metatypes
    echo "Merging Metatypes"
    # was: python ./merge-metatypes.py; rm metatypes-*.json; mv metatypes.json ../data/files/
    lux metatypes > $LOGS/metatypes.txt 2>&1 || fail metatypes

    ### Export referenced URIs
    echo "Exporting Referenced URIs to file"
    # was: python ./manage-data.py --write-refs
    lux write-refs > $LOGS/write-refs.txt 2>&1 || fail write-refs
fi


### Merge Phase
#
echo "Starting Merge Phase"

rm -f $LOGS/flags/merge_is_done-*txt
# was: nohup python ./run-merge.py $count 24 $1 > ../data/logs/merge_$count.txt 2>&1 &
run_phase merge merge --source $SOURCE
rm -f $LOGS/flags/merge_is_done-*.txt

# Now tag the records the YPM portal needs
# was: python ./post-build-portal.py --no-tqdm
lux portal > $LOGS/portal.txt 2>&1 || fail portal


### Export Phase
#
echo "Starting Export"
rm -f $EXPORTS/*jsonl
rm -f $LOGS/flags/export_is_done-*txt
# was: nohup python ./run-export.py $count 24 > ../data/logs/export_$count.txt 2>&1 &
run_phase export export
rm -f $LOGS/flags/export_is_done-*.txt
echo `date` [Success] Build was successful >> $STATUS_FILE
