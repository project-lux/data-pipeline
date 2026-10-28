#!/bin/bash
#
# Reconcile in 24 slices; $1 is --all or --<source>.
# Identify, metatypes and write-refs are left to run afterwards.
#
# reconcile_parallel.sh on `lux`, side by side with it until both have been compared.
#
# Every slice shares one --prefix: `lux watch <prefix>` shows them all and
# `scripts/wait-for-flags.sh` waits for them (the command is printed below).

if [ ! -n "$1" ]
then
  echo "Usage: `basename $0` --all|--[source]"
  exit 0
fi

SOURCE=${1#--}
LOGS=${LOGS:-../data/logs}
PREFIX=$LOGS/reconcile-$(date +%Y-%m-%d-%H%M%S)

for count in `seq 0 23`;
do
    echo $count
    # was: nohup python ./run-reconcile.py $count 24 $1 > ../data/logs/reconcile_$count.txt &
    nohup lux reconcile --source $SOURCE --no-finish --my-worker $count --max-workers 24 --no-ui --prefix $PREFIX > $LOGS/reconcile_$count.txt 2>&1 &
done

echo "Watch with: lux watch $PREFIX"
echo "Wait with:  scripts/wait-for-flags.sh reconcile $PREFIX 24 '$LOGS/reconcile_*.txt'"

echo "When all slices are done, run: lux identify && lux metatypes && lux write-refs"
echo "(resolves the identity map from assertions-*.tsv, merges the metatypes, writes the referenced URIs)"
