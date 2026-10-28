#!/bin/bash
#
# Reconcile in 20 slices to update external sources; $1 is --all or --<source>.
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
PREFIX=$LOGS/update_ext-$(date +%Y-%m-%d-%H%M%S)

for count in `seq 0 19`;
do
    echo $count
    # was: nohup python ./run-reconcile.py $count 20 $1 > ../data/logs/update_ext_$count.txt &
    nohup lux reconcile --source $SOURCE --no-finish --my-worker $count --max-workers 20 --no-ui --prefix $PREFIX > $LOGS/update_ext_$count.txt 2>&1 &
done

echo "Watch with: lux watch $PREFIX"
echo "Wait with:  scripts/wait-for-flags.sh update_ext $PREFIX 20 '$LOGS/update_ext_*.txt'"
