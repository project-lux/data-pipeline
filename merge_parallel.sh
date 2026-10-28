#!/bin/bash
#
# Merge in 24 slices; $1 is --all or --<source>.
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
PREFIX=$LOGS/merge-$(date +%Y-%m-%d-%H%M%S)

for count in `seq 0 23`;
do
    echo $count
    # was: nohup python ./run-merge.py $count 24 $1 > ../data/logs/merge_$count.txt &
    nohup lux merge --source $SOURCE --my-worker $count --max-workers 24 --no-ui --prefix $PREFIX > $LOGS/merge_$count.txt 2>&1 &
done

echo "Watch with: lux watch $PREFIX"
echo "Wait with:  scripts/wait-for-flags.sh merge $PREFIX 24 '$LOGS/merge_*.txt'"
