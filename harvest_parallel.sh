#!/bin/bash
#
# Harvest one source's activity stream pages in 50 slices; $1 is --<source>.
#
# Every slice shares one --prefix: `lux watch <prefix>` shows them all and
# `scripts/wait-for-flags.sh` waits for them (the command is printed below).

if [ ! -n "$1" ]
then
  echo "Usage: `basename $0` --[source]"
  exit 0
fi

SOURCE=${1#--}
LOGS=${LOGS:-../data/logs}
PREFIX=$LOGS/harvest-$(date +%Y-%m-%d-%H%M%S)

for count in `seq 0 49`;
do
    echo $count
    # was: nohup python ./run-harvest.py $count 50 --pages $1 > ../data/logs/harvest_$count$1.txt &
    nohup lux harvest --source $SOURCE --pages --my-worker $count --max-workers 50 --no-ui --prefix $PREFIX > $LOGS/harvest_${count}_$SOURCE.txt 2>&1 &
done

echo "Watch with: lux watch $PREFIX"
echo "Wait with:  scripts/wait-for-flags.sh harvest $PREFIX 50 '$LOGS/harvest_*_${SOURCE}.txt'"
