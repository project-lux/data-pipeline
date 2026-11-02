#!/bin/bash
#
# Load one source's dump into its datacache in 24 slices; $1 is --<source>.
#
# import_parallel.sh on `lux`, side by side with it until both have been compared.
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
PREFIX=$LOGS/load-$(date +%Y-%m-%d-%H%M%S)

for count in `seq 0 23`;
do
    echo $count
    # was: nohup python ./manage-data.py $count 24 --load $1 > ../data/logs/load_$1_$count.txt &
    nohup lux load --source $SOURCE --my-worker $count --max-workers 24 --no-ui --prefix $PREFIX > $LOGS/load_${SOURCE}_$count.txt 2>&1 &
done

echo "Watch with: lux watch $PREFIX"
echo "Wait with:  scripts/wait-for-flags.sh load $PREFIX 24 '$LOGS/load_${SOURCE}_*.txt'"
