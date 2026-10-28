#!/bin/bash
#
# Export the merged records for MarkLogic in 24 slices.
#
# Every slice shares one --prefix: `lux watch <prefix>` shows them all and
# `scripts/wait-for-flags.sh` waits for them (the command is printed below).

LOGS=${LOGS:-../data/logs}
PREFIX=$LOGS/export-$(date +%Y-%m-%d-%H%M%S)

for count in `seq 0 23`;
do
    echo $count
    # was: nohup python ./run-export.py $count 24 > ../data/logs/export_$count.txt &
    nohup lux export --my-worker $count --max-workers 24 --no-ui --prefix $PREFIX > $LOGS/export_$count.txt 2>&1 &
done

echo "Watch with: lux watch $PREFIX"
echo "Wait with:  scripts/wait-for-flags.sh export $PREFIX 24 '$LOGS/export_*.txt'"
