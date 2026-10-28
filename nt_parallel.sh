#!/bin/bash
#
# Export the merged records as N-Triples in 24 slices.
#
# Every slice shares one --prefix: `lux watch <prefix>` shows them all and
# `scripts/wait-for-flags.sh` waits for them (the command is printed below).

LOGS=${LOGS:-../data/logs}
PREFIX=$LOGS/export_nt-$(date +%Y-%m-%d-%H%M%S)

for count in `seq 0 23`;
do
    echo $count
    # was: nohup python ./manage-data.py $count 24 --nt > ../data/logs/export_nt_$count.txt &
    nohup lux export --type nt --my-worker $count --max-workers 24 --no-ui --prefix $PREFIX > $LOGS/export_nt_$count.txt 2>&1 &
done

echo "Watch with: lux watch $PREFIX"
echo "Wait with:  scripts/wait-for-flags.sh export_nt $PREFIX 24 '$LOGS/export_nt_*.txt'"
