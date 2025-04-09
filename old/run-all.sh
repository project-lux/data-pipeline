#!/bin/bash

export TQDM_DISABLE=1

### Clear records

if [ ! -n "$1" ]
then
  echo "Usage: `basename $0` --all|--[source]"
  exit 0
fi 

echo "Did you clear and update the token?"
echo "    python ./manage-data --clear-all --new-token"
echo "Did you check the flags for which units to run?"
echo ""

# Always update from google sheet
python ./google-sames-diffs.py
python ./load-csv-map2.py --same ../data/files/sameAs/google.csv
python ./load-csv-map2.py --different ../data/files/differentFrom/google.csv
echo "reloaded same/diffs from Google Sheet"

rm ../data/logs/*

### Reconciliation Phase
#
echo "Starting Reconciliation Phase"

rm ../data/logs/flags/reconcile_is_done*txt
rm metatypes-*.json

for count in `seq 0 23`;
do
    echo $count
    nohup python ./run-reconcile.py $count 24 $1 > ../data/logs/reconcile_$count.txt 2>&1 &
done

# Wait while the processes spin up and write to the log files
sleep 30

# And wait for reconcile to finish
current=`ls -1 ../data/logs/flags/reconcile_is_done*txt 2> /dev/null | wc -l`
while [[ $current -lt 24 ]]
do
    current=`ls -1 ../data/logs/flags/reconcile_is_done*txt 2> /dev/null | wc -l`
    failed=`grep Traceback ../data/logs/reconcile_* | wc -l 2> /dev/null`
    if [[ $failed -ge 1 ]]
    then
        echo "Error in Reconcile!"
        echo `grep Traceback ../data/logs/reconcile_*`
        echo `date` [Error] Error in reconcile >> /data/logs/pipeline_process_status.txt
        exit
    fi
    sleep 30
done
rm ../data/logs/flags/reconcile_is_done*txt

sleep 30

### Merge metatypes
echo "Merging Metatypes"
python ./merge-metatypes.py
rm metatypes-*.json
mv metatypes.json ../data/files/
sleep 30

### Export referenced URIs
echo "Exporting Referenced URIs to file"
python ./manage-data.py --write-refs
sleep 10


### Merge Phase
#
echo "Starting Merge Phase"

rm ../data/logs/flags/merge_is_done-*txt
for count in `seq 0 23`;
do
    echo $count
    nohup python ./run-merge.py $count 24 $1 > ../data/logs/merge_$count.txt 2>&1 &
done

# And wait for merge to finish
current=`ls -1 ../data/logs/flags/merge_is_done-*.txt 2> /dev/null | wc -l`
while [[ $current -lt 24 ]]
do
    current=`ls -1 ../data/logs/flags/merge_is_done-*.txt 2> /dev/null | wc -l`
    failed=`grep Traceback ../data/logs/merge_* | wc -l 2> /dev/null`
    if [[ $failed -ge 1 ]]
    then
        echo "Error in Merge!"
        echo `grep Traceback ../data/logs/merge_*`
        echo `date` [Error] Error in merge >> /data/logs/pipeline_process_status.txt
        exit
    fi
    sleep 30
done
rm ../data/logs/flags/merge_is_done-*.txt

sleep 30

# Now run post-build-portal to tag YPM records
python ./post-build-portal.py --no-tqdm
sleep 10


### Export Phase
#
echo "Starting Export"
rm /data-export/output/lux/latest/*jsonl
rm ../data/logs/flags/export_is_done-*txt
for count in `seq 0 23`;
do
    echo $count
    nohup python ./run-export.py $count 24 > ../data/logs/export_$count.txt 2>&1 &
done


# And wait for export to finish
current=`ls -1 ../data/logs/flags/export_is_done-*.txt 2> /dev/null | wc -l`
while [[ $current -lt 24 ]]
do
    current=`ls -1 ../data/logs/flags/export_is_done-*.txt 2> /dev/null | wc -l`
    failed=`grep Traceback ../data/logs/export_* | wc -l 2> /dev/null`
    if [[ $failed -ge 1 ]]
    then
        echo "Error in Merge!"
        echo `grep Traceback ../data/logs/export_*`
        echo `date` [Error] Error in export >> /data/logs/pipeline_process_status.txt
        exit
    fi
    sleep 30
done
rm ../data/logs/flags/export_is_done-*.txt
echo `date` [Success] Build was successful >> /data/logs/pipeline_process_status.txt
