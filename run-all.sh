#!/bin/bash

### Clear records

if [ ! -n "$1" ]
then
  echo "Usage: `basename $0` --all|--[source]"
  exit 0
fi 

echo "Did you update the datestamp token in run-merge, run-integrated?"
echo "Did you check the flags for which units to run?"
echo ""
sleep 1
echo "Clearing all records in 5"
sleep 1
echo "4"
sleep 1
echo "3"
sleep 1
echo "2"
sleep 1
echo "1"
sleep 1

python ./manage-data.py --clear-all
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

### Export Phase
#
echo "Starting Export"
rm /data-io2-2/output/lux/latest/*jsonl
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

#echo "Loading Sandbox"
#../tools/mlcp-11.0.0/bin/mlcp.sh import -ssl -host lux-ml-sbx.collections.yale.edu -port 8000 -username s_lux_deployer_sbx -password 'PASSWORD-HERE' -input_file_path /data-io2-2/output/lux/latest -database lux-content  -input_file_type delimited_json -output_permissions lux-endpoint-consumer,read,lux-writer,update -uri_id id -fastload -thread_count 64
