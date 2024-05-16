#!/bin/bash

for count in `seq 0 23`;
do
    echo $count
    nohup python ./manage-data.py $count 24 --nt > ../data/logs/export_nt_$count.txt &
done
