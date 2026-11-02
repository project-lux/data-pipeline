#!/bin/bash

#source ~/ENV/bin/activate

for count in `seq 0 23`;
do
    echo $count
    nohup python ./run-export.py $count 24 > ../data/logs/export_$count.txt &
done
