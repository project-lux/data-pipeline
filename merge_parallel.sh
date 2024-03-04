#!/bin/bash

#source ~/ENV/bin/activate

for count in `seq 0 23`;
do
    echo $count
    nohup python ./run-merge.py $count 24 --all > ../data/logs/merge_$count.txt &
done
