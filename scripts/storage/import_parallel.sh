#!/bin/bash

for count in `seq 0 23`;
do
    echo $count
    nohup python ./manage-data.py $count 24 --load $1 > ../data/logs/load_$1_$count.txt &
done
