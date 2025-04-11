#!/bin/bash

#source ~/ENV/bin/activate

for count in `seq 0 19`;
do
    echo $count
    nohup python ./run-load.py $count 20 > ../data/logs/load_$count.txt &
done
