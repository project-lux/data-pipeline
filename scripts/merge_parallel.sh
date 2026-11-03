#!/bin/bash

#source ~/ENV/bin/activate

if [ ! -n "$1" ]
then
  echo "Usage: `basename $0` --all|--[source]"
  exit 0
fi 

for count in `seq 0 23`;
do
    echo $count
    nohup python ./run-merge.py $count 24 $1 > ../data/logs/merge_$count.txt &
done
