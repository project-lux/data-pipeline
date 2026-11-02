#!/bin/bash

if [ ! -n "$1" ]
then
  echo "Usage: `basename $0` --[source]"
  exit 0
fi

for count in `seq 0 49`;
do
    echo $count
    nohup python ./run-harvest.py $count 50 --pages $1 > ../data/logs/harvest_$count$1.txt &
done
