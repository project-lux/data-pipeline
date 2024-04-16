#!/bin/bash

#source ~/ENV/bin/activate


# $1 should be --all or --source

if [ ! -n "$1" ]
then
  echo "Usage: `basename $0` --all|--[source]"
  exit 0
fi  

for count in `seq 0 23`;
do
    echo $count
    nohup python ./run-reconcile.py $count 24 $1 > ../data/logs/reconcile_$count.txt &
done
