#!/bin/bash

#source ~/ENV/bin/activate


# $1 should be --all or --source

if [ ! -n "$1" ]
then
  echo "Usage: `basename $0` --all|--[source]"
  exit 0
fi  

for count in `seq 0 19`;
do
    echo $count
    nohup python ./run-reconcile.py $count 20 $1 > ../data/logs/update_ext_$count.txt &
done