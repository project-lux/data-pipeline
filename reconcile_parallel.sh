#!/bin/bash

#source ~/ENV/bin/activate


# $1 should be --all or --source

if [ ! -n "$1" ]
then
  echo "Usage: `basename $0` --all|--[source]"
  exit 0
fi  

rm -f assertions-*.tsv

for count in `seq 0 23`;
do
    echo $count
    nohup python ./run-reconcile.py $count 24 $1 > ../data/logs/reconcile_$count.txt &
done

echo "When all slices are done, run: python ./run-identify.py"
echo "(resolves the identity map from assertions-*.tsv into the idmap)"
