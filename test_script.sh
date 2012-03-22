#!/bin/sh

rm -f testlog.txt

for i in `seq 1 2`
do
  ./start.sh >> testlog.txt
  ./test-lab-3-a.pl ./yfs1 >> testlog.txt
  ./stop.sh >> testlog.txt

  ./start.sh >> testlog.txt
  ./test-lab-3-b ./yfs1 ./yfs2 >> testlog.txt
  ./stop.sh >> testlog.txt

  ./start.sh >> testlog.txt
  ./test-lab-3-c ./yfs1 ./yfs2 >> testlog.txt
  ./stop.sh >> testlog.txt
done
