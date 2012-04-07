#!/bin/sh

rm -f testlog.txt

for i in `seq 1 2`
do
  ./rsm_tester.pl 0 1 2 3 4 5 6 7 >> testlog.txt
  killall lock_server; rm -f *.log
done
