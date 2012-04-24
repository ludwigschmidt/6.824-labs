#!/bin/sh

rm -f testlog.txt

for i in `seq 1 30`
do
  ./rsm_tester.pl 0 1 2 3 4 5 6 7 2>&1 >> testlog.txt
  killall lock_server; rm -f *.log
done
