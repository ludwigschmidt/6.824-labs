#!/usr/bin/env bash

ulimit -c unlimited

BASE_PORT=$RANDOM
BASE_PORT=$[BASE_PORT+2000]
FAB1=$BASE_PORT
FAB2=$[FAB1 + 2]
FAB3=$[FAB1 + 4]

rm -f paxos-*.log
killall fab_server &> /dev/null

echo "starting ./fab_server $FAB1 $FAB1 > fab1.log 2>&1 &"
./fab_server $FAB1 $FAB1 > fab1.log 2>&1 &

echo "starting ./fab_server $FAB1 $FAB2 > fab3.log 2>&1 &"
./fab_server $FAB1 $FAB2 > fab2.log 2>&1 &

echo "starting ./fab_server $FAB1 $FAB3 > fab3.log 2>&1 &"
./fab_server $FAB1 $FAB3 > fab3.log 2>&1 &

#watch -n1 "tail -n 50 fab1.log"

echo "started fab servers $FAB1, $FAB2 and $FAB3"
