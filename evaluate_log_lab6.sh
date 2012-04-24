#!/bin/sh

echo "Successful runs of all tests: "
grep "tests done OK" testlog.txt | wc -l
