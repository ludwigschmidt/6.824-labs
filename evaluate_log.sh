#!/bin/sh

echo "Successful runs of test a: "
grep "Passed all tests!" testlog.txt | wc -l

echo "Successful runs of test b: "
grep "test-lab-3-b: Passed all tests." testlog.txt | wc -l

echo "Successful runs of test c: "
grep "Create/delete in separate directories: tests completed OK" testlog.txt | wc -l
