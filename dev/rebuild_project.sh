#!/bin/sh -x

#
# designed to run in the top src directory:
#   $ ./dev/rebuild_project.sh
#
# Aug-05-2016 zhengq@cs.cmu.edu
#

MAKE="make -f ./dev/Makefile"

$MAKE clean
$MAKE -j4

echo "== Build tests in 1 seconds ..."
sleep 1
$MAKE -j4 t2tests

echo "== Run tests in 1 seconds ..."
sleep 1
$MAKE t2check

exit 0
