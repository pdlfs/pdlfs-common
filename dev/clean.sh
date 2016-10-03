#!/bin/sh -x

#
# designed to run in the top src directory:
#   $ ./dev/clean.sh
#
# Aug-05-2016 zhengq@cs.cmu.edu
#

MAKE="make -f ./dev/Makefile"
echo " Cleaning....."
$MAKE clean

exit 0
