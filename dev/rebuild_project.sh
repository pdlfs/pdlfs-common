#!/bin/sh -x

#
# designed to run in the top src directory:
#   $ ./dev/rebuild_project.sh
#
# Aug-05-2016 zhengq@cs.cmu.edu
#

make clean -f ./dev/Makefile
make -j4 -f ./dev/Makefile

exit 0
