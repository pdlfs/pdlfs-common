#!/bin/sh -x

#
# designed to run in the top src directory:
#   $ ./dev/build_project.sh
#
# Aug-05-2016 zhengq@cs.cmu.edu
#

make clean -f .Makefile
make -j8 -f .Makefile

exit 0
