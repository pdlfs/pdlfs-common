#!/bin/bash

set -x
workspace="$HOME/lu/pdlfs-common"
result_file="$HOME/lu/result"
iters=(
20
25
)
left_comp_opts=(
c
)
right_comp_opts=(
c
)
function setup() {
        sudo umount /dev/sda4
        sudo rm -rf /m
        sudo mkfs.ext4 /dev/sda4 -F
        sudo mkdir /m
        sudo mount -t ext4 /dev/sda4 /m
}

for k in 0 # comp_opts
do
        for i in 0 1 # iters
                do
                setup;
                cd /m;
                sudo mkdir tmp;
                cd tmp;
                echo ${iters[$i]} ${left_comp_opts[$k]} ${right_comp_opts[$k]} >> "${result_file}";
                sudo "${workspace}"/build/dualdb_test --benchmark dualdb . ${iters[$i]} ${left_comp_opts[$k]} ${right_comp_opts[$k]} 2>> "${result_file}";
                echo -e "\n" >> "${result_file}";
                sleep 5;
                cd $HOME;

        done
done
