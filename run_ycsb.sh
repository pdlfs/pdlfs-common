#!/bin/bash

setup() {
sudo umount /dev/xvdb;
sudo mkfs.ext4 /dev/xvdb -F;
sudo rm -rf /m;
sudo mkdir /m/;
sudo mount /dev/xvdb /m;
#sudo mkdir /m/ycsb_bench;
sudo chmod 777 -R /m;
}

echo -e "================YCSB=================="
echo -e "leveldb"
setup;
build/src/ycsbc -db leveldb -threads 4 -P ~/workloads/workload"$1".spec

cat /m/ycsb_bench/LOG | grep Compacted | awk 'BEGIN{sum=0;} {sum+=$9/1024/1024/1024} END{print "Total Write:", sum} '


echo -e "\n==============YCSB=================="
echo -e "columnardb1\n"
setup;
build/src/ycsbc -db columnardb1 -threads 4 -P ~/workloads/workload"$1".spec

cat /m/ycsb_bench/COLUMN-000000-LEVELDB/LOG | grep Compacted | awk 'BEGIN{sum=0;} {sum+=$9/1024/1024/1024} END{print "Total Write:", sum} '    
du -h /m/ycsb_bench/COLUMN-000000-VLOG
