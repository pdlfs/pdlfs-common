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

echo "key size:", "$1"
echo "number of entries:", "$2"

echo -e "=================db_bench============="
echo -e "leveldb"

setup;
build/src/db_bench  --db_impl=0 --value_size="$1" --num="$2"

cat /m/dbbench/LOG | grep Compacted | awk 'BEGIN{sum=0;} {sum+=$9/1024/1024/1024} END{print "Total Write:", sum} '



echo -e "\n==============db_bench=============="
echo -e "columnardb1\n"
setup;
build/src/db_bench  --db_impl=2 --value_size="$1" --num="$2"

cat /m/dbbench/COLUMN-000000-LEVELDB/LOG | grep Compacted | awk 'BEGIN{sum=0;} {sum+=$9/1024/1024/1024} END{print "Total Write:", sum} '    
du -h /m/dbbench/COLUMN-000000-VLOG
