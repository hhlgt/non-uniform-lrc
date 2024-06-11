#!/bin/bash
set -e

# ARRAY=('10.0.0.2' '10.0.0.3' '10.0.0.5' '10.0.0.6' '10.0.0.7' '10.0.0.8' '10.0.0.9' '10.0.0.10' '10.0.0.11' 
#         '10.0.0.12' '10.0.0.18') # else
ARRAY=('node2' 'node3' 'node5' 'node6' 'node7' 'node8' 'node9' 'node10' 'node11' 'node12' 'node18')
NUM=${#ARRAY[@]}
echo "cluster_number:"$NUM
NUM=`expr $NUM - 1`
SRC_PATH1=/home/nulrc/code/run_cluster_sh/
SRC_PATH2=/home/nulrc/code/project
SRC_PATH3=/home/nulrc/wondershaper
SRC_PATH4=/home/nulrc/code/kill_proxy_datanode.sh

# DIR_NAME=run_memcached
DIS_DIR1=/home/nulrc/code
DIS_DIR2=/home/nulrc/code/storage
DIS_DIR3=/home/nulrc/wondershaper
DIS_DIR4=/home/nulrc/code/res

if [ $1 == 3 ]; then
    ssh nulrc@node18 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;sudo ./wondershaper/wondershaper/wondershaper -a ib0 -d 1000000 -u 1000000'
elif [ $1 == 4 ]; then
    ssh nulrc@node18 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;echo done'
else
    echo "cluster_number:"${#ARRAY[@]}
for i in $(seq 0 $NUM)
    do
    temp=${ARRAY[$i]}
        echo $temp
        if [ $1 == 0 ]; then
            if [ $temp == 'node18' ]; then
                ssh nulrc@$temp 'pkill -9 run_datanode;'
            else
                ssh nulrc@$temp 'pkill -9 run_datanode;pkill -9 run_proxy'
            fi
            echo 'pkill  all'
            ssh nulrc@$temp 'ps -aux | grep run_datanode | wc -l'
            ssh nulrc@$temp 'ps -aux | grep run_proxy | wc -l'
        elif [ $1 == 1 ]; then
            ssh nulrc@$temp 'cd /home/nulrc/code;bash cluster_run_proxy_datanode.sh'
            echo 'proxy_datanode process number:'
            ssh nulrc@$temp 'ps -aux |grep run_datanode | wc -l;ps -aux |grep run_proxy | wc -l'
        elif [ $1 == 2 ]; then
            ssh nulrc@$temp 'mkdir -p' ${DIS_DIR1}
            ssh nulrc@$temp 'mkdir -p' ${DIS_DIR2}
            ssh nulrc@$temp 'mkdir -p' ${DIS_DIR3}
            ssh nulrc@$temp 'mkdir -p' ${DIS_DIR4}
            rsync -rtvpl ${SRC_PATH1}${i}/cluster_run_proxy_datanode.sh nulrc@$temp:${DIS_DIR1}
            rsync -rtvpl ${SRC_PATH2} nulrc@$temp:${DIS_DIR1}
            rsync -rtvpl ${SRC_PATH3} nulrc@$temp:${DIS_DIR3}
            rsync -rtvpl ${SRC_PATH4} nulrc@$temp:${DIS_DIR1}
        elif [ $1 == 5 ]; then
            ssh nulrc@$temp 'cd /home/nulrc/code/storage/;rm -rf *'
        fi
    done
fi