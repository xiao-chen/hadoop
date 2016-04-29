#!/bin/bash
source s3.sh
date
echo "**** fetching from s3..."
./s3nn.py list
echo "**** listed objects from s3"

#sleep 3

export PATH=$PATH:/Users/xiao/Desktop/repo/hdfs/xiao/hadoop/hadoop-dist/local/hadoop-3.0.0-SNAPSHOT/bin/
pushd hadoop-dist/local/hadoop-3.0.0-SNAPSHOT
./copy_config.sh

echo "**** starting namenode now..."
hdfs namenode -s3

