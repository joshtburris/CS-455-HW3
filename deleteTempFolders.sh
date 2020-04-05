#!/bin/sh
for i in `cat $HADOOP_CONF_DIR/workers`
do
	ssh $i "rm -rf /s/${i}/a/nobackup/cs455/$USER/*"
done
