#!/usr/bin/env bash
#
# Script for annotate WFDB records through Hadoop's MAP interface
#
# Written by Ikaro Silva
# Last Modified August 23, 2014
#

# Use NLineInputFormat to give a single line: key is offset, URI
read offset hdfsfile

#Run command that generates an annotation from a PhysioNet record
RECORD=`echo ${hdfsfile} |  sed -e s/hdfs:\\\/\\\\${HDFS_ROOT}\\\/\// | sed 's/.dat$//'`
DB=${RECORD%/*}

echo "*WFDB Running: $ANN -r $RECORD " >&2
tm=`(time $ANN -r $RECORD ) 2>&1 | grep "real\|user\|sys" | tr '\n' ' '`
echo "*WFDB Time to annotate: $tm " >&2

#Put the annotation file into HDFS
echo "*WFDB Running:  ${HADOOP_INSTALL}/bin/hadoop fs -copyFromLocal ${RECORD}.${ANN} ${HDFS_ROOT}/${DB}/" >&2
${HADOOP_INSTALL}/bin/hadoop fs -copyFromLocal ${RECORD}.${ANN} ${HDFS_ROOT}/${DB}/

echo "reporter:status:Counting lines written into file" >&2
count=`rdann -r ${RECORD} -a ${ANN} | wc -l`

echo -e "$hdfsfile\t$ANN:$RECORD-$count-$tm"
