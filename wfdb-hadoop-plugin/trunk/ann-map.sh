#!/usr/bin/env bash
#
# Script for annotate WFDB records through Hadoop's MAP interface
#
# Written by Ikaro Silva
# Last Modified August 23, 2014
#
# Use NLineInputFormat to give a single line: key is offset, URI
read offset data


#Extract full record name and path from data stream 
FILE=`echo ${data%.dat*} | sed 's/^.*\s//'`

#Extract database name 
RECNAME=${FILE##*/}
DB=`echo ${FILE%/*} | sed 's/\/.*\///g'`
#Fomat record to have the full HDFS Path but without file extension
RECORD=${HDFS_ROOT}/${DB}/${RECNAME}

echo "****WFDB Processing ${RECORD} ..." >&2

#Get header file in order to decode stream via STDIN into physical units
echo "reporter:status:****WFDB Running ${HADOOP_INSTALL}/bin/hadoop fs -copyToLocal ${RECORD}.stdin ." >&2 
${HADOOP_INSTALL}/bin/hadoop fs -copyToLocal ${RECORD}.stdin .

mv ${RECNAME}.stdin STDIN.hea

echo "reporter:status:Decoding stream and passing it via STDIN..." >&2
echo 'echo ${data} | sed 's/\`/\n/g' | uudecode -o - | $ANN -r STDIN' >&2

tm=`(time echo ${data} | sed 's/\`/\n/g' | uudecode -o - | $ANN -r STDIN ) 2>&1 | grep "real\|user\|sys" | tr '\n' ' '`

#Rename annotation to match expected record
mv STDIN.${ANN} ${RECNAME}.${ANN}

echo "****WFDB Pushing annotation to HDFS ..."
echo "${HADOOP_INSTALL}/bin/hadoop fs -copyFromLocal ${RECNAME}.${ANN} ${HDFS_ROOT}/${DB}/" > &2
${HADOOP_INSTALL}/bin/hadoop fs -copyFromLocal ${RECNAME}.${ANN} ${HDFS_ROOT}/${DB}/
echo -e "$RECORD\t$ANN\t$count\t$tm" 


#STR="*WFDB Generated $count annotations. Process time: $tm " 
#count=`rdann -r ${RECORD} -a ${ANN} | wc -l`
#echo ${STR} >&2
#echo "reporter:status:{STR}" >&2

#Put the annotation file into HDFS
#echo "*WFDB Running:  ${HADOOP_INSTALL}/bin/hadoop fs -copyFromLocal ${RECORD}.${ANN} ${HDFS_ROOT}/${DB}/" >&2
#${HADOOP_INSTALL}/bin/hadoop fs -copyFromLocal ${RECORD}.${ANN} ${HDFS_ROOT}/${DB}/



