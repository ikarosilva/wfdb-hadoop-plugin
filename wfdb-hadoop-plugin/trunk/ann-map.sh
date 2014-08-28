#!/usr/bin/env bash
#
# Script for annotate WFDB records through Hadoop's MAP interface
#
# Written by Ikaro Silva
# Last Modified August 23, 2014
#
# Use NLineInputFormat to give a single line: key is offset, URI
read offset data

#Check if we are to run in local mode vs streaming mode. The *.dat files are processed in local
# mode, meaning the files are dowloaded from HDFS into the local fs, processed and uploaded. In
# streaming mode, the contents of the *.enc files are passed through STDIN, the *.stdin headers
# are downloaded, and the data is processed in memory. The *.enc file hold the entire contents of the
# signal or record on a single row which is passed through STDIN through Hadoops' framework.
# The streaming mode is thus limited by the amount available to the JVM and its child processes,
# it may, however be more optimal because it preserves data locality by utlizing Hadoop's Task
# manager to assign the task to the note closest to where the data resides.  
STREAMING_MODE=`echo ${offset} | sed 's/\`/true/' `
 
if [ "$STREAMING_MODE" != "true" ] 
then 

	#Run command that generates an annotation from a PhysioNet record
	RECORD=`echo ${data} |  sed -e s/hdfs:\\\/\\\\${HDFS_ROOT}\\\/\// | sed 's/.dat$//'`
	DB=${RECORD%/*}

	echo "***WFDB Processing in Local Mode: $ANN -r $RECORD " >&2
	tm=`(time $ANN -r $RECORD ) 2>&1 | grep "real\|user\|sys" | tr '\n' ' '`
	
	#Uncomment this line to verify number of beats annotated
	#count=`rdann -r ${RECORD} -a ${ANN} | wc -l`
	STR="*WFDB Generated $count annotations. Process time: $tm "
	echo ${STR} >&2
	echo "reporter:status:{STR}" >&2

	#Put the annotation file into HDFS
	echo "*WFDB Running:  ${HADOOP_INSTALL}/bin/hadoop fs -copyFromLocal ${RECORD}.${ANN} ${HDFS_ROOT}/${DB}/" >&2
	${HADOOP_INSTALL}/bin/hadoop fs -copyFromLocal ${RECORD}.${ANN} ${HDFS_ROOT}/${DB}/

	echo -e "$data\t$ANN:$RECORD-$count-$tm"

else

	#Extract full record name and path from data stream 
	FILE=`echo ${data%.dat*} | sed 's/^.*\s//'`

	#Extract database name 
	RECNAME=${FILE##*/}
	DB=`echo ${FILE%/*} | sed 's/\/.*\///g'`
	#Fomat record to have the full HDFS Path but without file extension
	RECORD=${HDFS_ROOT}/${DB}/${RECNAME}

	echo "***WFDB Processing in Streaming Mode: ${RECORD} ..." >&2

	#Get header file in order to decode stream via STDIN into physical units
	echo "reporter:status:****WFDB Running ${HADOOP_INSTALL}/bin/hadoop fs -copyToLocal ${RECORD}.stdin ." >&2 
	${HADOOP_INSTALL}/bin/hadoop fs -copyToLocal ${RECORD}.stdin .

	mv ${RECNAME}.stdin STDIN.hea

	echo "reporter:status:Decoding stream and passing it via STDIN..." >&2
	echo 'echo ${data} | sed 's/\`/\n/g' | uudecode -o - | $ANN -r STDIN' >&2
	#tm=`(time echo ${data} | sed 's/\`/\n/g' | uudecode -o - | $ANN -r STDIN ) 2>&1 | grep "real\|user\|sys" | tr '\n' ' '`
	#Rename annotation to match expected record
	#mv STDIN.${ANN} ${RECNAME}.${ANN}

	#echo "****WFDB Pushing annotation to HDFS ..."
	#echo "${HADOOP_INSTALL}/bin/hadoop fs -copyFromLocal ${RECNAME}.${ANN} ${HDFS_ROOT}/${DB}/" > &2
	#${HADOOP_INSTALL}/bin/hadoop fs -copyFromLocal ${RECNAME}.${ANN} ${HDFS_ROOT}/${DB}/
	echo -e "$RECORD\t$ANN\t$count\t$tm" 
	#STR="*WFDB Generated $count annotations. Process time: $tm " 
	#count=`rdann -r ${RECORD} -a ${ANN} | wc -l`
	#echo ${STR} >&2
	#echo "reporter:status:{STR}" >&2

	#Put the annotation file into HDFS
	#echo "*WFDB Running:  ${HADOOP_INSTALL}/bin/hadoop fs -copyFromLocal ${RECORD}.${ANN} ${HDFS_ROOT}/${DB}/" >&2
	#${HADOOP_INSTALL}/bin/hadoop fs -copyFromLocal ${RECORD}.${ANN} ${HDFS_ROOT}/${DB}/
	
fi

