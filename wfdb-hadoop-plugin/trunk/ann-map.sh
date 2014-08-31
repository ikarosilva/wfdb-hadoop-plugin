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
 
 
if [ ${STREAMMING} != "true" ] ;
then 

	#Run command that generates an annotation from a PhysioNet record
	RECORD=`echo ${data} |  sed -e s/hdfs:\\\/\\\\${HDFS_ROOT}\\\/\// | sed 's/.dat$//'`
	DB=${RECORD%/*}
	RECNAME=`basename ${RECORD}`
	
	echo "***WFDB Processing in Local Mode: $ANN -r $RECNAME " >&2
	tm=`(time $ANN -r $RECNAME ) 2>&1 | grep "real\|user\|sys" | tr '\n' ' '`
	
	#Uncomment this line to verify number of beats annotated
	STR="*WFDB Process time: $tm "
	echo ${STR} >&2
	echo "reporter:status:{STR}" >&2

	#Put the annotation file into HDFS
	echo "hadoop fs -copyFromLocal ${RECNAME}.${ANN} ${DB}/" >&2
	hadoop fs -copyFromLocal ${RECNAME}.${ANN} ${DB}/
	echo -e "$data\t$ANN:$RECORD-$tm"

else

	echo ${data} >stream_dump
	#Clear streaming memory 
	data=""	
	sed -i 's/`/\n/g' stream_dump
	fsize=`du -sh stream_dump`
	echo "****WFDB Processing stream data of size: ${fsize}" >&2
	#Get file info 	
	data=`head -n 1 stream_dump`
	
	#Extract full record name and path from data stream 
	REC=`echo "${data##* }"`
	RECNAME=${REC%.dat}
	echo "****WFDB data = ${REC}" >&2

	#Convert stream dump to *.dat file
	echo "uudecode -o ${REC} stream_dump" >&2
	uudecode -o ${REC} stream_dump

	echo "***WFDB Processing in Streaming Mode: ${ANN} -r ${RECNAME} ..." >&2
	
        #Get header file in order to decode stream via STDIN into physical units
	echo "reporter:status:****WFDB Running hadoop fs -copyToLocal ${DB_DIR}/${RECNAME}.hea ." >&2 
	hadoop fs -copyToLocal ${DB_DIR}/${RECNAME}.hea .

	echo "reporter:status:Decoded stream. Processing..." >&2
	tm=`(time $ANN -r ${RECNAME} ) 2>&1 | grep "real\|user\|sys" | tr '\n' ' '`

	echo "****WFDB Pushing annotation to HDFS ..."
	hadoop fs -copyFromLocal ${RECNAME}.${ANN} ${DB_DIR}/
	echo -e "$REC\t$ANN\t$tm" 

	STR="*WFDB Generated $count annotations. Process time: $tm " 
	#count=`rdann -r ${RECNAME} -a ${ANN} | wc -l`
	echo ${STR} >&2
	echo "reporter:status:${STR}" >&2

	
fi

