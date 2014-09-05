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
 
#Run command that generates an annotation from a PhysioNet record
RECORD=`echo ${data} |  sed -e s/hdfs:\\\/\\\\${HDFS_ROOT}\\\/\// | sed 's/.dat$//'`
DB=${RECORD%/*}
RECNAME=`basename ${RECORD}`
rm -f ${RECNAME}.mse
echo "WFDB= ${WFDB}" >&2

for ecg in `wfdbdesc ${RECNAME} | grep -i ECG -B 3| grep "Group ., Signal .:" | sed 's/^.*Signal//;s/://'` 
do
    
    echo "***WFDB Processing : wqrs -r ${DATA_DIR}/$RECNAME -s ${ecg}" >&2
    wqrs -r ${DATA_DIR}/${RECNAME} -s ${ecg} 1>&2
    
    #Get MSE data 
    echo "ann2rr -r ${DATA_DIR}/${RECNAME} -a wqrs | mse -n 40" >&2
    output=`ann2rr -r ${DATA_DIR}/${RECNAME} -a wqrs | mse -n 40 | sed 's/\n/\;/g'`

    STR="${RECNAME}: $output "
    echo ${STR} >&2
    echo "reporter:status:{STR}" >&2
    
    echo -e "${RECNAME}\t${output}"
done


