#!/usr/bin/env bash
#
# Script for annotate WFDB records through Hadoop's MAP interface
#
# Written by Ikaro Silva
# Last Modified August 23, 2014
#
# Use NLineInputFormat to give a single line: key is offset, URI
read offset data


#Run command that generates an annotation from a PhysioNet record
RECORD=`echo ${data} |  sed -e s/hdfs:\\\/\\\\${HDFS_ROOT}\\\/\// | sed 's/.dat$//'`
RECNAME=`basename ${RECORD}`


for ecg in `wfdbdesc ${RECNAME} | grep -i ECG -B 3| grep "Group ., Signal .:" | sed 's/^.*Signal//;s/://'` 
do
    
    echo "***WFDB Processing : wqrs -r $RECNAME -s ${ecg}" >&2
    wqrs -r ${RECNAME} -s ${ecg} 1>&2
    
    #Get MSE data 
    rm -f rr-out
    echo "ann2rr -r ${RECNAME} -a wqrs > rr-out" >&2
    ann2rr -r ${RECNAME} -a wqrs > rr-out
    echo "./surrogate-test.sh rr-out" >&2
    output=`./surrogate-test.sh rr-out`

    STR="${RECNAME}: $output "
    echo ${STR} >&2
    echo -e "${RECNAME}\tsig_${ecg}\t${output}" >&2   
    echo -e "${RECNAME}\tsig_${ecg}\t${output}"
done


