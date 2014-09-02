#! /bin/bash
# file: octave-map.sh

# Use NLineInputFormat to give a single line: key is offset, URI
read offset data

OCTAVE='octave --quiet --eval '
if [ ${STREAMMING} != "true" ] ;
then 

    #Extract record into a Octave format
    RECORD=`echo ${data} |  sed -e s/hdfs:\\\/\\\\${HDFS_ROOT}\\\/\// | sed 's/.dat$//'`
    DB=${RECORD%/*}
    RECNAME=`basename ${RECORD}`
    
    echo "***WFDB Processing in Local Mode: wfdb2mat -r $RECNAME -t 60" >&2
    tm=`(time wfdb2mat -r $RECNAME -t 60) 2>&1 | grep "real\|user\|sys" | tr '\n' ' '`
    
    STR="*WFDB Process time: $tm "
    echo ${STR} >&2
    echo "reporter:status:{STR}" >&2
else
    echo ${data} >stream_dump
    #Clear streaming memory 
    data=""
    sed -i 's/`/\n/g' stream_dump
    
    #Get file info 
    data=`head -n 1 stream_dump`

    #Extract full record name and path from data stream 
    REC=`echo "${data##* }"`
    RECNAME=${REC%.dat}
    echo "****WFDB data = ${REC}" >&2

    #Convert stream dump to *.dat file
    echo "uudecode -o ${REC} stream_dump" >&2
    uudecode -o ${REC} stream_dump

    echo "***WFDB Processing in Streaming Mode: wfdb2mat -r ${RECNAME} -t 60" >&2

    #Get header file in order to decode stream via STDIN into physical units
    echo "reporter:status:****WFDB Running hadoop fs -copyToLocal ${DB_DIR}/${RECNAME}.hea ." >&2 
    hadoop fs -copyToLocal ${DB_DIR}/${RECNAME}.hea .

    echo "reporter:status:Decoded stream. Processing..." >&2
    tm=`(time wfdb2mat -r ${RECNAME} -t 60) 2>&1 | grep "real\|user\|sys" | tr '\n' ' '`
    STR="*WFDB Process time: $tm "
    echo ${STR} >&2
    echo "reporter:status:{STR}" >&2
fi

fs=`wfdbdesc ${RECORD}m.hea  | grep Sampling | cut -f3 -d" "`
STR="${OCTAVE} \"mapper('${RECORD}m',${fs}); quit;\" 1>&2"
echo "$STR" >&2
eval ${STR} 1>&2
output=`cat ${RECORD}m-mapper| sed 's/\n/:/g/'`
rm ${RECORD}m-mapper
#Pass output to Hadoop
echo -e "$REC\t$output"
