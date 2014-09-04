#! /bin/bash
# file: octave-map.sh

# Use NLineInputFormat to give a single line: key is offset, URI
read offset data

OCTAVE='octave --quiet --eval '

#Extract record into a Octave format
RECORD=`echo ${data} |  sed -e s/hdfs:\\\/\\\\${HDFS_ROOT}\\\/\// | sed 's/.dat$//'`
DB=${RECORD%/*}
RECNAME=`basename ${RECORD}`

echo "***WFDB Processing in Local Mode: wfdb2mat -r $RECNAME -t 60" >&2
tm=`(time wfdb2mat -r $RECNAME -t 60) 2>&1 | grep "real\|user\|sys" | tr '\n' ' '`

status="*WFDB Process time: $tm "
echo ${status} >&2
echo "reporter:status:${status}" >&2


fs=`wfdbdesc ${RECNAME}m.hea  | grep Sampling | cut -f3 -d" "`
command="output= ${OCTAVE} \"mapper('${RECNAME}m',${fs});quit;\""
echo "${command}" >&2
output=`${OCTAVE} "mapper('${RECNAME}m',${fs});quit;" | sed 's/\n/\;/g' | tr '\n' ';'`

echo "***WFDB ${RECNAME} out: ${output}" >&2

#Pass output to Hadoop if not empty!
if [ -n "$output" ]; then
    echo -e "${RECNAME}-mapper\t$output"
fi

