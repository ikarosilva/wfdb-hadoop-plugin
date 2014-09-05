#! /bin/bash
# file: octave-map.sh

OCTAVE='octave --quiet --eval '

#Generate 20 surrogate time series, saving each one to file
#under the name : surr_x
RR_FNAME=${1}
command="output= ${OCTAVE} \"shuffle('${RR_FNAME}');quit;\""
echo "${command}" >&2
output=`${OCTAVE} "shuffle('${RR_FNAME}');quit;" | sed 's/\n/\;/g' | tr '\n' ';'`

#Calculate the MSE for each series, first one is the original
echo "cat ${RR_FNAME} | mse -n 40 | sed 's/^m.*//' > mse-orig"
cat ${RR_FNAME} | mse -n 40 | sed 's/^m.*//' > mse-orig
for i in seq 1 .. 20 
do
    cat "surr_${i}" | mse -n 40 | sed 's/^m.*//' > mse-surr-${i}    
done

#Get the slopes points from all of the them
output=`${OCTAVE} "shuffle('${RR_FNAME}');quit;" | sed 's/\n/\;/g' | tr '\n' ';'`

echo "***WFDB ${RECNAME} out: ${output}" >&2

#Pass output to Hadoop if not empty!
if [ -n "$output" ]; then
    echo -e "${RECNAME}-mapper\t$output"
fi

