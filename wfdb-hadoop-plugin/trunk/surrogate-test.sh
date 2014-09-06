#! /bin/bash
# file: octave-map.sh

OCTAVE='octave --quiet --eval '

#Generate 20 surrogate time series, saving each one to file
#under the name : surr_x
RR_FNAME=${1}
M=20
command="output= ${OCTAVE} \"shuffle('${RR_FNAME}');quit;\""
echo "${command}" >&2
output=`${OCTAVE} "shuffle('${RR_FNAME}');quit;" | sed 's/\n/\;/g' | tr '\n' ';'`

#Calculate the MSE for each series, first one is the original
echo "cat ${RR_FNAME} | mse -n 40 | sed 's/^m.*//' > mse-orig" >2&
cat ${RR_FNAME} | mse -n 40 | sed 's/^m.*//' > mse-ser-orig
for i in `seq 1 ${M}`
do
    echo "surr_${i} | mse -n 40 | sed 's/^m.*//' > mse-ser-surr-${i}" >2&
    cat "surr_${i}" | mse -n 40 | sed 's/^m.*//' > mse-ser-surr-${i}  
done
paste mse-ser-* > surrg_test

#Get the slope of the time series if it passes the sorrogate test
output=`${OCTAVE} "m=least_sqfit('surrg_test');disp(m);quit;" | sed 's/\n/\;/g' | tr '\n' ' '`

#clean up
rm mse-ser-orig surrg_test
rm  mse-ser-* 
rm surr_[0-9]
rm surr_[0-9][0-9]

echo "slope=$output"


