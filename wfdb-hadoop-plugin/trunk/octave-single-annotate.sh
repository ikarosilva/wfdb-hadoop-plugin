#! /bin/bash
#

#Ignore this
if [ "$1" == "-h" ]; then
  echo -e "\n\tUsage: `basename $0` recordListFile \n"
  exit 0
fi

#Load configuration variables - This includes the database to be processed.
source wfdb-hadoop-configuration.sh


#The annotation command to be run in batch mode
FILE=${1}
OCTAVE='octave --quiet --eval '

#Export the WFDB variable so that it read records from NFS
DB=`basename ${FILE%/*}`
echo "Setting WFDB enviroment: export WFDB=".:${DATA_DIR}/${DB}/" " >&2
export WFDB=".:${DATA_DIR}/${DB}/"
mkdir -p OUTPUT
count=0

for i in `hadoop fs -cat "${FILE}"` 
do
    RECNAME=`basename ${i}`
    REC=${RECNAME%.dat}

    echo "***WFDB Processing in Single Mode: wfdb2mat -r $REC -t 60" >&2
    tm=`(time wfdb2mat -r $REC -t 60) 2>&1 | grep "real\|user\|sys" | tr '\n' ' '`
    fs=`wfdbdesc ${REC}m.hea  | grep Sampling | cut -f3 -d" "`
    STR="${OCTAVE} \"mapper('${REC}m',${fs}); quit;\" 1>&2"
    echo "$STR" >&2
    eval ${STR}
    output=`cat ${REC}m-mapper | sed 's/\n/:/g'`
    echo -e "$REC\t$output" > ./OUTPUT/${REC}-mapper && count=$(( count + 1 ))
    echo "Processed : $count files"
    #Remove any temporary files
    rm ${REC}*
done



