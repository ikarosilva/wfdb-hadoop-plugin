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
    echo "./octave-map.sh ${REC}"
    ./octave-map.sh ${REC} && count=$(( count + 1 ))
    
done

echo "Processed signals: $count"

