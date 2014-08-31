#! /bin/bash
#
# Annotates all records files in locally in one machine
# 
#
# Written by Ikaro Silva
# Last modified August 30, 2014
#
#

if [ "$1" == "-h" ]; then
  echo -e "\n\tUsage: `basename $0` recordListFile annotationCommand [annotationArgs]\n"
  echo -e "\nRuns the WFDB command 'annotationCommand' in all records in recordListFile"
  echo -e "generating annotation files without using Hadoop at all. Use this for purposes of benchmarking."
  echo ""
  echo -e "\tExample 1) To generate WQRS annotations in a list of the records," 
  echo -e "\tsingle-annotate.sh /physionet/mitdb/mitdb.ind wqrs\n"
  exit 0
fi

#Load configuration variables - This includes the database to be processed.
source wfdb-hadoop-configuration.sh


#The annotation command to be run in batch mode
FILE=${1}
ANN=${2}

#Check if the command is properly installed
${ANN} -h > /dev/null 2>&1
if [ "$?" != "0" ] ; then
   echo "Exiting: Annotation command ${ANN} cannot be executed! " >&2
   exit
fi


#Export the WFDB variable so that it read records from NFS
DB=`basename ${FILE%/*}`
echo "Setting WFDB enviroment: export WFDB="${DATA_DIR}/${DB}/" " >&2
export WFDB="${DATA_DIR}/${DB}/"

for i in `hadoop fs -cat "${FILE}"` 
do
    RECNAME=`basename ${i}`
    REC=${RECNAME%.dat}
    echo ${ANN} -r ${REC}
    ${ANN} -r ${REC}
    mv -vf ${REC}.${ANN} ${DATA_DIR}/${DB}/
done

