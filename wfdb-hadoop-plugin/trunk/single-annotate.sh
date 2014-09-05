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
ANN=wqrs

#Check if the command is properly installed
${ANN} -h > /dev/null 2>&1
if [ "$?" != "0" ] ; then
   echo "Exiting: Annotation command ${ANN} cannot be executed! " >&2
   exit
fi


#Export the WFDB variable so that it read records from NFS
DB=`basename ${FILE%/*}`
echo "Setting WFDB enviroment: export WFDB=".:${DATA_DIR}/${DB}/" " >&2
export WFDB=".:${DATA_DIR}/${DB}/"
rm -rf MSE
mkdir -p MSE
count=0
record=0
for i in `hadoop fs -cat "${FILE}"` 
do
    RECNAME=`basename ${i}`
    REC=${RECNAME%.dat}
    rm -f ${REC}.mse
    #Annotate all EGG signals in the record
    for ecg in `wfdbdesc ${REC} | grep -i ECG -B 3| grep "Group ., Signal .:" | sed 's/^.*Signal//;s/://'` 
    do
	echo "${ANN} -r ${REC} -s ${ecg}"
	${ANN} -r ${REC} -s ${ecg}
	#Calculate multiscale entropy with parameters provided by the tutorial
        echo "ann2rr -r ${REC} -a wqrs | mse -n 40 >> ${REC}.mse"
        ann2rr -r ${REC} -a wqrs | mse -n 40 >>./MSE/${REC}.mse
	mv -vf ${REC}.${ANN} ${DATA_DIR}/${DB}/${REC}.${ANN}_sig${ecg}
	count=$(( count + 1 ))
    done
    record=$(( record + 1 ))
done

echo "Processed signals: $count  from ${record} records"

