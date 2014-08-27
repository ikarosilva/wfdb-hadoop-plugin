#! /bin/bash
#
# Annotates all records files in HFDS system using Hadoop Streaming MAP API
# 
#
# Written by Ikaro Silva
# Last modified August 27, 2014
#
#
#This script require that PhysioNet files are on HDFS already. 
#Make sure that the HDFS cluster has been configured by running
#
#./prepare-dataset.sh
#./get-file-list.sh
#
#

#For more tuning information see : http://hadoop.apache.org/docs/r0.18.3/streaming.pdf

if [ "$1" == "-h" ]; then
  echo -e "\tUsage: `basename $0` annotationCommand [annotationArgs]\n"
  echo "Runs the WFDB command 'annotationCommand' generating annotation files in Hadoop."
  echo "Additional flags to the WFDB command can be passed through annotationArgs"
  echo "Do not pass record ('-r') or record-specific flags (such as '-a' ) to this"
  echo "command, as all records will be processed in a batch manner."
  echo ""
  echo -e "\tExample: To generate WQRS annotations with J point annotation and nocth filtering at" 
  echo "50 Hz in all the records in the current HDFS system "
  echo -e "\n\thadoop-annotate.sh wqrs -j -p 50\n"
  exit 0
fi

#Load configuration variables - This includes the database to be processed.
source wfdb-hadoop-configuration.sh


#The annotation command to be run in batch mode
ANN=${1}

#Check if the command is properly installed
${ANN} -h > /dev/null 2>&1
if [ "$?" != "0" ] ; then
   echo "Exiting: Annotation command ${ANN} cannot be executed! " >&2
   exit
fi

#Command operates on the UUENCODED files generated by the
#prepare-dataset.sh script

${HADOOP_INSTALL}/bin/hadoop jar ${HADOOP_INSTALL}/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
  -D mapreduce.job.reduces=0 \
  -D mapreduce.task.timeout=1000000 \
  -D mapreduce.job.jvm.numtasks=1 \
  -input hdfs://${HDFS_ROOT}/sample.txt \
  -output output \
  -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
  -mapper ann-map.sh \
  -file ann-map.sh \
  -cmdenv ANN=${ANN} \
  -cmdenv HDFS_ROOT=${HDFS_ROOT} \
  -cmdenv HADOOP_INSTALL=${HADOOP_INSTALL}

  
