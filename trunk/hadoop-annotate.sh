#! /bin/bash
#
# Annotates all records files in HFDS system using Hadoop Streaming MAP API
# 
#
# Written by Ikaro Silva
# Last modified August 23, 2014
#
#
#This script require that PhysioNet files are on HDFS already. 
#Make sure that the HDFS cluster has been configured by running
#
#./prepare-dataset.sh
#./get-file-list.sh
#
#
source wfdb-hadoop-configuration.sh

#The annotation command to be run in batch mode
ANN=${1}

#Check if the command is properly installed
${ANN} -h > /dev/null 2>&1
if [ "$?" != "0" ] ; then
   echo "Exiting: Annotation command ${ANN} cannot be executed! " >&2
   exit
fi


${HADOOP_INSTALL}/bin/hadoop jar ${HADOOP_INSTALL}/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
  -D mapred.reduce.tasks=0 \
  -D mapred.task.timeout=1000000 \
  -input hdfs://${HDFS_ROOT}/${PHYSIONET_RECORD_FILES} \
  -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
  -output output \
  -mapper ann-map.sh \
  -file ann-map.sh \
  -cmdenv ANN=${ANN} \
  -cmdenv HDFS_ROOT=${HDFS_ROOT}
