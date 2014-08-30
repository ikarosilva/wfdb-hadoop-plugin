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
  echo -e "\n\tUsage: `basename $0` recordListFile annotationCommand [annotationArgs]\n"
  echo -e  "\nRuns the WFDB command 'annotationCommand' in all records in recordListFile"
  echo -e "generating annotation files in Hadoop's HDFS.Parameters are: \n"
  echo -e "\trecordList \t-A '*.txt' or '*.enc' file in DFS containing all the records to be run in batch mode."
  echo -e "\t\t\tIf the recordListFile is a '*.txt' (similar to the ones generated by 'generate-file-list.sh' script,"
  echo -e "\t\t\tthen the nodes will download the records and process them through the local file system. If the"
  echo -e "\t\t\tis a '*.enc' file, then nodes will process the records in HDFS through the standard input, with each"
  echo -e "\t\t\trecord processed as a row in the '*.enc' file (this may consume a lot of memory in the JVM). To generate" 
  echo -e "\t\t\t'*.enc' files, run the 'prepare-dataset.sh' script (see the help on it for more information). " 
  echo ""
  echo -e "\tannotationArgs \t-Additional flags to the WFDB command can be passed through annotationArgs."
  echo -e "\t\t\tDo not pass record ('-r') or record-specific flags (such as '-a' ) to this"
  echo -e "\t\t\tcommand, as all records will be processed in a batch manner."
  echo ""
  echo -e "\tExample 1) To generate WQRS annotations with J point annotation and notch filtering at" 
  echo -e "\t50 Hz in a list of records, processing them in the locally:\n "
  echo -e "\thadoop-annotate.sh /physionet/physionet-samples.txt wqrs -j -p 50\n"
  echo ""
  echo -e "\tExample 2) To generate WQRS annotations in a list of the records," 
  echo -e "\tprocessing them in memory through STDIN passed by the Job tracker:\n "
  echo -e "\thadoop-annotate.sh /physionet/physionet-samples.enc wqrs\n"
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

#Determine if we will stream or process locally
STREAMMING=`echo ${FILE} | grep ".enc$"` 
if [ -n "${STREAMMING}" ] ;
then
	STREAMMING=true
else
    	STREAMMING=false
fi

#Command operates on the UUENCODED files generated by the
#prepare-dataset.sh script

hadoop jar /usr/lib/hadoop-0.20/contrib/streaming/hadoop-streaming-*.jar \
  -D mapreduce.job.reduces=0 \
  -D mapreduce.task.timeout=1000000 \
  -D mapreduce.job.jvm.numtasks=1 \
  -D mapred.child.java.opts=-Xmx2g \
  -D mapred.child.java.opts=-Xms2g \
  -D mapred.child.java.opts=-XX:-UseConcMarkSweepGC \
  -input ${FILE} \
  -output output \
  -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
  -mapper ann-map.sh \
  -file ann-map.sh \
  -cmdenv ANN=${ANN} \
  -cmdenv HDFS_ROOT=${HDFS_ROOT} \
  -cmdenv HADOOP_INSTALL=${HADOOP_INSTALL} \
  -cmdenv STREAMMING=${STREAMMING} \
  -cmdenv DB_DIR=${FILE%/*} \
  -cmdenv LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
  -cmdenv PATH=$PATH