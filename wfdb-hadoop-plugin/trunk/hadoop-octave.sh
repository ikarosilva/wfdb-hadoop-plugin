#! /bin/bash
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


#For more tuning information see : http://hadoop.apache.org/docs/r0.18.3/streaming.pdf

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

#In this mode, we want to avoid multiple nodes attempting to create the same file in NFS
speculative=true

#Configure environment
echo "export WFDB=\"\.:${DATA_DIR}/${DB}/\"" >&2
export WFDB=".:${DATA_DIR}/${DB}/"
echo "export LD_LIBRARY_PATH=${DATA_DIR}/mcode/nativelibs/linux-amd64/lib64/:\$LD_LIBRARY_PATH"
export LD_LIBRARY_PATH=${DATA_DIR}/mcode/nativelibs/linux-amd64/lib64/:$LD_LIBRARY_PATH
echo "export PATH=${DATA_DIR}/mcode/nativelibs/linux-amd64/bin/:\$PATH"
export PATH=${DATA_DIR}/mcode/nativelibs/linux-amd64/bin/:$PATH

hadoop jar /usr/lib/hadoop-0.20/contrib/streaming/hadoop-streaming-*.jar \
  -D mapreduce.job.reduces=1 \
  -D mapred.map.tasks.speculative.execution=$speculative \
  -D mapreduce.task.timeout=1000000 \
  -D mapred.child.java.opts=-Xmx2g \
  -D mapred.child.java.opts=-Xms2g \
  -D mapred.child.java.opts=-XX:-UseConcMarkSweepGC \
  -input ${FILE} \
  -output output \
  -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
  -mapper octave-map.sh \
  -file octave-map.sh \
  -file mapper.m \
  -cmdenv HDFS_ROOT=${HDFS_ROOT} \
  -cmdenv HADOOP_INSTALL=${HADOOP_INSTALL} \
  -cmdenv STREAMMING=${STREAMMING} \
  -cmdenv DB_DIR=${FILE%/*} \
  -cmdenv LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
  -cmdenv PATH=$PATH \
  -cmdenv WFDB=$WFDB