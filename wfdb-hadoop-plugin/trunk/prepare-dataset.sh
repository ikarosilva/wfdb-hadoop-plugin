#! /bin/bash
#Script for downloading data and loading into Hadoop HDFS
#To be run only by the master server. 
#This requires installation of the WFDB package on the machine
#The installation can be done by :
#
# wget http://physionet.org/physiotools/matlab/wfdb-app-matlab/wfdb-app-toolbox-0-9-6-1.zip
# unzip wfdb-app-toolbox-0-9-6-1.zip
# 
# #Note: we may want to set these export variables on .bashrc as well
# cd mcode/nativelibs/linux/
# LIB_PATH=`pwd`
# export PATH=$TB_PATH\bin\:$PATH
# export LD_LIBRARY_PATH=$TB_PATH\lib64\:$LD_LIBRARY_PATH:
#
# you can test the path by running something like:
# rdsamp -r mitdb/100 -t s3
#
# Written by Ikaro Silva
# Last Modified: September 24, 2014
#
#Note: To run on your local machine, in a pseudo-distributed mode, with the namenode configured on the volatile directory 
#the following command need to be run before running the script in order to re-format the file system:
#/opt/hadoop-2.2.0/bin/hdfs namenode -format
#and make sure that the  HDFS daemon has started:
#${HADOOP_INSTALL}/sbin/start-dfs.sh
#

#Source configuration environment
source wfdb-hadoop-configuration.sh

#Database name to push to HDFS                                                                                                                       
DB=mghdb


#Check if WFDB is installed, if not, exit
wfdb-config --version 2>/dev/null
if [ ${?} != "0"  ] ; then
    echo "It appears the toolbox is not installed. Checking path..."
    echo "export PATH=${DATA_DIR}/mcode/nativelibs/linux-amd64/bin/:$PATH"
    export PATH=${DATA_DIR}/mcode/nativelibs/linux-amd64/bin/:$PATH
    echo "export LD_LIBRARY_PATH=${DATA_DIR}/mcode/nativelibs/linux-amd64/lib64/:$LD_LIBRARY_PATH"
    export LD_LIBRARY_PATH=${DATA_DIR}/mcode/nativelibs/linux-amd64/lib64/:$LD_LIBRARY_PATH
fi

wfdb-config --version 2>/dev/null
if [ ${?} != "0"  ] ; then
    echo "The toolbox is not installed. Please install it before continuing."
    exit
fi

mkdir -p ${DATA_DIR}/${DB}
mkdir -p ${DATA_DIR}/udb

#Required for local workers to write to this location
chmod a+w -R ${DATA_DIR}

#Download general calibration and  DB files
echo "Downloading calibration and utility files..."
rsync -Cavz --ignore-existing physionet.org::physiobank-core/database/udb/ "${DATA_DIR}/udb"

#Dowload database to the WFDB standard directory that is searched by the binaries
echo "Downloading database: ${DB} ... "
rsync -CPavz --ignore-existing "physionet.org::${DB}" "${DATA_DIR}/${DB}"


hadoop fs -mkdir -p ${HDFS_ROOT}/${DB}/

echo "Uploading dataset to HDFS..."
echo "hadoop fs -put ${DATA_DIR}/${DB}/* /physionet/${DB}"
hadoop fs -put ${DATA_DIR}/${DB}/* /physionet/${DB}

#Generate index file list         
echo "Generating index file..."                                         
echo " find ${DATA_DIR}/${DB}/ -name "*.dat"  | sed "s/\/mnt\/database\//\\${HDFS_ROOT}\\//" > ${DB}.ind"
find ${DATA_DIR}/${DB}/ -name "*.dat"  | sed "s/\/mnt\/database\//\\${HDFS_ROOT}\\//" > ${DB}.ind
     
hadoop fs -copyFromLocal ${DB}.ind ${HDFS_ROOT}/${DB}/