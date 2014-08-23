#! /bin/sh
#Script for dowloading data and converting annotations to text for loading to HDFS
#To be run only by the maste server. 
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

DB=mghdb
DATA_DIR=/usr/database
HADOOP_ROOT=/opt/hadoop-2.2.0
HDFS_ROOT=/user/database

#Dowload database to the WFDB standard directory that is searched by the binariess
#sudo rsync -Cavz "physionet.org::${DB}" "${DATA_DIR}/${DB}"

#Start HDFS
${HADOOP_ROOT}/sbin/start-dfs.sh

#Make HDFS ROOT

#TODO: Load all the *.dat, *.hea, and *-ari.txt files into the HDFS system
# We can then process all the *.hea and *-ari.txt using standard MapReduced text based tools.
${HADOOP_ROOT}/bin/hdfs dfs -mkdir /user/
${HADOOP_ROOT}/bin/hdfs dfs -mkdir /user/databases
${HADOOP_ROOT}/bin/hdfs dfs -mkdir "/user/databases/${DB}"

#Put all files into HDFS 
${HADOOP_ROOT}/bin/hdfs dfs -put ${DATA_DIR}/${DB}/*.* ${HDFS_ROOT}/${DB}/








