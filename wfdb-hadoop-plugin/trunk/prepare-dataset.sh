#! /bin/bash
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
#
# Written by Ikaro Silva
# Last Modified: August 23, 2014
#
#Note: If this is dones in a pseudo-distributed mode, with the namenode configure on the volatile directory, the following command need to be run before running the script in order to re-format the file system:
#/opt/hadoop-2.2.0/bin/hdfs namenode -format
#
#Make sure that the  HDFS daemon has started:
#${HADOOP_INSTALL}/sbin/start-dfs.sh

#For streaming operations, the *.enc files are encoded into text using UUENCODE
#with a '`' delimiting the end of the file. For example, to encode:
#  uuencode -m foo.dat foo.dat > foo.enc
#  echo '`' >> foo.enc
#
# To decode on the local file system:
# cat foo.enc | sed 's/`$//' | uudecode -o foo.dat

#Source configuration environment
source wfdb-hadoop-configuration.sh

#Download general calibraion and  DB files
echo "Downloading calibration and utility files..."
#rsync -Cavz physionet.org::physiobank-core/database/udb/ "${DATA_DIR}/udb"

#Dowload database to the WFDB standard directory that is searched by the binariess
echo "Downloading database: ${DB} ... "
#rsync -CPavz "physionet.org::${DB}" "${DATA_DIR}/${DB}"

echo "Enconding files in  ${DATA_DIR}/${DB} ... "
for i in `find ${DATA_DIR}/${DB} -name "*.dat"` ;
do
echo "uuencode -m ${i} ${i} > ${i%.dat}.enc"
uuencode -m ${i} ${i} > ${i%.dat}.enc

#file Set delimiter for stream decoding
echo '`' >> ${i%.dat}.enc

echo "Setting header to file to read from standard input...."
k=`basename ${i}`
cat "${i%.dat}.hea" | sed "s/^${k%.dat} /stdin /" |sed "s/^${k} /- /" > ${i%.dat}.stdin

done

#TODO: Load all the local PhysioNet data into the HDFS system
#es into HDFS...."
#${HADOOP_INSTALL}/bin/hadoop distcp file://${DATA_DIR}/ ${HDFS_ROOT}/


