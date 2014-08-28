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
#
#
# This script will generated encoded datasets (*.enc) that are text based and several GB in size.
# And where each record *.dat file is represented by a row.
# To generate a sample dataset with 10 records only, run something like:
# head -n 10 /usr/database/mghdb/mghdb.enc > sample.txt
#
#
#For streaming operations, the *.enc files are encoded into text using UUENCODE
#with a '`' delimiting the end of the file. For example, to encode:
#  uuencode -m foo.dat foo.dat > foo.enc
#  echo '`' >> foo.enc
#
# To decode on the local file system:
# cat foo.enc | sed 's/`/\n/g' | uudecode -o foo.copy

#Source configuration environment
source wfdb-hadoop-configuration.sh

#Database name to push to HDFS
DB=mitdb

#Download general calibration and  DB files
echo "Downloading calibration and utility files..."
rsync -Cavz --ignore-existing physionet.org::physiobank-core/database/udb/ "${DATA_DIR}/udb"

#Dowload database to the WFDB standard directory that is searched by the binariess
echo "Downloading database: ${DB} ... "
rsync -CPavz --ignore-existing "physionet.org::${DB}" "${DATA_DIR}/${DB}"

echo "Enconding files in  ${DATA_DIR}/${DB} ... "

#Generate master file with backtick '`' as the record separator
#This assumes that UUENCODE will never use the character '`' on it's enconding scheme
	
master_file=${DATA_DIR}/${DB}/${DB}.enc
rm -f ${master_file}
 
 ${HADOOP_INSTALL}/bin/hadoop fs -mkdir -p ${HDFS_ROOT}/${DB}/
 
for i in `find ${DATA_DIR}/${DB} -name "*.dat"` ;
do
	#TODO: Implement a way to check that '`' is not being used before 
	#substitution for the newline character
	REC=`basename ${i} | sed 's/.dat//'`
	rm -f ${REC}_sig1.hea
	info=`head -n 1 ${REC}.hea | cut -f3- -d" "`
	echo "${REC} 1 $info" > ${REC}_sig1.hea
	head -n 2 ${i%*.dat}.hea | tail -n 1 >> ${REC}_sig1.hea
	echo "" >> ${REC}_sig1.hea
	echo "#" >> ${REC}_sig1.hea
	sed -i "s/${REC}/${REC}_sig1/" ${REC}_sig1.hea
	cp -v ${i%.dat}.* .
	echo "xform -i ${REC} -o ${REC}_sig1.hea -s 0"
	xform -i "${REC}" -o ${REC}_sig1.hea -s 0
	rm -vf ${REC}.dat ${REC}.hea
	
	echo "uuencode -m ${i} ${i} | sed ':a;N;$!ba;s/\n/\`/g' >> ${master_file}"
	uuencode -m ${i} ${i} | sed ':a;N;$!ba;s/\n/`/g' >> ${master_file}
	

	echo "Setting header to file to read from standard input...."
	k=`basename ${i}`
	cat "${i%.dat}.hea" | sed "s/^${k%.dat} /stdin /" |sed "s/^${k} /- /" > ${i%.dat}.stdin

	#Upload header file to HDFS
	#echo "${HADOOP_INSTALL}/bin/hadoop fs -put ${i%.dat}.stdin ${HDFS_ROOT}/${DB}/"
	${HADOOP_INSTALL}/bin/hadoop fs -put ${REC}.* ${HDFS_ROOT}/${DB}/
done

#fsize=`du -sh ${master_file}`
#echo "Uploading master data file to HDFS. File size: ${fsize}"
#echo "${HADOOP_INSTALL}/bin/hadoop fs -put ${master_file} ${HDFS_ROOT}/${DB}/"
#${HADOOP_INSTALL}/bin/hadoop fs -put ${master_file} ${HDFS_ROOT}/${DB}/

echo "To check how many records were correctly encoded, run:"
echo "grep '\`' ${master_file} | wc -l"

#TODO: Load all the local PhysioNet data into the HDFS system
#es into HDFS...."
#${HADOOP_INSTALL}/bin/hadoop distcp file://${DATA_DIR}/ ${HDFS_ROOT}/
