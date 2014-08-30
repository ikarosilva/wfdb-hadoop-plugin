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

#Check if WFDB is installed, if not, install it in /opt
wfdb-config --version 2>/dev/null
if [ ${?} != "0"  ] ; then 
	echo "Installing the WFDB Toolbox..."
	wget http://physionet.org/physiotools/matlab/wfdb-app-matlab/wfdb-app-toolbox-0-9-6-1.zip
	unzip wfdb-app-toolbox-0-9-6-1.zip 
    mv mcode /opt/
    sudo chmod a+x -R /opt/mcode/
    sudo chmod a+r -R /opt/mcode/
    export PATH=/opt/mcode/nativelibs/linux-amd64/bin/:$PATH
    export LD_LIBRARY_PATH=/opt/mcode/nativelibs/linux-amd64/lib/:$LD_LIBRARY_PATH
fi
exit

#Database name to push to HDFS
DB=mitdb
mkdir -p ${DATA_DIR}/${DB}
mkdir -p ${DATA_DIR}/udb

#Download general calibration and  DB files
echo "Downloading calibration and utility files..."
rsync -Cavz --ignore-existing physionet.org::physiobank-core/database/udb/ "${DATA_DIR}/udb"

#Dowload database to the WFDB standard directory that is searched by the binaries
echo "Downloading database: ${DB} ... "
rsync -CPavz --ignore-existing "physionet.org::${DB}" "${DATA_DIR}/${DB}"

echo "Encoding files in  ${DATA_DIR}/${DB} ... "

#Generate master file with backtick '`' as the record separator
#This assumes that UUENCODE will never use the character '`' on it's enconding scheme
	
master_file=${DB}.enc
rm -f ${master_file}
 
 ${HADOOP_INSTALL}/bin/hadoop fs -mkdir -p ${HDFS_ROOT}/${DB}/
 
for i in `find ${DATA_DIR}/${DB} -name "*.dat"` ;
do
	#TODO: Implement a way to check that '`' is not being used before 
	#substitution for the newline character
	REC=`basename ${i} | sed 's/.dat//'`
	cp -v ${i%.dat}.* .
	info=`head -n 1 ${REC}.hea | cut -f3- -d" "`
	k=`basename ${i}`
	NSIG=`cat ${REC}.hea | grep "^${REC}.dat " | wc -l`	
	echo -e "\n\n\n***Processing ${NSIG} signals in ${REC}\n\n\n"
	NSIG=$(( NSIG -1 ))

	#Generate a file for each signal
	for N in `seq 0 "${NSIG}"` ; do
		rm -f ${REC}_sig${N}.hea
		index=$(( N + 2 ))
		echo "${REC} 1 $info" > ${REC}_sig${N}.hea
		head -n ${index} ${i%*.dat}.hea | tail -n 1 >> ${REC}_sig${N}.hea
		echo "" >> ${REC}_sig${N}.hea
		echo "#" >> ${REC}_sig${N}.hea
		sed -i "s/${REC}/${REC}_sig${N}/" ${REC}_sig${N}.hea
		echo "xform -i ${REC} -o ${REC}_sig${N}.hea -s ${N}"
		REC_ENC="${REC}_sig${N}.dat"	
		xform -i "${REC}" -o ${REC}_sig${N}.hea -s ${N}
		echo "uuencode -m ${REC_ENC} ${REC_ENC} | sed ':a;N;$!ba;s/\n/\`/g' >> ${master_file}"
		#Save signal data as a encoded row in the master file
		uuencode -m ${REC_ENC} ${REC_ENC} | sed ':a;N;$!ba;s/\n/`/g' >> ${master_file}			
		
		#Upload header file to HDFS
		echo "${HADOOP_INSTALL}/bin/hadoop fs -put ${REC}_sig${N}.hea ${HDFS_ROOT}/${DB}/"
		${HADOOP_INSTALL}/bin/hadoop fs -put ${REC}_sig${N}.hea ${HDFS_ROOT}/${DB}/
	done
        #Remove temporary files (signal data is now encoded in ${master_file}
	rm -vf ${REC}*
	break
done

fsize=`du -sh ${master_file}`
echo "Uploading master data file to HDFS. File size: ${fsize}"
echo "${HADOOP_INSTALL}/bin/hadoop fs -put ${master_file} ${HDFS_ROOT}/${DB}/"
${HADOOP_INSTALL}/bin/hadoop fs  -D dfs.blocksize=512mb -put ${master_file} ${HDFS_ROOT}/${DB}/

echo "To check how many records were correctly encoded, run:"
echo "grep '\`' ${master_file} | wc -l"

