#! /bin/bash
#
#
# Generate a list of PhysioNet files in HDFS

source wfdb-hadoop-configuration.sh
echo "${HADOOP_INSTALL}/bin/hdfs dfs -ls -R ${HDFS_ROOT} > ${PHYSIONET_FILES}"
"${HADOOP_INSTALL}"/bin/hdfs dfs -ls -R "${HDFS_ROOT}" > "${PHYSIONET_FILES}"

echo "grep \"*.dat\" ${PHYSIONET_FILES} > ${PHYSIONET_RECORD_FILES}"
grep '.dat$' ${PHYSIONET_FILES} | sed 's/^.*\s/hdfs:\//' > ${PHYSIONET_RECORD_FILES}
${HADOOP_INSTALL}/bin/hdfs dfs -put ${PHYSIONET_RECORD_FILES} ${HDFS_ROOT}/

#Use this for debuggin over a sample set
head -n 1 ${PHYSIONET_RECORD_FILES} > ${PHYSIONET_RECORD_SAMPLE_SET} 
${HADOOP_INSTALL}/bin/hdfs dfs -put ${PHYSIONET_RECORD_SAMPLE_SET} ${HDFS_ROOT}/