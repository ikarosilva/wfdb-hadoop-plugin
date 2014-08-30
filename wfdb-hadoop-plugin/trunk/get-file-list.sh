#! /bin/bash
#
#
# Generate a list of PhysioNet files in HDFS

source wfdb-hadoop-configuration.sh
echo "hdfs dfs -ls -R ${HDFS_ROOT} > ${PHYSIONET_FILES}"
hdfs dfs -ls -R "${HDFS_ROOT}" > "${PHYSIONET_FILES}"

echo "grep \"*.dat\" ${PHYSIONET_FILES} > ${PHYSIONET_RECORD_FILES}"
grep '.dat$' ${PHYSIONET_FILES} | sed 's/^.*\s/hdfs:\//' > ${PHYSIONET_RECORD_FILES}
hdfs dfs -put ${PHYSIONET_RECORD_FILES} ${HDFS_ROOT}/

#Use this for debuggin over a sample set
head -n 1 ${PHYSIONET_RECORD_FILES} > ${PHYSIONET_RECORD_SAMPLE_SET}
hdfs dfs -put ${PHYSIONET_RECORD_SAMPLE_SET} ${HDFS_ROOT}/
