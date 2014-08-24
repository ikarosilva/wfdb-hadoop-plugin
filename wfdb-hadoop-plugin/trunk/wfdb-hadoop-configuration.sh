#! /bin/bash
#
# Sets environment variables used by other scripts on this framework
# These scripts source this script.
#
# Written by Ikaro Silva
# Last Modified: August, 23, 2014

#Local Database Directory (used for storing data prior to loading it into HDFS)
DATA_DIR=/usr/database

#Hadoop Installation Directory
HADOOP_INSTALL=/opt/hadoop-2.2.0

#Root Database location in HDFS 
HDFS_ROOT=/physionet

#List of files in HDFS avaialable for processing
PHYSIONET_FILES=physionet-files.txt
PHYSIONET_RECORD_FILES=physionet-record-files.txt
PHYSIONET_RECORD_SAMPLE_SET=physionet-samples.txt


#Set the PhysioNet Database name for batch processing
DB=mghdb