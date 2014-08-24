#!/usr/bin/env bash
#
# Script for annotate WFDB records through Hadoop's MAP interface
#
# Written by Ikaro Silva
# Last Modified August 23, 2014
#

# Use NLineInputFormat to give a single line: key is offset, URI
read offset hdfsfile

# Report processing of file
echo "reporter:status:Processing $hdfsfile" >&2

#Run command that generates an annotation from a PhysioNet record
#Log success if annotation is generated
RECORD=`echo ${hdfsfile} |  sed -e s/hdfs:\\\/\\\\${HDFS_ROOT}\\\/database\\\/\// | sed 's/.dat$//'`
$ANN -r $RECORD
echo -e "$hdfsfile\t$ANN:$RECORD"
