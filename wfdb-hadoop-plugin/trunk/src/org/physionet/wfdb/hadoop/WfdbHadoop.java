package org.physionet.wfdb.hadoop;

//Entry point for running WFDB applications in the Hadoop framework
//Required jars:
//./common/hadoop-common-2.2.0.jar
//./mapreduce/hadoop-mapreduce-client-core-2.2.0.jar
//./hdfs/lib/commons-cli-1.2.jar
//./common/lib/commons-logging-1.1.1.jar
//./hdfs/hadoop-hdfs-2.2.0.jar
// wfdb-app-JVM7-0-9-6-1.jar

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class WfdbHadoop {

	public static void printHelp(){
		System.err.println("Usage: hadoop [-D options] jar wfdb-hadoop.jar  COMMAND");
		System.err.println("  Where COMMAND is one of the following: ");
		System.err.println("\twqrs\tRuns WQRS detector in Hadoop Cluster");
		System.err.println("");
		System.err.println("Most commands print their help with a -h option");
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length>0){
			if(otherArgs[0].equalsIgnoreCase("wqrs")){
				Wqrs.main(args,otherArgs);
			}else{
				printHelp();
				System.exit(2);
			}
		}else{
			printHelp();
			System.exit(2);
		}
	}
}

