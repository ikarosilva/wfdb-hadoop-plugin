package org.physionet.wfdb.hadoop;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
//Required jars:
// ./common/hadoop-common-2.2.0.jar
// ./mapreduce/hadoop-mapreduce-client-core-2.2.0.jar
// ./hdfs/lib/commons-cli-1.2.jar
// ./common/lib/commons-logging-1.1.1.jar
// ./hdfs/hadoop-hdfs-2.2.0.jar


public class Wqrs extends Configured
implements Tool {

	static class SequenceFileMapper
	extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

		private Text filenameKey;

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filenameKey = new Text(path.toString());
		}

		@Override
		protected void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			//TODO: pass in value to WQRS System call
			//context.write(filenameKey, value);
		}

	}

	public static void printHelp(){
		System.err.println("Available commands:");
		System.err.println("\t\t wqrs <args>");
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf(); 
		Job job = new Job(conf);

		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(SequenceFileMapper.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			printHelp();
			System.exit(2);
		}else{
			int exitCode = ToolRunner.run(new  Wqrs(), args);
			System.exit(exitCode);
		}
	}