package org.physionet.wfdb.hadoop;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.physionet.wfdb.Wfdbexec;


public class Wqrs extends Configured
implements Tool {

	private static final String TAG="**Wfdb - Wqrs: ";
	//TODO: Have some of the parameters be configurable from the command line
	private static final String command="ls";
	private static final String recordExtension=".dat";
	private static final String headerExtension=".hea";
	//if useSystemBinaries=true -> uses commands in the system PATH
	//Otherwise, will attempt to use commands under ./nativelibs/linux/bin/
	private static final boolean useSystemBinaries=true;
	
	
	private static Path localDir;
	private static Configuration conf;
	
	//Inner Mapper Class
	static class WqrsMapper
	extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

		private Text fileNameKey;
		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			fileNameKey = new Text(path.toString());
		}
		protected boolean getHeaderFile(String fileNameKey,Path destDir,Configuration conf){
			//Fetch header file from HDFS
			FileSystem fs=null;
			try {
				fs = FileSystem.get(conf);
				fs.copyToLocalFile(new Path(fileNameKey),destDir);
				return true;
			} catch (Exception e) {
				return false;
			}
		}
		
		@Override
		protected void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			String fname=fileNameKey.toString();
			
			if(fname.endsWith(recordExtension)){
				//context.write(fileNameKey, value);
				
				//Fetch header file from 
				if(getHeaderFile(fname,localDir,conf)==false){
					System.err.print(TAG+ "Could not get header file for: " + fname);
					System.err.print(TAG+ "Exiting task");
					return;
				}
				
				//TODO: Copy STDIN header file from HDFS and save into a temporary space
				String[] args={fname.replace(headerExtension,"")};
				
				//Define WFDB command to execute
				Wfdbexec wqrs=new Wfdbexec(command,false);
				if(useSystemBinaries){
					//Set /usr/bin as the execution path
					wqrs.setWFDB_NATIVE_BIN("/usr/bin");
					
				}
				wqrs.setArguments(args);
				
				//Log execution details
				System.err.print(TAG+ "Running:  " + command +" ");
				for (int i=0;i<args.length;i++)
					System.err.print(args[i] + " ");
				System.err.println(TAG+ "\nRecord length: " + value.getLength());
				
				//Execute with standard input
				try {
					//wqrs.execWithStandardInput(value.getBytes());
					List<String> out=wqrs.execToStringList();
					System.err.println(TAG+ "Output:" + out.get(0));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.err.println(TAG+ "Execution failed!!");
					e.printStackTrace();
					System.exit(2);
				}
			
			}
		}

	}

	
	public static void printHelp(){
		System.err.println("Usage: hadoop [-D options] jar wfdb-hadoop.jar wqrs [OPTIONS] input-directory output-directory");
		System.err.println("  Where OPTIONS is any of the following: ");
		System.err.println("\t-h\tPrints help for this command");
		System.err.println("Example: ");
		System.err.println("\t hadoop jar wfdb-hadoop.jar wqrs /physionet/mghdb");
	}
	
	@Override
	public int run(String[] args) throws Exception {
		conf = getConf();
		Job job = new Job(conf);
		job.setInputFormatClass(WfdbRecordInputFormat.class);
		job.setMapperClass(WqrsMapper.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		FileInputFormat.addInputPath(job,new Path(args[args.length-1]));
		localDir=new Path(job.JOB_LOCAL_DIR);
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args, String[] otherArgs) throws Exception {
		if (otherArgs.length == 2 && otherArgs[1].equals("-h")) {
			printHelp();
			System.exit(2);
		}else{
			int exitCode = ToolRunner.run(new Wqrs(), args);
			System.exit(exitCode);
		}
	}


}