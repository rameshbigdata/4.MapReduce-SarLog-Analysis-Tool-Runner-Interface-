package com.ramesh.sarlog;

 
	 
	import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

	public class MemUtilSarLogAnalysis {

		/**
		 * @param args
		 */
		 public static void main(String[] args) throws Exception
		 {
			 Configuration conf = new Configuration(); 
			 
			 
				args = new String[] { 
						"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/SarLogAnalysis/data_script/data_script/logs/memory_sample_logs.txt",
						"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/SarLogAnalysis/data_script/data_script/logs/output/"};
						
				/* delete the output directory before running the job */
				FileUtils.deleteDirectory(new File(args[1]));
				if (args.length != 2) {
				System.err.println("Please specify the input and output path");
				System.exit(-1);
				}
			    Job wcJob = Job.getInstance(conf, "MapReduce SarLogAnalysis");
			    wcJob.setJarByClass(MemUtilSarLogAnalysis.class);
			    wcJob.setMapperClass(MemUtilSarLogAnalysisMapper.class);
			    wcJob.setReducerClass(MemUtilSarLogAnalysisReducer.class);
			    wcJob.setOutputKeyClass(Text.class);
			    wcJob.setOutputValueClass(DoubleWritable.class);
			    for (int i = 0; i < args.length - 1; ++i)
			    {
			      FileInputFormat.addInputPath(wcJob, new Path(args[i]));
			    }
			    FileOutputFormat.setOutputPath(wcJob, new Path(args[args.length - 1]));
			    System.exit(wcJob.waitForCompletion(true) ? 0 : 1);
			  }

	}
