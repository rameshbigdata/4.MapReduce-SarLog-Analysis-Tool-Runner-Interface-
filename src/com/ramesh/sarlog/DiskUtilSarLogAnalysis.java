package com.ramesh.sarlog;

 
	 
	import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

 

		public class DiskUtilSarLogAnalysis extends Configured implements Tool {	 
     public static void main(String[] args) throws Exception {
         int res = ToolRunner.run(new Configuration(), new DiskUtilSarLogAnalysis(), args);
         System.exit(res);
     }
     @Override
     public int run(String[] args) throws Exception {
			 Configuration conf = new Configuration(); 
			 
			 
				args = new String[] { 
						"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/SarLogAnalysis/data_script/data_script/logs/disk_sample_logs.txt",
						"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/SarLogAnalysis/data_script/data_script/logs/output/"};
						
				/* delete the output directory before running the job */
				FileUtils.deleteDirectory(new File(args[1]));
				if (args.length != 2) {
				System.err.println("Please specify the input and output path");
				System.exit(-1);
				}
			    Job job = Job.getInstance(conf, "MapReduce SarLogAnalysis");
			    job.setJarByClass(DiskUtilSarLogAnalysis.class);
			    job.setMapperClass(DiskUtilSarLogAnalysisMapper.class);
			    job.setReducerClass(DiskUtilSarLogAnalysisReducer.class);
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(DoubleWritable.class);
			    for (int i = 0; i < args.length - 1; ++i)
			    {
			      FileInputFormat.addInputPath(job, new Path(args[i]));
			    }
			    FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
			       // Execute job and return status
			       return job.waitForCompletion(true) ? 0 : 1;
			  }

	}
