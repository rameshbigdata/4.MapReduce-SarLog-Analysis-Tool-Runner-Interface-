package com.ramesh.sarlog;

 

	import java.io.IOException;

	import org.apache.hadoop.io.DoubleWritable; 
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Reducer;

	public class DiskUtilSarLogAnalysisReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{

		/**
		 * @param args
		 */
		public  void reduce(Text word,Iterable<DoubleWritable> values,Context con) throws IOException,InterruptedException
		{
			double total=0;
			int count=0;
			// TODO Auto-generated method stub
			for (DoubleWritable val : values)
			{ 
				count=count+1;
				total=total+val.get();
			}
			
			con.write(word, new DoubleWritable(total/count));
		}

	}