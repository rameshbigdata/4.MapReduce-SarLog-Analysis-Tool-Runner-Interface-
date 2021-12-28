package com.ramesh.sarlog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SarLogAnalysisMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
{

	public void map(LongWritable offset,Text line,Context con) throws IOException,InterruptedException
	{ 
		double cpuutil=0;
		   StringTokenizer stLine = new StringTokenizer(line.toString(), " "); 
			  List<String> elements = new ArrayList<String>();
			  while(stLine.hasMoreTokens()) { 
		            elements.add(stLine.nextToken());
		        } 
			  
			  if(!elements.get(0).contains("HostName"))
			  {
				  cpuutil=100.00f - Double.parseDouble(elements.get(9));
				  con.write(new Text(elements.get(0)+'-'+elements.get(1).substring(0,6)), new DoubleWritable(cpuutil));
			  } 
	 
		
		
	}

}
