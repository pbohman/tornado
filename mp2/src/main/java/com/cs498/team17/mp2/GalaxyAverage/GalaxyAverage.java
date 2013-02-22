package com.cs498.team17.mp2.GalaxyAverage;


import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;


public class GalaxyAverage {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
				throws IOException 
		{
			
			String galaxy = "";
			String tag = "";
			float avgMass = 0;
			float avgDistance = 0;
			float avgDiameter = 0;
			float avgRotation = 0;
			
			float sum = 0;
			float count = 0;

			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			String metric = "";

			while (tokenizer.hasMoreTokens()) 
			{
				String token = tokenizer.nextToken();
				if (token.startsWith("galaxy"))
				{
					
					galaxy = tokenizer.nextToken();
					continue;
				}
				
				else if (token.startsWith("mass") || token.startsWith("distance") || token.startsWith("diameter") || token.startsWith("rotation")
					)
				{

					if (metric.startsWith("mass"))
						avgMass = sum/count;
					else if (metric.startsWith("distance"))
						avgDistance = sum/count;
					else if (metric.startsWith("diameter"))
						avgDiameter = sum/count;
					else if (metric.startsWith("rotation"))
						avgRotation = sum/count;
					
					sum = 0;
					count = 0;
					metric = token.replaceAll(":", "");
					continue;
				}
				
				sum += Float.parseFloat(token);
				count++;
			}
			
			if(metric.equals("mass")){
				avgMass = sum/count;
			}
			else if(metric.equals("distance")){
				avgDistance = sum/count;
			}
			else if(metric.equals("diameter")){
				avgDiameter = sum/count;
			}
			else if(metric.equals("rotation")){
				avgRotation = sum/count;
			}
			
			StringBuilder outValue = new StringBuilder("massavg: " + avgMass + " distanceavg: " + avgDistance + " diameteravg: " + avgDiameter +"rotationavg: " + avgRotation); 
			output.collect( new Text(galaxy), new Text(outValue.toString()));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer <Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException{
			float avgMass = 0;
			float avgDistance = 0;
			float avgDiameter = 0;
			float avgRotation = 0;
			
			float sum = 0;
			float count = 0;
			String metric = "";
			output.collect(new Text(key), new Text(values.next()));
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(GalaxyAverage.class);
		conf.setJobName("mp2-team17-GalaxyAverage");
		
		// Output = [word: filenum-occurrence, ...]
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		//ChainMapper.addMapper(conf, Map.class, LongWritable.class, Text.class, Text.class, FloatWritable.class, true, new JobConf(false));
		//ChainReducer.setReducer(conf, Reduce.class, Text.class, FloatWritable.class, Text.class, Text.class, true, new JobConf(false));
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
