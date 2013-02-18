package com.cs498.team17.mp2;

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


public class GalaxyAverage {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
		private Text word = new Text();
		private FloatWritable measureNum = new FloatWritable();
		public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) 
				throws IOException 
		{
			
			String galaxy = "";

			StringTokenizer tokenizer = new StringTokenizer(value.toString());

			while (tokenizer.hasMoreTokens()) 
			{
				String token = tokenizer.nextToken();
				if (token.startsWith("galaxy"))
				{
					galaxy = tokenizer.nextToken().trim();
				}
				else if (token.startsWith("mass") 
						|| token.startsWith("distance")
						|| token.startsWith("diameter")
						|| token.startsWith("rotation")
						)
				{
					word.set(galaxy + "_avg" + token);
				}
				else
				{
					measureNum.set(Float.parseFloat(token));
	
					output.collect(word, measureNum);
				}
				
			}
		}
	}	

	public static class Reduce extends MapReduceBase implements Reducer <Text, FloatWritable, Text, FloatWritable> {
		
		public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter)
				throws IOException{
			float sum = 0;
			float count = 0;
			
			while (values.hasNext()){
				sum += values.next().get();
				count++;
			}
			
			output.collect(key, new FloatWritable(sum/count));
		}


	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(GalaxyAverage.class);
		conf.setJobName("mp2-team17-GalaxyAverage");
		
		// Output = [word: filenum-occurrence, ...]
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(FloatWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FloatWritable.class);
		
		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
