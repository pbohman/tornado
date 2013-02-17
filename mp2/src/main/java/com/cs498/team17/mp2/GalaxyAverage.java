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
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		private IntWritable measureNum = new IntWritable();
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) 
				throws IOException 
		{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			String token = "";
			String galaxy = "";
			String measure = "";
			
			while (tokenizer.hasMoreTokens()) 
			{
				token = tokenizer.nextToken();
				if (token.contentEquals("galaxy:"))
				{
					galaxy = tokenizer.nextToken();
					continue;
				}
				if (token.contentEquals("mass:"))
				{
					measure = "avgmass";
					continue;
				}
				if (token.contentEquals("distance:"))
				{
					measure = "avgdistance";
					continue;
				}
				if (token.contentEquals("diameter:"))
				{
					measure = "avgdiameter";
					continue;
				}
				if (token.contentEquals("rotation:"))
				{
					measure = "avgrotation";
					continue;
				}
				
				measureNum.set(Integer.parseInt(tokenizer.nextToken()));
				while (tokenizer.hasMoreTokens()) {
					word.set(tokenizer.nextToken());
					output.collect(word, measureNum);
				}
			}
		}
	}	

	public static class Reduce extends MapReduceBase implements Reducer <Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException{
			int sum = 0;
			int count = 0;
			
			while (values.hasNext()){
				sum += values.next().get();
				count++;
			}
			
			output.collect(key, new IntWritable(sum/count));
		}


	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(GalaxyAverage.class);
		conf.setJobName("mp2-team17-GalaxyAverage");
		
		// Output = [word: filenum-occurrence, ...]
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
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
