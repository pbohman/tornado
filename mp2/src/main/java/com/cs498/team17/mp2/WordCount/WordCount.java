package com.cs498.team17.mp2.WordCount;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;

import com.cs498.team17.mp2.IndexBuilder.IndexBuilder.Map;
import com.cs498.team17.mp2.IndexBuilder.IndexBuilder.Reduce;


public class WordCount {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) 
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	} 

	public static class Reduce extends MapReduceBase implements Reducer <Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException{
			
			int sum = 0;
			while (values.hasNext()){
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("mp2-team17-wordcount");
		
		// Output = [word: count, word: count ...]
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
