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


public class IndexBuilder {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		private IntWritable docNum = new IntWritable();
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) 
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			docNum.set(Integer.parseInt(tokenizer.nextToken()));
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, docNum);
			}
		}
	} 

	public static class Reduce extends MapReduceBase implements Reducer <Text, IntWritable, Text, Text> {
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException{
			Hashtable<Integer, Integer> hash = new Hashtable<Integer, Integer>();
			while (values.hasNext()){
				int docNum = values.next().get();
				int sum = (hash.get(docNum) == null) ? 1 : ((Integer) hash.get(docNum)) + 1;
				hash.put(docNum, sum);
			}
			String index = "";
			for(int docNum : hash.keySet()){
				index += (docNum + "-" + hash.get(docNum) + " ");
			}
			output.collect(key, new Text(index));
		}


	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(IndexBuilder.class);
		conf.setJobName("mp2-team17-indexbuilder");
		
		// Output = [word: filenum-occurrence, ...]
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
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
