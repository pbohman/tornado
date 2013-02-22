
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

			StringTokenizer tokenizer = new StringTokenizer(value.toString());

			while (tokenizer.hasMoreTokens()) 
			{
				String token = tokenizer.nextToken();
				if (token.startsWith("galaxy"))
				{
					galaxy = tokenizer.nextToken().trim();
				}

				else
				{
					output.collect(	new Text(galaxy),
									new Text(value.toString())
									);
					continue;
				}
				
			}
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
			
			while (values.hasNext()){
				StringTokenizer tokenizer = new StringTokenizer(values.next().toString());

				while (tokenizer.hasMoreTokens()) 
				{
					String token = tokenizer.nextToken();
					if (token.startsWith("galaxy"))
					{
						tokenizer.nextToken();
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
			}
			
			String outKey = String.format("%s %s ", "galaxy:", key.toString());
			String outValue = String.format("massavg: %s distanceavg: %s diameteravg: %s rotationavg: %s", 
					avgMass, avgDistance, avgDiameter, avgRotation);
				
			output.collect(new Text(outKey), new Text(outValue));
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
