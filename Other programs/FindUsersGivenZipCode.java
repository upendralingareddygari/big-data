import java.util.*;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FindUsersGivenZipCode {

	public static class FindUsersGivenZipCodeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static NullWritable dummyEntry = NullWritable.get();

		public void map(LongWritable key, Text value, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException { 
			String[] entries = value.toString().split("::");
			if(entries[4].contains(context.getConfiguration().get("ZipCode"))) {
				Output.collect(entries[0], dummyEntry);
			}			
		}
	}

	public static class FindUsersGivenZipCodeReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> { 
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			output.collect(key, NullWritable.get());
		}	
	}

	
	public static void main(String[] args) {
		Configuration config = new Configuration();
		conf.set("ZipCode", args[2]);

		Job job = new Job(config, "FindUsersGivenZipCode");
		job.setJarByClass(FingUsersGivenZipCode.class);
		job.setMapperClass(FindUsersGivenZipCodeMapper.class);
		job.setReducerClass(FindUsersGivenZipCodeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);	
	}
}
