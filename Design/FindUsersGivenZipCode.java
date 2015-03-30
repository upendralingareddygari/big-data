import java.util.*;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FindUsersGivenZipCode {
	
	public static class FindUsersGivenZipCodeMapper extends Mapper<LongWritable, Text, Text, IntWritable> implements JobConfigurable{
		private final static IntWritable one = new IntWritable(1);
        private static String zipCode;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
        	String[] entries = value.toString().split("::");
            if(entries[4].contains(zipCode)) {
            	context.write(new Text(entries[0]), one);
            }                       
        }

		@Override
		public void configure(JobConf conf) {
			zipCode = conf.get("ZipCode");
		}
	}

    public static class FindUsersGivenZipCodeReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
    	private IntWritable dummyValue = new IntWritable(1);
        public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {
        	context.write(key, dummyValue);
        }       
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    	Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        // get all args
        if(otherArgs.length != 3) {
        	System.err.println("Usage: FindUsersGivenZipCode <in> <out> <zipCode>");
        	System.exit(2);
        }
    		
        	
        config.set("ZipCode", otherArgs[2]);

        Job job = new Job(config, "FindUsersGivenZipCode");
        job.setJarByClass(FindUsersGivenZipCode.class);
        job.setMapperClass(FindUsersGivenZipCodeMapper.class);
        job.setReducerClass(FindUsersGivenZipCodeReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
                
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
                
        job.waitForCompletion(true);    
    }
}

                