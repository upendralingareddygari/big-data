import java.io.*;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
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

public class Question1NumberOfMaleUsers {

	public static class Question1NumberOfMaleUsersMapper extends Mapper<LongWritable, Text, Text, IntWritable> implements JobConfigurable {
		private HashMap<String, Integer> mapForUsers = new HashMap<String, Integer>();
		private String idOfMovie;
		private static final String maleIndicator= "M";

		public void setup(Context context) throws IOException, InterruptedException {
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());

			for (Path path : files) {
				File myFile = new File(path.getName());
				BufferedReader joinReader = new BufferedReader(new FileReader(myFile));

				String lineFromFile = null;
				while ((lineFromFile = joinReader.readLine()) != null) {
					String[] splitArray = lineFromFile.split("::");
					if (splitArray[1].equals(maleIndicator)) {
						mapForUsers.put(splitArray[0], 1);
					}
				}
			}
		}

		@Override
		public void configure(JobConf conf) {
			idOfMovie = conf.get("MovieId");
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("::");
			
			if (tokens[1].equals(idOfMovie) && mapForUsers.get(tokens[0]) != null) {
				context.write(new Text(tokens[1]), new IntWritable(1));
			}			
		}
	}

	public static class Question1NumberOfMaleUsersReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int total = 0;
			for(IntWritable value : values) {
				total++;
			}	
			context.write(key, new IntWritable(total));
		}
	}
	
	public static void main(String[] args) throws Exception {

		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage of Question1NumberOfMaleUsers <ratings file path> <out folder path> <users file path> <inputmovieId>");
			System.exit(2);
		}

		config.set("MovieId", otherArgs[3]);

		DistributedCache.addCacheFile(new URI(otherArgs[2]), config);

		Job job = new Job(config, "MapSideJoin");
		
		job.setJarByClass(Question1NumberOfMaleUsers.class);
		job.setMapperClass(Question1NumberOfMaleUsersMapper.class);
		job.setReducerClass(Question1NumberOfMaleUsersReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}