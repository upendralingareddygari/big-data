import java.io.IOException;
import java.util.Stack;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopTenAverageRatedMovies {
	public static class MoviesRatingsMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		DoubleWritable one = new DoubleWritable();		

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("::");
			
			int movieRating = Integer.parseInt(tokens[2]);
			one.set(movieRating);
			context.write(new Text(tokens[1]), one);
		}
	}

	public static class MoviesRatingsReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		DoubleWritable one = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			Double ratingCount = 0.0;
			Double ratingSum = 0.0;
			for (DoubleWritable val : values) {
				ratingCount++;
				ratingSum = ratingSum + val.get();
			}
			ratingSum = ratingSum / ratingCount;
			one.set(ratingSum);
			context.write(key, one);
		}
	}

	public static class TopTenMoviesMap extends Mapper<LongWritable, Text, Text, Text> {

		Text word = new Text();
		Double val;
		private TreeMap<String, Text> repToRecordMap = new TreeMap<String, Text>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Text one = new Text();
			String line = value.toString();
			String[] tokens = line.split("\t");

			one.set(tokens[0]);

			repToRecordMap.put(tokens[1] + "|" + tokens[0], one);

			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {

			for (Entry<String, Text> t : repToRecordMap.entrySet()) {
				String keyAgain = t.getKey().substring(0, t.getKey().indexOf("|"));
				context.write(new Text(keyAgain), new Text(t.getValue()));
			}
		}
	}

	public static class TopTenMoviesReduce extends Reducer<Text, Text, Text, Text> {

		Stack<String> stack = new Stack<String>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				stack.push(key.toString());
				stack.push(val.toString());
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(!stack.empty()){
				context.write(new Text(stack.pop()), new Text(stack.pop()));
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Path outputDirIntermediate = new Path("temp");

		Configuration conf = new Configuration();
		Job moviesRatingJob = new Job(conf, "MoviesRatings");

		moviesRatingJob.setOutputKeyClass(Text.class);
		moviesRatingJob.setOutputValueClass(DoubleWritable.class);
		moviesRatingJob.setJarByClass(TopTenAverageRatedMovies.class);
		moviesRatingJob.setMapperClass(MoviesRatingsMap.class);
		moviesRatingJob.setReducerClass(MoviesRatingsReduce.class);

		moviesRatingJob.setInputFormatClass(TextInputFormat.class);
		moviesRatingJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(moviesRatingJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(moviesRatingJob, outputDirIntermediate);

		int status = moviesRatingJob.waitForCompletion(true) ? 0 : 1;

		if (status == 0) {
			Configuration conf1 = new Configuration();
			Job topTenMoviesJob = new Job(conf1, "TopTenMovies");

			topTenMoviesJob.setOutputKeyClass(Text.class);
			topTenMoviesJob.setOutputValueClass(Text.class);
			topTenMoviesJob.setJarByClass(TopTenAverageRatedMovies.class);
			topTenMoviesJob.setMapperClass(TopTenMoviesMap.class);
			topTenMoviesJob.setReducerClass(TopTenMoviesReduce.class);

			topTenMoviesJob.setInputFormatClass(TextInputFormat.class);
			topTenMoviesJob.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(topTenMoviesJob, outputDirIntermediate);
			FileOutputFormat.setOutputPath(topTenMoviesJob, new Path(args[1]));

			topTenMoviesJob.waitForCompletion(true);
		}
	}
}
