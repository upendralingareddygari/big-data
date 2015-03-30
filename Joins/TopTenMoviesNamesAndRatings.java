import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TopTenMoviesNamesAndRatings {
	
	public static class MoviesRatingsMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		DoubleWritable one = new DoubleWritable();		

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String textLine = value.toString();
			String[] splitValues = textLine.split("::");
			
			int rating = Integer.parseInt(splitValues[2]);
			one.set(rating);
			context.write(new Text(splitValues[1]), one);
		}
	}

	public static class MoviesRatingsReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		DoubleWritable one = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			Double count = 0.0;
			Double total = 0.0;
			for (DoubleWritable val : values) {
				count++;
				total = total + val.get();
			}
			total = total / count;
			one.set(total);
			context.write(key, one);
		}
	}

	public static class TopTenMoviesMap extends Mapper<LongWritable, Text, Text, Text> {
		private TreeMap<String, Text> mapToRecord = new TreeMap<String, Text>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Text one = new Text();
			String line = value.toString();
			String[] tokens = line.split("\t");
			one.set(tokens[0]);
			mapToRecord.put(tokens[1] + "$" + tokens[0], one);
			if (mapToRecord.size() > 10) {
				mapToRecord.remove(mapToRecord.firstKey());
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<String, Text> t : mapToRecord.entrySet()) {
				String keyAgain = t.getKey().substring(0, t.getKey().indexOf("$"));
				context.write(new Text(keyAgain), new Text(t.getValue()));
			}
		}
	}

	public static class TopTenMoviesReduce extends Reducer<Text, Text, Text, Text> {
		Stack<String> stack = new Stack<String>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				stack.push(key.toString());
				stack.push(value.toString());
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(!stack.empty()){
				context.write(new Text(stack.pop()), new Text(stack.pop()));
			}
		}
	}
	
	public static class TopTenMovieRatingsMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			context.write(new Text(tokens[0]), new Text("L "+tokens[0]+" " + tokens[1]));
		}
	}

	public static class TopTenMoviesNamesMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("::");
			context.write(new Text(tokens[0]), new Text("M "+tokens[0]+" " + tokens[1]));
		}
	}
	
	public static class TopTenMoviesNamesAndRatingsReducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<Text> averageRatings = new ArrayList<Text>();
			ArrayList<Text> movieNames = new ArrayList<Text>();
				
			for (Text val : values) {
				String value = val.toString();
				if(value.charAt(0) == 'L') {
					averageRatings.add(new Text(value.substring(2)));
				} 
				else if (val.charAt(0) == 'M'){
					movieNames.add(new Text(value.substring(2)));
				}
			}
				
			for(Text rating : averageRatings) {
				String ratingsArray[] = rating.toString().split(" ");
				for(Text movie: movieNames) {
					String movieName[] = rating.toString().split(" ");
					if(ratingsArray[0].equals(movieName[0])) {
						context.write(movie, new Text(""));
					}
				}					
			}
			
		}

	}
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 4) {
			System.err.println("Usage of TopTenMoviesNamesAndRatings <ratings file path> <intermediate out folder path> <Movies file path> <Main output path>");
			System.exit(2);
		}

		
		Path outputDirIntermediate = new Path("temp");
		Configuration conf = new Configuration();
		Job moviesRatingJob = new Job(conf, "MoviesPlusRatings");
		moviesRatingJob.setOutputKeyClass(Text.class);
		moviesRatingJob.setOutputValueClass(DoubleWritable.class);
		moviesRatingJob.setJarByClass(TopTenMoviesNamesAndRatings.class);
		moviesRatingJob.setMapperClass(MoviesRatingsMap.class);
		moviesRatingJob.setReducerClass(MoviesRatingsReduce.class);
		moviesRatingJob.setInputFormatClass(TextInputFormat.class);
		moviesRatingJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(moviesRatingJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(moviesRatingJob, outputDirIntermediate);
		int status = moviesRatingJob.waitForCompletion(true) ? 0 : 1;

		if (status == 0) {
			Configuration listOfTopTenMoviesConfig = new Configuration();
			Job topTenMoviesJob = new Job(listOfTopTenMoviesConfig, "ListOfTopTenMovies");
			topTenMoviesJob.setOutputKeyClass(Text.class);
			topTenMoviesJob.setOutputValueClass(Text.class);
			topTenMoviesJob.setJarByClass(TopTenMoviesNamesAndRatings.class);
			topTenMoviesJob.setMapperClass(TopTenMoviesMap.class);
			topTenMoviesJob.setReducerClass(TopTenMoviesReduce.class);
			topTenMoviesJob.setInputFormatClass(TextInputFormat.class);
			topTenMoviesJob.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(topTenMoviesJob, outputDirIntermediate);
			FileOutputFormat.setOutputPath(topTenMoviesJob, new Path(args[1]));

			if(topTenMoviesJob.waitForCompletion(true)) {
				Configuration topTenMoviesNamesAndRatingsJob = new Configuration();
				Job jobForFinalJob = new Job(topTenMoviesNamesAndRatingsJob, "TopTenMoviesNamesAndRatings");
				jobForFinalJob.setOutputKeyClass(Text.class);
				jobForFinalJob.setOutputValueClass(Text.class);
				jobForFinalJob.setJarByClass(TopTenMoviesNamesAndRatings.class);
				MultipleInputs.addInputPath(jobForFinalJob, new Path(args[1]), TextInputFormat.class, TopTenMovieRatingsMapper.class);				
		        MultipleInputs.addInputPath(jobForFinalJob, new Path(args[2]), TextInputFormat.class, TopTenMoviesNamesMapper.class);
		        jobForFinalJob.setReducerClass(TopTenMoviesNamesAndRatingsReducer.class);
		    	FileOutputFormat.setOutputPath(jobForFinalJob, new Path(args[3]));
		    	jobForFinalJob.waitForCompletion(true);	    	
				
			}
		}
	}
}
