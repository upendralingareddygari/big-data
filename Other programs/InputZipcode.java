import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class InputZipcode{
        
 public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> 
 {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	String input;
    	input = conf.get("Zipcode");
        String[] Zipcodelist = input.split(",");
    	String[] A = value.toString().split("::");
    	boolean flag = true;
        for(int i =0;i<Zipcodelist.length;i++)
        {
        	if(A[4].contains(Zipcodelist[i]))
        	{
        		continue;	
        	}
        	else
        	{
        		flag = false;
        	}
        }
        if (flag)
        {
        	context.write(new Text(A[0]),NullWritable.get() );
        }
   	
     }
    }
  
        
 public static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException 
    {
        context.write(key, NullWritable.get());
    }
 }
        
 public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();        
    conf.set("Zipcode", args[4]);
    Job job = new Job(conf, "Userid");
   
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setJarByClass(InputZipcode.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[4]));
    FileOutputFormat.setOutputPath(job, new Path(args[0]));
        
    job.waitForCompletion(true);
 }
        
}
