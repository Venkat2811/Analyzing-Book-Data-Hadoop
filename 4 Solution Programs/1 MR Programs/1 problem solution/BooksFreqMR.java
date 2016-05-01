



import java.io.IOException;
import java.lang.String;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 
 * @author Venkatraman
 * 
 * Map Reduce Program to find frequency of books published each year
 * 
 * Map Reduce Program using Gen2 (YARN)
 *
 */
public class BooksFreqMR {

	/**
	 * 
	 * @author Venkatraman
	 * 
	 * Mapper class uses the same logic as word count program. But data cleaning is needed and its done here 
	 *
	 */
	
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {


		
		@Override
		public void map(LongWritable key, Text value,
				Context context)
				throws IOException,InterruptedException {
		
			String line = value.toString();
			String[] splittedLines = line.split("\";\"");//splits line into strings based on ( ";" )
		
			
		
			try{
			if(Integer.valueOf(splittedLines[3]) > 0)// From the given data set 4th column is for year
				context.write( new Text(splittedLines[3]),new IntWritable(1));
			}catch(NumberFormatException nfe){
				
				
			}
		
	}
	}
	
	/**
	 * 
	 * @author Venkatraman
	 * 
	 * Counting the occurrence of each year is done here. Same logic as word count program
	 *
	 */

	public static class Reduce extends
	Reducer<Text, IntWritable, Text, IntWritable> {


public void reduce(Text key, Iterable<IntWritable>values,
		Context context)
		throws IOException,InterruptedException {
	
	int sum=0;
	
	for(IntWritable x: values)
		sum+=x.get();
	
	context.write(key, new IntWritable(sum));

		
	
}
}

	
	public static void main(String[] args) throws Exception {

	
		Configuration conf= new Configuration();
		Job job = new Job(conf,"BooksFreqPerYear");
		job.setJarByClass(BooksFreqMR.class);

		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	

	
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		System.exit(job.waitForCompletion(true)?0:1);

	}


	
}


