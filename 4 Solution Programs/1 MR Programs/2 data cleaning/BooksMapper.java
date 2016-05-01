



import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class BooksMapper {

	
	
	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {


		
		@Override
		public void map(LongWritable key, Text value,
				Context context)
				throws IOException,InterruptedException {
			
			String line = value.toString();
			String[] splittedLines = line.split("\";\"");//splits line into strings based on ( ";" )
		
			
		
			try{
			if(Integer.valueOf(splittedLines[3])==2002)// From the given data set 4th column is for year
				context.write( new Text(splittedLines[0].substring(1,splittedLines[0].length())),new Text(splittedLines[3]));
			}catch(NumberFormatException nfe){
				
				
			}
	
			
	}
	}



	
	public static void main(String[] args) throws Exception {

	
		Configuration conf= new Configuration();
		Job job = new Job(conf,"booksM");
		job.setJarByClass(BooksMapper.class);

		
		job.setMapperClass(Map.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	

	
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		System.exit(job.waitForCompletion(true)?0:1);

	}


	
}


