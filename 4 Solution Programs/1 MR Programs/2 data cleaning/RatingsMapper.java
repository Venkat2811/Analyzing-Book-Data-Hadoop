



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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class RatingsMapper {

	
	
	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {


		
		@Override
		public void map(LongWritable key, Text value,
				Context context)
				throws IOException,InterruptedException {
		
			String line = value.toString();
			
			String[] splittedLines = line.split("\";\"");//splits line into strings based on ( ";" )
	
		if(splittedLines.length==3)
		{
			
			try{
			if(Integer.valueOf(splittedLines[2].substring(0,splittedLines[2].length()-1))>0)
				context.write(new Text(splittedLines[1].toString()), new Text(splittedLines[2].substring(0,splittedLines[2].length()-1)));
				
			}catch(NumberFormatException nfe){
				
				
			}	
				
		}
			
	
			
			
	}
	}





	
	public static void main(String[] args) throws Exception {

	
		Configuration conf= new Configuration();
		Job job = new Job(conf,"ratingsM");
		job.setJarByClass(RatingsMapper.class);


		
		job.setMapperClass(Map.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		

	
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		System.exit(job.waitForCompletion(true)?0:1);

	}
}


