



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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class UsersMapper {

	
	
	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {


		
		@Override
		public void map(LongWritable key, Text value,
				Context context)
				throws IOException,InterruptedException {
			String[] line= value.toString().split("\n");
			int  lineLength = line[0].length();
			String userId="",location="",age="";
			int i=0;
			int semicolonCounter=0;
			for(;i<lineLength;++i)
			{
				if(line[0].charAt(i)==';')
				{
					//skip
					semicolonCounter++;
				}
				else if(line[0].charAt(i)=='"')
				{
					//skip 
					
				}
				
				else if(semicolonCounter<1)
					userId+=line[0].charAt(i);
				else if(semicolonCounter<2)
					location+=line[0].charAt(i);
				else 
				    age+=line[0].charAt(i);
					
			}
			
	
				context.write(new Text(userId), new Text(location+"\t"+age));
				
				
				
			
			
	}
	}



	
	public static void main(String[] args) throws Exception {

	
		Configuration conf= new Configuration();
		Job job = new Job(conf,"usersM");
		job.setJarByClass(UsersMapper.class);

		
		job.setMapperClass(Map.class);
		//job.setReducerClass(Reduce.class);

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	

	
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		System.exit(job.waitForCompletion(true)?0:1);

	}
}


