import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Music_2
{
	public static class mapperClass extends Mapper<Object,Text,Text,Text>
	{
		public void map(Object key,Text value,Context context) throws InterruptedException,IOException
		{
			StringTokenizer itr=new StringTokenizer(value.toString(),",");
			while(itr.hasMoreTokens())
			{
				Text user=new Text(itr.nextToken());
				Text track=new Text(itr.nextToken());
				itr.nextToken();
				itr.nextToken();
				itr.nextToken();
				if(user.charAt(0)=='U')
				{
					
				}
				else
				{
					context.write(track,user);
				}
				
			}
			
		}
	}
	
	public static class reducerClass extends Reducer<Text,Text,Text,IntWritable>
	{
		public void reduce(Text key,Iterable<Text> values,Context context) throws InterruptedException,IOException
		{
			Set<Text> s = new HashSet <Text>();
			for(Text user:values)
			{
				s.add(user);
			}
			context.write(key,new IntWritable(s.size()));
			
		}
//		public void cleanup(Context context) throws InterruptedException,IOException
//		{
//			context.write(new Text("Total Users"), new IntWritable(s.size()));
//		}
	}
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "music 2");
		job.setJarByClass(Music_2.class);
		job.setMapperClass(mapperClass.class);
//		job.setCombinerClass(reducerClass.class);
		job.setReducerClass(reducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}