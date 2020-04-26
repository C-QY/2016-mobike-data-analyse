package com.cz.mobike;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Arrays;
/**
 * Top20:取出前20(在线时间、登录次数、首次登录)
 * @author CQY
 *
 */
public class User_count {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job = Job.getInstance(conf, "top20");
		job.setJarByClass(User_count.class);
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setMapOutputKeyClass(Text.class);   
		job.setMapOutputValueClass(Text.class);  
		job.setOutputKeyClass(Text.class);  
		job.setOutputValueClass(NullWritable.class);  
		job.setInputFormatClass(KeyValueTextInputFormat.class); 
		FileInputFormat.addInputPath(job, new Path("/mobike/countUser_out/part-r-00000"));
		Path outputPath = new Path("/mobike/Time_count_User_out/");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
	public static class CountMapper extends Mapper<Text, Text, Text, Text>{
		
		protected void map(Text key, Text value,Context context) throws java.io.IOException ,InterruptedException {
			context.write(key, value);
		}
	}
	
	
	//*  1	87	5	2016/8/26 13:52
	public static class CountReducer extends Reducer<Text, Text, Text, NullWritable>{
		private Text key = new Text();
		private int Time_count[] = new int [26];
		private int i;
		protected void reduce(Text key, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			for (Text value : iterable) {
				i=Integer.parseInt(value.toString().split("\\s+")[1]);
				Time_count[i]++;
			}
			
			
		}
		
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			key.set("Time_count:"+Arrays.toString(Time_count));
			context.write(key, NullWritable.get());
		}
	
	}
	

}
