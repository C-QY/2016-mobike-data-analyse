package com.cz.mobike;

import java.io.IOException;
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


public class Bike {

	public static void main(String[] argS) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();  
		conf.set("fs.defaultFS", "hdfs://192.168.158.128:9000");  
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
		Job job = Job.getInstance(conf,"mobike");
		job.setJarByClass(Bike.class);  
		job.setMapperClass(mobikeMapper.class);  
		job.setReducerClass(mobikeReducer.class);  
		job.setMapOutputKeyClass(Text.class);   
		job.setMapOutputValueClass(Text.class);  
		job.setOutputKeyClass(Text.class);  
		job.setOutputValueClass(NullWritable.class);  
		job.setInputFormatClass(KeyValueTextInputFormat.class); 
		FileInputFormat.addInputPath(job, new Path("/mobike/2.txt"));   
		Path outputPath = new Path("/mobike/Bike_out/");
		FileSystem.get(conf).delete(outputPath, true); 
		FileOutputFormat.setOutputPath(job,outputPath);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
	/**
	 * 78387,158357,10080,2016/8/20 6:57,2016/8/20 7:04,7 
	  * 累计订单数、累计用户数、人均时长、次均时长
	 * @author CQY
	 */
	public static class mobikeMapper extends Mapper<Text, Text, Text, Text>{
protected void map(Text key, Text value,Context context) throws IOException ,InterruptedException {
			
			context.write(key, value);
		}
	
	}
	
	
	public static class mobikeReducer extends Reducer<Text, Text, Text, NullWritable>{
		private Text key = new Text();
		private int totalOrder; // 累计订单数
		private int totalBike;//累计用户数
		private float avg_Order;//平均一辆车的订单数
		private float avg_Bikehours;//平均一辆车的使用时长
		private float avg_ride_duration;//平均一笔订单的时长
		private long duration;  //总时长
		
		
		/**
		 * 78387,158357,10080,2016/8/20 6:57,2016/8/20 7:04,7 
		  * 24小时时间段统计
		 * @author CQY
		 */
		
		
		@Override
		protected void reduce(Text key, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			totalBike++;
			for (Text value : iterable) {
				totalOrder++;
				duration += Long.parseLong(value.toString().split(",")[4]);
			}
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
		duration=duration/60;
		avg_Order=(float)totalOrder/(float)totalBike;
		avg_Bikehours=(float)duration/(float)totalBike;
		avg_ride_duration=(float)duration/(float)totalOrder;
		key.set("totalOrder:"+totalOrder+" totalBike:"+totalBike+" duration:"+duration+"\n"
		+"avg_Order:"+avg_Order+" avg_Bikehours:"+avg_Bikehours+"avg_ride_duration:"+avg_ride_duration+"\n"
		);
		context.write(key, NullWritable.get());
		}
	
		
	}
	
	
}
