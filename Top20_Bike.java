package com.cz.mobike;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Top20:取出前20(在线时间、登录次数、首次登录)
 * @author CQY
 *
 */
public class Top20_Bike {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job = Job.getInstance(conf, "top20");
		job.setJarByClass(Top20_Bike.class);
		job.setMapperClass(Top20Mapper.class);
		job.setReducerClass(Top20Reducer.class);
		job.setSortComparatorClass(Top20Sort.class);
		job.setGroupingComparatorClass(Top20Sort.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/mobike/countBike_out/part-r-00000"));
		Path outputPath = new Path("/mobike/top20Bike_out/");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
	public static class Top20Mapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			context.write(value, NullWritable.get());
		}
	}
	
	public static class Top20Reducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		int num = 0;
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			if (num++<20) {
				context.write(key, NullWritable.get());
			}
		}
	}
	
	/**
	 * 排序类  在线时间、登录次数、首次登录
	 * a b  a>b?a:b   
	 * a b c a>b?a>c?a:c:
	 * a>b?b>c?c>d?d:c:b
	 * @author lyd
	 *
	 */
	public static class Top20Sort extends WritableComparator{
		
		public Top20Sort() {
			super(Text.class,true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			
			Text aText = (Text) a;
			Text bText = (Text) b;
			
			
			String[] awords = aText.toString().split("\\s+");
			String[] bwords = bText.toString().split("\\s+");
			
			//在线时长
			Integer aTime = Integer.parseInt(awords[1]);
			Integer bTime = Integer.parseInt(bwords[1]);
			
			//登录次数
			Integer aCount = Integer.parseInt(awords[2]);
			Integer bCount = Integer.parseInt(bwords[2]);
			
			return bTime.compareTo(aTime) == 0?bCount.compareTo(aCount)==0?
					awords[3].compareTo(bwords[3]):bCount.compareTo(aCount):bTime.compareTo(aTime);
					
		}
		
		
	}

}
