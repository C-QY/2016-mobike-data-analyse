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

/**
 * 每个用户的id、使用时间、使用次数、首次登陆时间
 * @author CQY
 *
 */
public class Top_User {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
		Job job = Job.getInstance(conf, "topInfo");
		job.setJarByClass(Top_User.class);

		job.setMapperClass(TopInfoMapper.class);
		job.setReducerClass(TopInfoReducer.class);
		

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.addInputPath(job, new Path("/mobike/1.txt"));
		
		Path outputPath = new Path("/mobike/countUser_out/");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	public static class TopInfoMapper extends Mapper<Text, Text, Text, Text>{
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			context.write(key, value);
			
		}
	}
	
	// 78387,158357,10080,2016/8/20 6:57,2016/8/20 7:04,7 
	public static class TopInfoReducer extends Reducer<Text, Text, Text, NullWritable>{
		
		private Text outputKey = new Text();

		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			int duration = 0;  //使用时长
			int count = 0; //使用次数
			String firstLogin = null; //首次使用时间
			
			for (Text value : values) {
				count++;
				duration+=Integer.parseInt(value.toString().split(",")[4]);
				if (count == 1) {
					firstLogin = value.toString().split(",")[2];
				}
			}
			
			outputKey.set(key+"\t"+duration+"\t"+count+"\t"+firstLogin);
			context.write(outputKey, NullWritable.get());
		}
	}

}
