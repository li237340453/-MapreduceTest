package cn.mpareduce.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobSubmitterWindowsLocal {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "file:///");
//		conf.set("mapreduce.framework.name", "local");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(JobSubmitterWindowsLocal.class);
		
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		
		job.setCombinerClass(WordcountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
