package cn.mpareduce.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.OutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobSubmitterWindowsToYarn {
	public static void main(String[] args) throws Exception {
		
		System.setProperty("HADOOP_USER_NAME", "root");
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.80.11:9000");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname","192.168.80.11");
		conf.set("mapreduce.app-submission.cross-platform", "true");
		Job job = Job.getInstance(conf);
		
//		job.setJarByClass(JobSubmitter.class);
		job.setJar("D:/mr.jar");
		
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path path = new Path("/wordcount/output5");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
		FileOutputFormat.setOutputPath(job, new Path("/wordcount/output5"));
		
		job.setNumReduceTasks(2);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
