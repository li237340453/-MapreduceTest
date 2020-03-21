package page.topn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.mpareduce.test.JobSubmitterWindowsLocal;
import cn.mpareduce.test.WordcountMapper;
import cn.mpareduce.test.WordcountReducer;

public class JobSubmitter {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		conf.setInt("top.n", 3);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(JobSubmitter.class);
		
		job.setMapperClass(PageTopnMapper.class);
		job.setReducerClass(PageTopnReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\input\\url"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\output"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
