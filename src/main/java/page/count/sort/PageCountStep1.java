package page.count.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageCountStep1 {
	public static class PageCountStep1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String string = value.toString();
			String[] split = string.split(" ");
			context.write(new Text(split[1]), new IntWritable(1));
		}
	}
	public static class PageCountStep1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count=0;
			for (IntWritable value : values) {
				count+=value.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 
		 Job job = Job.getInstance(conf);
		 
//		 job.setJarByClass(PageCountStep1.class);
		 
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 
		 FileInputFormat.setInputPaths(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\input\\url"));
		 FileOutputFormat.setOutputPath(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\countout"));
		 
		 job.setMapperClass(PageCountStep1Mapper.class);
		 job.setReducerClass(PageCountStep1Reducer.class);
		 
		 job.setNumReduceTasks(3);
		 
		 job.waitForCompletion(true);
		 
	}
}
