package index.sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexStepTwo {
	public static class IndexStepTwoMapper extends Mapper<Text, IntWritable, Text, Text> {
		@Override
		protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] split = key.toString().split("-");
			context.write(new Text(split[0]), new Text(split[1]+"-->"+value));
		}
	}

	public static class IndexStepTwoReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				sb.append(value.toString()).append("\t");
			}
			context.write(key, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance();

		job.setJarByClass(IndexStepTwo.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(IndexStepTwoMapper.class);
		job.setReducerClass(IndexStepTwoReducer.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\index\\output-seq-1"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\index\\output-seq-2"));
		
		job.waitForCompletion(true);

	}
}
