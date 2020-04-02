package friends;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriendsTwo {
	public static class CommonFriendsTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text k=new Text();
		private Text v=new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] split = line.split("\t");
			k.set(split[0]);
			v.set(split[1]);
			context.write(k, v);
		}
	}
	public static class CommonFriendsTwoReducer extends Reducer<Text, Text, Text, Text>{
		private Text v=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringBuilder stringBuilder = new StringBuilder();
			for (Text value : values) {
				stringBuilder.append(value.toString()).append(' ');
			}
			v.set(stringBuilder.toString());
			context.write(key, v);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance();
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CommonFriendsTwoMapper.class);
		job.setReducerClass(CommonFriendsTwoReducer.class);
		job.setJarByClass(CommonFriendsTwo.class);
		
		FileInputFormat.setInputPaths(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\friends\\output"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\friends\\output1"));
		
		job.waitForCompletion(true);
		
	}
}
