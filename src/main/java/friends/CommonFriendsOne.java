package friends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriendsOne {
	public static class CommonFriendsOneMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text k=new Text();
		private Text value=new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] split = line.split(":");
			value.set(split[0]);
			String[] friends = split[1].split(",");
			for (String friend : friends) {
				k.set(friend);
				context.write(k, value);
			}
		}
	}
	public static class CommonFriendsOneReducer extends Reducer<Text, Text, Text, Text>{
		private Text a=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			ArrayList<String> arrayList = new ArrayList<String>();
			for (Text value : values) {
				arrayList.add(value.toString());
			}
			Collections.sort(arrayList);
			for (int i = 0; i < arrayList.size()-1; i++) {
				for (int j = i+1; j < arrayList.size(); j++) {
					a.set(arrayList.get(i)+"-"+arrayList.get(j));
					context.write(a, key);
				}
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance();
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CommonFriendsOneMapper.class);
		job.setReducerClass(CommonFriendsOneReducer.class);
		job.setNumReduceTasks(1);
		job.setJarByClass(CommonFriendsOne.class);
		
		FileOutputFormat.setOutputPath(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\friends\\output"));
		FileInputFormat.setInputPaths(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\friends\\input"));
		
		job.waitForCompletion(true);
	}
}
