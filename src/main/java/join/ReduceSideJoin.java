package join;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import index.IndexStepOne;
import index.IndexStepOne.IndexStepOneMapper;
import index.IndexStepOne.IndexStepOneReducer;

public class ReduceSideJoin {
	public static class ReduceSideJoinMapper extends Mapper<LongWritable, Text, Text, JoinBean> {
		String name;
		JoinBean bean = new JoinBean();
		Text k = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			name = inputSplit.getPath().getName();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] fields = value.toString().split(",");
			JoinBean bean = new JoinBean();
			if (name.startsWith("order")) {
				bean.set(fields[0], fields[1], "NULL", -1, "NULL", "order");
			} else {
				bean.set("NULL", fields[0], fields[1], Integer.parseInt(fields[2]), fields[3], "user");
			}
			k.set(bean.getUserId());
			context.write(k, bean);
		}
	}

	public static class ReduceSideJoinReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable> {
		ArrayList<JoinBean> arrayList = new ArrayList<JoinBean>();
		JoinBean userbean;

		@Override
		protected void reduce(Text key, Iterable<JoinBean> values,
				Reducer<Text, JoinBean, JoinBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			try {
				for (JoinBean joinBean : values) {
					if ("order".equals(joinBean.getTableName())) {
						JoinBean bean = new JoinBean();
						BeanUtils.copyProperties(bean, joinBean);
						arrayList.add(bean);
					} else {
						userbean = new JoinBean();
						BeanUtils.copyProperties(userbean, joinBean);
					}
				}
				for (JoinBean joinBean : arrayList) {
					joinBean.setUserAge(userbean.getUserAge());
					joinBean.setUserFriend(userbean.getUserFriend());
					joinBean.setUserId(userbean.getUserId());
					joinBean.setUserName(userbean.getUserName());
					context.write(joinBean, NullWritable.get());
				}
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance();

		job.setJarByClass(IndexStepOne.class);
		job.setOutputKeyClass(JoinBean.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(JoinBean.class);

		job.setMapperClass(ReduceSideJoinMapper.class);
		job.setReducerClass(ReduceSideJoinReducer.class);
		
		job.setNumReduceTasks(2);
		
		FileInputFormat.setInputPaths(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\join\\input"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\join\\output"));
		
		job.waitForCompletion(true);
	}
}
