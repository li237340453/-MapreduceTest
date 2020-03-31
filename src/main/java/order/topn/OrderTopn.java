package order.topn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderTopn {
	public static class OrderTopnMapper extends Mapper<LongWritable, Text, Text, OrderBean>{
		private OrderBean ob=new OrderBean();
		private Text text=new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] split = line.split(",");
			text.set(split[0]);
			ob.set(split[0], split[1], split[2], Float.parseFloat(split[3]), Integer.parseInt(split[4]));
			context.write(text, ob);
		}
	}
	public static class OrderTopnReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int num = context.getConfiguration().getInt("order.topn.n", 3);
			ArrayList<OrderBean> list = new ArrayList<OrderBean>(); 
			for (OrderBean orderBean : values) {
				OrderBean ob=new OrderBean();
				ob.set(orderBean.getOrderId(), orderBean.getUserId(), orderBean.getPdtName(), orderBean.getPrice(), orderBean.getNumber());
				list.add(ob);
			}
			Collections.sort(list);
			
			for (int i = 0; i < num; i++) {
				context.write(list.get(i), NullWritable.get());
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("order.topn.n", 2);
		Job job = Job.getInstance(conf);
		job.setNumReduceTasks(2);
		
		job.setJarByClass(OrderBean.class);
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OrderBean.class);
		job.setMapperClass(OrderTopnMapper.class);
		job.setReducerClass(OrderTopnReducer.class);
		
		FileInputFormat.setInputPaths(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\order\\input"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\code\\eclipse-workspace\\mapreducetest\\data\\order\\output"));
		
		job.waitForCompletion(true);
	}
}
