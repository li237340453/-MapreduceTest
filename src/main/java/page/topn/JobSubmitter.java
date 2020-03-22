package page.topn;

import java.io.IOException;
import java.util.Properties;

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
		
		/*
		 *  通过加载classpath下的core-default.xml、core-site.xml、hdfs-site.xml、hdfs-default.xml文件解析参数
		 */
		Configuration conf = new Configuration();
//		conf.addResource(file);
		
		
		/*
		 * 挺难过过porp文件加载
		 * 
		 */
//		Properties props = new Properties();
//		props.load(JobSubmitter.class.getResourceAsStream("topn.properties"));
//		conf.setInt("top.n", Integer.parseInt(props.getProperty("top.n")));
		
		
		/*
		 * 通过代码设置参数
		 */
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
