package flow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] fields = line.split("\t");
		String phone=fields[1];
		int upFlow=new Integer(fields[fields.length-3]);
		int dFlow=new Integer(fields[fields.length-2]);
		
		context.write(new Text(phone), new FlowBean(phone, upFlow, dFlow));		
	}
}
