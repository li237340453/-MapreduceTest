package provinceflow;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int upSum=0;
		int dSum=0;
		for (FlowBean value : values) {
			upSum+=value.getUpFlow();
			dSum+=value.getdFlow();
		}
		context.write(key, new FlowBean(key.toString(), upSum, dSum));
	}
}
