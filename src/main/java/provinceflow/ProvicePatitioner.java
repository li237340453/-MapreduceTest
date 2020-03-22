package provinceflow;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvicePatitioner extends Partitioner<Text, FlowBean>{
	
	static HashMap<String, Integer> map = new HashMap<String, Integer>();
	
	static {
//		map=AttributejdbcUtil.loadTable();
		map.put("135", 0);
		map.put("136", 1);
		map.put("137", 2);
		map.put("138", 3);
		map.put("139", 4);

	}


	@Override
	public int getPartition(Text key, FlowBean value, int numPatitions) {
		// TODO Auto-generated method stub
		Integer code = map.get(key.toString().substring(0, 3));
		return code==null?5:code;
	}
	
}
