package topn.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable> {
	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitioner) {
		// TODO Auto-generated method stub
		return (key.getOrderId().hashCode()&Integer.MAX_VALUE)%numPartitioner;
	}
}
