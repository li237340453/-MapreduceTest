package topn.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderIdGroupingComparator extends WritableComparator{
	
	public OrderIdGroupingComparator() {
		// TODO Auto-generated constructor stub
		super(OrderBean.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		OrderBean aa=(OrderBean)a;
		OrderBean bb=(OrderBean)b;
		return aa.getOrderId().compareTo(bb.getOrderId());
	}
}
