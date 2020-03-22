package provinceflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{
	private int upFlow;
	private int dFlow;
	private int amountFlow;
	private String phone;
	
	public FlowBean(String phone,int upFlow, int dFlow) {
		this.phone=phone;
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.amountFlow=upFlow+dFlow;
	}
	
	public int getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(int upFlow) {
		this.upFlow = upFlow;
	}
	public int getdFlow() {
		return dFlow;
	}
	public void setdFlow(int dFlow) {
		this.dFlow = dFlow;
	}
	public int getAmountFlow() {
		return amountFlow;
	}
	public void setAmountFlow(int amountFlow) {
		this.amountFlow = amountFlow;
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		upFlow=arg0.readInt();
		dFlow=arg0.readInt();
		amountFlow=arg0.readInt();
		phone=arg0.readUTF();
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(upFlow);
		arg0.writeInt(dFlow);
		arg0.writeInt(amountFlow);
		arg0.writeUTF(phone); 	 
	}
	
	public FlowBean() {
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return upFlow+","+dFlow+","+amountFlow;
	}
	

}
