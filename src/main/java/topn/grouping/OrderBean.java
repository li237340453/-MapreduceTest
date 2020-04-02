package topn.grouping;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean>{
	private String orderId;
	private String userId;
	private String pdtName;
	private float price;
	private int number;
	private float amountFee;
	public void set(String orderId, String userId, String pdtName, float price, int number) {
		this.orderId = orderId;
		this.userId = userId;
		this.pdtName = pdtName;
		this.price = price;
		this.number = number;
		this.amountFee=price*number;
	}
	public float getAmountFee() {
		return amountFee;
	}
	public void setAmountFee(float amountFee) {
		this.amountFee = amountFee;
	}
	public String getOrderId() {
		return orderId;
	}
	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getPdtName() {
		return pdtName;
	}
	public void setPdtName(String pdtName) {
		this.pdtName = pdtName;
	}
	public float getPrice() {
		return price;
	}
	public void setPrice(float price) {
		this.price = price;
	}
	public int getNumber() {
		return number;
	}
	public void setNumber(int number) {
		this.number = number;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return orderId+','+userId+','+pdtName+','+price+','+number+','+amountFee;
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		orderId=in.readUTF();
		userId=in.readUTF();
		pdtName=in.readUTF();
		price=in.readFloat();
		number=in.readInt();
		amountFee=price*number;
	}
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(orderId);
		out.writeUTF(userId);
		out.writeUTF(pdtName);
		out.writeFloat(price);
		out.writeInt(number);
	}
	
	public int compareTo(OrderBean o) {
		// TODO Auto-generated method stub
		return (orderId.compareTo(o.getOrderId())==0)?Float.compare(o.amountFee, this.getAmountFee()):orderId.compareTo(o.getOrderId());
		
	}
	
	
}
