package page.count.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PageCount implements WritableComparable<PageCount>{
	private String page;
	private int count;
	
	
	public void set(String page, int count) {
		this.page = page;
		this.count = count;
	}
	public String getPage() {
		return page;
	}
	public void setPage(String page) {
		this.page = page;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	public int compareTo(PageCount o) {
		// TODO Auto-generated method stub
		return o.getCount()-this.getCount()==0?this.page.compareTo(o.page):o.getCount()-this.getCount();
	}
	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(page);
		out.writeInt(count);
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		page = in.readUTF();
		count = in.readInt();
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return page+","+count;
	}
	
}
