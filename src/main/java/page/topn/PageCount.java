package page.topn;import org.apache.hadoop.yarn.webapp.view.HtmlPage.Page;

public class PageCount implements Comparable<PageCount>{
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
	
}
