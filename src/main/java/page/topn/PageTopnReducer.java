package page.topn;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.view.HtmlPage.Page;

public class PageTopnReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	TreeMap<PageCount,Object> treemap = new TreeMap<PageCount, Object>();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int count=0;
		for (IntWritable intWritable : values) {
			count+=intWritable.get();
		}
		PageCount page=new PageCount();
		page.set(key.toString(), count);
		treemap.put(page, null);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		int topn = conf.getInt("top.n", 5);
		Set<Entry<PageCount, Object>> set = treemap.entrySet();
		int i=0;
		for (Entry<PageCount, Object> entry : set) {
			context.write(new Text(entry.getKey().getPage()), new IntWritable(entry.getKey().getCount()));
			i++;
			if (i==topn) {
				return;
			}
		}
	}
}
