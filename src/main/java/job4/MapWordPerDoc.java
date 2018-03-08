package job4;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapWordPerDoc extends Mapper<Object, Text, Text, IntWritable> {
	
	private Text text = new Text();
	private IntWritable number = new IntWritable();

	public void map(Object key, Text line, Context context) throws IOException, InterruptedException {

		StringTokenizer st = new StringTokenizer(line.toString().toLowerCase(), " \t");
		number.set(Integer.parseInt(st.nextToken().toString()));

		if (st.hasMoreTokens()) {
			text.set(st.nextToken().toString());
			context.write(text, number);
		}

	}

}
