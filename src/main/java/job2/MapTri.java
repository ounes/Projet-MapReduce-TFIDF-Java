package job2;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapTri extends Mapper<Object, Text, IntWritable, Text> {
	
	private IntWritable number = new IntWritable();
	private Text text = new Text();
	
	public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
		
		StringTokenizer st = new StringTokenizer(line.toString().toLowerCase(),"\t");
		text.set(st.nextToken().toString());
		
		if(st.hasMoreTokens()) {
			number.set(Integer.parseInt(st.nextToken().toString()));
			context.write(number,text);
		}
		
	}

}
