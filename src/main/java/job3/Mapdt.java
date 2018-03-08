package job3;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import app.CustomKey;

public class Mapdt extends Mapper<Object, Text, CustomKey, IntWritable> {

	private IntWritable number = new IntWritable();
	private CustomKey text = new CustomKey();

	public void map(Object key, Text line, Context context) throws IOException, InterruptedException {

		StringTokenizer st = new StringTokenizer(line.toString().toLowerCase(), " \t");
		String fileName = st.nextToken().toString();
		String mot = st.nextToken().toString();
		number.set(Integer.parseInt(st.nextToken().toString()));

		text.set(mot, fileName);
		context.write(text, number);

	}
}
