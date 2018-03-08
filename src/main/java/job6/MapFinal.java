package job6;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import app.CustomKey;

public class MapFinal extends Mapper<Object, Text, DoubleWritable, CustomKey> {

	private CustomKey text = new CustomKey();
	private DoubleWritable number = new DoubleWritable();

	public void map(Object key, Text line, Context context) throws IOException, InterruptedException {

		StringTokenizer st = new StringTokenizer(line.toString().toLowerCase(), " \t");
		String word = st.nextToken().toString();
		String fileName = st.nextToken().toString();
		text.set(fileName, word);
		Double tfidf = Double.parseDouble(st.nextToken().toString());
		number.set(tfidf);
		context.write(  number, text);

	}

}
