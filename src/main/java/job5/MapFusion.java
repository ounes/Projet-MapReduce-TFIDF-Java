package job5;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import app.CustomKey;
import app.CustomValue;

public class MapFusion extends Mapper<Object, Text, CustomKey, CustomValue> {

	private CustomKey text = new CustomKey();
	private CustomValue value = new CustomValue();

	public void map(Object key, Text line, Context context) throws IOException, InterruptedException {

		StringTokenizer st = new StringTokenizer(line.toString().toLowerCase(), " \t");

		String fileName = st.nextToken().toString();
		String mot = st.nextToken();
		Integer number = Integer.parseInt(st.nextToken().toString());
		Integer numberTotal = Integer.parseInt(st.nextToken().toString());
		
		text.set(mot, fileName);
		value.set(numberTotal, number);
		
		context.write(text, value);
		
	}

}
