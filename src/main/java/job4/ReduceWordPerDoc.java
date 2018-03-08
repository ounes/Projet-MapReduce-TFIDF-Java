package job4;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceWordPerDoc extends Reducer<Text,IntWritable,Text,IntWritable> {

	private IntWritable resultat = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> valeurs, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : valeurs) {
			sum += val.get();
		}
		resultat.set(sum);
		context.write(key,resultat);
	}

}
