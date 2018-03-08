package job1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import app.COUNTER;
import app.CustomKey;


public class Reduce extends Reducer<CustomKey,IntWritable,CustomKey,IntWritable> {

	private IntWritable resultat = new IntWritable();

	public void reduce(CustomKey key, Iterable<IntWritable> valeurs, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : valeurs) {
			sum += val.get();
		}
		resultat.set(sum);
		context.write(key,resultat);
		context.getCounter(COUNTER.JOB1).increment(1L);
	}

}
