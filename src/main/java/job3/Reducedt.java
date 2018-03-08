package job3;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import app.COUNTER;
import app.CustomKey;
import app.CustomValue;

public class Reducedt extends Reducer<CustomKey, IntWritable, CustomKey, CustomValue> {

	private CustomValue resultat = new CustomValue();

	public void reduce(CustomKey key, Iterable<IntWritable> valeurs, Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		Map<CustomKey, Integer> toto = new LinkedHashMap<CustomKey, Integer>();
		
		for (IntWritable val : valeurs) {
			sum += val.get();
			CustomKey keys = new CustomKey(new Text(key.getFilename()),new Text(key.getWord()));
			toto.put(keys, new Integer(val.get()));
			//System.out.println("result "+val.get());
		}

		for (Entry<CustomKey, Integer> entry : toto.entrySet()) {
			CustomKey clef = entry.getKey();
			Integer number = entry.getValue();
			resultat.set(sum, number);
			context.write(clef, resultat);
			context.getCounter(COUNTER.JOB2).increment(1L);
		}
		
	}

}
