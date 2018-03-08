package job5;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import app.CustomKey;
import app.CustomValue;

public class ReduceFusion extends Reducer<CustomKey, CustomValue, CustomKey, DoubleWritable> {

	public void reduce(CustomKey key, Iterable<CustomValue> valeurs, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		Map<Double, Double> toto = new LinkedHashMap<Double, Double>();

		for (CustomValue customValue : valeurs) {
			Double wc = Double.parseDouble(customValue.getWordCount().toString());
			Double wpd = Double.parseDouble(customValue.getWordPerDoc().toString());
			toto.put(wc, wpd);
			sum += 1;
		}

		for (Entry<Double, Double> entry : toto.entrySet()) {
			Double wc = entry.getKey();
			Double wpd = entry.getValue();
			Double tfidf = (wc / wpd) * Math.log(2 / sum);
			context.write(key, new DoubleWritable(tfidf));
		}

	}
}
