package job5;
import org.apache.hadoop.mapreduce.Partitioner;

import app.CustomKey;
import app.CustomValue;

public class CustomPartitioner extends Partitioner<CustomKey, CustomValue> {

	@Override
	public int getPartition(CustomKey key, CustomValue value, int numPartition) {
		
		return Math.abs(key.getWord().hashCode()) % numPartition;
		
	}

}
