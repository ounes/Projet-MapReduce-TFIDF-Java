package job6;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import app.CustomKey;

public class CustomPartitioner extends Partitioner<CustomKey,DoubleWritable> {

	@Override
	public int getPartition( CustomKey key, DoubleWritable value,  int numPartition) {
		
		return Math.abs(key.getWord().hashCode()) % numPartition;
		
	}

}
