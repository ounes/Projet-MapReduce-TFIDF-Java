package job3;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import app.CustomKey;

public class CustomPartionerStop extends Partitioner<CustomKey, IntWritable> {

	@Override
	public int getPartition(CustomKey key, IntWritable value, int numPartition) {
		
		return Math.abs(key.getFilename().hashCode()) % numPartition;
		
	}

}