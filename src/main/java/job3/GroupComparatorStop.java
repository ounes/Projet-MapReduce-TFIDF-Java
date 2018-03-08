package job3;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import app.CustomKey;

public class GroupComparatorStop extends WritableComparator {
	
	public GroupComparatorStop() {
		super(CustomKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CustomKey lhs = (CustomKey) a;
		CustomKey rhs = (CustomKey) b;
		return lhs.getFilename().compareTo(rhs.getFilename());
	}
	
}