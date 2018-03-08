 package job6;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import app.CustomKey;

public class GroupComparator extends WritableComparator {
	
	public GroupComparator() {
		super(CustomKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CustomKey lhs = (CustomKey) a;
		CustomKey rhs = (CustomKey) b;
		return lhs.getWord().compareTo(rhs.getWord());
	}
	
}
