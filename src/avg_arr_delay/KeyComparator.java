package avg_arr_delay;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator{
	protected KeyComparator() {
		super();
	}
	
	@SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        LongWritable key1 = (LongWritable) w1;
        LongWritable key2 = (LongWritable) w2;          
        return -1 * key1.compareTo(key2);
	}
}
