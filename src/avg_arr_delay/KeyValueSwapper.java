package avg_arr_delay;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class KeyValueSwapper extends Mapper <Object, Text, Text, Text>{
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String [] keyVal = value.toString().split("\t");
		String k = keyVal[0];
		String v = keyVal[1];
		context.write(new Text(v), new Text(k));
	}

}
