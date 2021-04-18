package cancelled_flights;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//Secondary sort is a technique that allows the MapReduce programmer 
//to control the order that the values show up within a reduce function call.

public class CancelReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
	  int totalFlights = 0;
	  int sum = 0;

		for (DoubleWritable value : values) {
			totalFlights ++;
			if (value.get() == 1)
				sum++;
			}
		Double n = new Double(sum);
		Double d = new Double(totalFlights);
		double avg = n/d;
		context.write(key, new DoubleWritable(avg));
  }
}
