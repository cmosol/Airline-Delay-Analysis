package cancelled_flights;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CancelMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = value.toString();
	  ArrayList<String> row = new ArrayList<String>();
	  
	  for (String entry : line.split(",")) {
	        row.add(entry);
	      }
	  
	  context.write(new Text(row.get(1)), new DoubleWritable(Double.parseDouble(row.get(15))));
}
  }

