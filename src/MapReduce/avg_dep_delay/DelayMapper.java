package avg_dep_delay;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DelayMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = value.toString();
	  ArrayList<String> row = new ArrayList<String>();
	  
	  for (String entry : line.split(",")) {
	        row.add(entry);
	      }
	  
	  context.write(new Text(row.get(1)), new Text(row.get(7)));
}
  }

