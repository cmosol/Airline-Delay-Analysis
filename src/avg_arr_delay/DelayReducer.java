package avg_arr_delay;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DelayReducer extends Reducer<Text,Text, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  int totalFlights = 0;
	  double sum = 0;

		for (Text v : values) {
			totalFlights ++;
			String value = v.toString();
			if (value.startsWith("-")){
				double num = ParseDouble(value.substring(1));
				sum-= num;
			}
			else{
				double num = ParseDouble(value);
				sum+= num;
			}
		}
		Double d = new Double(totalFlights);
		double avg = sum/d;
		context.write(key, new DoubleWritable(avg));
  }


  double ParseDouble(String strNumber) {
	   if (strNumber != null && strNumber.length() > 0) {
	       try {
	          return Double.parseDouble(strNumber);
	       } catch(Exception e) {
	          return -1;   // or some value to mark this field is wrong. or make a function validates field first ...
	       }
	   }
	   else return 0;
	}
}