package avg_arr_delay;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class DelayDriver {

  public static void main(String[] args) throws Exception {


    if (args.length != 2) {
      System.out.printf("Usage: CancelledFlights <input dir> <output dir>\n");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "Average Arrival Delay");
    
    job1.setJarByClass(DelayDriver.class);
    job1.setMapperClass(DelayMapper.class);
    job1.setReducerClass(DelayReducer.class);
    
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));

    job1.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "swap");
    
    job2.setJarByClass(DelayDriver.class);
    job2.setMapperClass(KeyValueSwapper.class);
    job2.setNumReduceTasks(0);
    
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(DoubleWritable.class);
    
    FileInputFormat.addInputPath(job1, new Path(args[1]));
    FileOutputFormat.setOutputPath(job1, new Path(args[2]));
    System.exit(job2.waitForCompletion(true) ? 0: 1);
    
  }
}

