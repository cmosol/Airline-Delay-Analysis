package avg_arr_delay;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class DelayDriver {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.out.printf("Usage: CancelledFlights <input dir> <output dir>\n");
      System.exit(-1);
    }

    /*
     * Instantiate a Job object for your job's configuration. 
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Average Word Length");
    
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     */
    job.setJarByClass(DelayDriver.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    /*
     * Specify the mapper and reducer classes.
     */
    job.setMapperClass(DelayMapper.class);
    job.setReducerClass(DelayReducer.class);

    /*
     * For the word count application, the input file and output 
     * files are in text format - the default format.
     * 
     * In text format files, each record is a line delineated by a 
     * by a line terminator.
     * 
     * When you use other input formats, you must call the 
     * SetInputFormatClass method. When you use other 
     * output formats, you must call the setOutputFormatClass method.
     */
      
    /*
     * For the word count application, the mapper's output keys and
     * values have the same data types as the reducer's output keys 
     * and values: Text and IntWritable.
     * 
     * When they are not the same data types, you must call the 
     * setMapOutputKeyClass and setMapOutputValueClass 
     * methods.
     */
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    /*
     * Specify the job's output key and value classes.
     */
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setSortComparatorClass(KeyComparator.class);
    
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}

