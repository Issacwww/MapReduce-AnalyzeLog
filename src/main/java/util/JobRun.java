package util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRun {
  public static void run(Job job, Class mapper, Class reducer, Path in, Path out) throws Exception {
    job.setMapperClass(mapper);
    job.setCombinerClass(reducer);
    job.setReducerClass(reducer);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, out);
  }
}
