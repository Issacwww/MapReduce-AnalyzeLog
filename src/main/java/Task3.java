import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import util.JobRun;
import util.MapHelper;
import util.ReduceHelper;

import java.io.IOException;

public class Task3 {
  private static class AnalyzeMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text res = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      String lines[] = value.toString().split("\\s");
      if (lines.length >= 6) {
        res.set(lines[6]);
        context.write(res, one);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String in = args[0], out = args[1];
    Path tempDir = new Path(out + "tempRes");
    if (args.length < 2) {
      System.err.println("Usage: task3 <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "task3 step1");
    job.setJarByClass(Task3.class);
    JobRun.run(
        job, Task3.AnalyzeMapper.class, ReduceHelper.SumReducer.class, new Path(in), tempDir);
    job.waitForCompletion(true);

    Job jobS2 = Job.getInstance(conf, "task3 step2");
    jobS2.setJarByClass(MapHelper.class);
    JobRun.run(
        jobS2,
        MapHelper.RunMapper.class,
        ReduceHelper.MaxReducer.class,
        tempDir,
        new Path(out + "finalRes"));
    System.exit(jobS2.waitForCompletion(true) ? 0 : 1);
  }
}
