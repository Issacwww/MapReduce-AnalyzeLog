import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import util.JobRun;
import util.ReduceHelper;

import java.io.IOException;

public class Task1 {
  private static class AnalyzeMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private String target = "/assets/img/home-logo.png";
    private Text res = new Text(target);

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String lines[] = value.toString().split("\\s");

      if (lines.length >= 6 && lines[6].equals(target)) {
        context.write(res, one);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length < 2) {
      System.err.println("Usage: Task1 <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "task1");
    job.setJarByClass(Task1.class);
    JobRun.run(
        job,
        Task1.AnalyzeMapper.class,
        ReduceHelper.SumReducer.class,
        new Path(args[0]),
        new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
