package util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceHelper {
  public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class MaxReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int maxNum = 0;
    private Text one = new Text();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
      for (IntWritable val : values) {
        if (val.get() > maxNum) {
          maxNum = val.get();
        }
      }
      one = key;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(one, new IntWritable(maxNum));
    }
  }
}
