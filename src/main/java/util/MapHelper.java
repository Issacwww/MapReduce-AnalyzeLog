package util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapHelper {
  public static class RunMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text res = new Text();

    private int maxNum = 0;

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      String lines[] = value.toString().split("\\r?\\n");

      Pattern p1 = Pattern.compile("\\s+?[1-9]\\d*$");

      for (int i = 0; i < lines.length; i++) {
        Matcher m = p1.matcher(lines[i]);
        while (m.find()) {
          int temp = Integer.parseInt(m.group().trim());
          if (temp > maxNum) {
            maxNum = temp;
          }
        }
      }

      String Num = maxNum + "";
      for (int j = 0; j < lines.length; j++) {
        if (lines[j].contains(Num)) {
          int begin = 0;
          int end = lines[j].indexOf("\t");
          String ip = lines[j].substring(begin, end);

          res.set(ip);
          context.write(res, new IntWritable(maxNum));
        }
      }
    }
  }
}
