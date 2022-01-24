import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Project {

  public static class FirstMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "-");
      String u = itr.nextToken();
      String v = itr.nextToken();
      context.write(new Text(u), new Text(u + ";" + v));
      context.write(new Text(v), new Text(u + ";" + v));
    }
  }

  public static class FirstReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      ArrayList<String> countedValues = new ArrayList<>();
      int size = 0;
      for (Text val : values) {
        countedValues.add(val.toString());
        size++;
      }
                           
      for (String val : countedValues) {
        StringTokenizer itr = new StringTokenizer(val, ";");
        String u = itr.nextToken();
        String v = itr.nextToken();
        String valS;
        if (u.equals(key.toString())) {
          valS = String.valueOf(size) + ";";
        }
        else {
          valS = ";" + String.valueOf(size);
        }
        context.write(new Text(u + ";" + v), new Text(valS));
      }
    }
  }

  public static class SecondReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      StringTokenizer keyItr = new StringTokenizer(key.toString(), ";");
      String u = keyItr.nextToken();
      String v = keyItr.nextToken();

      String du = "";
      String dv = "";
      for (Text val : values) {
        StringTokenizer itr = new StringTokenizer(val.toString(), ";");
        String duP = itr.nextToken();
        String dvP = itr.nextToken();
        if (!duP.equals("")) {
          du = duP;
        }
        if (!dvP.equals("")) {
          dv = dvP;
        }
      }
      context.write(new Text(u + "-" + v),new Text("deg(" + u + ")=" + du + ",deg(" + dv + ")=" + dv));
    }
  }

  public static void runFirstJob(Path input, Path output) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "first job");
    job.setJarByClass(Project.class);
    job.setMapperClass(FirstMapper.class);
    job.setReducerClass(FirstReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    job.waitForCompletion(true);
  }

  public static void runSecondJob(Path input, Path output) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "second job");
    job.setJarByClass(Project.class);
    job.setReducerClass(SecondReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
    runFirstJob(new Path(args[0]), new Path(args[2]));
    runSecondJob(new Path(args[2]), new Path(args[1]));
  }
}