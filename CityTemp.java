/* MapReduce program to calculate maximum temperature of each city */
/* Input set of records are in the format :- (city1, temp1) \n (city2, temp2) \n (city1, temp3) ... */
package citytemp;

/**
 * @author yogiraj
*/
 
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CityTemp {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private IntWritable temp = new IntWritable();
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String[] tokens = value.toString().split("\\s+");
      
      word.set(tokens[0]);
      temp.set(Integer.parseInt(tokens[1]));
      context.write(word, temp);
    }
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      int maxTemp = 0;
      for (IntWritable val : values) {
        if(val.get() > maxTemp)
            maxTemp = val.get();
      }
      result.set(maxTemp);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Max temp of city");
    job.setJarByClass(CityTemp.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


/*

$ bin/hadoop com.sun.tools.javac.Main CityTemp.java
$ jar cf wc.jar CityTempw*.class

Assuming that:

    /user/joe/citytemp/input - input directory in HDFS
    /user/joe/citytemp/output - output directory in HDFS


Sample text-files as input:

$ bin/hadoop fs -ls /user/joe/citytemp/input/ /user/joe/citytemp/input/file01 /user/joe/citytemp/input/file02

$ bin/hadoop fs -cat /user/joe/citytemp/input/file01
Hello World Bye World

$ bin/hadoop fs -cat /user/joe/citytemp/input/file02
Hello Hadoop Goodbye Hadoop


Run the application:

$ bin/hadoop jar wc.jar citytemp /user/joe/citytemp/input /user/joe/citytemp/output


Output:

$ bin/hadoop fs -cat /user/joe/citytemp/output/part-r-00000`
Pune 40
Chennai 42
Delhi 43
Chandigarh 36

*/
