

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
//import org.apache.hadoop.util.*;


public class WordCount2 {

	String words_to_count[];
	
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      
      StringTokenizer tokenizer = new StringTokenizer(line," )(;'\",.%$_#-/!~*<>=&^");
      String S1;
      while (tokenizer.hasMoreTokens()) 
      {
    	  S1 = tokenizer.nextToken().toUpperCase();
          if(S1.charAt(0) >= 'A' && S1.charAt(0)<='Z')
          {
        	  word.set(S1.substring(0,1));
        	  output.collect(word, one);
          }
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      int sum = 0;
  
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
	  
	  if (args.length != 2) {
	      System.err.println("Usage: MaxTemperature <input path> <output path>");
	      System.exit(-1);
	    }
	  
    JobConf conf = new JobConf(WordCount2.class);
    conf.setJobName("wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    conf.setNumReduceTasks(1);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}