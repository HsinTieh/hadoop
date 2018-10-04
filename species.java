import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.concurrent.TimeUnit;

public class species {
 
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            context.write(new Text(token), new IntWritable(1));

        }
	 //context.write(new Text("kind"), new IntWritable(0));
    }
 } 
 
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    //private int kind=0;
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
	Configuration conf = context.getConfiguration();	
	int  number = Integer.parseInt(conf.get("kind"));
	number+=1;
	
//        for (IntWritable val : values) {
 //           sum += val.get();
//	    context.write(key, new IntWritable(sum));
//	    break;
 //       }
	
        //context.write(new Text("kind"),new IntWritable(number));
        conf.set("kind",Integer.toString(number));
 //      TimeUnit.SECONDS.sleep(1);
    }
    public void cleanup(Context context) throws IOException, InterruptedException{

        Configuration conf = context.getConfiguration();
        context.write(new Text("kind"),new IntWritable(Integer.parseInt(conf.get("kind"))));


    }

}


 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    conf.set("kind","0");     
    Job job = new Job(conf, "species");
   
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(WordCount.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}

