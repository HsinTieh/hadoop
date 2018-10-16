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
        
public class datahandle {
        
 public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
	line = line.replaceAll("[-#*xX]","");
	line = line.replaceAll("NR","0");
        StringTokenizer tokenizer = new StringTokenizer(line);
        String s="";
	double threshold=0.1;
	
	while (tokenizer.hasMoreTokens()) {
		double total=0,maxVal=-1,minVal=-1;
		int count=0;
		String read = tokenizer.nextToken();
		StringTokenizer token_ = new StringTokenizer(read,",");
                StringTokenizer token_2 = new StringTokenizer(read,",");

		while(token_.hasMoreTokens())
		{
			String word=token_.nextToken();
			if(word.matches("[0.0-9.0]{0,}"))
			{
				double word_d = Double.parseDouble(word);
				total += word_d;
				count +=1;
				if(maxVal == -1 && minVal == -1)
				{
					minVal = word_d;
					maxVal = word_d;
				}
				maxVal = word_d > maxVal ? word_d : maxVal;
				minVal = word_d < minVal ? word_d : minVal;
			}
		}
		while(token_2.hasMoreTokens())
          	{
                        String word=token_2.nextToken();
			String formatVal;
                        
			if(word.matches("[0.0-9.0]{0,}"))
                        {
                                double word_d = Double.parseDouble(word);
                                if((word_d - minVal)/(maxVal - minVal) <= threshold)
					word=Double.toString(Math.rint((total/count)*10)/10);
                	}
			if(s == "")
				s+=word;
			else
	           		s += ","+ word;
                }
            	context.write(new Text(s),new DoubleWritable(total/count));
		s="";
	}
    }
 } 
        
 public static class Reduce extends Reducer<Text,DoubleWritable, Text, Text> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
      throws IOException, InterruptedException {
       
       // for (DoubleWritable val: values) {
        	context.write(key,new Text(""));
	//}
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "datahandle");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(datahandle.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
