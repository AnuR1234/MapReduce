
import java.io.IOException;
import java.util.Iterator;
//import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class average {

  public static class avgmapper
       extends Mapper<LongWritable, Text, Text, Text>{


    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String [] str=value.toString().split(",");
      String num=str[1];
        context.write(new Text(str[0]), new Text(num));
      }
    }


public static class avgcombiner extends Reducer<Text,Text,Text,Text> {

	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 int count = 0;
			   int sum = 0;
			    Iterator<Text> itr = values.iterator();
			   while (itr.hasNext()) {
			     String text = itr.next().toString();
			     double value = Double.parseDouble(text);
			    count++;
			    sum += value;
			   }
			   String s1=String.format("%d", sum);
			   String s2=String.format("%d", count);
			   String s=s1+","+s2;
	context.write(key,new Text(s));
}
}

  public static class avgreducer  extends Reducer<Text,Text,Text,DoubleWritable> {
   // private DoubleWritable result = new DoubleWritable();
//Text t=new Text("A");
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
    {
    	double sum = 0D;
    	   int  totalcount = 0;
    	    Iterator<Text> itr = values.iterator();
    	   while (itr.hasNext()) {
    	     String text = itr.next().toString();
    	     String[] tokens = text.split(",");
    	     double sum1 = Double.parseDouble(tokens[0]);
    	     int count = Integer.parseInt(tokens[1]);
    	    sum += sum1;
    	    totalcount += count;
    	   }

    	    double average = sum / totalcount;

    	   context.write(key, new DoubleWritable(average));
    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    Job job = Job.getInstance(conf, "avg");
    job.setJarByClass(average.class);
    job.setMapperClass(avgmapper.class);
    job.setCombinerClass(avgcombiner.class);
    job.setReducerClass(avgreducer.class);
   //job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
