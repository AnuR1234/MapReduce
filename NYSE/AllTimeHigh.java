//import java.io.*;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

//import StockVolume.MapClass;
//import StockVolume.ReduceClass;

public class AllTimeHigh {

	
	public static class MapClass extends Mapper<LongWritable,Text,Text,DoubleWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");	 
	            double high = Double.parseDouble(str[4]);
	            context.write(new Text(str[1]),new DoubleWritable(high));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	 public static class ReduceClass extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	   {
		    private DoubleWritable high = new DoubleWritable();
		    
		    public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		      double max = 0;
				
		         for (DoubleWritable i : values)
		         {       	
		        	 double val=i.get();
		        	 if(val>max)
		        	 {
		        		 max=val;
		        	 }
		         }
		         
		      high.set(max);		      
		      context.write(key, high);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //conf.set("name", "value")
	    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
	    Job job = Job.getInstance(conf, "All time high");
	    job.setJarByClass(AllTimeHigh.class);
	    job.setMapperClass(MapClass.class);
	    //job.setCombinerClass(ReduceClass.class);
	    job.setReducerClass(ReduceClass.class);
	    //job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
