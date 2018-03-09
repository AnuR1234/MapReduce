import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import AllTimeHigh.MapClass;
//import AllTimeHigh.ReduceClass;


public class AllTimeLow {

	
	public static class MapClass extends Mapper<LongWritable,Text,Text,DoubleWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");	 
	            double low = Double.parseDouble(str[5]);
	            context.write(new Text(str[1]),new DoubleWritable(low));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	 public static class ReduceClass extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	   {
		    private DoubleWritable low = new DoubleWritable();
		    
		    public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		      double min = Integer.MAX_VALUE;
			
		         for (DoubleWritable i : values)
		         {       	
		        	 double val=i.get();
		        	 if(val<min)
		        	 {
		        		 min=val;
		        	 }
		         }
		         
		      low.set(min);		      
		      context.write(key, low);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }

	 
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //conf.set("name", "value")
	    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
	    Job job = Job.getInstance(conf, "All time low");
	    job.setJarByClass(AllTimeLow.class);
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


