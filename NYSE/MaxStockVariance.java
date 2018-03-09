import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MaxStockVariance {
	public static class MapClass extends Mapper<LongWritable,Text,Text,FloatWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");	 
	            float low = Float.parseFloat(str[5]);
	            float high=Float.parseFloat(str[4]);
	            float stockvar=((high-low)*100)/low;
	            context.write(new Text(str[1]),new FloatWritable(stockvar));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
      public static class ReduceClass extends Reducer<Text,FloatWritable,Text,FloatWritable>
	   {
		    private FloatWritable maxStockVar = new FloatWritable();
		    
		    public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
		      float max = 0;
			
		         for (FloatWritable i : values)
		         {       	
		        	 float val=i.get();
		        	 if(val>max)
		        	 {
		        		 max=val;
		        	 }
		         }
		         
		      maxStockVar.set(max);		      
		      context.write(key, maxStockVar);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //conf.set("name", "value")
	    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
	    Job job = Job.getInstance(conf, "stock var");
	    job.setJarByClass(MaxStockVariance.class);
	    job.setMapperClass(MapClass.class);
	    //job.setCombinerClass(ReduceClass.class);
	    job.setReducerClass(ReduceClass.class);
	    //job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}



