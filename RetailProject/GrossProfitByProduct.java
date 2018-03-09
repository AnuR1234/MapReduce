import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GrossProfitByProduct {

	
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable> 
	   {
		Text prod_id=new Text();
	      public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
	      {	    	  
	         
	            String[] str = value.toString().split(";");	
	            prod_id.set(str[5]);
	            long total_cost=Long.parseLong(str[7]);
	            long total_sales=Long.parseLong(str[8]);
	            long profit=total_cost-total_sales;
	            context.write(prod_id,new LongWritable(profit));
	         
	      }
	   }
	
	public static class ReduceClass extends Reducer<Text,LongWritable,Text,LongWritable>
	   {
		   LongWritable gross_profit=new LongWritable(); 
		    
		    public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
		      long sum= 0;
		         for (LongWritable i : values)
		         {    
		        	
		        	sum+=i.get();
		         }
		         gross_profit.set(sum);
		     		      
		      context.write(key, gross_profit);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    
		    //conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ;");
		    conf.set("mapreduce.output.keyvaluelinerecordreader.key.value.separator", " ;");
		    Job job = Job.getInstance(conf, "High Transactions");
		    job.setJarByClass(GrossProfitByProduct.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    //job.setMapOutputKeyClass(Text.class);
			//job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		  }
	
}
	

