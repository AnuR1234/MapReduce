import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class HighestTransactions {

	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text> 
	   {
	      public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
	      {	    	  
	         
	            String[] str = value.toString().split(";");	
	            String s="common";
	            String val=str[0]+ "," +str[1]+ "," +str[8];
	            context.write(new Text(s),new Text(val));
	         
	
	      }
	   }
	
	public static class ReduceClass extends Reducer<Text,Text,NullWritable,Text>
	   {
		   
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      long max = 0;
		      String cust_id="";
		      String dateOfTransaction="";
		         for (Text t : values)
		         {    
		        	 String [] rec=t.toString().split(",");
		        	 String customer_id=rec[1];
		        	 String date1=rec[0];
		        	 long high_transaction=Long.parseLong(rec[2]);
		        	 if(high_transaction>max)
		        	 {
		        		 cust_id=customer_id;
		        		 dateOfTransaction=date1;
		        		 max=high_transaction;
		        	 }
		        	   
		         }
		         String val1=cust_id+ "," +dateOfTransaction+ "," +max;
		     		      
		      context.write(NullWritable.get(), new Text(val1));
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    
		    //conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ;");
		    conf.set("mapreduce.output.keyvaluelinerecordreader.key.value.separator", " ;");
		    Job job = Job.getInstance(conf, "High Transactions");
		    job.setJarByClass(HighestTransactions.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		  }
	
}

