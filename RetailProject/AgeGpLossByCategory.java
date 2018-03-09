import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class AgeGpLossByCategory extends Configured implements Tool
{
   //Map class
	
   public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
   {
	   private Text cust_id=new Text();
      public void map(LongWritable key, Text value, Context context)
      {
         try{
            String[] str = value.toString().split(";");
            String cid="CategoryID:"+str[4]+" "+ "Age Group:"+str[2];
            cust_id.set(cid);
            context.write(cust_id,new Text(value));
         }
         catch(Exception e)
         {
            System.out.println(e.getMessage());
         }
      }
   }
   
   //Reducer class
	
   public static class ReduceClass extends Reducer<Text,Text,Text,Text>
   {
	 //  private IntWritable total_profit=new IntWritable();
      int loss=0;
      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
      {
    	  String cust_id="";
    	  String total_loss="";
			
         for (Text val : values)
         {
        	

        	String [] str = val.toString().split(";");
        	int sales=Integer.parseInt(str[8]);
        	int cost=Integer.parseInt(str[7]);
        	String id="CategoryID:"+str[4]+"age "+str[2];
        	int loss1=cost-sales;
            if(loss1>0)
            {	
            	loss=loss1;
            	cust_id=id;
            	total_loss="Loss is:"+String.format("%d",loss);
            }	
         }
			
         context.write(new Text(cust_id),new Text(total_loss));
      }
   }
   
   //Partitioner class
	
   public static class AgePartitioner extends
   Partitioner < Text, Text >
   {
      @Override
      public int getPartition(Text key, Text value, int numReduceTasks)
      {
         String[] str = value.toString().split(";");
         String age=str[2];
         //switch(age)
         if(age.contains("A"))
         {
        	 return 0;
        	 
         }
         else
        	 if(age.contains("B"))
         {
        	 return 1 % numReduceTasks;
        	 
         }
        	 else
        		 if(age.contains("C"))
         {
        	 return 2 % numReduceTasks;

         }
        		 else
        			 if(age.contains("D"))
         {
        	 return 3 % numReduceTasks;
        	 
         }
        			 else
        				 if(age.contains("E"))
         
         {
        	 return 4 % numReduceTasks;
        	 
         }
        				 else if(age.contains("F"))
         {
        	 return 5 % numReduceTasks;
        	 
         }
        				 else if(age.contains("G"))
         {
        	 return 6 % numReduceTasks;
        	 
         }
        				 else if(age.contains("H"))
         {
        	 return 7 % numReduceTasks;
        	 
         }
        				 else if(age.contains("I"))
         {
        	 return 8 % numReduceTasks;
        	 
         }
        				 else 
         {
        	 return 9 % numReduceTasks;
        	 
         }
         }
       }
   public int run(String[] arg) throws Exception
   {
	
	   
	  Configuration conf = new Configuration();
	  Job job = Job.getInstance(conf);
	  job.setJarByClass(AgeGpLossByCategory.class);
	  job.setJobName("Age group loss by category");
      FileInputFormat.setInputPaths(job, new Path(arg[0]));
      FileOutputFormat.setOutputPath(job,new Path(arg[1]));
		
      job.setMapperClass(MapClass.class);
		
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      //set partitioner statement
		
      job.setPartitionerClass(AgePartitioner.class);
      job.setReducerClass(ReduceClass.class);
      job.setNumReduceTasks(10);
      job.setInputFormatClass(TextInputFormat.class);
		
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      System.exit(job.waitForCompletion(true)? 0 : 1);
      return 0;
   }
   
   public static void main(String ar[]) throws Exception
   {
      ToolRunner.run(new Configuration(), new AgeGpLossByCategory(),ar);
      System.exit(0);
   }
}
