


//import java.io.IOException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;




public class LongDisTotalDuration {

	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
		Text PhoneNo=new Text();
		IntWritable durationInMin=new IntWritable();
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");	 
	            if(str[4].equals("1"))
	            {
	            	PhoneNo.set(str[0]);
	            	String callEndTime=str[3];
	            	String callStartTime=str[2];
	            	long duration=toMillis(callEndTime)-toMillis(callStartTime);
	            	durationInMin.set((int)duration/(1000 *60));
	            }
	           context.write(PhoneNo, durationInMin);
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	         private long toMillis(String date) throws java.text.ParseException {
	        	 
		            SimpleDateFormat format = new SimpleDateFormat(
		                    "yyyy-MM-dd HH:mm:ss");
		            Date dateFrm = null;
		 
		            
		             dateFrm = format.parse(date);

		           
		            return dateFrm.getTime();
		        }
	      }
	 public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>
	   {
		    private IntWritable TotalDurationLongDis = new IntWritable();
		    
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		      int sum = 0;
				
		         for (IntWritable i : values)
		         {       	
		        	sum=+i.get();
		        	
		        	 if(sum>60)
		        	 {
		        		 TotalDurationLongDis.set(sum);
		        		 
		        	 }
		        	
		         }
		         context.write(key,TotalDurationLongDis );		      
		      
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
	    //conf.set("name", "value")
	    conf.set("mapreduce.output.fileoutputformat.seperator",",");
	    Job job = Job.getInstance(conf, "LongDistance high duration");
	    job.setJarByClass(LongDisTotalDuration.class);
	    job.setMapperClass(MapClass.class);
	    //job.setCombinerClass(ReduceClass.class);
	    job.setReducerClass(ReduceClass.class);
	    //job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

	}

