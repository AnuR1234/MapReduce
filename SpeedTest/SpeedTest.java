import java.io.IOException;
//import java.text.SimpleDateFormat;
//import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpeedTest {
		
		public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
		   {
		      public void map(LongWritable key, Text value, Context context)
		      {	    	  
		         try{
		            String[] str = value.toString().split(",");	 
		            int speed=Integer.parseInt(str[1]);
		            context.write(new Text(str[0]), new IntWritable(speed));
		         }
		         catch(Exception e)
		         {
		            System.out.println(e.getMessage());
		         }
		      }
		   }
		 public static class ReduceClass extends Reducer<Text,IntWritable,Text,Text>
		   {
			
			    
			    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			      int total = 0;
					int offence_count=0;
			         for (IntWritable i : values)
			         {       	
			    if(i.get()>65)
			        	 {
			    	offence_count++;
			        		 
			        	 }
			    total++;
			        	
			         }
			         double offence_per=((offence_count*100)/total);
			         String percentvalue=String.format("%f", offence_per);
			         String persign=percentvalue+"%";
			         context.write(key, new Text(persign));		      
			      
			      //context.write(key, new LongWritable(sum));
			      
			    }
		   }

		public static void main(String[] args) throws Exception{
			Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    conf.set("mapreduce.output.fileoutputformat.seperator",",");
		    Job job = Job.getInstance(conf, "SpeedTest");
		    job.setJarByClass(SpeedTest.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(Text.class);
		   job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }


	}


