import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class C11a {
	public static class MapClass extends Mapper<LongWritable,Text,Text, IntWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(";");
	            
	            Text id = new Text (str[5].trim());
	            int sales = Integer.parseInt(str[8]);
	            int cost =  Integer.parseInt(str[7]);
	            int profit = sales - cost;
	            String k = id + "," +str[2].trim();
		        context.write(new Text(k), new IntWritable(profit));
	            
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	public static class ReduceClass extends Reducer<Text, IntWritable, IntWritable, Text>{	
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		      int sum = 0;
		      for (IntWritable val : values){
		    	  int profit = val.get();
		    	  sum += profit;
		      }
		      
		      if(sum > 0) {
		    	  context.write(new IntWritable(sum), key);
		      }     
		}
	}
	
	public static class Mapper2 extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valueArr = value.toString().split("\t");
			context.write(new LongWritable(Integer.parseInt(valueArr[0])), new Text(valueArr[1]));
		}
	}
	
	public static class Reducer2 extends Reducer<LongWritable, Text,Text,LongWritable>
	{
		int count = 0;
		@Override
		public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
			
			String v="";
			for(Text val : value)
			{	
				if(count<5) {
					count++;
					v = val.toString().split(",")[0];
					context.write(new Text(v), key);
				}
				else {
					break;
				}
			}
			
		}
	}
	public static class PartitionerClass extends
	   Partitioner < LongWritable, Text >
	   {
	      @Override
	      public int getPartition(LongWritable key, Text value, int numReduceTasks)
	      {
	         char ageGroup = value.toString().split(",")[1].charAt(0);
	         if (ageGroup == 'A') {
	        	 return 0;
	         }
	         else if (ageGroup == 'B') {
	        	 return 1;
	         }
	         else if (ageGroup == 'C') {
	        	 return 2;
	         }
	         else if (ageGroup == 'D') {
	        	 return 3;
	         }
	         else if (ageGroup == 'E') {
	        	 return 4;
	         }
	         else if (ageGroup == 'F') {
	        	 return 5;
	         }
	         else if (ageGroup == 'G') {
	        	 return 6;
	         }
	         else if (ageGroup == 'H') {
	        	 return 7;
	         }
	         else if (ageGroup == 'I') {
	        	 return 8;
	         }
	         else {
	        	 return 9;
	         }
	         
	         
	         
	      }
	   }
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Viable Products");

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    Path outputPath1 = new Path("Mapper");
	    FileOutputFormat.setOutputPath(job, outputPath1);
	    job.setJarByClass(C11a.class);
	    job.setMapperClass(MapClass.class);
	    job.setReducerClass(ReduceClass.class);
	    job.setNumReduceTasks(1);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileSystem.get(conf).delete(outputPath1, true);
	    
	    job.waitForCompletion(true);

	    
	    Job job1 = Job.getInstance(conf, "Viable Products Top 5");
	    FileInputFormat.addInputPath(job1, outputPath1);
	    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	    job1.setJarByClass(C11a.class);
	    job1.setMapperClass(Mapper2.class);
	    job1.setPartitionerClass(PartitionerClass.class);
	    job1.setReducerClass(Reducer2.class);
	    job1.setSortComparatorClass(LongWritable.DecreasingComparator.class);
	    job1.setNumReduceTasks(10);
	    job1.setMapOutputKeyClass(LongWritable.class);
	    job1.setMapOutputValueClass(Text.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(LongWritable.class);
	    System.exit(job1.waitForCompletion(true) ? 0 : 1);
	    

	    
	  }
	
	
}
