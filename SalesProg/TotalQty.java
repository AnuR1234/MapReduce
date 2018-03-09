import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


public class TotalQty {
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
        
		
		private Map<String, String> Map1 = new HashMap<String, String>();
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

		    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
		
		   
		    
		    FileSystem fs = FileSystem.get(context.getConfiguration());		    
		
			if (p.getName().equals("store_master")) {
					
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));

					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split(",");
						String store_id = tokens[0];
						String state = tokens[2];
						Map1.put(store_id, state);//key,value pair
						line = reader.readLine();
					}
					reader.close();
				}
			if (Map1.isEmpty()) {
				throw new IOException("MyError:Unable to load salary data.");
			}

		}

		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	
        	
        	String row = value.toString();
        	String[] tokens = row.split(",");
        	String Prod_id = tokens[1];
        	String Store_id=tokens[0];//common elements in the main file and the cache files
        	String qty=tokens[2];
        	String state1 = Map1.get(Store_id);//using the key to get the value
        	String qty_val = qty + "," + state1; 
        	outputKey.set(Prod_id);
        	outputValue.set(qty_val);
      	  	context.write(outputKey,outputValue);
        }  
}
	
	public static class StatePartitioner extends
	   Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	         String[] str = value.toString().split(",");
	         String state=str[1];
	         //switch(age)
	         if(state.equals("MAH"))
	         {
	        	 return 0;
	        	 
	         }
	         else
	         {
	        	 return 1 % numReduceTasks;
	        	 
	       }
	   }
	   }
	 public static class ReduceClass extends Reducer<Text,Text,Text,Text>
	   {
		
	  	String result="";	
	      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException{
	    	  int sum=0;
		     
	         for (Text val : values)
	         {
	        	String [] str = val.toString().split(",");
	        	int qty=Integer.parseInt(str[0]);
	        	sum+=qty;
	        	String sum1=String.format("%d", sum);
	        	String state=str[1];
	        	result=sum1+","+state;
	      }
	         
	         context.write(key,new Text(result));
	   }
	   }
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	Configuration conf = new Configuration();
	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
    job.setJarByClass(TotalQty.class);
    job.setJobName("TotalQty");
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.addCacheFile(new Path(args[1]).toUri());
    job.setPartitionerClass(StatePartitioner.class);
    job.setReducerClass(ReduceClass.class);
    job.setNumReduceTasks(2);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    job.waitForCompletion(true);
    
    
  }
}
	 
