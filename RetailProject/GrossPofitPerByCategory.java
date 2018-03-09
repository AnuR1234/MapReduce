import java.io.IOException;
import java.text.DecimalFormat;

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


public class GrossProfitPerByCategory {

	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text> 
	   {
		Text prod_id=new Text();
	      public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
	      {	    	  
	         
	            String[] str = value.toString().split(";");	
	            prod_id.set(str[4]);
	            String profitparams=str[7]+","+str[8];
	            context.write(prod_id,new Text(profitparams));
	         
	      }
	   }
	
	public static class ReduceClass extends Reducer<Text,Text,Text,Text>
	   {
		   LongWritable gross_profit=new LongWritable(); 
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      float profit1= 0;
		      int total_sales1=0;
		      int total_cost1=0;
		      
		         for (Text t : values)
		         {    
		        	String [] str=t.toString().split(",");
		        	int total_sales=Integer.parseInt(str[1]);
		        	
		        	int total_cost=Integer.parseInt(str[0]);
		        	
		        	float profit=total_sales-total_cost;
		        	profit1=profit;
		        	
		        	total_sales1=total_sales;
		        	total_cost1=total_cost;
		         }
		         float per=((profit1*100)/total_sales1);
		         DecimalFormat df=new DecimalFormat("0.00");
		         String formate = df.format(per)+"%"; 
		         //String s3=String.format("%f",formate )+"%";
		         String s1=String.format("%d",total_cost1 );
		         String s2=String.format("%d",total_sales1 );
		         String val=s1+','+s2+','+formate;
		 
		     		      
		      context.write(key, new Text(val));
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    
		    //conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ;");
		    conf.set("mapreduce.output.keyvaluelinerecordreader.key.value.separator", " ;");
		    Job job = Job.getInstance(conf, "Gross profit %");
		    job.setJarByClass(GrossProfitPerByProduct.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    //job.setMapOutputKeyClass(Text.class);
			//job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		  }
	
}
	

