import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordRatio {


	
  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{

   private final static IntWritable one = new IntWritable(1);
    
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      while (itr.hasMoreTokens()) {
    	  String myword = itr.nextToken().toLowerCase();
    	  word.set(myword);
        context.write(word, one);
        //context.write(word, new IntWritable(1));
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,Text> {
	  
	  private TreeMap<String, Integer> WordMap = new TreeMap<String, Integer>();
	  //private IntWritable result = new IntWritable();
	  long total_count = 0;
	  
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
    	String myKey = key.toString();
    	int sum = 0;
    	for (IntWritable val : values) {
        sum += val.get();
        total_count += val.get(); 
      }
      

      //result.set(sum);
      WordMap.put(myKey, new Integer(sum));
      //WordMap.put(key, new IntWritable(sum));
      //context.write(key, result);
    }
    
	protected void cleanup(Context context) throws IOException,
	InterruptedException 
	{
	
		for (Map.Entry<String, Integer> entry : WordMap.entrySet())
		{
			String mykey = entry.getKey();
			Integer value = entry.getValue();
			String myvalue1 = String.format("%d", value);
			String myvalue2 = String.format("%d", total_count);
			String myvalue = myvalue1 + '/' + myvalue2;
			context.write(new Text(mykey), new Text(myvalue));

		}	
	}
    
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
    //conf.set("name", "value");
    Job job = Job.getInstance(conf, "word count and their ratio on total count");
    job.setJarByClass(WordRatio.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
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