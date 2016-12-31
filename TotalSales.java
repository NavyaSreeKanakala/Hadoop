import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class TotalSales {

	 public static class MapClass extends Mapper<LongWritable, Text, LongWritable, LongWritable>
	 {

 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
	  try{
			String[] str= value.toString().split(",");
			long store_id=Long.parseLong(str[0]);
			
			String qty=str[2];
			String price=str[3];
		    long myoutput= (Long.parseLong(qty))*(Long.parseLong(price));
			context.write(new LongWritable(store_id),new LongWritable(myoutput));
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	  
    }
  }


public static class ReduceClass extends Reducer<Text,Text,Text,LongWritable> {
  
	private Text outputKey = new Text();
	private LongWritable result = new LongWritable();

  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
  {
    long product = 0;
    for (Text val : values) 
    {
      outputKey.set(key);	
      String[] str=val.toString().split(",");
      long qty=Long.parseLong(str[0]);
      long price=Long.parseLong(str[1]);
                        result.set(product);
    context.write(key, result);
  }
}

public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "total sales for each store");
  job.setJarByClass(TotalSales.class);
  job.setMapperClass(MapClass.class);
//  job.setCombinerClass(ReduceClass.class);
 // job.setReducerClass(ReduceClass.class);
  job.setNumReduceTasks(0);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(LongWritable.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
}
	
	
	
}
