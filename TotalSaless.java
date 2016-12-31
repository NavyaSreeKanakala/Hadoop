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


public class TotalSaless {

	 public static class MapClass extends Mapper<LongWritable, Text, LongWritable, LongWritable>
	 {

 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
	  try{
			String[] str= value.toString().split(",");
			long store_id=Long.parseLong(str[0]);
			String qty=str[2];
			String price=str[3];
		    long myoutput = (Long.parseLong(qty))*(Long.parseLong(price));
			context.write(new LongWritable(store_id),new LongWritable(myoutput));
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	  
    }
  }


public static class ReduceClass extends Reducer<LongWritable,LongWritable, LongWritable,LongWritable> {
  
	private LongWritable result = new LongWritable();

  public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
  {
    long total = 0;
    for (LongWritable val : values) 
    {	
      
      total+=val.get();
    }
      result.set(total);
     context.write(key, result);
  
}
}

public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "total sales for each store");
  job.setJarByClass(TotalSaless.class);
  job.setMapperClass(MapClass.class);
  job.setCombinerClass(ReduceClass.class);
  job.setReducerClass(ReduceClass.class);
 // job.setNumReduceTasks(0);
  job.setOutputKeyClass(LongWritable.class);
  job.setOutputValueClass(LongWritable.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
}
	
	
	
	
}
