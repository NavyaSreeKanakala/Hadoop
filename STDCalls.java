import java.io.IOException;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class STDCalls {
	
	public static class MapClass extends Mapper<Object, Text, Text, IntWritable>
	{     
		 Text phoneNumber = new Text();
		 IntWritable durationInMinutes = new IntWritable();

  

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
	 try{
			String[] str= value.toString().split(",");
			if(str[4].equals("1"))
			{
				phoneNumber.set(str[0]);
				String callendtime=str[3];
				String callstarttime=str[2];
				long duration= toMillis1(callendtime)-toMillis1(callstarttime);
				durationInMinutes.set((int)(duration/(1000*60)));
				context.write(phoneNumber,durationInMinutes);
			}
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
    }
 private long toMillis1(String date) 
 {
     
     SimpleDateFormat format = new SimpleDateFormat(
             "yyyy-MM-dd HH:mm:ss");
     Date dateFrm = null;
     try {
         dateFrm = format.parse(date);

     } 
     catch (Exception e) 
     {

         e.printStackTrace();
     }
     return dateFrm.getTime();
 }

}

	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		
		private IntWritable result = new IntWritable();
		
		
		
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
		{
			long sum=0;
			for(IntWritable val : values)
			{
				sum+=val.get();
			}
	         result.set((int) sum);
	         if(sum>=60)
	         {
	        	 context.write(key, result); 
	         }
		      
			
         }
	  }
	
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator",",");
	    Job job = Job.getInstance(conf, "std calls");
	    job.setJarByClass(STDCalls.class);
	    job.setMapperClass(MapClass.class);
	    job.setCombinerClass(ReduceClass.class);
	    job.setReducerClass(ReduceClass.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	

	
	
	

}
