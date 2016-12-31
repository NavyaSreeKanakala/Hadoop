import java.io.*;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;


public class PartitionClass extends Configured implements Tool  {

	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().split(",");
	            String gender=str[3];
	            context.write(new Text(gender), new Text(value));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	   
	   
		
	   public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
	   {
	      public int max = -1;
	      private Text outputKey = new Text();
	      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	      {
	         max = -1;
				
	         for (Text val : values)
	         {
	        	
	        	outputKey.set(key);
	        	String [] str = val.toString().split(",");
	            if(Integer.parseInt(str[4])>max)
	            max=Integer.parseInt(str[4]);
	         }
				
	         context.write(outputKey, new IntWritable(max));
	      }
	   }
	   
	  
		
	   public static class CaderPartitioner extends
	   Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	         String[] str = value.toString().split(",");
	         int age = Integer.parseInt(str[2]);
	         
	         if(numReduceTasks == 0)
	         {
	            return 0;
	         }
	         
	         if(age<=20)
	         {
	            return 0;
	         }
	         else if(age>20 && age<=30)
	         {
	            return 1 % numReduceTasks;
	         }
	         else
	         {
	            return 2 % numReduceTasks;
	         }
	      }
	   }
	   

	   public int run(String[] arg) throws Exception
	   {
		
		   
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf);
		  job.setJarByClass(PartitionClass.class);
		  job.setJobName("Top Salaried Employees");
	      FileInputFormat.setInputPaths(job, new Path(arg[0]));
	      FileOutputFormat.setOutputPath(job,new Path(arg[1]));
			
	      job.setMapperClass(MapClass.class);
			
	      job.setMapOutputKeyClass(Text.class);
	      job.setMapOutputValueClass(Text.class);
	      
	     
			
	      job.setPartitionerClass(CaderPartitioner.class);
	      job.setReducerClass(ReduceClass.class);
	      job.setNumReduceTasks(3);
	      job.setInputFormatClass(TextInputFormat.class);
			
	      job.setOutputFormatClass(TextOutputFormat.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
			
	      System.exit(job.waitForCompletion(true)? 0 : 1);
	      return 0;
	   }
	   
	   public static void main(String ar[]) throws Exception
	   {
	      ToolRunner.run(new Configuration(), new PartitionClass(),ar);
	      System.exit(0);
	   }
	
}
