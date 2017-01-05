import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProfitReport {

	
	public static class salesMapper extends
	Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			
			context.write(new Text(parts[0]), new Text("s#" + parts[1]+"#"+parts[2]));
		}
	}
	
	public static class prodMapper extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			
			
			context.write(new Text(parts[0]), new Text("p#" + parts[2]));
		}
	}

	

	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
			
			float pr1 = 0;
			float pr2 = 0;
			float sp1=0,cp1=0;
			float sp2=0,cp2=0;
			int quantity1=0,quantity2=0;
			float net_sold=0;
		
			for (Text t : values) {
				String parts[] = t.toString().split("#");
				
				
				if (parts[0].equals("s")) {
					quantity1=quantity2;
					quantity2=Integer.parseInt(parts[1]);
					sp1=sp2;
					sp2=Float.parseFloat(parts[2]);
					
				}
				
				if (parts[0].equals("p")) {
					
					cp1=cp2;
					cp2=Float.parseFloat(parts[1]);
					
				}
				
			}
			pr1=(quantity1*(sp1-cp1));
			pr2=(quantity2*(sp2-cp2));
			net_sold=pr1+pr2;
			
			String str=String.format("%f",net_sold);
			
			context.write(new Text(key), new Text(str));
		
	}
}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
	    job.setJarByClass(ProfitReport.class);
	    job.setJobName("Reduce Side Join");
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, prodMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, salesMapper.class);
		
		Path outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}