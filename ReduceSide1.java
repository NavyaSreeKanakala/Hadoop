import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class ReduceSide1 {
	
	public static class ProdMapper extends
	Mapper<Object, Text, Text, Text> {
public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
	String record = value.toString();
	String[] parts = record.split(",");
	context.write(new Text(parts[0]), new Text("p\t" + parts[1]));
}
}

public static class SalesMapper extends
	Mapper<Object, Text, Text, Text> {
public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
	String record = value.toString();
	String[] parts = record.split(",");
	context.write(new Text(parts[0]), new Text("s\t" + parts[1]));
}
}

public static class ReduceJoinReducer extends
	Reducer<Text, Text, Text, Text> {
public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
    long prod=0,sales=0;
	for (Text t : values) {
		String parts[] = t.toString().split("\t");
		if (parts[0].equals("p")) {
			prod += Long.parseLong(parts[1]);
		} else if (parts[0].equals("s")) {
			sales += Long.parseLong(parts[1]);
		}
	}
	String str=String.valueOf(prod);
	String str1=String.valueOf(sales);
	String str2= str + " " + str1;
	context.write(new Text(key), new Text(str2));
}
}

public static void main(String[] args) throws Exception {

Configuration conf = new Configuration();
Job job = Job.getInstance(conf);
job.setJarByClass(ReduceSide1.class);
job.setJobName("Reduce Side Join");
job.setReducerClass(ReduceJoinReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);

MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, ProdMapper.class);
MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, SalesMapper.class);

Path outputPath = new Path(args[2]);
FileOutputFormat.setOutputPath(job, outputPath);
//outputPath.getFileSystem(conf).delete(outputPath);

System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
