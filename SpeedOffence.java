import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpeedOffence {

	public static class MapClass extends Mapper <LongWritable, Text, Text, FloatWritable>
    {
        public void map (LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
             try{
                  String[] str = value.toString().split(",");
                  float speed = Float.parseFloat(str[1]);
                  con.write(new Text(str[0]), new FloatWritable (speed));
              }
              catch(Exception e)
              {
                  e.printStackTrace();
              }
        }
    }
    public static class ReduceClass extends Reducer <Text, FloatWritable, Text, FloatWritable>
    {
        private FloatWritable result = new FloatWritable();
        public void reduce (Text key, Iterable<FloatWritable> values, Context con) throws IOException, InterruptedException
        {
            float totalcount=0;
            float offcount = 0;
            for (FloatWritable spd:values)
            {
                if (spd.get()>65)
                {
                    offcount++;
                }
               
                totalcount++;
            }
            result.set((offcount*100/totalcount));
            con.write(key, result);
        }
    }
       
        public static void main(String[] args) throws Exception
        {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "speed offence");
            job.setJarByClass(SpeedOffence.class);
            job.setMapperClass(MapClass.class);
            job.setReducerClass(ReduceClass.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
       
        }
	
}
