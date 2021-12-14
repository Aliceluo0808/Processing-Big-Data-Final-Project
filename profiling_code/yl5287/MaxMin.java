import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class MaxMin {
    public static void main(String[] args) throws Exception { 
        if (args.length != 2) {
        System.err.println("Usage: MaxMin <input path> <output path>");
        System.exit(-1);
    }
        Job job = new Job(); 
        job.setJarByClass(MaxMin.class); 
        job.setJobName("Max min");
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MaxMinMapper.class);
        job.setReducerClass(MaxMinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1); }
}
