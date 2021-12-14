import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


class NullLinesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
 
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String line = value.toString();
        String[] input = line.split(",");
        if (input[18].equals("NULL") || input[18].equals(" ")){
            context.write(new Text("Number of Null Housing lines:"),new IntWritable(1));
        } 
        if (input[33].equals("NULL") || input[33].equals(" ")){
            context.write(new Text("Number of Null Employee lines:"),new IntWritable(1));
        } 
        if (input[22].equals("NULL") || input[22].equals(" ")){
            context.write(new Text("Number of Null Income lines:"),new IntWritable(1));
        } 
        if (input[53].equals("NULL") || input[53].equals(" ")){
            context.write(new Text("Number of Null Credit lines:"),new IntWritable(1));
        } 
        if (input[6].equals("NULL") || input[6].equals(" ")){
            context.write(new Text("Number of Null Gender lines:"),new IntWritable(1));
        } 
        if (input[50].equals("NULL") || input[50].equals(" ")){
            context.write(new Text("Number of Null Age lines:"),new IntWritable(1));
        } 
    }
}

class NullLinesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
 
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
 		int count = 0;
        // Counting the frequency of the keys which gives the number of lines passed into the mapper function
        for (IntWritable value : values) {
            count = count+value.get();
        }
        context.write(key, new IntWritable(count)); 
    }
}

public class NullLines{
 
    public static void main(String[] args) throws Exception { 
        
        if (args.length != 2) {
        System.err.println("Usage: Null Lines <input path> <output path>");
        System.exit(-1);
        }
 
        Job job = new Job(); 
        job.setJarByClass(NullLines.class); 
        job.setJobName("Null Lines");
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(NullLinesMapper.class);
        job.setReducerClass(NullLinesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}
