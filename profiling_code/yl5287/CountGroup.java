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


class CountGroupMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
 
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        
        String line = value.toString();
        String[] input = line.split(",");
        if (!input[53].equals("NULL") && !input[53].equals(" ")){
            String credit = input[53];
            context.write(new Text("Credit: "+ credit),new IntWritable(1));
        } 
        if (!input[18].equals("NULL") && !input[18].equals(" ")){
            String housing = input[18];
            context.write(new Text("Housing: "+ housing),new IntWritable(1));
        } 
        if (!input[33].equals("NULL") && !input[33].equals(" ")){
            String employ = input[33];
            context.write(new Text("Employ: "+ employ),new IntWritable(1));
        } 
        if (!input[6].equals("NULL") && !input[6].equals(" ")){
            String gender = input[6];
            context.write(new Text("Gender: "+ gender),new IntWritable(1));
        } 
    }
}

class CountGroupReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
 
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
 		int count = 0;
        // get the frequency of good record by summing all the 1s together
        for (IntWritable value : values) {
            count = count+value.get();
        }
        context.write(key, new IntWritable(count)); 
    }
}

public class CountGroup{
 
    public static void main(String[] args) throws Exception { 
        
        if (args.length != 2) {
        System.err.println("Usage: Count Group <input path> <output path>");
        System.exit(-1);
        }
 
        Job job = new Job(); 
        job.setJarByClass(CountGroup.class); 
        job.setJobName("Count Group: ");
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(CountGroupMapper.class);
        job.setReducerClass(CountGroupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}
