import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


class SumIncomeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
 
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String line = value.toString();
        String[] input = line.split(",");
        if (!input[22].equals("NULL") && !input[22].equals(" ")){
            double income = Double.parseDouble(input[22]);
            context.write(new Text("Income sum:"),new DoubleWritable(income));
        } 
    }
}

class SumIncomeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
 
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
 	    double sum = 0.0;
        // Counting the frequency of the keys which gives the number of lines passed into the mapper function
        for (DoubleWritable value : values) {
            sum = sum+value.get();
        }
        context.write(key, new DoubleWritable(sum)); 
    }
}

public class SumIncome{
 
    public static void main(String[] args) throws Exception { 
        
        if (args.length != 2) {
        System.err.println("Usage: Sum Income <input path> <output path>");
        System.exit(-1);
        }
 
        Job job = new Job(); 
        job.setJarByClass(SumIncome.class); 
        job.setJobName("Sum Income");
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(SumIncomeMapper.class);
        job.setReducerClass(SumIncomeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}
