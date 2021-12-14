import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;


public class c3
        extends Reducer<NullWritable, Text, NullWritable, Text> {
    public void reduce(NullWritable key, Text values, Context context) throws IOException, InterruptedException {
        context.write(key, new Text(""));
    }
}
