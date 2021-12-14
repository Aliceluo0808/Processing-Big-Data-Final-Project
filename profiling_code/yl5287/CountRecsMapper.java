import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class CountRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
 
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        // all lines have same key with value 1

        context.write(new Text("Total number of records:"),new IntWritable(1));
    }
}
