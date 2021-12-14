
import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable; 

class CleanReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
 
    public void reduce(NullWritable key, Text values, Context context) throws IOException, InterruptedException {
 		
 		context.write(key, values); 
    }
}