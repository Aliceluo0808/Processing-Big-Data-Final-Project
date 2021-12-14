
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MaxMinReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double maxValue = Double.MIN_VALUE; 
        double minValue = Double.MAX_VALUE; 
        for (DoubleWritable value : values) {
            maxValue = Math.max(maxValue, value.get());
            minValue = Math.min(minValue, value.get());
        }
        context.write(new Text("Max " + key), new DoubleWritable(maxValue)); 
        context.write(new Text("Min " + key), new DoubleWritable(minValue)); 
    }
}