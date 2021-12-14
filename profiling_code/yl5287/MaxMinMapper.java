import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable; import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class MaxMinMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] input = line.split(",");
        if (!input[50].equals("NULL") && !input[50].equals(" ")){
            double age = Double.parseDouble(input[50]);
            context.write(new Text("Age:"),new DoubleWritable(age));
        } 
        if (!input[22].equals("NULL") && !input[22].equals(" ")){
            double income = Double.parseDouble(input[22]);
            context.write(new Text("Income:"),new DoubleWritable(income));
        }
}
}