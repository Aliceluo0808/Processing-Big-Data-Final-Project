import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class TotalMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
 
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String line = value.toString();
        String[] input = line.split(",");
        double score = Double.parseDouble(input[0]);
        for (int i = 2; i < input.length; i++){
        	score += Double.parseDouble(input[i]);
        }
        String output = String.format("%.2f", score);
        
        context.write(NullWritable.get(), new Text(output));
    	}
	}

