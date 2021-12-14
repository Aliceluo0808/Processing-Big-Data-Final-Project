import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
 
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String line = value.toString();
        String[] input = line.split(",");
        String columns = "";
        double avg = 886.6784365999981;


        if (!input[6].equals("NULL") && !input[6].equals(" ") && !input[6].equals("N") && !input[18].equals("NULL") && !input[18].equals(" ")){
        	String housing = input[18];
	        if (housing.equals("1")){
	        	columns += housing;
	        }
	        else {
	        	columns += "0";
	        }
	        columns += ",";
	        int age = Integer.parseInt(input[50]);
	        if (age < 25){
	            columns += "1";
	        }else if (age > 25 && age <= 34){
	            columns += "2";
	        }else if (age >= 35 && age <= 44){
	            columns += "3";
	        }else if (age >= 45 && age <= 54){
	            columns += "4";
	        }else if (age >= 55 && age <= 64){
	            columns += "5";
	        }else if (age >= 65 && age <= 74){
	            columns += "6";
	        }else{
	            columns += "7";
	        }
	        columns += ",";
        	String sex = input[6];
	        if (sex.equals("M")){
	        	columns += "1";
	        }
	        else {
	        	columns += "0";
	        }
        	columns += ",";
	        String employ = input[33];
	        if (employ.equals("Y")){
	        	columns += "0"; 
	        }
	        else {
	        	columns += "1";
	        }
	        columns += ",";
	        double income = Double.parseDouble(input[22]);
	        income = income /avg;
	        columns += String.valueOf(income);
	        columns += ",";
	        String target = input[53];
	        if (target.equals("1")){
	        	columns += "0"; 
	        }
	        else {
	        	columns += "1";
	        }
        	context.write(NullWritable.get(), new Text(columns));
    	}
	}
}
