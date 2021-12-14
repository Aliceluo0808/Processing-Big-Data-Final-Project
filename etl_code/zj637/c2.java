import java.io.IOException;
import java.util.ArrayList;

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


public class c2
        extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException  {
        String line  = value.toString();

        line = line.trim();

        String[] list_of_columns = line.split(",");
        ArrayList<String> output1 = new ArrayList<>();
        String housing = " ";
        if (list_of_columns[15].equals("A152")){
            housing = "1";
        }else{
            housing = "0";
        }
        output1.add(housing); // housing
        int age = Integer.parseInt(list_of_columns[13]) ;

        String final_age = "";
        if (age < 25){
            final_age = "1";
        }else if (age > 25 && age <= 34){
            final_age = "2";
        }else if (age >= 35 && age <= 44){
            final_age = "3";
        }else if (age >= 45 && age <= 54){
            final_age = "4";
        }else if (age >= 55 && age <= 64){
            final_age = "5";
        }else if (age >= 65 && age <= 74){
            final_age = "6";
        }else{
            final_age = "7";
        }
        output1.add(final_age); // age

        String sex = " ";
        if (list_of_columns[9].equals("A91") || list_of_columns[9].equals("A93") || list_of_columns[9].equals("A94")){
            sex = "1";
        }else{
            sex = "0";
        }
        output1.add(sex); // sex







        String job = " ";
        if (list_of_columns[17].contains("A171") || list_of_columns[17].contains("A172")){
            job = "0";
        }else{
            job = "1";
        }
        output1.add(job); // job


        double monthly_income = Double.parseDouble(list_of_columns[5].trim())/Integer.parseInt(list_of_columns[2].trim());
        monthly_income = monthly_income/Double.parseDouble(list_of_columns[8].trim())*100;
        output1.add(String.valueOf(monthly_income/8616.099562)); // this average number if calculated previously to conduct normalization

        //credit
        if(list_of_columns[list_of_columns.length-1].equals("1")){
            output1.add("1");
        }else{
            output1.add("0");
        }
        String output = "";
        for (String v : output1 ){
            output += v;
            output += ",";
        }
        output = output.substring(0,output.length()-1);

        context.write(NullWritable.get(),new Text(output));


    }
}