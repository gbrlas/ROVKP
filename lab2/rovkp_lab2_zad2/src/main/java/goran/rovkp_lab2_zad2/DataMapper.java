package goran.rovkp_lab2_zad2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataMapper extends Mapper<LongWritable,Text,IntWritable,Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        DEBSRecordParser parser = new DEBSRecordParser();
        
        String tokens[] = value.toString().split(",");
 
        Float pickup_longitude = Float.parseFloat(tokens[6]);
        Float pickup_latitude = Float.parseFloat(tokens[7]);
        double total_amount = Double.parseDouble(tokens[16]);
        int firstCoord = parser.latitude_to_grid(pickup_latitude);
        int secondCoord = parser.longitude_to_grid(pickup_longitude);

        //example
        //2013-01-20 23:55:31
        String pickup_datetime = tokens[2];
        String datetimeSplit[] = pickup_datetime.split(" ");

        //23:55:31
        String timeSplit[] = datetimeSplit[1].split(":");
        IntWritable hour = new IntWritable(Integer.parseInt(timeSplit[0]));

        Text tex = new Text(firstCoord + "-" +secondCoord + "-" + total_amount);
        context.write(hour, tex);
    }
}
