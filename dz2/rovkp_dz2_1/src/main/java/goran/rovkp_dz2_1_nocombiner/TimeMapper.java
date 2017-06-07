package goran.rovkp_dz2_1_nocombiner;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TimeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        DEBSRecordParser parser = new DEBSRecordParser();

        //skip the first line
        if (key.get() > 0) {
            String record = value.toString();
            try {
                parser.parse(record);
                context.write(new Text(parser.getMedallion()), new DoubleWritable(parser.getTime()));
            } catch (Exception ex) {
                //System.out.println("Cannot parse: " + record + "due to the " + ex);
            }
        }
    }
}
