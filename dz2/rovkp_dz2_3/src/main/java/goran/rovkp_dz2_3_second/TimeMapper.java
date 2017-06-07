package goran.rovkp_dz2_3_second;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TimeMapper extends Mapper<LongWritable, Text, Text, CountAverageTriple> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        DEBSRecordParser parser = new DEBSRecordParser();

        //skip the first line
        if (key.get() > 0) {
            String record = value.toString();
            try {
                parser.parse(record);
                double time = parser.getTime();
                context.write(new Text(parser.getMedallion()), new CountAverageTriple(time, time, time));
            } catch (Exception ex) {
                //System.out.println("Cannot parse: " + record + "due to the " + ex);
            }
        }
    }
}
