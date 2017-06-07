package goran.rovkp_dz2_1_nocombiner;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TimeReducer
        extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double totalTime = 0;
        double min = 999999999;
        double max = 0;
        for (DoubleWritable value : values) {
            totalTime += value.get();
            
            if (value.get() < min) {
                min = value.get();
            }
            if (value.get() > max) {
                max = value.get();
            }
        }
        
        String result = totalTime + "   " + min + "   " + max;
        context.write(key, new Text(result));
    }
}
