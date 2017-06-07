package goran.rovkp_dz2_3_second;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TimeReducer
        extends Reducer<Text, CountAverageTriple, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<CountAverageTriple> values, Context context) throws IOException, InterruptedException {
        double totalTime = 0;
        double min = 999999999;
        double max = 0;
        
        for (CountAverageTriple value : values) {
            totalTime += value.getTotal();
            if (value.getMin() < min) {
                min = value.getMin();
            }
            if (value.getMax() > max) {
                max = value.getMax();
            }
        }
        
        String result = totalTime + "\t" + min + "\t" + max;
        context.write(key, new Text(result));
    }
}
