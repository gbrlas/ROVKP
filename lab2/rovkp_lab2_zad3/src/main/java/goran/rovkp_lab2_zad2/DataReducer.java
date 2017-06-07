package goran.rovkp_lab2_zad2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataReducer
        extends Reducer<IntWritable, Text, NullWritable, Text> {
    
    public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
                           ) throws IOException, InterruptedException {
            int numRides[][] = new int[151][151];
            double totalAmount[][] = new double[151][151];
            for (Text val : values)
            {      
                String tokens[] = val.toString().split("-");
                int firstCoord = Integer.parseInt(tokens[0]);
                int secondCoord = Integer.parseInt(tokens[1]);
                numRides[firstCoord][secondCoord]++;
               
                double total_amount = Double.parseDouble(tokens[2]);
                totalAmount[firstCoord][secondCoord]+= total_amount;
            }
           
            int firstCoordNumRides = 0;
            int secondCoordNumRides = 0;
           
            int firstCoordTotalAmount = 0;
            int secondCoordTotalAmount = 0;
            for(int i=0;i<151;i++){
                for(int j=0; j<151; j++){
                    if (numRides[i][j] > numRides[firstCoordNumRides][secondCoordNumRides]){
                        firstCoordNumRides = i;
                        secondCoordNumRides = j;
                    }
                    if (totalAmount[i][j] > totalAmount[firstCoordTotalAmount][secondCoordTotalAmount]){
                        firstCoordTotalAmount = i;
                        secondCoordTotalAmount = j;
                    }
                }
            }
            context.write(NullWritable.get(), new Text("Hour - " + key));
            context.write(NullWritable.get(), new Text("Cell with the most rides is ["+ (firstCoordNumRides +1) + ", "+ (secondCoordNumRides+1) +"] with total number of rides - " + numRides[firstCoordNumRides][secondCoordNumRides]));
            context.write(NullWritable.get(), new Text("Cell with the most amount earned is ["+ (firstCoordTotalAmount+1) + ", "+ (secondCoordTotalAmount+1) +"] with total amount earned - " + totalAmount[firstCoordTotalAmount][secondCoordTotalAmount]));
        }
}
