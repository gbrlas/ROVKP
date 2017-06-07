/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_dz4_3;

import java.util.NoSuchElementException;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 *
 * @author goran
 */
public class Demo {
    private static final SparkConf conf = new SparkConf().setAppName("SensorScopeStreaming");
    private static final JavaStreamingContext context;
    private static final JavaDStream<String> lines;
    
    static {
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException e) {
            conf.setMaster("local");
        }
        
        context = new JavaStreamingContext(conf, Durations.seconds(5));
        lines = context.socketTextStream("localhost", 10002);
    }
    
    public static void main(String[] args) {
        String output = "./output/";
        
        JavaPairDStream<String, Double> result = lines
                .map(SensorScopeReading::new)
                .filter(SensorScopeReading.isParsable)
                .mapToPair((reading) -> new Tuple2<>(reading.getStationID(), reading.getSolarPanelCurrent()))
                .reduceByKeyAndWindow(Math::max, Durations.seconds(60), Durations.seconds(10));
        
        result.dstream().saveAsTextFiles(output, "txt");
    }
}
