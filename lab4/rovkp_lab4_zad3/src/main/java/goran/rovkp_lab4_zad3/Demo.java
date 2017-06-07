/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_lab4_zad3;

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
    private static final SparkConf conf = new SparkConf().setAppName("PollutionStreaming");
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
    
    public static void main(String[] args) throws InterruptedException {
        String output = "./output/";
        
        JavaPairDStream<String, Double> result = lines
                .map(PollutionReading::new)
                .filter(PollutionReading.isParsable)
                .mapToPair((reading) -> new Tuple2<>(reading.getStationID(), reading.getOzone()))
                .reduceByKeyAndWindow(Math::max, Durations.seconds(45), Durations.seconds(15));
        
        result.dstream().saveAsTextFiles(output, "txt");
        
        context.start();
        context.awaitTermination();
    }
}
