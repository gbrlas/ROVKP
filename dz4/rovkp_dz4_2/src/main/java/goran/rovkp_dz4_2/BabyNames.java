/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_dz4_2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.concurrent.KVNode;

/**
 *
 * @author goran
 */
public class BabyNames {
    private static final SparkConf conf = new SparkConf().setAppName("BabyNames");
    private static JavaSparkContext context = null;
    private JavaRDD<String> lines;
    
    static {
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException e) {
            conf.setMaster("local");
        }
        
        context = new JavaSparkContext(conf);
    }

    public BabyNames(String input) {
        lines = context.textFile(input).persist(StorageLevel.MEMORY_ONLY());
    }
    
    public JavaRDD<USBabyNameRecord> getRecords() {
        return lines.map(USBabyNameRecord::new)
                .filter((Function<USBabyNameRecord, Boolean>) USBabyNameRecord.parsable);
    }
    
    public String unpopularFemaleNames() {
        return getRecords()
                .filter(USBabyNameRecord::isFemale)
                .mapToPair((record) -> new Tuple2<>(record.getName(), record.getCount()))
                .reduceByKey((sum, count) -> sum + count)
                .top(1, (Serializable & Comparator<Tuple2<String, Long>>)(kv1,kv2) 
                        -> -Long.compare(kv1._2, kv2._2))
                .get(0)._1;
    }
    
    public Stream<String> popularMaleNames(int num) {
        return getRecords()
                .filter(USBabyNameRecord::isMale)
                .mapToPair((record) -> new Tuple2<>(record.getName(), record.getCount()))
                .reduceByKey((sum, count) -> sum + count)
                .top(num, (Serializable & Comparator<Tuple2<String, Long>>)(kv1,kv2) 
                        -> -Long.compare(kv1._2, kv2._2))
                .stream()
                .map((kv) -> kv._1);
    }
    
    public String stateWithMostBirths() {
        return getRecords()
                .filter((record) -> record.getYear() == 1946)
                .mapToPair((record) -> new Tuple2<>(record.getState(), record.getCount()))
                .reduceByKey((sum, count) -> sum + count)
                .top(1, (Serializable & Comparator<Tuple2<String, Long>>)(kv1,kv2) 
                        -> -Long.compare(kv1._2, kv2._2))
                .get(0)._1;
    }
    
    public List<Tuple2<Integer, Long>> femaleBirthrateDistribution() {
        return getRecords()
                .filter(USBabyNameRecord::isFemale)
                .mapToPair((record) -> new Tuple2<>(record.getYear(), record.getCount()))
                .reduceByKey((sum, count) -> sum + count)
                .sortByKey().collect();
    }
    
    public List<Tuple2<Integer, Long>> maryDistribution() {
        return getRecords()
                .filter((record) -> record.getName().equals("Mary"))
                .mapToPair((record) -> new Tuple2<>(record.getYear(), record.getCount()))
                .reduceByKey((sum, count) -> sum + count)
                .sortByKey().collect();
    }
    
    public Long totalBorn() {
        return getRecords()
                .map((record) -> record.getCount())
                .reduce((sum, count) -> sum + count);
    }
    
    public Long countDistinctNames() {
        return getRecords()
                .map((record) -> record.getName())
                .distinct()
                .count();
    }
}
