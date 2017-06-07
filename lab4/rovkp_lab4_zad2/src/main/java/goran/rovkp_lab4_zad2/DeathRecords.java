/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_lab4_zad2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *
 * @author goran
 */
public class DeathRecords {
    private static final SparkConf conf = new SparkConf().setAppName("DeathRecords");
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
    
    public DeathRecords(String input) {
        lines = context.textFile(input).persist(StorageLevel.MEMORY_ONLY());
    }
    
     public JavaRDD<USDeathRecord> getRecords() {
        return lines.map(USDeathRecord::new)
                .filter((Function<USDeathRecord, Boolean>) USDeathRecord.parsable);
    }
     
    public Long deathsInJune() {
        return getRecords()
                .filter(USDeathRecord::isFemale)
                .filter((record) -> record.getMonthOfDeath() == 6)
                .count();
    }
    
    public Integer dayOfMostMaleDeaths() {
        return getRecords()
                .filter(USDeathRecord::isMale)
                .filter((record) -> record.getAge() > 50)
                .mapToPair((record) -> new Tuple2<>(record.getDayOfWeek(), 1))
                .reduceByKey((sum, count) -> sum + count)
                .top(1, (Serializable & Comparator<Tuple2<Integer, Integer>>)(kv1,kv2) 
                        -> Long.compare(kv1._2, kv2._2))
                .get(0)._1;
    }
    
    public int totalAutopsies() {
        return getRecords()
                .filter((record) -> record.autopsyDone())
                .map((record) -> 1)
                .reduce((sum, count) -> sum + count);
    }
    
    public List<Tuple2<Integer, Integer>> maleDeathsByMonth() {
        return getRecords()
                .filter(USDeathRecord::isMale)
                .filter((record) -> record.getAge() >= 45 && record.getAge() <= 65)
                .mapToPair((record) -> new Tuple2<>(record.getMonthOfDeath(), 1))
                .reduceByKey((sum, count) -> sum + count)
                .sortByKey().collect();
    }
    
    public List<Tuple2<Integer, Integer>> marriedDistribution() {
        return getRecords()
                .filter(USDeathRecord::isMale)
                .filter(USDeathRecord::isMarried)
                .filter((record) -> record.getAge() >= 45 && record.getAge() <= 65)
                .mapToPair((record) -> new Tuple2<>(record.getMonthOfDeath(), 1))
                .reduceByKey((sum, count) -> sum + count)
                .sortByKey().collect();
    }
     
    public Integer totalDeathsInAccidents() {
        return getRecords()
                .filter((record) -> record.getMannerOfDeath() == 1)
                .map((record) -> 1)
                .reduce((sum, count) -> sum + count);
    }
     
    public Long countDistinctAges() {
        return getRecords()
                .map((record) -> record.getAge())
                .distinct()
                .count();
    }
}
