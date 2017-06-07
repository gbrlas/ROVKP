/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_dz4_2;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import scala.Tuple2;
import scala.tools.nsc.NewLinePrintWriter;

/**
 *
 * @author goran
 */
public class Demo {
    public static void main(String[] args) throws IOException {
        final String input = "./StateNames.csv";
        BabyNames rdd = new BabyNames(input);
        PrintWriter writer = new NewLinePrintWriter(new FileWriter("output.txt"));
        
        writer.println(String.format("Most unpopular female name: " + rdd.unpopularFemaleNames() + "\n"));
        
        writer.println("Most popular male names:\n");
        rdd.popularMaleNames(10).forEach(writer::println);
        writer.println();
        
        writer.println(String.format("1946 most prolific state: " + rdd.stateWithMostBirths() + "\n"));
        
        writer.println("Female birth timeline:");
        List<Tuple2<Integer, Long>> femaleTimeline = rdd.femaleBirthrateDistribution();
        femaleTimeline.forEach((kv) -> writer.println(String.format("%d %d", kv._1, kv._2)));
        writer.println();
        
        writer.println("Mary birth timeline:");
        List<Tuple2<Integer, Long>> maryTimeline = rdd.maryDistribution();
        for(int i = 0; i < maryTimeline.size(); ++i) {
            Tuple2<Integer, Long> females = femaleTimeline.get(i);
            Integer year = females._1;
            Long total = females._2;
            
            Long maries = maryTimeline.get(i)._2;
            writer.println(String.format("%d %f", year, 1.0 * maries /total));
        }
        writer.println();
        
        writer.println(String.format("total born: %s\n",rdd.totalBorn()));
        writer.println(String.format("distinct names: %d\n", rdd.countDistinctNames()));
        
        writer.flush();     
    }
}
