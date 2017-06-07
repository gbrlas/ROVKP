/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_lab4_zad2;

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
        final String input = "./DeathRecords.csv";
        DeathRecords rdd = new DeathRecords(input);
        PrintWriter writer = new NewLinePrintWriter(new FileWriter("output.txt"));

        writer.println(String.format("Female deaths in june: " + rdd.deathsInJune() + "\n"));
        writer.println(String.format("Day with most male deaths aged over 50: " + rdd.dayOfMostMaleDeaths() + "\n"));
        writer.println(String.format("Total autopsies done: " + rdd.totalAutopsies() + "\n"));
        
        writer.println("Male deaths between ages 45 and 65 timeline:");
        List<Tuple2<Integer, Integer>> maleTimeline = rdd.maleDeathsByMonth();
        maleTimeline.forEach((kv) -> writer.println(String.format("%d %d", kv._1, kv._2)));
        writer.println();
        
        writer.println("Married men deaths distribution:");
        List<Tuple2<Integer, Integer>> marriedDistribution = rdd.marriedDistribution();
        for(int i = 0; i < marriedDistribution.size(); ++i) {
            Tuple2<Integer, Integer> males = marriedDistribution.get(i);
            Integer month = males._1;
            Integer total = males._2;
            
            Integer dist = marriedDistribution.get(i)._2;
            writer.println(String.format("%d %f", month, 1.0 * dist /total));
        }
        writer.println();
        

        writer.println(String.format("Total deaths in accidents: " + rdd.totalDeathsInAccidents() + "\n"));
       
        writer.println(String.format("Distinct ages: %d\n", rdd.countDistinctAges()));
    }
}
