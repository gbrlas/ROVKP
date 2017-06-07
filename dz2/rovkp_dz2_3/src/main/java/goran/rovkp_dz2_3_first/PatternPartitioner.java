/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_dz2_3_first;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author Ivana
 */
public class PatternPartitioner extends Partitioner<IntWritable, Text> {

    @Override
    public int getPartition(IntWritable key, Text value, int numberOfPartitions) {
        DEBSRecordParser parser = new DEBSRecordParser();
        try {
            parser.parse(value.toString());
        } catch (Exception ex) {
            Logger.getLogger(PatternPartitioner.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        switch (key.get()) {
            case 0:
                switch(getCategory(parser.getCustomers())) {
                    case 1:
                        return 0;
                    case 2:
                        return 1;
                    case 3:
                        return 2;
                    default:
                        return 6;
                }
            case 1:
                switch(getCategory(parser.getCustomers())) {
                    case 1:
                        return 3;
                    case 2:
                        return 4;
                    case 3:
                        return 5;
                    default:
                        return 6;
                }
            default:
                return 6;
        }
    }
    
    private int getCategory(int customers) {
        if (customers == 1) {
            return 1;
        } else if (customers == 2 || customers == 3) {
            return 2;
        } else {
            return 3;
        }
    }

}
