/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_dz2_3_first;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Ivana
 */
public class PatternMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        DEBSRecordParser parser = new DEBSRecordParser();

        //skip the first line
        if (key.get() > 0) {
            String record = value.toString();
            try {
                parser.parse(record);
                context.write(new IntWritable(parser.innerCity()), new Text(parser.getInput()));
            } catch (Exception ex) {
                System.out.println("Cannot parse: " + record + "due to the " + ex);
            }
        }
    }
}
