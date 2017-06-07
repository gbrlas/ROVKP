/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_lab2_zad1;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Ivana
 */
public class FilterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        DEBSRecordParser parser = new DEBSRecordParser();

        
        String record = value.toString();
        try {
            parser.parse(record);
            if (parser.filter()) {
                context.write(NullWritable.get(), new Text(parser.getInput()));
            }
        } catch (Exception ex) {
            System.out.println("Cannot parse: " + record + "due to the " + ex);
        }
       
    }
}
