/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_dz2_2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author goran
 */
public class Demo {

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(Demo.class);
        job.setJobName("Partitioner");
        
        

        FileInputFormat.addInputPath(job, new Path("/user/rovkp/gbrlas/trip_data.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/user/rovkp/gbrlas/results_dz2_zad2"));

        job.setMapperClass(PatternMapper.class);
        job.setPartitionerClass(PatternPartitioner.class);
        job.setReducerClass(PatternReducer.class);
        job.setNumReduceTasks(6);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
