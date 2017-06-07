/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_lab2_zad2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author Ivana
 */
public class Demo {

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(Demo.class);
        job.setJobName("Partitioner");
        
        FileInputFormat.addInputPath(job, new Path("/user/rovkp/gbrlas/results_lab2_zad1/part-m-00000"));
        FileInputFormat.addInputPath(job, new Path("/user/rovkp/gbrlas/results_lab2_zad1/part-m-00001"));
        FileInputFormat.addInputPath(job, new Path("/user/rovkp/gbrlas/results_lab2_zad1/part-m-00002"));
        FileOutputFormat.setOutputPath(job, new Path("/user/rovkp/gbrlas/results_lab2_zad2"));

        job.setMapperClass(DataMapper.class);
        job.setPartitionerClass(DataPartitioner.class);
        job.setReducerClass(DataReducer.class);
        job.setNumReduceTasks(24);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
