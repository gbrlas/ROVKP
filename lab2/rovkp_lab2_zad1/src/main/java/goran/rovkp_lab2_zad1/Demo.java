/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_lab2_zad1;

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
        job.setJobName("WeekDayPartitioner Example");
        
        FileInputFormat.addInputPath(job, new Path("/user/rovkp/gbrlas/sorted_data.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/user/rovkp/gbrlas/results_lab2_zad1"));

        job.setMapperClass(FilterMapper.class);
        
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
