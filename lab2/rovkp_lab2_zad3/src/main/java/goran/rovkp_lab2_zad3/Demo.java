/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_lab2_zad3;

import goran.rovkp_lab2_zad1.FilterMapper;
import goran.rovkp_lab2_zad2.DataMapper;
import goran.rovkp_lab2_zad2.DataPartitioner;
import goran.rovkp_lab2_zad2.DataReducer;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author goran
 */
public class Demo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        Job job1 = Job.getInstance();
        job1.setJarByClass(Demo.class);
        job1.setJobName("Cell preprocessing job");
        
        FileInputFormat.addInputPath(job1, new Path("/user/rovkp/debs2015full/sorted_data.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("/user/rovkp/gbrlas/results_lab2_zad3"));

        job1.setMapperClass(FilterMapper.class);
        job1.setNumReduceTasks(0);

        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);

        boolean success = job1.waitForCompletion(true);
        ArrayList<String> inputs = new ArrayList<>();
        ArrayList<String> outputs = new ArrayList<>();
        
        
        if (success) {
            Job job = Job.getInstance();
            job.setJarByClass(Demo.class);
            job.setJobName("Cell data calculator job");

            FileInputFormat.setInputDirRecursive(job, true);

            FileInputFormat.addInputPath(job, new Path("/user/rovkp/gbrlas/results_lab2_zad3/"));
            FileOutputFormat.setOutputPath(job, new Path("/user/rovkp/gbrlas/results_lab2_zad3_final"));

            job.setMapperClass(DataMapper.class);
            job.setPartitionerClass(DataPartitioner.class);
            job.setReducerClass(DataReducer.class);

            job.setNumReduceTasks(24);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.waitForCompletion(true);
        }
        
        Path folderToDelete = new Path("/user/rovkp/gbrlas/results_lab2_zad3/");
        FileSystem hdfs = FileSystem.get(conf);

        if (hdfs.exists(folderToDelete)) {
            hdfs.delete(folderToDelete, true);
        }

    }
}
