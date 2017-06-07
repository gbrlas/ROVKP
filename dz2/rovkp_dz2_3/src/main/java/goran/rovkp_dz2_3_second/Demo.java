/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_dz2_3_second;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

        Job job = Job.getInstance();
        job.setJarByClass(Demo.class);
        job.setJobName("Total distance");

        FileInputFormat.addInputPath(job, new Path("/user/rovkp/gbrlas/trip_data.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/user/rovkp/gbrlas/results_dz2_zad1_combiner"));
        
        job.setMapperClass(TimeMapper.class);
        job.setCombinerClass(TimeCombiner.class);
        job.setReducerClass(TimeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CountAverageTriple.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
