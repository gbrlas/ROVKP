/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_dz2_3;


import goran.rovkp_dz2_3_first.PatternMapper;
import goran.rovkp_dz2_3_first.PatternPartitioner;
import goran.rovkp_dz2_3_first.PatternReducer;
import goran.rovkp_dz2_3_second.CountAverageTriple;
import goran.rovkp_dz2_3_second.TimeCombiner;
import goran.rovkp_dz2_3_second.TimeMapper;
import goran.rovkp_dz2_3_second.TimeReducer;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
        Job job1 = Job.getInstance();
        job1.setJarByClass(Demo.class);
        job1.setJobName("Partitioner job");
        
        FileInputFormat.addInputPath(job1, new Path("/user/rovkp/gbrlas/trip_data.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("/user/rovkp/gbrlas/results_dz2_zad3_partitions"));

        job1.setMapperClass(PatternMapper.class);
        job1.setPartitionerClass(PatternPartitioner.class);
        job1.setReducerClass(PatternReducer.class);
        job1.setNumReduceTasks(6);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);

        boolean success = job1.waitForCompletion(true);
        ArrayList<String> inputs = new ArrayList<>();
        ArrayList<String> outputs = new ArrayList<>();
        
        inputs.add("/user/rovkp/gbrlas/results_dz2_zad3_partitions/part-r-00000");
        inputs.add("/user/rovkp/gbrlas/results_dz2_zad3_partitions/part-r-00001");
        inputs.add("/user/rovkp/gbrlas/results_dz2_zad3_partitions/part-r-00002");
        inputs.add("/user/rovkp/gbrlas/results_dz2_zad3_partitions/part-r-00003");
        inputs.add("/user/rovkp/gbrlas/results_dz2_zad3_partitions/part-r-00004");
        inputs.add("/user/rovkp/gbrlas/results_dz2_zad3_partitions/part-r-00005");
        
        outputs.add("/user/rovkp/gbrlas/results_dz2_zad3_final-00");
        outputs.add("/user/rovkp/gbrlas/results_dz2_zad3_final-01");
        outputs.add("/user/rovkp/gbrlas/results_dz2_zad3_final-02");
        outputs.add("/user/rovkp/gbrlas/results_dz2_zad3_final-03");
        outputs.add("/user/rovkp/gbrlas/results_dz2_zad3_final-04");
        outputs.add("/user/rovkp/gbrlas/results_dz2_zad3_final-05");
        
        if (success) {
            for (int i = 0; i < 6; i++) {
                Job job = Job.getInstance();
                job.setJarByClass(Demo.class);
                job.setJobName("Total, min and max time");

                FileInputFormat.addInputPath(job, new Path(inputs.get(i)));
                FileOutputFormat.setOutputPath(job, new Path(outputs.get(i)));

                job.setMapperClass(TimeMapper.class);
                job.setCombinerClass(TimeCombiner.class);
                job.setReducerClass(TimeReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(CountAverageTriple.class);
                
                job.waitForCompletion(true);
            }
        }
    }
}
