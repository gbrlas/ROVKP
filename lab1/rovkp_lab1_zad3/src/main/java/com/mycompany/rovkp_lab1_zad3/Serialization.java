/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.rovkp_lab1_zad3;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

/**
 *
 * @author goran
 */
public class Serialization {
    
    public static void main(String[] args) {
        
        Configuration conf = new Configuration();

        Path output = new Path("/user/rovkp/gbrlas/ocitanja.bin");
        Path input = output;
        
        IntWritable key = new IntWritable();
        FloatWritable value = new FloatWritable();
               
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(output),
                    SequenceFile.Writer.keyClass(key.getClass()),
                    SequenceFile.Writer.valueClass(value.getClass()));
        } catch (IOException ex) {
            System.err.println("Couldn't create writer for a file...");
        }
        
        int numOfReadings = 100000;
        Random random = new Random();
        
        long startTime = System.currentTimeMillis();
        for(int i=0; i< numOfReadings; ++i){
            key.set(random.nextInt(100));
            value.set((float) (random.nextFloat() * 100.0));
            try {
                writer.append(key, value);
            } catch (IOException ex) {
                System.err.println("Couldn't append (key, val) pair to the file...");
            }
        }
        long endTime = System.currentTimeMillis();
        
        System.out.println("Creating file with readings took : " + (endTime - startTime) + " ms");
        
        try {
            writer.close();
        } catch (IOException ex) {
            System.err.println("Couldn't closr file...");
        }
        
        int[] sensorReadingNumbers = new int[100];
        for(int iter=0 ; iter<100; ++iter){
            sensorReadingNumbers[iter] = 0;
        }
        
        float[] sensorReadings = new float[100];
        for(int iter=0 ; iter<100; ++iter){
            sensorReadings[iter] = (float) 0.0;
        }
        
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(input));
        } catch (IOException ex) {
            System.err.println("Couldn't open file...");
        }
        
        startTime = System.currentTimeMillis();
        try {
            while((reader.next(key, value)) != false){
                sensorReadingNumbers[key.get()] += 1;
                sensorReadings[key.get()] += value.get();
            }
        } catch (IOException ex) {
            System.err.println("Problem while reading sensor readings...");
        }
        endTime = System.currentTimeMillis();
        System.out.println("Reading from file took : " + (endTime - startTime) + " ms");
        
        for(int readingNum = 0; readingNum < sensorReadingNumbers.length; ++readingNum){
            if(sensorReadingNumbers[readingNum] != 0){
                System.out.format("Sensor %d: %.5f\n", readingNum+1, sensorReadings[readingNum]/sensorReadingNumbers[readingNum]);
            } else {
                System.out.format("Sensor %d: %.5f\n", readingNum+1, 0.0);
            }
        }
    }
}
