/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.rovkp_lab1_zad2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author goran
 */
public class GutenbergLab {
     static int fileCounter = 0;
    
    public static void main(String[] args) throws URISyntaxException {
        
        Configuration conf = new Configuration();
        
        LocalFileSystem lfs = null;
        try {
            lfs = FileSystem.getLocal(conf);
        } catch (IOException ex) {
            System.out.println("Cannot create Local File System!");
            System.exit(-1);
        }
       
        FileSystem dfs = null;
        try {
            dfs = FileSystem.get(new URI("hdfs://cloudera2:8020"), conf);
        } catch (IOException ex) {
            System.out.println("Cannot create Distributed File System!");
            System.exit(-1);
        }

        Path output = new Path("/user/rovkp/gbrlas/gutenberg_books.txt");
        Path input = new Path("/home/rovkp/gbrlas/gutenberg");
        
        int counter = 0;
        FileStatus[] files = null; 
        try {
            files = lfs.listStatus(input);
        } catch (IOException ex) {
            Logger.getLogger(GutenbergLab.class.getName()).log(Level.SEVERE, null, ex);
        }
              
        PrintWriter out = null;
        try {
             out =  new PrintWriter(new BufferedWriter(new OutputStreamWriter(dfs.create(output))));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    
        long startTime = System.currentTimeMillis();
        if(out != null){
            try {
                counter = copyAllLines(files, out, lfs, counter);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        } else {
            System.err.println("Cannot write to file! Closing program...");
            System.exit(-1);
        }
        long endTime = System.currentTimeMillis();
        
        System.out.println("Copying took: " + (endTime - startTime) + " ms");
        System.out.println("Number of lines: " + counter);
        System.out.println("Number of files: " + fileCounter);
    }

    public static int copyAllLines(FileStatus[] files, PrintWriter out, LocalFileSystem lfs, int counter) throws IOException {
        for (FileStatus file : files) {
            if (file.isDirectory()) {
                counter = copyAllLines(lfs.listStatus(file.getPath()), out, lfs, counter);
            } else {
                fileCounter++;
                if (lfs.pathToFile(file.getPath()).getName().startsWith(".")) {
                    continue;
                }
                String line;
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(lfs.pathToFile(file.getPath())),
                                            Charset.forName("UTF-8")));
                while ((line = br.readLine()) != null) {
                    counter++;
                    out.print(line);
                }
            }
        }
        return counter;
    }
    
}
