/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.rovkp_dz1;
 
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
 
/**
 *
 * @author goran
 */
@SuppressWarnings("UnusedAssignment")
public class ReadWriteHadoop {
   
    /**
     * @param args the command line arguments
     */
    @SuppressWarnings({"CallToPrintStackTrace", "SuspiciousIndentAfterControlStatement"})
    public static void main(String[] args) {
       
        Configuration conf = new Configuration();
        LocalFileSystem lfs = null;
        
        try {
            lfs = FileSystem.getLocal(conf);
        } catch (IOException ex) {
            Logger.getLogger(ReadWriteHadoop.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        FileSystem dfs = null;
        try {
            dfs = FileSystem.get(conf);
        } catch (IOException ex) {
            System.out.println("Unable to create DFS! Closing program...");
            System.exit(-1);
        }
        
        String file1 = "/home/rovkp/ROVKP_DZ1/gutenberg.zip";
        String file2 = "/user/rovkp/gutenberg.zip";
        
        Path lsPath = new Path(file1);
        Path dfsPath = new Path(file2);
        
        try {
            if (!lfs.isFile(lsPath)) {
                System.out.println("File " + file1 + " doesn't exist on the local file system.");
            } else {
                System.out.println("File " + file1 + " exists on the local file system.");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        
        try {
            if (!dfs.isFile(dfsPath)) {
                System.out.println("File " + file2 + " doesn't exist on the local file system.");
            } else {
                System.out.println("File " + file2 + " exists on the local file system.");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        
        String folder1 = "/home/rovkp/ROVKP_DZ1/";
        String folder2 = "/user/rovkp/";
        lsPath = new Path(folder1);
        dfsPath = new Path(folder2);
        
        try {
            if (!lfs.isDirectory(lsPath)) {
                System.out.println("Directory " + folder1 + " doesn't exist on the local file system.");
            } else {
                System.out.println("Directory " + folder1 + " exists on the distributed local file system.");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        
        try {
            if (!dfs.isDirectory(dfsPath)) {
                System.out.println("Directory " + folder2 + " doesn't exist on the local file system.");
            } else {
                System.out.println("Directory " + folder2 + " exists on the distributed local file system.");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
   
}