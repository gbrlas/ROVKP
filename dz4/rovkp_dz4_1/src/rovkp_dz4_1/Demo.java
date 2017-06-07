/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp_dz4_1;

/**
 *
 * @author goran
 */
public class Demo {

    public static void main(String[] args) throws Exception {
        String INPUT_DIR = "./sensorscope-monitor/";
        String OUTPUT_FILE = "./sensorscope-monitor-all.csv";
        
        Sensorscope.writeResults(INPUT_DIR, OUTPUT_FILE);
    }
    
}
