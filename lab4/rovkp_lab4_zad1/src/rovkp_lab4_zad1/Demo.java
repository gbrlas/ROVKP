/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp_lab4_zad1;

/**
 *
 * @author goran
 */
public class Demo {

    public static void main(String[] args) throws Exception {
        String INPUT_DIR = "./pollutionData/";
        String OUTPUT_FILE = "./pollutionData-all.csv";
        
        Pollution.writeResults(INPUT_DIR, OUTPUT_FILE);
    }
    
}
