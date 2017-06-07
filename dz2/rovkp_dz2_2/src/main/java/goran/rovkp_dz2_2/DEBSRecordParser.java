/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_dz2_2;

import java.util.ArrayList;
/**
 *
 * @author goran
 */
public class DEBSRecordParser {

    private String input;
    private int customers;
    private ArrayList<Double> startingCoordinates = new ArrayList<>();
    private ArrayList<Double> endingCoordinates = new ArrayList<>();

    public void parse(String record) throws Exception {
        input = record;
        String[] splitted = record.split(",");

        customers = Integer.parseInt(splitted[7]);
        startingCoordinates.add(Double.parseDouble(splitted[10]));
        startingCoordinates.add(Double.parseDouble(splitted[11]));
        endingCoordinates.add(Double.parseDouble(splitted[12]));
        endingCoordinates.add(Double.parseDouble(splitted[13]));
    }

    public String getInput() {
        return input;
    }

    public int getCustomers() {
        return customers;
    }

    public ArrayList<Double> getStartingCoordinates() {
        return startingCoordinates;
    }

    public ArrayList<Double> getEndingCoordinates() {
        return endingCoordinates;
    }
    
    public int innerCity() {
        if (startingCoordinates.get(0) >= -74 && startingCoordinates.get(0) <= -73.95 
                && endingCoordinates.get(0) >= -74 && endingCoordinates.get(0) <= -73.95
                && startingCoordinates.get(1) >= 40.75 && startingCoordinates.get(1) <= 40.8
                && endingCoordinates.get(1) >= 40.75 && endingCoordinates.get(1) <= 40.8) {
            return 0;
        }
        
        return 1;
    }
    
    
}
