/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_lab2_zad1;

import java.util.ArrayList;
/**
 *
 * @author goran
 */
public class DEBSRecordParser {

    private String input;
    private double totalAmount;
    private ArrayList<Double> startingCoordinates = new ArrayList<>();
    private ArrayList<Double> endingCoordinates = new ArrayList<>();
    private String hour;
    
    private final double BEGIN_LON = -74.913585;
    private final double BEGIN_LAT = 41.474937;
    private final double GRID_LENGTH = 0.011972;
    private final double GRID_WIDTH = 0.008983112;

    public void parse(String record) throws Exception {
        input = record;
        String[] splitted = record.split(",");

        hour = calculateHour(splitted[2]);
        startingCoordinates.add(Double.parseDouble(splitted[7]));
        startingCoordinates.add(Double.parseDouble(splitted[6]));
        endingCoordinates.add(Double.parseDouble(splitted[9]));
        endingCoordinates.add(Double.parseDouble(splitted[8]));
        
        totalAmount = Double.parseDouble(splitted[16]);
    }

    public String getInput() {
        return input;
    }

    public ArrayList<Double> getStartingCoordinates() {
        return startingCoordinates;
    }

    public ArrayList<Double> getEndingCoordinates() {
        return endingCoordinates;
    }
    
    public boolean filter() {
        int[] startingCell = {firstCellInt(startingCoordinates.get(0)), 
            secondCellInt(startingCoordinates.get(1))};
        int[] endingCell = {firstCellInt(endingCoordinates.get(0)), 
            secondCellInt(endingCoordinates.get(1))};
        
        if (startingCell[0] >= 0 && startingCell[0] <= 150
                && startingCell[1] >= 0 && startingCell[1] <= 150
                && endingCell[0] >= 0 && endingCell[0] <= 150
                && endingCell[1] >= 0 && endingCell[1] <= 150
                && totalAmount > 0) {
            return true;
        }
        
        return false;
    }
    
    private int secondCellInt (double longitude) {
        return (int) ((longitude - BEGIN_LON) / GRID_LENGTH);
    }
    
    private int firstCellInt (double latitude) {
        return (int) ((BEGIN_LAT - latitude) / GRID_WIDTH);
    }

    private String calculateHour(String str) {
        return str.split("\\s+")[1].split(":")[0];
    }
    
}
