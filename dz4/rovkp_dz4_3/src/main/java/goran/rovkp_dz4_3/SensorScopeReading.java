package goran.rovkp_dz4_3;


import java.util.Comparator;
import java.util.function.Predicate;
import org.apache.spark.api.java.function.Function;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author goran
 */
public class SensorScopeReading {
    String[] tokens;
    
    public static final Comparator<SensorScopeReading> readTimeComparator = Comparator.comparing(SensorScopeReading::getTime);
    public static final Function<SensorScopeReading, Boolean> isParsable = (reading) -> {
        try{
            reading.getTime();
        } catch(Exception ex) {
            System.err.println(ex.getMessage());
            return false;
        }
        return true;
    };
    
    
    public SensorScopeReading(String line) {
        tokens = line.split("\\s");
    }
    
    public long getTime() {
        return Long.parseLong(tokens[7]);
    }
    
    public String getStationID() {
        return tokens[0];
    }
    
    public double getSolarPanelCurrent() {
        return Double.parseDouble(tokens[16]);
    }
    
    @Override
    public String toString() {
        return String.join(",", tokens);
    }
    
    public String toLine() {
        return this.toString() + '\n';
    }
}
