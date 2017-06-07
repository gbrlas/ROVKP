package rovkp_lab4_zad1;


import java.util.Comparator;
import java.util.function.Predicate;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author goran
 */
public class PollutionReading {
    String[] tokens;
    
    public static final Comparator<PollutionReading> readTimeComparator = Comparator.comparing(PollutionReading::getTime);
    public static final Predicate<PollutionReading> isParsable = (reading) -> {
        try{
            reading.getTime();
        } catch(Exception ex) {
            System.err.println(ex.getMessage());
            return false;
        }
        return true;
    };
    
    
    public PollutionReading(String line) {
        tokens = line.split(",");
    }
    
    public String getTime() {
        return tokens[7];
    }
    
    @Override
    public String toString() {
        return String.join(",", tokens);
    }
    
    public String toLine() {
        return this.toString() + '\n';
    }
}
