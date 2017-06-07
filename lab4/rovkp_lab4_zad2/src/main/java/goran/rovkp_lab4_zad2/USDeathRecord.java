/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_lab4_zad2;

import org.apache.spark.api.java.function.Function;

/**
 *
 * @author goran
 */
public class USDeathRecord {
    String[] tokens;
    
    public static final Function<USDeathRecord, Boolean> parsable = 
            (record) -> {
        
        try {
            record.getId();
            record.getGender();
            record.getAge();
            record.getAutopsy();
            record.getMaritalStatus();
            record.getMannerOfDeath();
            record.getMonthOfDeath();
            record.getDayOfWeek();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return false;
        }
        
        return true;
    };

    public USDeathRecord(String data) {
        tokens = data.split(",");
    }
    
    public long getId() {
        return Long.parseLong(tokens[0]);
    }
    
    public String getGender() {
        return tokens[6];
    }
    
    public int getAge() {
        return Integer.parseInt(tokens[8]);
    }
    
    public String getAutopsy() {
        return tokens[21];
    }
    
    public String getMaritalStatus() {
        return tokens[15];
    }
    
    public int getMannerOfDeath() {
        return Integer.parseInt(tokens[19]);
    }
    
    public int getMonthOfDeath() {
        return Integer.parseInt(tokens[5]);
    }
    
    public int getDayOfWeek() {
        return Integer.parseInt(tokens[16]);
    }
    
    public boolean isMale() {
        return getGender().equals("M");
    }
    
    public boolean isFemale() {
        return getGender().equals("F");
    }
    
    public boolean autopsyDone() {
        return getAutopsy().equals("Y");
    }
    
    public boolean isMarried() {
        return getMaritalStatus().equals("M");
    }
    
    @Override
    public String toString() {
        return String.join(" ", tokens);
    } 
    
    
    
}
