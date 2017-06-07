/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_dz4_2;

import java.io.Serializable;
import org.apache.spark.api.java.function.Function;

/**
 *
 * @author goran
 */
public class USBabyNameRecord implements Serializable {
    String[] tokens;
    public static final Function<USBabyNameRecord, Boolean> parsable = 
            (record) -> {
        try {
            record.getId();
            record.getName();
            record.getYear();
            record.getGender();
            record.getState();
            record.getCount();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return false;
        }
        
        return true;
    };

    public USBabyNameRecord(String data) {
        tokens = data.split(",");
    }
    

    public long getId() {
        return Long.parseLong(tokens[0]);
    }

    public String getName() {
        return tokens[1];
    }
    
    public int getYear() {
        return Integer.parseInt(tokens[2]);
    }

    public String getGender() {
        return tokens[3];
    }
    
    public String getState() {
        return tokens[4];
    }

    public long getCount() {
        return Long.parseLong(tokens[5]);
    }

    public boolean isMale() {
        return getGender().equals("M");
    }
    
    public boolean isFemale() {
        return getGender().equals("F");
    }

    @Override
    public String toString() {
        return String.join(" ", tokens);
    }  
}
