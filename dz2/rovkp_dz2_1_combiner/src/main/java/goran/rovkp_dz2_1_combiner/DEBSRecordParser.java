/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_dz2_1_combiner;

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class DEBSRecordParser {

    private String medallion;
    private double time;

    public void parse(String record) throws Exception {
        String[] splitted = record.split(",");
        medallion = splitted[0];
        time = Double.parseDouble(splitted[8]);
    }

    public String getMedallion() {
        return medallion;
    }

    public double getTime() {
        return time;
    }

}
