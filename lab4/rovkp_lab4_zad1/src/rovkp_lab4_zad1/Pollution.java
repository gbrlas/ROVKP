/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp_lab4_zad1;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 *
 * @author goran
 */
public class Pollution {
    private static final Pattern pattern = Pattern.compile("pollutionData\\d+\\.csv");
    
    public static Stream<String> lineStream(String path) throws Exception {
        File directory = new File(path);
        
        Stream<String> stream = Stream.empty();
        for(String fileName : directory.list()) {
            System.out.println(fileName);
            stream = Stream.concat(stream, Files.lines(Paths.get(path, fileName)));
        }
        
        return stream;
    }
    
    public static Stream<PollutionReading> readingStream(Stream<String> lines) {
        return lines.map(PollutionReading::new)
                .filter(PollutionReading.isParsable)
                .sorted(PollutionReading.readTimeComparator);
    }
    
    public static void writeResults(String inputPath, String outputPath) throws Exception{
        Stream<String> lines = lineStream(inputPath);
        Stream<PollutionReading> readings = readingStream(lines);
        Stream<String> sortedLines = readings.map(PollutionReading::toLine);
        
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath));
        PrintWriter pw = new PrintWriter(outputPath, "UTF-8");
        
        sortedLines.forEach(pw::write);
    }
}