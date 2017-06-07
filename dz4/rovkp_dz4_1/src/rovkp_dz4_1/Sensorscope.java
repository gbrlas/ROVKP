/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp_dz4_1;

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
public class Sensorscope {
    private static final Pattern pattern = Pattern.compile("sensorscope-monitor-\\d+\\.txt");
    public static Stream<String> lineStream(String path) throws Exception {
        File directory = new File(path);
        
        Stream<String> stream = Stream.empty();
        for(String fileName : directory.list((file, name) -> pattern.matcher(name).matches())) {
            stream = Stream.concat(stream, Files.lines(Paths.get(path, fileName)));
        }
        
        return stream;
    }
    
    public static Stream<SensorScopeReading> readingStream(Stream<String> lines) {
        return lines.map(SensorScopeReading::new)
                .filter(SensorScopeReading.isParsable)
                .sorted(SensorScopeReading.readTimeComparator);
    }
    
    public static void writeResults(String inputPath, String outputPath) throws Exception{
        Stream<String> lines = lineStream(inputPath);
        Stream<SensorScopeReading> readings = readingStream(lines);
        Stream<String> sortedLines = readings.map(SensorScopeReading::toLine);
        
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath));
        PrintWriter pw = new PrintWriter(outputPath, "UTF-8");
        
        sortedLines.forEach(pw::write);
    }
}