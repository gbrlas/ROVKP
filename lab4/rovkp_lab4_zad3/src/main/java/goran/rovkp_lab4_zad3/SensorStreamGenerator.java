package goran.rovkp_lab4_zad3;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 *
 * @author goran
 */
public class SensorStreamGenerator {

    private static final int WAIT_PERIOD_IN_MILLISECONDS = 1;
    private static final int PORT = 10002;
 
    public static void main(String[] args) throws Exception {
        
        String input = "./pollutionData-all.csv";

        System.out.println("Waiting for client connection");

        try (ServerSocket serverSocket = new ServerSocket(PORT);
                Socket clientSocket = serverSocket.accept()) {

            System.out.println("Connection successful");

            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),
                    true);

            Stream<String> lines = Files.lines(Paths.get(args[0]));

            lines.forEach(line -> {
                out.println(line);
                try {
                    Thread.sleep(WAIT_PERIOD_IN_MILLISECONDS);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            });

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}