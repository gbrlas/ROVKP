/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.tel.rovkp.lab1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;

/**
 *
 * @author goran
 */
public class ReadAndWrite {
	static int counter = 0;

	public static void main(String[] args) throws IOException {
		long startTime = System.currentTimeMillis();

		File[] files = new File("/Users/goran/Downloads/gutenberg").listFiles();
		File f = new File("/Users/goran/Downloads/gutenberg/gutenberg_books.txt");

		if (!f.exists() && !f.isDirectory()) {
			f.createNewFile();
		}
		copyLines(files, f);

		long endTime = System.currentTimeMillis();
		System.out.println("Copying took " + (endTime - startTime) + " ms.");

		System.out.println("Broj procitanih redaka: " + counter);
	}

	public static void copyLines(File[] files, File f) throws IOException {
		for (File file : files) {
			if (file.isDirectory()) {
				copyLines(file.listFiles(), f);
			} else {
				if (file.getName().startsWith(".")) {
					continue;
				}

				String line;
				try (InputStream fis = new FileInputStream(file);
						InputStreamReader isr = new InputStreamReader(fis,
								Charset.forName("UTF-8"));
						BufferedReader br = new BufferedReader(isr);
						FileWriter fw = new FileWriter(f, true);
						BufferedWriter bw = new BufferedWriter(fw);
						PrintWriter out = new PrintWriter(bw);) {
					while ((line = br.readLine()) != null) {
						counter++;
						out.print(line);
					}
				}
			}
		}
	}
}
