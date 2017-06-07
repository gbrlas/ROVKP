/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_dz3_1;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/**
 *
 * @author goran
 */
public class Demo {
    
    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
        Map<Integer, String> jokes = new HashMap<>();
        String filePath = "./jester_dataset_2/jester_items.dat";
        String outputPath = "./item-similarity.csv";
        StandardAnalyzer analyzer = new StandardAnalyzer();
        Directory index;
        
        jokes = parseFile(filePath);
        index = createLuceneDocuments(analyzer, jokes);
        
        float[][] similarityMatrix = searchIndex(analyzer, index, jokes);
        similarityMatrix = normalizeMatrix(similarityMatrix);
        
        writeResults(similarityMatrix, outputPath);
        
        getAnswers(outputPath);
        
    }
    
    private static float[][] normalizeMatrix(float[][] similarityMatrix) {
        for (int i = 0; i < similarityMatrix.length; i++) {
            float max = 0;
            for (int j = 0; j < similarityMatrix.length; j++) {
                if (similarityMatrix[i][j] > max) {
                    max = similarityMatrix[i][j];
                }
            }
            
            for (int j = 0; j < similarityMatrix.length; j++) {
                similarityMatrix[i][j] = similarityMatrix[i][j] / max;
            }
        }
        
        for (int i = 0; i < similarityMatrix.length; i++) {
            for (int j = i; j < similarityMatrix.length; j++) {
                float value = (similarityMatrix[i][j] + similarityMatrix[j][i]) / 2;
                similarityMatrix[i][j] = similarityMatrix[j][i] = value;
            }
        }
        
        return similarityMatrix;
    }
    
    private static float[][] searchIndex(StandardAnalyzer analyzer, Directory index, Map<Integer, String> jokes) throws IOException, ParseException {
        int n = 150;
        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);
        float[][] similarityMatrix = new float[n][n];
        
        Iterator it = jokes.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            Query query = new QueryParser("text", analyzer).parse(QueryParser.escape(pair.getValue().toString()));
            TopDocs docs = searcher.search(query, n);
            ScoreDoc[] hits = docs.scoreDocs;
            
            for(int i=0;i < hits.length; ++i) {
                int docId = hits[i].doc;
                similarityMatrix[(int)pair.getKey()-1][docId] = hits[i].score;
            } 
        }
        
        return similarityMatrix;
    }
    
    private static Directory createLuceneDocuments(StandardAnalyzer analyzer, Map<Integer, String> jokes) throws IOException {
        
        Directory index = new RAMDirectory();

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter w = new IndexWriter(index, config);
        
        Iterator it = jokes.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            
            FieldType idFieldType = new FieldType(); 
            idFieldType.setStored(true); 
            idFieldType.setTokenized(false);

            idFieldType.setIndexOptions(IndexOptions.NONE);
            Field idField = new Field("ID", pair.getKey().toString(), idFieldType);
            
            FieldType textFieldType = new FieldType(); 
            textFieldType.setStored(false); 
            textFieldType.setTokenized(true);

            textFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
            Field textField = new Field("text", pair.getValue().toString(), textFieldType);
            
            addDoc(w, idField, textField);
        }
        
        w.close();
        
        return index;
    }
    
    private static void addDoc(IndexWriter w, Field idField, Field textField) throws IOException {
        Document doc = new Document();
        doc.add(idField);
        doc.add(textField);
        w.addDocument(doc);
    }

    private static Map<Integer, String> parseFile(String filePath) throws FileNotFoundException, IOException {
        InputStream fis = new FileInputStream(filePath);
	InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
	BufferedReader br = new BufferedReader(isr);
        String line;
        Map<Integer, String> jokes = new HashMap<>();
        StringBuilder builder = new StringBuilder();
        
        Integer key = 0;
        while ((line = br.readLine()) != null) {
            if (line.endsWith(":")) {
                key = Integer.parseInt(line.split(":")[0]);
            } else {
                if (line.trim().isEmpty()) {
                    String text = builder.toString();
                    text = StringEscapeUtils.unescapeXml(text.toLowerCase().replaceAll("\\<.*?\\>", ""));
                    builder.setLength(0);
                    jokes.put(key, text);
                    key = 0;
                } else {
                    builder.append(line);
                }
            }
	}
        
        return jokes;
    }

    private static void printMap(Map<Integer, String> jokes) {
        Iterator it = jokes.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());
        }

    }

    private static void writeResults(float[][] similarityMatrix, String filePath) {
        try{
            PrintWriter writer = new PrintWriter(filePath, "UTF-8");
            for (int i = 0; i < similarityMatrix.length; i++) {
                for (int j = i + 1; j < similarityMatrix.length; j++) {
                    if (similarityMatrix[i][j] != 0) {
                        writer.println((i+1) + "," + (j + 1) + "," + similarityMatrix[i][j]);
                    }
                }
            }
            writer.close();
        } catch (IOException e) {
           // do something
        }
    }

    private static void getAnswers(String outputPath) throws FileNotFoundException, IOException {
        InputStream fis = new FileInputStream(outputPath);
	InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
	BufferedReader br = new BufferedReader(isr);
        String line;
        int counter = 0;
        float max = 0;
        int id = 0;
        float similarityFinal = 0;
        
        while ((line = br.readLine()) != null) {
            counter++;
            int firstId = Integer.parseInt(line.split(",")[0]);
            int secondId = Integer.parseInt(line.split(",")[1].replaceAll("\\s+", ""));
            float similarity = Float.parseFloat(line.split(",")[2].replaceAll("\\s+", ""));
            
            if (firstId == 1) {
                if (similarity > max) {
                    max = similarity;
                    id = secondId;
                    similarityFinal = similarity;
                }
            }
	}
        
        System.out.println("U izlaznoj datoteci ima " + counter + " zapisa.");
        System.out.println("Sala najslicnija sa ID1 je " + id + ".");
        System.out.println("Similarity: " + similarityFinal);
    }
    

}
