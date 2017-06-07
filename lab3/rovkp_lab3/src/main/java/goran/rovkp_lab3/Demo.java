/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_lab3;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

/**
 *
 * @author goran
 */
public class Demo {
    public static void main(String[] args) throws IOException, TasteException {
        double[][] collaborativeMatrix = new double[150][150];
        double[][] itemMatrix = new double[150][150];
        String outputPath = "./hybrid_item_similarity.csv";
        
        DataModel model = new FileDataModel(
                new File("./jester_dataset_2/jester_ratings.dat"), "\\s+");
        
        ItemSimilarity similarity = new FileItemSimilarity(
                new File("./item-similarity.csv"));
        CollaborativeItemSimilarity cls = new CollaborativeItemSimilarity(model);
        
        for (int i = 0; i < 150; i++) {
            for (int j = 0; j < 150; j++) {
                collaborativeMatrix[i][j] = cls.itemSimilarity(i, j);
                itemMatrix[i][j] = similarity.itemSimilarity(i, j);
            }
        }
        
        collaborativeMatrix = normalizeMatrix(collaborativeMatrix);
        itemMatrix = normalizeMatrix(itemMatrix);
        
        double[][] hybridMatrix = calculateHybridMatrix(collaborativeMatrix, itemMatrix, 0.5, 0.5);
        writeResults(hybridMatrix, outputPath);
        
        evaluate(model, outputPath);
        
    }  
    
    private static double[][] normalizeMatrix(double[][] similarityMatrix) {
        for (int i = 0; i < similarityMatrix.length; i++) {
            double max = 0;
            double min = Double.MAX_VALUE;
            
            for (int j = 0; j < similarityMatrix.length; j++) {
                if (Double.isNaN(similarityMatrix[i][j])) {
                    similarityMatrix[i][j] = 0;
                }
                if (similarityMatrix[i][j] > max) {
                    max = similarityMatrix[i][j];
                }
                if (similarityMatrix[i][j] < min) {
                    min = similarityMatrix[i][j];
                }
            }
            
            for (int j = 0; j < similarityMatrix.length; j++) {
                if ((max - min) != 0) {
                    similarityMatrix[i][j] = (similarityMatrix[i][j] - min) / (max - min);
                } else {
                    if (max == 0 && min == 0) {
                        similarityMatrix[i][j] = 0;
                    }
                }
            }
        }
       
        return similarityMatrix;
    }
    
    private static void writeResults(double[][] similarityMatrix, String filePath) {
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

    private static double[][] calculateHybridMatrix(double[][] m1, double[][] m2, double w1, double w2) {
        double[][] matrix = new double[151][151];
        
        for (int i = 0; i < 150; i++) {
            for (int j = 0; j < 150; j++) {
                matrix[i][j] = m1[i][j] * w1 + m2[i][j] * w2;
            }
        }
        
        return matrix;
    }

    private static void evaluate(DataModel model, String path) throws TasteException {
        RecommenderBuilder builder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel model) throws TasteException {
                ItemSimilarity similarity = new FileItemSimilarity(new File(path));

                return new GenericItemBasedRecommender(model, similarity);
            }
        };

        RecommenderEvaluator recEvaluator = new RMSRecommenderEvaluator();
        double score = recEvaluator.evaluate(builder, null, model, 0.3, 0.7);
        System.out.println(score);
    }
}
