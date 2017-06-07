/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goran.rovkp_lab3;

import com.google.common.collect.HashBiMap;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

/**
 *
 * @author goran
 */
public class CollaborativeItemSimilarity implements ItemSimilarity {
    private final double[][] matrix;
    private final double[] norms;
    private final Map<Integer, Long> seqIdMap;
    private final Map<Long, Integer> idSeqMap;
    private static final double SIMILARITY_TRESHOLD = 0.9;

    public CollaborativeItemSimilarity(DataModel model) throws TasteException {
        int n = model.getNumItems();
        matrix = new double[n][n];
        norms = new double[n];
        seqIdMap = new HashMap<>();
        idSeqMap = new HashMap<>();

        calculateCollaborativeModelMatrix(model);
    }

    private void calculateCollaborativeModelMatrix(DataModel model) throws TasteException {

        int counter = 0;
        LongPrimitiveIterator iterator = model.getUserIDs();
        while (iterator.hasNext()) {
            long userId = iterator.nextLong();

            for (long ratedItemId1 : model.getItemIDsFromUser(userId)) {

                //get correct item1 seq num
                Integer seqId1 = idSeqMap.get(ratedItemId1);
                if (seqId1 == null) {
                    seqId1 = counter++;
                    seqIdMap.put(seqId1, ratedItemId1);
                    idSeqMap.put(ratedItemId1, seqId1);
                }

                norms[seqId1] += Math.pow(model.getPreferenceValue(userId, ratedItemId1), 2);

                for (long ratedItemId2 : model.getItemIDsFromUser(userId)) {

                    //get correct item2 seq num
                    Integer seqId2 = idSeqMap.get(ratedItemId2);
                    if (seqId2 == null) {
                        seqId2 = counter++;
                        seqIdMap.put(seqId2, ratedItemId2);
                        idSeqMap.put(ratedItemId2, seqId2);
                    }

                    matrix[seqId1][seqId2] += model.getPreferenceValue(userId, ratedItemId1)
                            * model.getPreferenceValue(userId, ratedItemId2);
                }
            }
        }

	//get cosine similarity from similarity sums
        for (int seqId1 = 0; seqId1 < matrix.length; seqId1++) {
            for (int seqId2 = 0; seqId2 < matrix.length; seqId2++) {
                if (matrix[seqId1][seqId2] != 0) {                                       
                    matrix[seqId1][seqId2] /= Math.sqrt(norms[seqId1]) * Math.sqrt(norms[seqId2]);
                }
            }
        }
    }
    
    
    
    @Override
    public double itemSimilarity(long l, long l1) throws TasteException {
        if (idSeqMap.get(l) == null || idSeqMap.get(l1) == null) {
            return 0;
        }
        
        int seqId1 = idSeqMap.get(l);
        int seqId2 = idSeqMap.get(l1);
      
        return matrix[seqId1][seqId2];
    }

    @Override
    public double[] itemSimilarities(long l, long[] longs) throws TasteException {
        double[] similarities = new double[longs.length];
        
        for (int i = 0; i < longs.length; i++) {
            similarities[i] = itemSimilarity(l, longs[i]);
        }
        
        return similarities;
    }

    @Override
    public long[] allSimilarItemIDs(long l) throws TasteException {
        int seqId = idSeqMap.get(l);
        ArrayList<Long> ids = new ArrayList<>();
        
        for (int i = 0; i < 150; i++) {
            if (matrix[seqId][i] >= SIMILARITY_TRESHOLD) {
                long id = seqIdMap.get(i);
                ids.add(id);
            }
        }
        
        long[] results = new long[ids.size()];
        for (int i = 0; i < ids.size(); i++) {
            results[i] = ids.get(i);
        }
        
        return results;
    }

    @Override
    public void refresh(Collection<Refreshable> clctn) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

     
}
