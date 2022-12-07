package it.unipi.querymanager;


import it.unipi.bean.RafInvertedIndex;
import it.unipi.bean.Posting;
import it.unipi.bean.Results;
import it.unipi.bean.TermStats;
import it.unipi.builddatastructures.Tokenizer;
import it.unipi.utils.AuxObject;
import it.unipi.utils.Compression;
import org.mapdb.DB;

import java.io.*;
import java.util.*;

import static it.unipi.Main.num_docs;

public class QueryProcess {

    public static void parseQuery(String query, int k, DB db, RafInvertedIndex rafInvertedIndex) throws IOException {
        Tokenizer tokenizer = new Tokenizer(query);
        Map<String, Integer> query_term_frequency = tokenizer.tokenize();
        Integer query_length = 0;
        for (String token : query_term_frequency.keySet()) {
            query_length += query_term_frequency.get(token);
            System.out.println(token + " " + query_term_frequency.get(token));
        }
        System.out.println("Query length = " + query_length);
        daatScoring(query_term_frequency, query_length, k, db, rafInvertedIndex);
    }

    private static void daatScoring(Map<String, Integer> query_term_frequency, int query_length, int k, DB db, RafInvertedIndex rafInvertedIndex) throws IOException {

        long offset_doc_id;
        long offset_term_freq;
        int size;
        Map<Integer, Integer> doc_scores;

        ArrayList<List> L = new ArrayList<>(query_length);
        PriorityQueue<Results> R = new PriorityQueue<>(k);

        int[] pos = new int[query_term_frequency.size()];
        Arrays.fill(pos,0);

        // for all term w in Q
        //    li <-- invertedList(wi, I)
        //    L.add(li)

        for(String term : query_term_frequency.keySet()){
            List<Posting> query_posting_list = new ArrayList<>();
            try {
                offset_doc_id = Objects.requireNonNull((TermStats) db.hashMap("lexicon").open().get(term)).getActual_offset_doc_id();
                offset_term_freq = Objects.requireNonNull((TermStats) db.hashMap("lexicon").open().get(term)).getActual_offset_term_freq();
                size = Objects.requireNonNull((TermStats) db.hashMap("lexicon").open().get(term)).getSize();

                byte[] doc_id_buffer = new byte[size*4];
                byte[] term_freq_buffer = new byte[size*4];
                RafInvertedIndex.getIndex_doc_id().get(doc_id_buffer, (int) offset_doc_id, size*4);
                RafInvertedIndex.getIndex_term_freq().get(term_freq_buffer, (int) offset_doc_id, size*4);

                AuxObject auxObj=new AuxObject(0);
                for (int i = 0; i < size; i++) {
                    
                    int term_freq=Compression.decodingUnaryList(BitSet.valueOf(term_freq_buffer),auxObj.getPosU());
                    int doc_id = Compression.gammaDecodingList(BitSet.valueOf(doc_id_buffer),auxObj.getPosG());

                    query_posting_list.add(new Posting(doc_id, term_freq));
                }

                L.add(query_posting_list);
            }
            catch (NullPointerException e){
                //significa che questo termine non sta nel lexicon e non ha quindi posting list
            }
        }

        int docid = minDocId(pos, num_docs);
        int last_docid = maxDocId(pos, num_docs);

        // Set score of each doc equal to 0
        // For all posting list in L
    }

    private static int maxDocId(int[] pos, int num_docs) {
        return 0;
    }

    private static int minDocId(int[] pos, int num_docs) {
        return 0;
    }
}
