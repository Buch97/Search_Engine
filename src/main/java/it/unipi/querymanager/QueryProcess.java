package it.unipi.querymanager;


import it.unipi.bean.RafInvertedIndex;
import it.unipi.bean.Posting;
import it.unipi.bean.Results;
import it.unipi.bean.TermStats;
import it.unipi.builddatastructures.Tokenizer;
import org.mapdb.DB;

import java.io.*;
import java.util.*;

import static it.unipi.Main.num_docs;

public class QueryProcess {

    public static void parseQuery(String query, int k, DB db) throws IOException {
        Tokenizer tokenizer = new Tokenizer(query);
        Map<String, Integer> query_term_frequency = tokenizer.tokenize();
        Integer query_length = 0;
        for (String token : query_term_frequency.keySet()) {
            query_length += query_term_frequency.get(token);
            System.out.println(token + " " + query_term_frequency.get(token));
        }
        System.out.println("Query length = " + query_length);
        daatScoring(query_term_frequency, query_length, k, db);
    }

    private static void daatScoring(Map<String, Integer> query_term_frequency, int query_length, int k, DB db) throws IOException {

        long offset_doc_id;
        long offset_term_freq;
        int size;
        Map<Integer, Integer> doc_scores;

        ArrayList<List> L = new ArrayList<>(query_length);
        PriorityQueue<Results> R = new PriorityQueue<>(k);

        int[] pos = new int[query_term_frequency.size()];
        Arrays.fill(pos,0);

        RandomAccessFile file_reader = new RandomAccessFile(new File("./src/main/resources/output/inverted_index.tsv"), "r");

        // for all term w in Q
        //    li <-- invertedList(wi, I)
        //    L.add(li)
        for(String term : query_term_frequency.keySet()){
            List<Posting> query_posting_list = new ArrayList<>();
            try {
                offset_doc_id = Objects.requireNonNull((TermStats) db.hashMap("lexicon").open().get(term)).getActual_offset_doc_id();
                offset_term_freq = Objects.requireNonNull((TermStats) db.hashMap("lexicon").open().get(term)).getActual_offset_term_freq();
                size = Objects.requireNonNull((TermStats) db.hashMap("lexicon").open().get(term)).getSize();

                // fare
                int doc_id = RafInvertedIndex.getIndex_doc_id().getInt((int) offset_doc_id);
                int term_freq = RafInvertedIndex.getIndex_doc_id().getInt((int) offset_term_freq);

                //L.add(retrieved_posting);
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
