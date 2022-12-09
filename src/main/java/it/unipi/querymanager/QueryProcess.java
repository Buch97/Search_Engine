package it.unipi.querymanager;


import it.unipi.bean.RafInvertedIndex;
import it.unipi.bean.Posting;
import it.unipi.bean.Results;
import it.unipi.bean.TermStats;
import it.unipi.builddatastructures.Tokenizer;
import it.unipi.utils.AuxObject;
import it.unipi.utils.Compression;
import org.mapdb.DB;
import org.mapdb.HTreeMap;

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
        Map<Integer, Integer> doc_scores = new HashMap<>();

        ArrayList<List<Posting>> L = new ArrayList<List<Posting>>(query_length);
        PriorityQueue<Results> R = new PriorityQueue<>(k);

        int[] pos = new int[query_term_frequency.size()];
        Arrays.fill(pos,0);

        for(String term : query_term_frequency.keySet()){
            List<Posting> query_posting_list = new ArrayList<>();
            try {
                HTreeMap<String, TermStats> myMapLexicon =(HTreeMap<String, TermStats>) Objects.requireNonNull((db.hashMap("lexicon").open());
                offset_doc_id = Objects.requireNonNull((TermStats) db.hashMap("lexicon").open().get(term)).getActual_offset_doc_id();
                offset_term_freq = Objects.requireNonNull((TermStats) db.hashMap("lexicon").open().get(term)).getActual_offset_term_freq();
                size = Objects.requireNonNull((TermStats) db.hashMap("lexicon").open().get(term)).getSize();

                Objects.requireNonNull((TermStats) db.hashMap("lexicon").open().

                byte[] doc_id_buffer = new byte[size*4];
                byte[] term_freq_buffer = new byte[size*4];
                RafInvertedIndex.getIndex_doc_id().get(doc_id_buffer, (int) offset_doc_id, size*4);
                RafInvertedIndex.getIndex_term_freq().get(term_freq_buffer, (int) offset_term_freq, size*4);

                AuxObject auxObj=new AuxObject(0);
                for (int i = 0; i < size; i++) {

                    int term_freq=Compression.decodingUnaryList(BitSet.valueOf(term_freq_buffer),auxObj.getPosU());
                    int doc_id = Compression.gammaDecodingList(BitSet.valueOf(doc_id_buffer),auxObj.getPosG());

                    query_posting_list.add(new Posting(doc_id, term_freq));
                }

                L.add(query_posting_list);
            }
            catch (NullPointerException e){
                System.out.println("Term not in collection");
                return;
            }

        }

        int currentDocId = minDocId(pos, num_docs);
        int lastDocId = maxDocId(pos, num_docs);

        while(currentDocId <= lastDocId){
            doc_scores.put(currentDocId, 0);
            // for all inverted list li in L
            for (List<Posting> posting_list : L){
                Iterator<Posting> iterator = posting_list.iterator();
                while (iterator.hasNext()){
                    Posting posting = iterator.next();
                    int doc_id = posting.getDoc_id();
                    int term_freq = posting.getTerm_frequency();

                    if(currentDocId == doc_id){
                        // aggiorna score
                    }
                }
                currentDocId++;
            }
        }

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
