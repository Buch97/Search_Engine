package it.unipi.querymanager;


import it.unipi.bean.Posting;
import it.unipi.bean.RafInvertedIndex;
import it.unipi.bean.Results;
import it.unipi.bean.TermStats;
import it.unipi.builddatastructures.MergeBlocks;
import it.unipi.builddatastructures.Tokenizer;
import it.unipi.utils.Compression;
import org.mapdb.DB;
import org.mapdb.DataInput2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static it.unipi.Main.num_docs;
import static it.unipi.bean.RafInvertedIndex.fileChannel_term_freq;
import static org.mapdb.DataInput2.*;

public class QueryProcess {

    private static final String mode = "READ";

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

        long offset_doc_id_start;
        long offset_doc_id_end;
        long offset_term_freq_start;
        long offset_term_freq_end;

        Map<Integer, Integer> doc_scores = new HashMap<>();

        ArrayList<List<Posting>> L = new ArrayList<List<Posting>>(query_length);
        PriorityQueue<Results> R = new PriorityQueue<>(k);

        new RafInvertedIndex("src/main/resources/output/inverted_index_doc_id_bin.dat",
                "src/main/resources/output/inverted_index_term_frequency_bin.dat", mode);

        for (String term : query_term_frequency.keySet()) {
            List<Posting> query_posting_list = new ArrayList<>();
            try {

                TermStats termStats = Objects.requireNonNull((TermStats) db.hashMap("lexicon").open().get(term));

                offset_doc_id_start = termStats.getOffset_doc_id_start();
                offset_doc_id_end = termStats.getOffset_doc_id_end();

                int size_doc_id = (int) (offset_doc_id_end - offset_doc_id_start + 1);

                offset_term_freq_start = termStats.getOffset_term_freq_start();
                offset_term_freq_end = termStats.getOffset_term_freq_end();

                int size_term_freq = (int) (offset_term_freq_end - offset_term_freq_start + 1);

                ByteBuffer doc_id_buffer = ByteBuffer.allocate(size_doc_id);
                ByteBuffer term_freq_buffer = ByteBuffer.allocate(size_term_freq);

                int read_doc_id = RafInvertedIndex.fileChannel_doc_id.read(doc_id_buffer, (int)offset_doc_id_start);
                System.out.println("READ : " + read_doc_id);
                int read_term_freq = RafInvertedIndex.fileChannel_term_freq.read(term_freq_buffer, (int)offset_term_freq_start);
                System.out.println("READ: " + read_term_freq);

                Compression compression = new Compression();
                // build posting list query term

                System.out.println("PRINT BIT SET DOC");
                doc_id_buffer.flip();
                MergeBlocks.printBitSet(BitSet.valueOf(doc_id_buffer), size_doc_id*8);

                System.out.println("PRINT BIT SET TERM");
                term_freq_buffer.flip();
                MergeBlocks.printBitSet(BitSet.valueOf(term_freq_buffer), size_term_freq*8);

                int term_freq = compression.decodingUnaryList(BitSet.valueOf(term_freq_buffer), size_term_freq*8);
                System.out.println("TERM: " + term_freq);
                int doc_id = compression.gammaDecodingList(BitSet.valueOf(doc_id_buffer), size_doc_id*8);
                System.out.println("DOCID: " + doc_id);

                query_posting_list.add(new Posting(doc_id, term_freq));

                L.add(query_posting_list);
            } catch (NullPointerException e) {
                System.out.println("Term not in collection");
                return;
            }
        }

        /*int currentDocId = minDocId(pos, num_docs);
        int lastDocId = maxDocId(pos, num_docs);

        while (currentDocId <= lastDocId) {
            doc_scores.put(currentDocId, 0);
            // for all inverted list li in L
            for (List<Posting> posting_list : L) {
                Iterator<Posting> iterator = posting_list.iterator();
                while (iterator.hasNext()) {
                    Posting posting = iterator.next();
                    int doc_id = posting.getDoc_id();
                    int term_freq = posting.getTerm_frequency();

                    if (currentDocId == doc_id) {
                        // aggiorna score
                    }
                }
                currentDocId++;
            }
        }*/

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
