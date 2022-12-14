package it.unipi.querymanager;


import it.unipi.bean.Posting;
import it.unipi.bean.Results;
import it.unipi.bean.TermStats;
import it.unipi.builddatastructures.MergeBlocks;
import it.unipi.builddatastructures.Tokenizer;
import it.unipi.utils.Compression;
import it.unipi.utils.FileChannelInvIndex;
import org.mapdb.DB;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

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

        Map<Integer, Integer> doc_scores = new HashMap<>();

        ArrayList<List<Posting>> L = new ArrayList<List<Posting>>(query_length);
        PriorityQueue<Results> R = new PriorityQueue<>(k);

        FileChannelInvIndex.openFileChannels(mode);

        /* for all terms wi in Q do
                 li !InvertedList(wi, I)
                 L.add( li )
           end for     */

        for (String term : query_term_frequency.keySet()) {
            List<Posting> query_posting_list = new ArrayList<>();
            try {

                TermStats termStats = Objects.requireNonNull((TermStats) db.hashMap("lexicon").open().get(term));

                int size_doc_id_list = extractSize(termStats.getOffset_doc_id_start(), termStats.getOffset_doc_id_end());
                int size_term_freq_list = extractSize(termStats.getOffset_term_freq_start(), termStats.getOffset_term_freq_end());

                ByteBuffer doc_id_buffer = ByteBuffer.allocate(size_doc_id_list);
                ByteBuffer term_freq_buffer = ByteBuffer.allocate(size_term_freq_list);

                FileChannelInvIndex.read(doc_id_buffer, term_freq_buffer, termStats.getOffset_doc_id_start(), termStats.getOffset_term_freq_start());
                Compression compression = new Compression();

                // Solo per debug
                printBitsetDecompressed(size_doc_id_list, size_term_freq_list, doc_id_buffer, term_freq_buffer);

                int n_posting = 0;
                while (n_posting < termStats.getDoc_frequency()) {
                    int term_freq = compression.decodingUnaryList(BitSet.valueOf(term_freq_buffer), size_term_freq_list * 8);
                    int doc_id = compression.gammaDecodingList(BitSet.valueOf(doc_id_buffer), size_doc_id_list * 8);
                    query_posting_list.add(new Posting(doc_id, term_freq));
                    n_posting++;
                }
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

    private static void printBitsetDecompressed(int size_doc_id, int size_term_freq, ByteBuffer doc_id_buffer, ByteBuffer term_freq_buffer) {
        System.out.println("PRINT BIT SET DOC");
        doc_id_buffer.flip();
        MergeBlocks.printBitSet(BitSet.valueOf(doc_id_buffer), size_doc_id * 8);

        System.out.println("PRINT BIT SET TERM");
        term_freq_buffer.flip();
        MergeBlocks.printBitSet(BitSet.valueOf(term_freq_buffer), size_term_freq * 8);
    }

    private static int extractSize(long start, long end) {
        return (int) (end - start);
    }

    private static int maxDocId(int[] pos, int num_docs) {
        return 0;
    }

    private static int minDocId(int[] pos, int num_docs) {
        return 0;
    }
}
