package it.unipi.querymanager;


import it.unipi.bean.*;
import it.unipi.builddatastructures.Tokenizer;
import it.unipi.utils.*;
import org.mapdb.DB;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class QueryProcess {

    private static final String mode = "READ";

    public static void parseQuery(String query, int k, DB db_lexicon, DB db_document_index) throws IOException {
        Tokenizer tokenizer = new Tokenizer(query);
        Map<String, Integer> query_term_frequency = tokenizer.tokenize();
        int query_length = 0;
        for (String token : query_term_frequency.keySet()) {
            query_length += query_term_frequency.get(token);
            System.out.println(token + " " + query_term_frequency.get(token));
        }
        System.out.println("Query length = " + query_length);
        daatScoring(query_term_frequency, query_length, k, db_lexicon, db_document_index);
    }

    private static void daatScoring(Map<String, Integer> query_term_frequency, int query_length, int k, DB db_lexicon, DB db_document_index) throws IOException {

        ArrayList<InvertedList> L = new ArrayList<>(query_length);

        Comparator<Results> comparator = new ResultsComparator();
        PriorityQueue<Results> R = new PriorityQueue<>(k, comparator);

        FileChannelInvIndex.openFileChannels(mode);

        for (String term : query_term_frequency.keySet()) {
            List<Posting> query_posting_list = new ArrayList<>();
            try {

                TermStats termStats = Objects.requireNonNull((TermStats) db_lexicon.hashMap("lexicon").open().get(term));

                int size_doc_id_list = extractSize(termStats.getOffset_doc_id_start(), termStats.getOffset_doc_id_end());
                int size_term_freq_list = extractSize(termStats.getOffset_term_freq_start(), termStats.getOffset_term_freq_end());

                ByteBuffer doc_id_buffer = ByteBuffer.allocate(size_doc_id_list);
                ByteBuffer term_freq_buffer = ByteBuffer.allocate(size_term_freq_list);

                FileChannelInvIndex.read(doc_id_buffer, term_freq_buffer, termStats.getOffset_doc_id_start(), termStats.getOffset_term_freq_start());
                Compression compression = new Compression();

                doc_id_buffer.flip();
                term_freq_buffer.flip();

                int n_posting = 0;
                while (n_posting < termStats.getDoc_frequency()) {
                    int term_freq = compression.decodingUnaryList(BitSet.valueOf(term_freq_buffer), size_term_freq_list * 8);
                    int doc_id = compression.gammaDecodingList(BitSet.valueOf(doc_id_buffer), size_doc_id_list * 8);
                    query_posting_list.add(new Posting(doc_id, term_freq));
                    n_posting++;
                }
                L.add(new InvertedList(term, query_posting_list, 0));
            } catch (NullPointerException e) {
                System.out.println("Term not in collection");
                return;
            }
        }

        int num_docs = CollectionStatistics.num_docs;

        int current_doc_id = min_doc_id(L);
        while (current_doc_id != num_docs) {
            double score = 0;

            for (InvertedList invertedList : L) {
                Iterator<Posting> iterator = invertedList.getPostingArrayList().iterator();
                while (iterator.hasNext()) {

                    Posting posting = iterator.next();
                    int doc_id = posting.getDoc_id();
                    int term_freq = posting.getTerm_frequency();

                    if (current_doc_id == doc_id) {
                        score += getScore(query_term_frequency, query_length, db_document_index, invertedList, doc_id, term_freq);
                        invertedList.setPos(invertedList.getPos() + 1);
                    }
                }
            }
            R.add(new Results(current_doc_id, score));
            current_doc_id = min_doc_id(L);
        }

        int count = 0;
        while (count < k){
            Results results = R.peek();
            double score = results.getScore();
            int doc_id = results.getDoc_id();
            R.poll();
            if (R.size() == 0)
                break;
            System.out.println("DOC ID: " + doc_id + " SCORE: " + score);
            count++;
        }
    }

    private static double getScore(Map<String, Integer> query_term_frequency, int query_length, DB db_document_index, InvertedList invertedList, int doc_id, int term_freq) {

        int doc_len = Objects.requireNonNull((DocumentIndexStats) db_document_index.hashMap("document_index")
                        .open().get(doc_id)).getDoc_len();
        int query_term_freq = query_term_frequency.get(invertedList.getTerm());

        double w_td = (double) term_freq / (double) doc_len;
        double w_tq = (double) query_term_freq / (double) query_length;
        return w_td * w_tq;
    }

    private static int min_doc_id(ArrayList<InvertedList> L) {
        int min_doc_id = CollectionStatistics.num_docs;

        for (InvertedList invertedList : L){
            if (invertedList.getPos() < invertedList.getPostingArrayList().size()){
                min_doc_id = Math.min(invertedList.getPostingArrayList().get(invertedList.getPos()).getDoc_id(), min_doc_id);
            }
        }
        return min_doc_id;
    }

    private static int extractSize(long start, long end) {
        return (int) (end - start);
    }
}
