package it.unipi.querymanager;


import it.unipi.bean.InvertedList;
import it.unipi.bean.Posting;
import it.unipi.bean.Results;
import it.unipi.bean.TermStats;
import it.unipi.builddatastructures.Tokenizer;
import it.unipi.utils.CollectionStatistics;
import it.unipi.utils.Compression;
import it.unipi.utils.FileChannelInvIndex;
import it.unipi.utils.ResultsComparator;
import org.mapdb.DB;

import java.io.IOException;
import java.util.*;

public class QueryProcess {
    private static long startTime;

    public static void parseQuery(String query, int mode, int k, DB db_lexicon, DB db_document_index) throws IOException {
        startTime = System.nanoTime();
        Tokenizer tokenizer = new Tokenizer(query);
        Map<String, Integer> query_term_frequency = tokenizer.tokenize();
        int query_length = 0;
        for (String token : query_term_frequency.keySet()) {
            query_length += query_term_frequency.get(token);
        }
        if (mode == 0)
            daatScoringDisjunctive(query_term_frequency, query_length, k, db_lexicon);
        else if (mode == 1)
            daatScoringConjunctive(query_term_frequency, query_length, k, db_lexicon);
    }

    private static void daatScoringDisjunctive(Map<String, Integer> query_term_frequency, int query_length, int k, DB db_lexicon) throws IOException {

        ArrayList<InvertedList> L = new ArrayList<>(query_length);

        Comparator<Results> comparator = new ResultsComparator();
        PriorityQueue<Results> R = new PriorityQueue<>(k, comparator);

        retrievePostingLists(query_term_frequency, db_lexicon, L);

        int current_doc_id = min_doc_id(L);
        int cicli = 0;

        HashMap<String, ListIterator<Posting>> iteratorList = new HashMap<>();
        for (InvertedList invertedList : L){
            iteratorList.put(invertedList.getTerm(), invertedList.getPostingArrayList().listIterator());
        }

        while (current_doc_id != CollectionStatistics.num_docs) {
            double score = 0;

            for (InvertedList invertedList : L) {
                cicli ++;
                if (iteratorList.get(invertedList.getTerm()).hasNext()) {

                    Posting posting = iteratorList.get(invertedList.getTerm()).next();
                    int doc_id = posting.getDoc_id();
                    int term_freq = posting.getTerm_frequency();

                    if (current_doc_id == doc_id) {
                        int doc_freq = Objects.requireNonNull((TermStats) db_lexicon.hashMap("lexicon").open()
                                .get(invertedList.getTerm())).getDoc_frequency();
                        score += tfIdfScore(term_freq, doc_freq);
                        invertedList.setPos(invertedList.getPos() + 1);
                    }
                    else {
                        iteratorList.get(invertedList.getTerm()).previous();
                    }
                }
            }
            R.add(new Results(current_doc_id, score));
            current_doc_id = min_doc_id(L);
        }

        System.out.println("CICLI " + cicli);
        printRankedResults(k, R);
    }

    private static void daatScoringConjunctive(Map<String, Integer> query_term_frequency, int query_length, int k, DB db_lexicon) throws IOException {
        ArrayList<InvertedList> L = new ArrayList<>(query_length);

        Comparator<Results> comparator = new ResultsComparator();
        PriorityQueue<Results> R = new PriorityQueue<>(k, comparator);

        retrievePostingLists(query_term_frequency, db_lexicon, L);

        HashMap<String, ListIterator<Posting>> iteratorList = new HashMap<>();

        int min = CollectionStatistics.num_docs;
        String term_to_delete = null;
        int index_to_remove = 0;
        ListIterator<Posting> min_list = null;

        for (InvertedList invertedList : L){
            iteratorList.put(invertedList.getTerm(), invertedList.getPostingArrayList().listIterator());
            if(invertedList.getPostingArrayList().size() < min) {
                index_to_remove = L.indexOf(invertedList);
                min = invertedList.getPostingArrayList().size();
                min_list = invertedList.getPostingArrayList().listIterator();
                term_to_delete = invertedList.getTerm();
            }
        }

        iteratorList.remove(term_to_delete);
        L.remove(index_to_remove);

        while (min_list.hasNext()) {
            double score = 0;
            boolean flag = true;
            boolean flag2 = false;

            Posting posting = min_list.next();
            int doc_id = posting.getDoc_id();

            for (InvertedList invertedList : L) {
                Posting current_posting = skipPosting(doc_id, iteratorList.get(invertedList.getTerm()));

                if (current_posting == null){
                    flag = false;
                    break;
                }

                if(current_posting.getDoc_id() > doc_id) {
                    if(!min_list.hasNext()){
                        flag = false;
                        break;
                    }
                    Posting p = min_list.next();
                    doc_id = p.getDoc_id();
                }

                if (doc_id == current_posting.getDoc_id()) {
                    flag2 = true;
                    int doc_freq = Objects.requireNonNull((TermStats) db_lexicon.hashMap("lexicon").open()
                            .get(invertedList.getTerm())).getDoc_frequency();
                    score += tfIdfScore(current_posting.getTerm_frequency(), doc_freq);
                }
            }
            if (!flag)
                break;

            if (flag2)
                R.add(new Results(doc_id, score));
        }

        printRankedResults(k, R);
    }

    private static Posting skipPosting(int doc_id, ListIterator<Posting> postingListIterator) {
        while (postingListIterator.hasNext()){
            Posting posting = postingListIterator.next();
            if(posting.getDoc_id() >= doc_id){
                return posting;
            }
        }
        return null;
    }

    private static void printRankedResults(int k, PriorityQueue<Results> r) {
        try {
            for (int i = 0; i < k; i++) {
                Results results = r.peek();
                assert results != null;
                System.out.println((i + 1) + ". " + "DOC ID: " + results.getDoc_id() + " SCORE: " + results.getScore());
                r.poll();
                if (r.size() == 0)
                    break;
            }
        }

        catch (NullPointerException e){
            System.out.println("No results found for this query");
        }

        long elapsedTime = System.nanoTime() - startTime;
        System.out.println("Total elapsed time: " + elapsedTime / 1000000 + " ms");
    }

    private static void retrievePostingLists(Map<String, Integer> query_term_frequency, DB db_lexicon, ArrayList<InvertedList> L) throws IOException {
        for (String term : query_term_frequency.keySet()) {
            List<Posting> query_posting_list = new ArrayList<>();
            try {

                TermStats termStats = Objects.requireNonNull((TermStats) db_lexicon.hashMap("lexicon").open().get(term));

                int size_doc_id_list = extractSize(termStats.getOffset_doc_id_start(), termStats.getOffset_doc_id_end());
                int size_term_freq_list = extractSize(termStats.getOffset_term_freq_start(), termStats.getOffset_term_freq_end());

                byte[] doc_id_buffer = new byte[size_doc_id_list];
                byte[] term_freq_buffer = new byte[size_term_freq_list];

                FileChannelInvIndex.readMappedFile(doc_id_buffer, term_freq_buffer, termStats.getOffset_doc_id_start(), termStats.getOffset_term_freq_start());
                Compression compression = new Compression();

                for (int i = 0; i < termStats.getDoc_frequency(); i++) {
                    int term_freq = compression.decodingUnaryList(BitSet.valueOf(term_freq_buffer));
                    //int doc_id = compression.gammaDecodingList(BitSet.valueOf(doc_id_buffer));
                    int doc_id = compression.decodingVariableByte(doc_id_buffer);
                    query_posting_list.add(new Posting(doc_id, term_freq));
                }
                L.add(new InvertedList(term, query_posting_list, 0));
            } catch (NullPointerException e) {
                System.out.println("Term " + term + " not in collection");
            }
        }
    }

    private static double tfIdfScore(int term_freq, int doc_freq) {
        double tf, idf;
        tf = Math.log(term_freq);
        idf = Math.log((double) CollectionStatistics.num_docs / (double) doc_freq);
        return (tf + 1) * (idf);
    }

    private static double getScore(Map<String, Integer> query_term_frequency, int query_length, int doc_len, InvertedList invertedList, int term_freq) {

        int query_term_freq = query_term_frequency.get(invertedList.getTerm());

        double w_td = (double) term_freq / (double) doc_len;
        double w_tq = (double) query_term_freq / (double) query_length;
        return w_td * w_tq;
    }

    private static int min_doc_id(ArrayList<InvertedList> L) {
        int min_doc_id = CollectionStatistics.num_docs;

        for (InvertedList invertedList : L) {
            if (invertedList.getPos() < invertedList.getPostingArrayList().size()) {
                min_doc_id = Math.min(invertedList.getPostingArrayList().get(invertedList.getPos()).getDoc_id(), min_doc_id);
            }
        }
        return min_doc_id;
    }

    private static int extractSize(long start, long end) {
        return (int) (end - start);
    }
}
