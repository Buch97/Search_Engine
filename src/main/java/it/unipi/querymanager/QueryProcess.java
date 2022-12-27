package it.unipi.querymanager;


import it.unipi.bean.InvertedList;
import it.unipi.bean.Posting;
import it.unipi.bean.Results;
import it.unipi.bean.TermStats;
import it.unipi.builddatastructures.Tokenizer;
import it.unipi.utils.*;
import org.mapdb.DB;
import org.mapdb.HTreeMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static it.unipi.Main.db_lexicon;
import static it.unipi.Main.k;
import static it.unipi.querymanager.Score.tfIdfScore;

public class QueryProcess {
    private static long startTime;

    public static void submitQuery(BufferedReader reader, String query) throws IOException {
        Tokenizer tokenizer = new Tokenizer(query);
        Map<String, Integer> query_term_frequency = tokenizer.tokenize();

        if (query_term_frequency.size() == 1) {
            startTime = System.nanoTime();
            daat(query_term_frequency, k, db_lexicon, 0);
        } else {
            int mode;
            System.out.println("Select which method to use to parse the query: Disjunctive(0) Conjunctive(1)");
            String type = reader.readLine();
            try {
                if ((Integer.parseInt(type) != 0) && (Integer.parseInt(type) != 1)) {
                    System.out.println("Not valid input, mode is set to default (0)");
                    mode = 0;
                } else
                    mode = Integer.parseInt(type);
                System.out.println("Your request: " + query + "\n");
            } catch (NumberFormatException e) {
                System.out.println("Not valid input, mode is set to default (0)");
                mode = 0;
            }
            startTime = System.nanoTime();
            daat(query_term_frequency, k, db_lexicon, mode);
        }
    }

    private static void daat(Map<String, Integer> query_term_frequency, int k, DB db_lexicon, int mode){
        Comparator<Results> comparator = new ResultsComparator();
        PriorityQueue<Results> R = new PriorityQueue<>(k, comparator);

        final ExecutorService executor = Executors.newFixedThreadPool(query_term_frequency.size());
        final List<Future<?>> futures = new ArrayList<>();
        HTreeMap<?, ?> lexicon = db_lexicon.hashMap("lexicon").open();

        ArrayList<InvertedList> L = getL(query_term_frequency, executor, futures, lexicon);
        if (L.isEmpty()) return;

        if (mode == 0)
            daatScoringDisjunctive(L, lexicon, R);
        else daatScoringConjunctive(L, lexicon, R);

        printRankedResults(k, R);
    }

    private static void daatScoringDisjunctive(ArrayList<InvertedList> L, HTreeMap<?, ?> lexicon, PriorityQueue<Results> R) {
        int current_doc_id = min_doc_id(L);

        HashMap<String, ListIterator<Posting>> iteratorList = new HashMap<>();
        for (InvertedList invertedList : L) {
            iteratorList.put(invertedList.getTerm(), invertedList.getPostingArrayList().listIterator());
        }

        while (current_doc_id != CollectionStatistics.num_docs) {
            double score = 0;

            for (InvertedList invertedList : L) {
                if (iteratorList.get(invertedList.getTerm()).hasNext()) {

                    Posting posting = iteratorList.get(invertedList.getTerm()).next();
                    int doc_id = posting.getDoc_id();
                    int term_freq = posting.getTerm_frequency();

                    if (current_doc_id == doc_id) {
                        int doc_freq = Objects.requireNonNull((TermStats) lexicon.get(invertedList.getTerm())).getDoc_frequency();
                        score += tfIdfScore(term_freq, doc_freq);
                        invertedList.setPos(invertedList.getPos() + 1);
                    } else {
                        iteratorList.get(invertedList.getTerm()).previous();
                    }
                }
            }
            R.add(new Results(current_doc_id, score));
            current_doc_id = min_doc_id(L);
        }
    }

    private static void daatScoringConjunctive(ArrayList<InvertedList> L, HTreeMap<?, ?> lexicon, PriorityQueue<Results> R) {

        HashMap<String, ListIterator<Posting>> iteratorList = new HashMap<>();

        int min = CollectionStatistics.num_docs;
        String term_to_delete = null;
        int index_to_remove = 0;
        ListIterator<Posting> min_list = null;

        for (InvertedList invertedList : L) {
            iteratorList.put(invertedList.getTerm(), invertedList.getPostingArrayList().listIterator());
            if (invertedList.getPostingArrayList().size() < min) {
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

                if (current_posting == null) {
                    flag = false;
                    break;
                }

                if (current_posting.getDoc_id() > doc_id) {
                    if (!min_list.hasNext()) {
                        flag = false;
                        break;
                    }
                    Posting p = min_list.next();
                    doc_id = p.getDoc_id();
                }

                if (doc_id == current_posting.getDoc_id()) {
                    flag2 = true;
                    int doc_freq = Objects.requireNonNull((TermStats) lexicon.get(invertedList.getTerm())).getDoc_frequency();
                    score += tfIdfScore(current_posting.getTerm_frequency(), doc_freq);
                }
            }
            if (!flag)
                break;

            if (flag2)
                R.add(new Results(doc_id, score));
        }
    }

    private static ArrayList<InvertedList> getL(Map<String, Integer> query_term_frequency, ExecutorService executor, List<Future<?>> futures, HTreeMap<?, ?> lexicon) {
        ArrayList<InvertedList> L = new ArrayList<>();
        for (String term : query_term_frequency.keySet()) {
            Future<?> future = executor.submit(() -> {
                try {
                    return retrievePostingLists(term, lexicon);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            futures.add(future);
        }
        try {
            for (Future<?> future : futures) {
                if (future.get() != null)
                    L.add((InvertedList) future.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return L;
    }

    private static Posting skipPosting(int doc_id, ListIterator<Posting> postingListIterator) {
        while (postingListIterator.hasNext()) {
            Posting posting = postingListIterator.next();
            if (posting.getDoc_id() >= doc_id) {
                return posting;
            }
        }
        return null;
    }

    private static void printRankedResults(int k, PriorityQueue<Results> r) {
        System.out.println("Results: ");
        try {
            for (int i = 0; i < k; i++) {
                Results results = r.poll();
                assert results != null;
                System.out.println((i + 1) + ". " + "DOC ID: " + results.getDoc_id() + " SCORE: " + results.getScore());
                if (r.size() == 0)
                    break;
            }
        } catch (NullPointerException e) {
            System.out.println("No results found for this query");
        }

        long elapsedTime = System.nanoTime() - startTime;
        System.out.println("Total elapsed time: " + elapsedTime / 1000000 + " ms");
    }

    private static InvertedList retrievePostingLists(String term, HTreeMap<?, ?> lexicon) throws IOException {
        List<Posting> query_posting_list = new ArrayList<>();
        try {
            TermStats termStats = Objects.requireNonNull((TermStats) lexicon.get(term));

            int size_doc_id_list = extractSize(termStats.getOffset_doc_id_start(), termStats.getOffset_doc_id_end());
            int size_term_freq_list = extractSize(termStats.getOffset_term_freq_start(), termStats.getOffset_term_freq_end());

            byte[] doc_id_buffer = new byte[size_doc_id_list];
            byte[] term_freq_buffer = new byte[size_term_freq_list];

            FileChannelInvIndex.readMappedFile(doc_id_buffer, term_freq_buffer, termStats.getOffset_doc_id_start(), termStats.getOffset_term_freq_start());
            Compression compression = new Compression();

            InvertedList newObj = GuavaCacheService.invertedListLoadingCache.getIfPresent(term);
            if (newObj != null) {
                System.out.println("dentro if " + newObj.getPos());
                newObj.setPos(0);
                return newObj;
            }

            for (int i = 0; i < termStats.getDoc_frequency(); i++) {
                int term_freq = compression.decodingUnaryList(BitSet.valueOf(term_freq_buffer));
                int doc_id = compression.decodingVariableByte(doc_id_buffer);
                query_posting_list.add(new Posting(doc_id, term_freq));
            }
            System.out.println("Decompressed " + term);

            newObj = new InvertedList(term, query_posting_list, 0);
            GuavaCacheService.invertedListLoadingCache.put(term, newObj);

            return newObj;
        } catch (NullPointerException e) {
            System.out.println("Term " + term + " not in collection");
            return null;
        }
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
