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

    private static void daat(Map<String, Integer> query_term_frequency, int k, DB db_lexicon, int mode) {
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
        String term_shortest_pl = null;
        ListIterator<Posting> shortest_pl = null;

        for (InvertedList invertedList : L) {
            iteratorList.put(invertedList.getTerm(), invertedList.getPostingArrayList().listIterator());
            if (invertedList.getPostingArrayList().size() < min) {
                min = invertedList.getPostingArrayList().size();
                shortest_pl = invertedList.getPostingArrayList().listIterator();
                term_shortest_pl = invertedList.getTerm();
            }
        }

        //delete the iterator relative to the shortest posting list, because it is contained inside shortest_pl
        iteratorList.remove(term_shortest_pl);

        int current_doc_id = 0;
        assert shortest_pl != null;
        Posting posting = null;
        if(shortest_pl.hasNext()) {
            posting = shortest_pl.next();
            current_doc_id = posting.getDoc_id();
        }
        int size = iteratorList.size();

        while (true){
            double score = 0;

            HashMap<String, Posting> current_postings = new HashMap<>();
            current_postings.put(term_shortest_pl, posting);

            int max = current_doc_id;

            for (Map.Entry<String, ListIterator<Posting>> entry : iteratorList.entrySet())
                max = nextGeqPostingLists(entry, current_postings, current_doc_id, max);

            if (current_doc_id < max) {
                //current_doc_id exceeded on reference posting list, so we need to reach a valid posting
                posting = nextGeqReferencePostingList(shortest_pl, current_postings, current_doc_id, max, term_shortest_pl);
                if (posting == null)
                    break;
                else {
                    current_doc_id = posting.getDoc_id();
                    max = current_doc_id;
                }
            }

            int needToScore = 0;
            //check if all postings have the same doc_id
            for (Map.Entry<String, Posting> entry : current_postings.entrySet()) {
                if (entry.getValue().getDoc_id() == max) {
                    needToScore++;
                }
            }

            //this current_doc_id is present in every posting, so it is possible to compute the score
            if (needToScore == size + 1){
                for(Map.Entry<String, Posting> entry : current_postings.entrySet()) {
                    int doc_freq = Objects.requireNonNull((TermStats) lexicon.get(entry.getKey())).getDoc_frequency();
                    score += tfIdfScore(entry.getValue().getTerm_frequency(), doc_freq);
                }
                R.add(new Results(current_doc_id, score));
            }

            //go to next posting on reference list
            posting = nextGeqReferencePostingList(shortest_pl, current_postings, current_doc_id, max, term_shortest_pl);
            if (posting == null)
                break;
            else
                current_doc_id = posting.getDoc_id();
        }
    }

    private static Posting nextGeqReferencePostingList(ListIterator<Posting> min_list, HashMap<String, Posting> current_postings, int doc_id, int max, String term_shortest_pl) {
        while (min_list.hasNext()) {
            Posting p = min_list.next();
            if (p.getDoc_id() >= max) {
                doc_id = p.getDoc_id();
                current_postings.put(term_shortest_pl, p);
                return p;
            }
        }
        return null;
    }

    private static int nextGeqPostingLists(Map.Entry<String, ListIterator<Posting>> entry, HashMap<String, Posting> current_postings, int doc_id, int max) {
        while (entry.getValue().hasNext()) {
            Posting p = entry.getValue().next();
            if (p.getDoc_id() >= doc_id) {
                if(p.getDoc_id() >= max)
                    max = p.getDoc_id();
                //System.out.println("Skippo a questa: " + p.getDoc_id());
                current_postings.put(entry.getKey(), p);
                return max;
            }
        }
        return max;
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
