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
        System.out.println("Results: ");
    }

    private static void daatScoringConjunctive(ArrayList<InvertedList> L, HTreeMap<?, ?> lexicon, PriorityQueue<Results> R) {

        //System.out.println("CONJUNCTIVE");
        HashMap<String, ListIterator<Posting>> iteratorList = new HashMap<>();

        int min = CollectionStatistics.num_docs;
        String term_shortest_pl = null;
        int index_to_remove = 0;
        ListIterator<Posting> min_list = null;

        for (InvertedList invertedList : L) {
            iteratorList.put(invertedList.getTerm(), invertedList.getPostingArrayList().listIterator());
            if (invertedList.getPostingArrayList().size() < min) {
                index_to_remove = L.indexOf(invertedList);
                min = invertedList.getPostingArrayList().size();
                min_list = invertedList.getPostingArrayList().listIterator();
                term_shortest_pl = invertedList.getTerm();
            }
        }

        iteratorList.remove(term_shortest_pl);
        L.remove(index_to_remove);
        /*for (InvertedList inv : L)
            System.out.println(inv.getTerm());*/

        int doc_id = 0;
        assert min_list != null;
        Posting starting_posting = null;
        if(min_list.hasNext()) {
            starting_posting = min_list.next();
            doc_id = starting_posting.getDoc_id();
        }
        Posting p = starting_posting;
        int size = iteratorList.size();
        //System.out.println(size);

        while (true){
            double score = 0;
            boolean finished = false;

            HashMap<String, Posting> current_postings = new HashMap<>();
            current_postings.put(term_shortest_pl, p);

            int max = doc_id;
            //skippo tutte le posting, tranne quella cardine fino a nextGEQ e mi salvo in un array le posting nella posizione
            //a cui mi fermo
            for (Map.Entry<String, ListIterator<Posting>> entry : iteratorList.entrySet()) {
                while (entry.getValue().hasNext()) {
                    p = entry.getValue().next();
                    if (p.getDoc_id() >= doc_id) {
                        if(p.getDoc_id() >= max)
                            max = p.getDoc_id();
                        //System.out.println("Skippo a questa: " + p.getDoc_id());
                        current_postings.put(entry.getKey(), p);
                        break;
                    }
                }
            }
            //System.out.println("MAX: " + max);

            if (doc_id < max) {
                //se ho superato il doc_id corrente allora devo andare avanti sulla posting di riferimento
                finished = true;
                while (min_list.hasNext()) {
                    p = min_list.next();
                    if (p.getDoc_id() >= max) {
                        doc_id = p.getDoc_id();
                        max = doc_id;
                        current_postings.put(term_shortest_pl, p);
                        finished = false;
                        break;
                    }
                }
            }
            else if (doc_id > max)
                continue;

            if (finished)
                break;

            //System.out.println("DOC_ID: " + doc_id);
            int needToScore = 0;
            //scorro le posting correnti e guardo se hanno tutte stesso docid allora devo fare lo score, altrimenti mi salvo
            //il massimo docid
            for (Map.Entry<String, Posting> entry : current_postings.entrySet()) {
                /*if (entry.getValue().getDoc_id() > max)
                    max = entry.getValue().getDoc_id();*/
                if (entry.getValue().getDoc_id() == max && doc_id == max) {
                    //System.out.println("+++++++");
                    needToScore++;
                }
                //System.out.println("Actual docid: " + doc_id + " max: " + max + " current_posting: " + entry.getValue());
            }

            //System.out.println("\n" + "needTOSCORE= " + needToScore);

            //devo fare lo score perche ho trovato un docid comune a tutte le posting list
            if (needToScore == size + 1){
                //System.out.println("FACCIO LO SCORE DI " + doc_id);
                for(Map.Entry<String, Posting> entry : current_postings.entrySet()) {
                    int doc_freq = Objects.requireNonNull((TermStats) lexicon.get(entry.getKey())).getDoc_frequency();
                    score += tfIdfScore(entry.getValue().getTerm_frequency(), doc_freq);
                }
                R.add(new Results(doc_id, score));
            }

            finished = true;
            while (min_list.hasNext()) {
                p = min_list.next();
                if (p.getDoc_id() >= max) {
                    doc_id = p.getDoc_id();
                    current_postings.put(term_shortest_pl, p);
                    finished = false;
                    break;
                }
            }
            if (finished)
                break;
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
