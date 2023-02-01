package it.unipi.dii.aide.mircv.querymanager;

import it.unipi.dii.aide.mircv.common.bean.InvertedList;
import it.unipi.dii.aide.mircv.common.bean.Posting;
import it.unipi.dii.aide.mircv.common.bean.Results;
import it.unipi.dii.aide.mircv.common.bean.TermStats;
import it.unipi.dii.aide.mircv.common.cache.GuavaCache;
import it.unipi.dii.aide.mircv.common.inMemory.AuxiliarStructureOnMemory;
import it.unipi.dii.aide.mircv.common.textProcessing.Tokenizer;
import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;
import it.unipi.dii.aide.mircv.common.utils.Flags;
import it.unipi.dii.aide.mircv.common.utils.Utils;
import it.unipi.dii.aide.mircv.common.utils.boundedpq.BoundedPriorityQueue;
import it.unipi.dii.aide.mircv.common.utils.comparator.ResultsComparator;
import it.unipi.dii.aide.mircv.common.utils.filechannel.FileChannelInvIndex;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static it.unipi.dii.aide.mircv.common.inMemory.AuxiliarStructureOnMemory.documentIndexMemory;
import static it.unipi.dii.aide.mircv.common.inMemory.AuxiliarStructureOnMemory.lexiconMemory;
import static it.unipi.dii.aide.mircv.querymanager.MaxScore.maxScore;

public class QueryProcess {

    private static final String doc_id_path = "resources/output/inverted_index_doc_id_bin.dat";
    private static final String term_freq_path = "resources/output/inverted_index_term_frequency_bin.dat";
    private static final String stats = "resources/stats/stats.txt";
    private static final String mode = "READ";
    private static FileChannel document_index;
    private static FileChannel lexicon;
    private static long startTime;
    private static final ExecutorService executor = Executors.newFixedThreadPool(10);

    public static void submitQuery(String query) throws IOException {

        Tokenizer tokenizer = new Tokenizer(query);
        Map<String, Integer> query_term_frequency = tokenizer.tokenize();

        if (query_term_frequency.isEmpty()) {
            System.out.println("Not valid input.");
            return;
        }

        String mode = Flags.getQueryMode();

        if(!Flags.isEvaluation()){
            System.out.println("Your request: " + query + "\n");
            startTime = System.nanoTime();
        }
        daat(query_term_frequency, mode);
    }

    public static void daat(Map<String, Integer> query_term_frequency, String mode) {
        Comparator<Results> comparator = new ResultsComparator();
        int k = Flags.getK();

        BoundedPriorityQueue results = new BoundedPriorityQueue(comparator, k);
        System.out.println(query_term_frequency.size());
        ArrayList<InvertedList> L = getL(query_term_frequency);
        if (L.isEmpty()) return;

        if (Objects.equals(Flags.getQueryAlgorithm(), "maxScore")) {
            maxScore(query_term_frequency.keySet().toArray(new String[0]), L,results,k);
        }else {
            if (Objects.equals(mode, "d"))
                daatScoringDisjunctive(L, results);
            else if (Objects.equals(mode, "c"))
                daatScoringConjunctive(L, results);
        }

        if (!Flags.isEvaluation()){
            results.printRankedResults();
            long elapsedTime = System.nanoTime() - startTime;
            System.out.println("Total elapsed time: " + elapsedTime / 1000000 + " ms");

            GuavaCache guavaCache = GuavaCache.getInstance();
            System.out.println(guavaCache.getStats());
        }
    }

    private static void daatScoringDisjunctive(ArrayList<InvertedList> L, BoundedPriorityQueue results) {
        int current_doc_id = min_doc_id(L);
        if(!Flags.isEvaluation())
            System.out.println("Scoring");

        HashMap<String, ListIterator<Posting>> iteratorList = new HashMap<>();
        HashMap<String, Integer> doc_freqs = new HashMap<>();

        for (InvertedList invertedList : L) {
            iteratorList.put(invertedList.getTerm(), invertedList.getPostingArrayList().listIterator());
            doc_freqs.put(invertedList.getTerm(), lexiconMemory.get(invertedList.getTerm()).getDoc_frequency());
        }

        while (current_doc_id != CollectionStatistics.num_docs) {

            double score = 0;
            for (InvertedList invertedList : L) {
                score += getScore(current_doc_id, iteratorList, doc_freqs, invertedList);
            }

            results.add(current_doc_id, score);
            current_doc_id = min_doc_id(L);
        }
    }

    private static double getScore(int current_doc_id, HashMap<String, ListIterator<Posting>> iteratorList, HashMap<String, Integer> doc_freqs, InvertedList invertedList) {

        double score = 0;
        if (iteratorList.get(invertedList.getTerm()).hasNext()) {

            Posting posting = iteratorList.get(invertedList.getTerm()).next();
            int doc_id = posting.getDoc_id();
            int term_freq = posting.getTerm_frequency();

            if (current_doc_id == doc_id) {
                if (Objects.equals(Flags.getScoringFunction(), "bm25")) {
                    int doc_len = documentIndexMemory.get(doc_id).getDoc_len();
                    score = Score.BM25Score(term_freq, doc_freqs.get(invertedList.getTerm()), doc_len);
                } else
                    score = Score.tfIdfScore(term_freq, doc_freqs.get(invertedList.getTerm()));

                invertedList.setPos(invertedList.getPos() + 1);
            } else {
                iteratorList.get(invertedList.getTerm()).previous();
            }
        }
        return score;
    }

    private static void daatScoringConjunctive(ArrayList<InvertedList> L, BoundedPriorityQueue results) {

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
        if (shortest_pl.hasNext()) {
            posting = shortest_pl.next();
            current_doc_id = posting.getDoc_id();
        }
        int size = iteratorList.size();

        while (true) {
            double score = 0;

            HashMap<String, Posting> current_postings = new HashMap<>();
            current_postings.put(term_shortest_pl, posting);

            int max = current_doc_id;

            for (Map.Entry<String, ListIterator<Posting>> entry : iteratorList.entrySet())
                max = nextGeqPostingLists(entry, current_postings, current_doc_id, max);

            if (current_doc_id < max) {
                //current_doc_id exceeded on reference posting list, so we need to reach a valid posting
                posting = nextGeqReferencePostingList(shortest_pl, current_postings, current_doc_id, term_shortest_pl);
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
            if (needToScore == size + 1) {
                for (Map.Entry<String, Posting> entry : current_postings.entrySet()) {

                    int doc_freq = lexiconMemory.get(entry.getKey()).getDoc_frequency();

                    if (Objects.equals(Flags.getScoringFunction(), "bm25")){
                        int doc_len = documentIndexMemory.get(current_doc_id).getDoc_len();
                        score += Score.BM25Score(entry.getValue().getTerm_frequency(), doc_freq, doc_len);
                    } else score += Score.tfIdfScore(entry.getValue().getTerm_frequency(), doc_freq);
                }
                results.add(current_doc_id, score);
            }

            //go to next posting on reference list
            posting = nextGeqReferencePostingList(shortest_pl, current_postings, current_doc_id, term_shortest_pl);
            if (posting == null)
                break;
            else
                current_doc_id = posting.getDoc_id();
        }
    }

    private static Posting nextGeqReferencePostingList(ListIterator<Posting> min_list, HashMap<String, Posting> current_postings, int max, String term_shortest_pl) {
        while (min_list.hasNext()) {
            Posting p = min_list.next();
            if (p.getDoc_id() >= max) {
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
                if (p.getDoc_id() >= max)
                    max = p.getDoc_id();
                //System.out.println("Skippo a questa: " + p.getDoc_id());
                current_postings.put(entry.getKey(), p);
                return max;
            }
        }
        return max;
    }

    private static ArrayList<InvertedList> getLCache(Map<String, Integer> query_term_frequency) {

        final List<Future<?>> futures = new ArrayList<>();
        ArrayList<InvertedList> L = new ArrayList<>();

        for (String term : query_term_frequency.keySet()) {
            Future<?> future = executor.submit(() -> {
                GuavaCache guavaCache = GuavaCache.getInstance();
                List<Posting> posting_list = guavaCache.getOrLoadPostingList(term);
                if (posting_list != null) {
                    return new InvertedList(term, posting_list, 0);
                } else return null;
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

    public static ArrayList<InvertedList> getL(Map<String, Integer> query_term_frequency){
        final List<Future<?>> futures = new ArrayList<>();
        ArrayList<InvertedList> L = new ArrayList<>();

        for (String term : query_term_frequency.keySet()) {
            Future<?> future = executor.submit(() -> {
                TermStats termStats = lexiconMemory.get(term);
                if (termStats != null) {
                    return Utils.retrievePostingLists(term, termStats);
                } else {
                    if (!Flags.isEvaluation())
                        System.out.println(term + " not in collection.");
                    return null;
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

    private static int min_doc_id(ArrayList<InvertedList> L) {
        int min_doc_id = CollectionStatistics.num_docs;

        for (InvertedList invertedList : L) {
            if (invertedList.getPos() < invertedList.getPostingArrayList().size()) {
                min_doc_id = Math.min(invertedList.getPostingArrayList().get(invertedList.getPos()).getDoc_id(), min_doc_id);
            }
        }
        return min_doc_id;
    }

    public static void closeQueryProcessor() throws IOException {
        lexicon.close();
        document_index.close();
        FileChannelInvIndex.unmapBuffer();
        FileChannelInvIndex.closeFileChannels();
        System.exit(0);
    }

    public static void startQueryProcessor() throws IOException, InterruptedException {

        if (!new File(doc_id_path).exists() || !new File(term_freq_path).exists() || !new File(stats).exists()) {
            System.out.println("Cannot find data structures.");
            System.out.println("Please make sure that structures are present");
            System.exit(0);
        }

        document_index = (FileChannel) Files.newByteChannel(Paths.get("resources/output/document_index"),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        lexicon = (FileChannel) Files.newByteChannel(Paths.get("resources/output/lexicon"),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        System.out.println("Loading lexicon in memory...");
        long start = System.currentTimeMillis();
        AuxiliarStructureOnMemory.loadLexicon(lexicon);
        long finish = System.currentTimeMillis();
        System.out.println("Time for loading lexicon in memory: " + (finish - start)/1000 + " s");

        if (Objects.equals(Flags.getScoringFunction(), "bm25")) {
            System.out.println("Loading document index in memory...");
            start = System.currentTimeMillis();
            AuxiliarStructureOnMemory.loadDocumentIndex(document_index);
            finish = System.currentTimeMillis();
            System.out.println("Time for loading document index in memory: " + (finish - start)/1000 + " s");
        }

        CollectionStatistics.setParameters();

        FileChannelInvIndex.openFileChannels(mode);
        FileChannelInvIndex.MapFileChannel();

        //GuavaCache guavaCache = GuavaCache.getInstance();
        //guavaCache.preloadCache();
    }
}
