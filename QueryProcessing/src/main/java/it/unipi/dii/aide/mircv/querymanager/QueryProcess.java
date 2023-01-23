package it.unipi.dii.aide.mircv.querymanager;

import it.unipi.dii.aide.mircv.common.bean.*;
import it.unipi.dii.aide.mircv.common.cache.GuavaCache;
import it.unipi.dii.aide.mircv.common.textProcessing.Tokenizer;
import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;
import it.unipi.dii.aide.mircv.common.utils.Flags;
import it.unipi.dii.aide.mircv.common.utils.comparator.ResultsComparator;
import it.unipi.dii.aide.mircv.common.utils.filechannel.FileChannelInvIndex;
import it.unipi.dii.aide.mircv.common.utils.serializers.CustomSerializerDocumentIndexStats;
import it.unipi.dii.aide.mircv.common.utils.serializers.CustomSerializerTermStats;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static it.unipi.dii.aide.mircv.common.cache.GuavaCache.invertedListLoadingCache;
import static it.unipi.dii.aide.mircv.common.cache.GuavaCache.startCache;

public class QueryProcess {

    private static final String doc_id_path = "resources/output/inverted_index_doc_id_bin.dat";
    private static final String term_freq_path = "resources/output/inverted_index_term_frequency_bin.dat";
    private static final String stats = "resources/stats/stats.txt";
    private static final String mode = "READ";
    public static DB db_lexicon;
    public static DB db_document_index;
    //private static long startTime;


    public static void startQueryProcessor() throws IOException {

        if(!new File(doc_id_path).exists() || !new File(term_freq_path).exists() || !new File(stats).exists()){
            System.out.println("Cannot find data structures.");
            System.out.println("Please make sure that structures are present");
            System.exit(0);
        }

        db_document_index = DBMaker.fileDB("resources/output/document_index.db")
                .fileMmapEnable()
                .fileMmapPreclearDisable()
                .closeOnJvmShutdown()
                .readOnly()
                .make();

        db_lexicon = DBMaker.fileDB("resources/output/lexicon.db")
                .fileMmapEnable()
                .fileMmapPreclearDisable()
                .closeOnJvmShutdown()
                .readOnly()
                .make();

        CollectionStatistics.setParameters();

        FileChannelInvIndex.openFileChannels(mode);
        FileChannelInvIndex.MapFileChannel();

        startCache(db_lexicon);
    }

    public static void submitQuery(String query) throws IOException {

        Tokenizer tokenizer = new Tokenizer(query);
        Map<String, Integer> query_term_frequency = tokenizer.tokenize();

        if (query_term_frequency.isEmpty()) {
            System.out.println("Not valid input.");
            return;
        }
        System.out.println("Your request: " + query + "\n");
        String mode = Flags.getQueryMode();
        //startTime = System.nanoTime();
        daat(query_term_frequency, db_lexicon, db_document_index, mode);
    }

    public static void daat(Map<String, Integer> query_term_frequency, DB db_lexicon, DB db_document_index,String mode) {
        Comparator<Results> comparator = new ResultsComparator();
        int k = Flags.getK();
        PriorityQueue<Results> R = new PriorityQueue<>(k, comparator);

        HTreeMap<?, ?> lexicon = db_lexicon.hashMap("lexicon")
                .keySerializer(Serializer.STRING)
                .valueSerializer(new CustomSerializerTermStats())
                .open();
        HTreeMap<?, ?> document_index = db_document_index.hashMap("document_index")
                .keySerializer(Serializer.INTEGER)
                .valueSerializer(new CustomSerializerDocumentIndexStats())
                .open();

        ArrayList<InvertedList> L = getL(query_term_frequency);
        if (L.isEmpty()) return;

        if (Objects.equals(mode, "d"))
            daatScoringDisjunctive(L, lexicon, document_index, R);
        else if (Objects.equals(mode, "c"))
            daatScoringConjunctive(L, lexicon, document_index, R);

        printRankedResults(k, R);
        System.out.println(invertedListLoadingCache.stats());
    }

    private static void daatScoringDisjunctive(ArrayList<InvertedList> L, HTreeMap<?, ?> lexicon, HTreeMap<?, ?> document_index, PriorityQueue<Results> R) {
        int current_doc_id = min_doc_id(L);
        System.out.println("Scoring");

        HashMap<String, ListIterator<Posting>> iteratorList = new HashMap<>();
        HashMap<String, Integer> doc_freqs = new HashMap<>();

        for (InvertedList invertedList : L) {
            iteratorList.put(invertedList.getTerm(), invertedList.getPostingArrayList().listIterator());
            doc_freqs.put(invertedList.getTerm(), Objects.requireNonNull((TermStats) lexicon.get(invertedList.getTerm())).getDoc_frequency());
        }

        while (current_doc_id != CollectionStatistics.num_docs) {

            double score = 0;
            for (InvertedList invertedList : L) {
                score += getScore(document_index, current_doc_id, iteratorList, doc_freqs, invertedList);
            }

            R.add(new Results(current_doc_id, score));
            current_doc_id = min_doc_id(L);
        }
    }

    private static double getScore(HTreeMap<?, ?> document_index, int current_doc_id, HashMap<String, ListIterator<Posting>> iteratorList, HashMap<String, Integer> doc_freqs, InvertedList invertedList) {

        double score = 0;
        if (iteratorList.get(invertedList.getTerm()).hasNext()) {

            Posting posting = iteratorList.get(invertedList.getTerm()).next();
            int doc_id = posting.getDoc_id();
            int term_freq = posting.getTerm_frequency();

            if (current_doc_id == doc_id) {
                if(Objects.equals(Flags.getScoringFunction(), "bm25")){
                    int doc_len = Objects.requireNonNull((DocumentIndexStats) document_index.get(doc_id)).getDoc_len();
                    score = Score.BM25Score(term_freq, doc_freqs.get(invertedList.getTerm()), doc_len);
                }
                else
                    score = Score.tfIdfScore(term_freq, doc_freqs.get(invertedList.getTerm()));

                invertedList.setPos(invertedList.getPos() + 1);
            } else {
                iteratorList.get(invertedList.getTerm()).previous();
            }
        }
        return score;
    }

    private static void daatScoringConjunctive(ArrayList<InvertedList> L, HTreeMap<?, ?> lexicon, HTreeMap<?, ?> document_index, PriorityQueue<Results> R) {

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
                    int doc_freq = Objects.requireNonNull((TermStats) lexicon.get(entry.getKey())).getDoc_frequency();
                    //score += tfIdfScore(entry.getValue().getTerm_frequency(), doc_freq);
                    int doc_len = Objects.requireNonNull((DocumentIndexStats) document_index.get(current_doc_id)).getDoc_len();
                    score += Score.BM25Score(entry.getValue().getTerm_frequency(), doc_freq, doc_len);
                }
                R.add(new Results(current_doc_id, score));
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

    private static ArrayList<InvertedList> getL(Map<String, Integer> query_term_frequency) {

        final ExecutorService executor = Executors.newFixedThreadPool(query_term_frequency.size());
        final List<Future<?>> futures = new ArrayList<>();
        ArrayList<InvertedList> L = new ArrayList<>();

        for (String term : query_term_frequency.keySet()) {
            Future<?> future = executor.submit(() -> {
                List<Posting> posting_list = GuavaCache.getPostingList(term);
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

    private static void printRankedResults(int k, PriorityQueue<Results> r) {
        System.out.println("Results: ");
        try {
            for (int i = 0; i < k; i++) {
                Results result = r.poll();
                assert result != null;
                System.out.println((i + 1) + ". " + "DOC ID: " + result.getDoc_id() + " SCORE: " + result.getScore());
                if (r.size() == 0)
                    break;
            }
        } catch (NullPointerException e) {
            System.out.println("No results found for this query");
        }

        //long elapsedTime = System.nanoTime() - startTime;
        //System.out.println("Total elapsed time: " + elapsedTime / 1000000 + " ms");
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
        db_lexicon.close();
        db_document_index.close();
        FileChannelInvIndex.unmapBuffer();
        FileChannelInvIndex.closeFileChannels();
        System.exit(0);
    }

}
