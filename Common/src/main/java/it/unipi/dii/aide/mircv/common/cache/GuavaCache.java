package it.unipi.dii.aide.mircv.common.cache;

import com.google.common.cache.*;
import it.unipi.dii.aide.mircv.common.bean.Posting;
import it.unipi.dii.aide.mircv.common.bean.TermStats;
import it.unipi.dii.aide.mircv.common.textProcessing.Tokenizer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static it.unipi.dii.aide.mircv.common.inMemory.AuxiliarStructureOnMemory.lexiconMemory;
import static it.unipi.dii.aide.mircv.common.utils.Utils.retrievePostingLists;

public class GuavaCache {
    private static final long MEMORY_THRESHOLD = 100 * 1024 * 1024;
    private static GuavaCache instance = null;
    TermStats termStats;
    LoadingCache<String, List<Posting>> invertedListLoadingCache;

    private GuavaCache() {
        this.invertedListLoadingCache = CacheBuilder.newBuilder()
                .maximumWeight(MEMORY_THRESHOLD)
                .weigher((Weigher<String, List<Posting>>) (term, postingList)
                        -> (5 + 4 + 4) * postingList.size() + term.length())
                .recordStats()
                .build(
                        new CacheLoader<>() {
                            @NotNull
                            @Override
                            public List<Posting> load(@NotNull String term) throws IOException {
                                return retrievePostingLists(term, termStats).getPostingArrayList();
                            }
                        }
                );
    }

    public static GuavaCache getInstance() {
        if (instance == null) {
            synchronized (GuavaCache.class) {
                if (instance == null) {
                    instance = new GuavaCache();
                }
            }
        }
        return instance;
    }

    public void preloadCache() throws IOException {
        Map<String, Integer> popularTerms;
        System.out.println("Start preloading cache.");

        String queries_path = "PerformanceTest/src/main/resources/queries/queries.train.tsv";
        String text = Files.readString(Paths.get(queries_path));
        Tokenizer tokenizer = new Tokenizer(text.replace("\n", " ").replaceAll("\\d", ""));
        popularTerms = sortByValue(tokenizer.tokenize());

        HashMap<String, List<Posting>> termsToAdd = new HashMap<>();
        long memoryUsed = 0;

        for (Map.Entry<String, Integer> item : popularTerms.entrySet()) {
            String term = item.getKey();
            termStats = lexiconMemory.get(term);

            if (termStats != null){
                List<Posting> postingList = retrievePostingLists(term, termStats).getPostingArrayList();
                memoryUsed += (long) (5 + 4 + 4) * postingList.size() + term.length();
                if (memoryUsed > MEMORY_THRESHOLD * 0.9)
                    break;
                termsToAdd.put(term, postingList);
            }
            invertedListLoadingCache.putAll(termsToAdd);
            System.out.println("End preloading cache.");
        }
    }

    public synchronized List<Posting> getOrLoadPostingList(String term) throws ExecutionException {
        termStats = lexiconMemory.get(term);
        if (termStats == null) {
            System.out.println(term + " not in collection");
            return null;
        } else {
            return invertedListLoadingCache.get(term);
        }
    }

    private <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Collections.reverseOrder(Map.Entry.comparingByValue()));

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }


    public CacheStats getStats() {
        return invertedListLoadingCache.stats();
    }

}
