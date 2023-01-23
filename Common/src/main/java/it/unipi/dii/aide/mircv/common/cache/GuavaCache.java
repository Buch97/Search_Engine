package it.unipi.dii.aide.mircv.common.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import it.unipi.dii.aide.mircv.common.bean.Posting;
import it.unipi.dii.aide.mircv.common.bean.TermStats;
import it.unipi.dii.aide.mircv.common.textProcessing.Tokenizer;
import org.jetbrains.annotations.NotNull;
import org.mapdb.HTreeMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static it.unipi.dii.aide.mircv.common.inMemory.AuxiliarStructureOnMemory.lexiconMemory;
import static it.unipi.dii.aide.mircv.common.utils.Utils.retrievePostingLists;

public class GuavaCache {
    private static GuavaCache instance = null;
    private static final HTreeMap<?, ?> lexicon = null;
    TermStats termStats;
    LoadingCache<String, List<Posting>> invertedListLoadingCache;

    private GuavaCache() {
        this.invertedListLoadingCache = CacheBuilder.newBuilder()
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

    private void preloadCache() throws IOException, ExecutionException {
        Map<String, Integer> popularTerms;

        String queries_path = "src/main/resources/queries/queries.eval.tsv";
        String text = Files.readString(Paths.get(queries_path));
        Tokenizer tokenizer = new Tokenizer(text.replace("\n", " ").replaceAll("\\d", ""));
        popularTerms = sortByValue(tokenizer.tokenize());

        for (Map.Entry<String, Integer> item : popularTerms.entrySet()) {
            String term = item.getKey();
            List<Posting> posting_list = instance.getOrLoadPostingList(term);
        }
    }

    public List<Posting> getOrLoadPostingList(String term) throws ExecutionException {
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

    /*public void startCache(DB db_lexicon) {
        if (lexicon == null) {
            lexicon = db_lexicon.hashMap("lexicon")
                    .keySerializer(Serializer.STRING)
                    .valueSerializer(new CustomSerializerTermStats())
                    .open();
        }
    }*/

    public CacheStats getStats() {
        return invertedListLoadingCache.stats();
    }

}
