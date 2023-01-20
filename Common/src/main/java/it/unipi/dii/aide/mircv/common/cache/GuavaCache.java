package it.unipi.dii.aide.mircv.common.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.unipi.dii.aide.mircv.common.bean.Posting;
import it.unipi.dii.aide.mircv.common.bean.TermStats;
import it.unipi.dii.aide.mircv.common.textProcessing.Tokenizer;
import it.unipi.dii.aide.mircv.common.utils.serializers.CustomSerializerTermStats;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static it.unipi.dii.aide.mircv.common.utils.Utils.retrievePostingLists;

public class GuavaCache {
    private static final int maxSize = 0;
    private static final String queries_path = "src/main/resources/queries/queries.eval.tsv";
    private static HTreeMap<?, ?> lexicon = null;
    private static TermStats termStats;

    public static LoadingCache<String, List<Posting>> invertedListLoadingCache = CacheBuilder.newBuilder()
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

    public static void preloadCache() throws IOException, ExecutionException {
        Map<String, Integer> popularTerms;

        String text = Files.readString(Paths.get(queries_path));
        Tokenizer tokenizer = new Tokenizer(text.replace("\n", " ").replaceAll("\\d", ""));
        popularTerms = sortByValue(tokenizer.tokenize());

        for (Map.Entry<String, Integer> item : popularTerms.entrySet()) {
            String term = item.getKey();
            List<Posting> posting_list = GuavaCache.getPostingList(term);
        }
    }

    public static List<Posting> getPostingList(String term) throws ExecutionException {
        try {
            termStats = Objects.requireNonNull((TermStats) lexicon.get(term));
            return invertedListLoadingCache.get(term);
        } catch (NullPointerException e) {
            System.out.println("Term not in collection");
            return null;
        }
    }

    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Collections.reverseOrder(Map.Entry.comparingByValue()));

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    public static void startCache(DB db_lexicon) {
        lexicon = db_lexicon.hashMap("lexicon")
                .keySerializer(Serializer.STRING)
                .valueSerializer(new CustomSerializerTermStats())
                .open();
    }
}
