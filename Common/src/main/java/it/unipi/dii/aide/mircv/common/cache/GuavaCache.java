package it.unipi.dii.aide.mircv.common.cache;

import com.google.common.cache.*;
import it.unipi.dii.aide.mircv.common.bean.Posting;
import it.unipi.dii.aide.mircv.common.bean.TermStats;
import it.unipi.dii.aide.mircv.common.textProcessing.Tokenizer;
import it.unipi.dii.aide.mircv.common.utils.Flags;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static it.unipi.dii.aide.mircv.common.inMemory.AuxiliarStructureOnMemory.lexiconMemory;
import static it.unipi.dii.aide.mircv.common.utils.Utils.retrievePostingLists;

public class GuavaCache {
    private static final long MEMORY_THRESHOLD = 80 * 1024 * 1024;
    private static GuavaCache instance = null;
    TermStats termStats;
    LoadingCache<String, List<Posting>> invertedListLoadingCache;

    /*
    Constructor of Guava Cache. Set the maximum weight of elements in cache and implements the
    method load.
     */
    private GuavaCache() {
        this.invertedListLoadingCache = CacheBuilder.newBuilder()
                .maximumWeight(MEMORY_THRESHOLD)
                .weigher((Weigher<String, List<Posting>>) (term, postingList)
                        -> (4 + 4) * postingList.size())
                .recordStats()
                .build(
                        new CacheLoader<>() {
                            @NotNull
                            @Override
                            public List<Posting> load(@NotNull String term) throws IOException, InterruptedException {
                                return retrievePostingLists(term, termStats).getPostingArrayList();
                            }
                        }
                );
    }

    /*
    Return an instance of Guava Cache. Only one instance per run is allowed.
     */
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

    /*
    This method retrieve a term's posting list from cache if it is presents, or upload that term on cache
    and returns it's posting list.
     */
    public synchronized List<Posting> getOrLoadPostingList(String term) throws ExecutionException {
        termStats = lexiconMemory.get(term);
        if (termStats == null) {
            if (!Flags.isEvaluation())
                System.out.println(term + " not in collection.");
            return null;
        } else {
            return invertedListLoadingCache.get(term);
        }
    }

    /*
    Return cache statistics
     */
    public CacheStats getStats() {
        return invertedListLoadingCache.stats();
    }

}
