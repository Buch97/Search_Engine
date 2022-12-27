package it.unipi.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.unipi.bean.InvertedList;
import it.unipi.bean.Posting;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class GuavaCacheService {
    static final int expiration = 60;
    public static LoadingCache<String, List<Posting>> invertedListLoadingCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .recordStats()
            .expireAfterAccess(expiration, TimeUnit.SECONDS)
            .build(
                    new CacheLoader<>() {
                        public List<Posting> load(String term) {
                            return null;
                        }
                    }
            );
}
