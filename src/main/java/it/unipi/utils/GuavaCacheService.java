package it.unipi.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import it.unipi.bean.InvertedList;

import java.util.concurrent.TimeUnit;

public class GuavaCacheService {
    public static LoadingCache<String, InvertedList> invertedListLoadingCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(60, TimeUnit.SECONDS)
            .build(
                    new CacheLoader<>() {
                        public InvertedList load(String term) {
                            System.out.println("LOAD");
                            return null;
                        }
                    }
            );

}
