package it.unipi.dii.aide.mircv.querymanager;

import it.unipi.dii.aide.mircv.common.bean.InvertedList;
import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;

import java.util.Map;

public class Score {
    private static final float k = (float) 1.2;
    private static final float b = 0.75F;

    /*
    This function implements the scoring function bm25
     */
    protected static double BM25Score(int term_freq, int doc_freq, long doc_len) {
        double B = ((1 - b) + b * doc_len / CollectionStatistics.avg_doc_len);
        double idf = Math.log((double) CollectionStatistics.num_docs / (double) doc_freq);
        return idf * term_freq / (k * B + term_freq);
    }


    /*
    This function implements the scoring function TFIDF.
     */
    protected static double tfIdfScore(int term_freq, int doc_freq) {
        double tf, idf;
        tf = Math.log(term_freq);
        idf = Math.log((double) CollectionStatistics.num_docs / (double) doc_freq);
        return (tf + 1) * (idf);
    }
}
