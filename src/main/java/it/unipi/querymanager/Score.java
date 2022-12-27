package it.unipi.querymanager;

import it.unipi.bean.InvertedList;
import it.unipi.utils.CollectionStatistics;

import java.util.Map;

public class Score {
    private static float k= (float) 1.2;
    private static float b= 0.75F;

    protected static double BM25Score(int term_freq, int doc_freq,long doc_len){
        double B=((1-b)+b*doc_len/CollectionStatistics.avg_doclen);
        double idf = Math.log((double) CollectionStatistics.num_docs / (double) doc_freq);
        return idf*term_freq/(k*B+term_freq);
    }

    protected static double tfIdfScore(int term_freq, int doc_freq) {
        double tf, idf;
        tf = Math.log(term_freq);
        idf = Math.log((double) CollectionStatistics.num_docs / (double) doc_freq);
        return (tf + 1) * (idf);
    }

    private static double getScore(Map<String, Integer> query_term_frequency, int query_length, int doc_len, InvertedList invertedList, int term_freq) {

        int query_term_freq = query_term_frequency.get(invertedList.getTerm());

        double w_td = (double) term_freq / (double) doc_len;
        double w_tq = (double) query_term_freq / (double) query_length;
        return w_td * w_tq;
    }
}
