package it.unipi.bean;

import java.io.Serializable;

public class TermStats implements Serializable {
    int doc_frequency;
    int coll_frequency;
    long offset_doc_id_start;
    long offset_term_freq_start;
    long offset_doc_id_end;
    long offset_term_freq_end;
    long actual_offset;

    // Costruttore per la versione testuale dell'inverted index
    public TermStats(int doc_frequency, int coll_frequency, long actual_offset) {
        this.doc_frequency = doc_frequency;
        this.coll_frequency = coll_frequency;
        this.actual_offset = actual_offset;
    }

    public long getOffset_doc_id_end() {
        return offset_doc_id_end;
    }

    public void setOffset_doc_id_end(long offset_doc_id_end) {
        this.offset_doc_id_end = offset_doc_id_end;
    }

    public long getOffset_term_freq_end() {
        return offset_term_freq_end;
    }

    public void setOffset_term_freq_end(long offset_term_freq_end) {
        this.offset_term_freq_end = offset_term_freq_end;
    }

    public TermStats(int doc_frequency, int coll_frequency, long offset_doc_id_start, long offset_term_freq_start, long offset_doc_id_end, long offset_term_freq_end) {
        this.doc_frequency = doc_frequency;
        this.coll_frequency = coll_frequency;
        this.offset_doc_id_start = offset_doc_id_start;
        this.offset_term_freq_start = offset_term_freq_start;
        this.offset_doc_id_end = offset_doc_id_end;
        this.offset_term_freq_end = offset_term_freq_end;
    }

    public int getDoc_frequency() {
        return doc_frequency;
    }

    public void setDoc_frequency(int doc_frequency) {
        this.doc_frequency = doc_frequency;
    }

    public int getColl_frequency() {
        return coll_frequency;
    }

    public void setColl_frequency(int coll_frequency) {
        this.coll_frequency = coll_frequency;
    }

    public long getOffset_doc_id_start() {
        return offset_doc_id_start;
    }

    public long getOffset_term_freq_start() {
        return offset_term_freq_start;
    }

    public void setOffset_doc_id_start(long offset_doc_id_start) {
        this.offset_doc_id_start = offset_doc_id_start;
    }

    public void setOffset_term_freq_start(long offset_term_freq_start) {
        this.offset_term_freq_start = offset_term_freq_start;
    }

    @Override
    public String toString() {
        return "TermStats{" +
                "doc_frequency=" + doc_frequency +
                ", coll_frequency=" + coll_frequency +
                ", offset_doc_id_start=" + offset_doc_id_start +
                ", offset_term_freq_start=" + offset_term_freq_start +
                ", offset_doc_id_end=" + offset_doc_id_end +
                ", offset_term_freq_end=" + offset_term_freq_end +
                ", actual_offset=" + actual_offset +
                '}';
    }
}

