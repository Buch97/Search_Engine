package it.unipi.bean;

import java.io.Serializable;

public class TermStats implements Serializable {
    int doc_frequency;
    int coll_frequency;
    long actual_offset_doc_id;
    long actual_offset_term_freq;
    int size;
    public TermStats(int doc_frequency, int coll_frequency, long actual_offset_doc_id, long actual_offset_term_freq, int size) {
        this.doc_frequency = doc_frequency;
        this.coll_frequency = coll_frequency;
        this.actual_offset_doc_id = actual_offset_doc_id;
        this.actual_offset_term_freq = actual_offset_term_freq;
        this.size = size;
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

    public long getActual_offset_doc_id() {
        return actual_offset_doc_id;
    }

    public long getActual_offset_term_freq() {
        return actual_offset_term_freq;
    }

    public void setActual_offset_doc_id(long actual_offset_doc_id) {
        this.actual_offset_doc_id = actual_offset_doc_id;
    }

    public void setActual_offset_term_freq(long actual_offset_term_freq) {
        this.actual_offset_term_freq = actual_offset_term_freq;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "Term_Stats{" +
                "doc_frequency=" + doc_frequency +
                ", coll_frequency=" + coll_frequency +
                ", actual_offset_doc_id=" + actual_offset_doc_id +
                ", actual_offset_term_freq=" + actual_offset_term_freq +
                '}';
    }
}

