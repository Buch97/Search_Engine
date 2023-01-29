package it.unipi.dii.aide.mircv.common.bean;

public class Posting {
    int doc_id;
    int term_frequency;

    public Posting(int doc_id, int term_frequency) {
        this.doc_id = doc_id;
        this.term_frequency = term_frequency;
    }

    public Posting() {
    }

    public int getDoc_id() {
        return doc_id;
    }

    public void setDoc_id(int doc_id) {
        this.doc_id = doc_id;
    }

    public int getTerm_frequency() {
        return term_frequency;
    }

    public void setTerm_frequency(int term_frequency) {
        this.term_frequency = term_frequency;
    }

    @Override
    public String toString() {
        return doc_id + ":" + term_frequency;
    }
}
