package it.unipi.bean;

public class Token {
    String term;
    int doc_id;
    int frequency;

    public Token(String term, int doc_id, int frequency) {
        this.term = term;
        this.doc_id = doc_id;
        this.frequency = frequency;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public int getDoc_id() {
        return doc_id;
    }

    public void setDoc_id(int doc_id) {
        this.doc_id = doc_id;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }
}
