package it.unipi;

public class Doc_Stats {
    int doc_id;
    int doc_length;

    public Doc_Stats(int doc_id, int length) {
        this.doc_id = doc_id;
        this.doc_length = length;
    }

    public int getDoc_id() {
        return doc_id;
    }

    public void setDoc_id(int doc_id) {
        this.doc_id = doc_id;
    }

    public int getLength() {
        return doc_length;
    }

    public void setLength(int length) {
        this.doc_length = length;
    }
}
