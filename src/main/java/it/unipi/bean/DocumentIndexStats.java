package it.unipi.bean;

import java.io.Serializable;

public class DocumentIndexStats implements Serializable {
    String doc_no;
    int doc_len;

    public DocumentIndexStats(String doc_no, int doc_len) {
        this.doc_no = doc_no;
        this.doc_len = doc_len;
    }

    public int getDoc_len() {
        return doc_len;
    }
}
