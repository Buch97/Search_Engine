package it.unipi.bean;

import java.io.Serializable;

public class DocumentIndexStats implements Serializable {
    String doc_no;
    int doc_len;

    public DocumentIndexStats(String doc_no, int doc_len) {
        this.doc_no = doc_no;
        this.doc_len = doc_len;
    }

    public String getDoc_no() {
        return doc_no;
    }

    public void setDoc_no(String doc_no) {
        this.doc_no = doc_no;
    }

    public int getDoc_len() {
        return doc_len;
    }

    public void setDoc_len(int doc_len) {
        this.doc_len = doc_len;
    }

    @Override
    public String toString() {
        return "DocumentIndexStats{" +
                "doc_no='" + doc_no + '\'' +
                ", doc_len=" + doc_len +
                '}';
    }
}
