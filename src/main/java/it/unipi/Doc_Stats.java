package it.unipi;

public class Doc_Stats {
    String doc_no;
    int doc_length;

    public Doc_Stats(String doc_no, int length) {
        this.doc_no= doc_no;
        this.doc_length = length;
    }

    public String getDoc_no() {
        return doc_no;
    }

    public void setDoc_no(String doc_no) {
        this.doc_no = doc_no;
    }

    public int getLength() {
        return doc_length;
    }

    public void setLength(int length) {
        this.doc_length = length;
    }
}
