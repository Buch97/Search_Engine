package it.unipi;

import java.util.List;

public class Document_Index {
    List<Doc_Stats> doc_index;

    public Document_Index(List<Doc_Stats> doc_index) {
        this.doc_index = doc_index;
    }

    public List<Doc_Stats> getDoc_index() {
        return doc_index;
    }

    public void setDoc_index(List<Doc_Stats> doc_index) {
        this.doc_index = doc_index;
    }

    public void print(){
        for (int i = 0; i < this.doc_index.size(); i++){
            System.out.println("DOC_ID: " + doc_index.get(i).getDoc_id() + "    DOC_LEN: " + doc_index.get(i).getLength());
        }
    }
}
