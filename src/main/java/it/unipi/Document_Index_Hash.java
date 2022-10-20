package it.unipi;

import java.util.HashMap;

//• Document index:
//  • Contains information on the single documents, such as the URL, title, document length, pagerank, etc…
//• Contains one element for each indexed document
//• Keeps the mapping between documents and docids
//• Given docid, we need to be able to look up URL
//• Maybe store document size and pagerank
//• Simplest approach: records ordered by docid
//• More space-efficient:
//  • Store URLs in alphabetic order, maybe compressed
//  • Replace URL in above table by pointer or offset
//  • Also allows lookup of docID by URL

public class Document_Index_Hash {
    HashMap<Integer, Doc_Stats> doc_index;

    public Document_Index_Hash(HashMap<Integer, Doc_Stats> doc_index) {
        this.doc_index = doc_index;
    }

    public HashMap<Integer, Doc_Stats> getDoc_index() {
        return doc_index;
    }

    public void setDoc_index(HashMap<Integer, Doc_Stats> doc_index) {
        this.doc_index = doc_index;
    }

    public void print(){
        for (Integer key: doc_index.keySet()) {
            String docno = doc_index.get(key).getDoc_no();
            int len = doc_index.get(key).getLength();
            System.out.println("DOC_NO: " + docno + "  DOC_LEN: " + len);
        }
    }
}
