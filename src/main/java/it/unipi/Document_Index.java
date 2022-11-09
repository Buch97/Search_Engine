package it.unipi;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

public class Document_Index {
    Map<Integer, Doc_Stats> doc_index;

    public Document_Index(Map<Integer, Doc_Stats> doc_index) {
        this.doc_index = doc_index;
    }

    public Map<Integer, Doc_Stats> getDoc_index() {
        return doc_index;
    }

    public void setDoc_index(Map<Integer, Doc_Stats> doc_index) {
        this.doc_index = doc_index;
    }

    public void save_to_file() throws IOException {
        BufferedWriter file = new BufferedWriter(new FileWriter("C:\\Users\\pucci\\Desktop\\AIDE\\" +
                "Multimedia Information Retrieval and Computer Vision\\document_index.tsv"));

        for (Integer key: doc_index.keySet()) {
            String docno = doc_index.get(key).getDoc_no();
            int len = doc_index.get(key).getLength();
            file.write("DOC_ID: " + key + "\t" + "DOC_NO: " + docno + "\t" + "DOC_LEN: " + len + "\n");
        }
        file.close();
    }
}
