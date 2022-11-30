package it.unipi.utils;

//• Collection statistics:
//  • Stored in a separate file, containing total number of documents, total number of terms, total number of postings, etc…

//non è obbligatoria però ce nelle sue slide e non ci vuole nulla a farla

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Collection_Statistics {

    public static int computeDocs() throws IOException {
        int docs = 0;
        BufferedReader doc_index = new BufferedReader(new FileReader("./src/main/resources/output/document_index.tsv"));
        BufferedReader lexicon = new BufferedReader(new FileReader("./src/main/resources/output/lexicon.tsv"));
        while (doc_index.readLine() != null)
            docs += 1;

        return docs;
    }

    public static int computeTerms() throws IOException {
        int terms = 0;
        BufferedReader doc_index = new BufferedReader(new FileReader("./src/main/resources/output/document_index.tsv"));
        BufferedReader lexicon = new BufferedReader(new FileReader("./src/main/resources/output/lexicon.tsv"));
        while (lexicon.readLine() != null)
            terms += 1;

        return terms;
    }
}
