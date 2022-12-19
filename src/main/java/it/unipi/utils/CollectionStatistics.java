package it.unipi.utils;

//• Collection statistics:
//  • Stored in a separate file, containing total number of documents, total number of terms, total number of postings, etc…

//non è obbligatoria però ce nelle sue slide e non ci vuole nulla a farla

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CollectionStatistics {
    public static int num_docs;

    public static void computeNumDocs() throws IOException {
        int rows = 0;
        BufferedReader collection = new BufferedReader(new FileReader("./src/main/resources/collections/small_collection.tsv"));
        while (collection.readLine() != null)
            rows += 1;
        CollectionStatistics.num_docs = rows;
    }
}
