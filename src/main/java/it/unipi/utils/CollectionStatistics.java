package it.unipi.utils;

//• Collection statistics:
//  • Stored in a separate file, containing total number of documents, total number of terms, total number of postings, etc…

//non è obbligatoria però ce nelle sue slide e non ci vuole nulla a farla

import it.unipi.bean.DocumentIndexStats;
import org.mapdb.DB;
import org.mapdb.HTreeMap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class CollectionStatistics {
    public static int num_docs;
    public static double avg_doclen;

    public static void computeAvgDocLen(DB db_document_index){
        HTreeMap<Integer, DocumentIndexStats> document_index_map = (HTreeMap<Integer, DocumentIndexStats>) db_document_index
                .hashMap("document_index")
                .createOrOpen();
        long sum=0;

        for (Map.Entry<Integer, DocumentIndexStats> entry : document_index_map.entrySet()){
            sum+=entry.getValue().getDoc_len();
        }
        CollectionStatistics.avg_doclen=sum/num_docs;
    }

    public static void computeNumDocs() throws IOException {
        int rows = 0;
        BufferedReader collection = new BufferedReader(new FileReader("./src/main/resources/collections/collection.tsv"));
        while (collection.readLine() != null) rows++;
        CollectionStatistics.num_docs = rows;
    }
}
