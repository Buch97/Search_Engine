package it.unipi;

import it.unipi.build_data_structures.Index_Construction;
import it.unipi.query_manager.QueryProcess;
import it.unipi.utils.Collection_Statistics;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;

public class Main {
    public static int k = 20;
    public static int num_docs;
    public static int num_terms;
    public static DB db = DBMaker.fileDB("./src/main/resources/output/lexicon_disk_based.db").checksumHeaderBypass().make();

    public static void main(String[] args) throws IOException {
        Index_Construction.buildDataStructures(db);
        num_docs = Collection_Statistics.computeDocs();
        num_terms = Collection_Statistics.computeTerms();
        for(;;) {
            System.out.println("Please, submit your query! Otherwise digit \"!exit\" to stop the execution.");
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));
            String query = reader.readLine();
            if (Objects.equals(query, "!exit")) {
                db.close();
                System.exit(0);
            }
            System.out.println("Your request: " + query);
            QueryProcess.parseQuery(query, k);
        }


    }
}
