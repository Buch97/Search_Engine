package it.unipi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class App {
    public static int k = 20;
    public static int num_docs;
    public static int num_terms;

    public static void main(String[] args) throws IOException {
        Index_Construction.buildDataStructures();
        num_docs = Collection_Statistics.computeDocs();
        num_terms = Collection_Statistics.computeTerms();
        for(;;) {
            System.out.println("Please, submit your query! Otherwise digit \"!exit\" to stop the execution.");
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));
            String query = reader.readLine();
            if (Objects.equals(query, "!exit"))
                System.exit(0);
            System.out.println("Your request: " + query);
            QueryProcess.parseQuery(query, k);
        }

    }
}
