package it.unipi.dii.aide.mircv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;

import static it.unipi.dii.aide.mircv.querymanager.QueryProcess.*;

public class QueryProcessingMain {

    public static void main( String[] args ) throws IOException {

        // GuavaCache.preloadCache();
        // System.out.println(GuavaCache.invertedListLoadingCache.asMap());

        startQueryProcessor();

        for (;;) {
            System.out.println("Please, submit your query! Otherwise digit \"!exit\" to stop the execution.");
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));
            String query = reader.readLine();
            if (Objects.equals(query, "!exit")) {
                closeQueryProcessor();
            }
            else if (Objects.equals(query, "") || query.trim().length() == 0) {
                System.out.println("The query is empty.");
            }
            else submitQuery(query, null);
        }
    }
}
