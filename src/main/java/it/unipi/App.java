package it.unipi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public class App {
    public static void main(String[] args) throws IOException {
        Index_Construction.buildDataStructures();
        for(;;) {
            System.out.println("Please, submit your query! Otherwise digit \"CTRL+F2\" to stop the execution.");
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));
            String query = reader.readLine();
            System.out.println("Your request: " + query);
            Tokenizer tokenizer = new Tokenizer(query);
            Map<String, Integer> query_term_frequency = tokenizer.tokenize();
            Integer query_length = 0;
            for (String token : query_term_frequency.keySet()) {
                query_length += query_term_frequency.get(token);
                System.out.println(token + " " + query_term_frequency.get(token));
            }
            System.out.println("Query length = " + query_length);
        }

    }
}
