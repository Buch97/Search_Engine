package it.unipi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public class App {
    public static void main(String[] args) throws IOException {
        //Inverted_Index_Construction.buildDataStructures();
        for(;;) {
            System.out.println("Please, submit your query! Otherwise digit \"CTRL+F2\" to stop the execution.");
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));
            String query = reader.readLine();
            System.out.println("Your request: " + query);
            Tokenizer tokenizer = new Tokenizer(query);
            Map<String, Integer> results = tokenizer.tokenize();
            for (String token : results.keySet()) {
                System.out.println(token + " " + results.get(token));
            }
        }

    }
}
