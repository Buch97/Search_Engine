package it.unipi.evaluation;

import it.unipi.utils.textProcessing.Tokenizer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Scanner;

import static it.unipi.Main.*;
import static it.unipi.querymanager.QueryProcess.daat;

public class Evaluator {
    private static final int mode = 0;  //0-Disjunctive


    public static void evaluateQueriesTest() throws IOException {
        Scanner myReader = new Scanner(new File("./src/main/resources/queries/queries.train.tsv"), StandardCharsets.UTF_8);
        BufferedWriter bw = new BufferedWriter(new FileWriter("./src/main/resources/output/testResult" + mode + ".txt"));

        int num_queries = 0;
        long sum_elapsedTime = 0;
        String min_query = "", max_query = "";
        long min_elaps = 0, max_elaps = 0;
        int count = 0;
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();

            String[] row = data.split("\t");
            String doc_no = row[0];
            String query = row[1];
            //System.out.println(doc_no+" "+query);
            long startTime = System.nanoTime();
            Tokenizer tokenizer = new Tokenizer(query);
            Map<String, Integer> query_term_frequency = tokenizer.tokenize();
            daat(query_term_frequency, k, db_lexicon, db_document_index, mode);
            long elapsedTime = (System.nanoTime() - startTime) / 1000000;
            sum_elapsedTime += elapsedTime;
            num_queries += 1;
            if (elapsedTime < min_elaps || num_queries == 1) {
                min_elaps = elapsedTime;
                min_query = query;
            } else if (elapsedTime > max_elaps) {
                max_elaps = elapsedTime;
                max_query = query;
            }

            bw.write(doc_no + " " + elapsedTime);
            bw.newLine();
            count += 1;
            if (count == 1000) break;
        }

        System.out.println("STATISTICS FOR QUERIES MODE " + mode + ":");
        System.out.println("Average time elapsed: " + sum_elapsedTime / num_queries + " ms");
        System.out.println("Query with min time elapsed: " + min_query + "- " + min_elaps + "ms");
        System.out.println("Query with max time elapsed: " + max_query + "- " + max_elaps + "ms");

        bw.write("\nAverage time elapsed: " + sum_elapsedTime / num_queries + " ms");

        myReader.close();
        bw.close();
    }
}
