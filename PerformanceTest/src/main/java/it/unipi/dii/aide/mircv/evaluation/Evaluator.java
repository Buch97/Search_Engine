package it.unipi.dii.aide.mircv.evaluation;

import it.unipi.dii.aide.mircv.common.cache.GuavaCache;
import it.unipi.dii.aide.mircv.common.utils.Flags;
import it.unipi.dii.aide.mircv.querymanager.QueryProcess;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class Evaluator {
    private static final String queriesPathDev = "PerformanceTest/src/main/resources/queries/queries.dev.tsv";
    private static final String queriesPathEval = "PerformanceTest/src/main/resources/queries/queries.eval.tsv";
    private static final String queriesPathTrain = "PerformanceTest/src/main/resources/queries/queries.train.tsv";

    public static void evaluateQueriesTest() throws IOException {
        Scanner myReader = new Scanner(new File(queriesPathTrain), StandardCharsets.UTF_8);
        // BufferedWriter bw = new BufferedWriter(new FileWriter("PerformanceTest/src/main/resources/results/testResult.txt"));

        System.out.println("Starting query performances evaluation..." + "\n");
        int num_queries = 0;
        long sum_elapsedTime = 0;
        String min_query = "", max_query = "";
        long min_elaps = 0, max_elaps = 0;

        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();

            String[] row = data.split("\t");
            String doc_no = row[0];
            String query = row[1];

            long startTime = System.nanoTime();
            QueryProcess.submitQuery(query);

            //System.out.println("Query processed: " + num_queries);
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

            /*if (num_queries == 1000)
                break;*/

            //bw.write(doc_no + " " + elapsedTime);
            //bw.newLine();
        }

        System.out.println("\n" + "Evaluation finished!" + "\n");
        System.out.println("STATISTICS FOR QUERIES, IN " + Flags.getQueryMode() + " MODE :");
        System.out.println("Average time elapsed: " + sum_elapsedTime / num_queries + " ms");
        System.out.println("Query with min time elapsed: " + min_query + "- " + min_elaps + "ms");
        System.out.println("Query with max time elapsed: " + max_query + "- " + max_elaps + "ms");
        System.out.println("Cache hit rate: " + GuavaCache.getInstance().getStats().hitRate() + ".");

        //bw.write("\nAverage time elapsed: " + sum_elapsedTime / num_queries + " ms");

        myReader.close();
        //bw.close();
    }
}
