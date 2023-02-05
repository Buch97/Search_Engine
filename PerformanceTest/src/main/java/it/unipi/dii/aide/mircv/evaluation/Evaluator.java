package it.unipi.dii.aide.mircv.evaluation;

import it.unipi.dii.aide.mircv.common.bean.Results;
import it.unipi.dii.aide.mircv.common.cache.GuavaCache;
import it.unipi.dii.aide.mircv.common.utils.Flags;
import it.unipi.dii.aide.mircv.common.utils.boundedpq.BoundedPriorityQueue;
import it.unipi.dii.aide.mircv.querymanager.QueryProcess;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.PriorityQueue;
import java.util.Scanner;

public class Evaluator {
    private static final String queriesPathTrain = "PerformanceTest/src/main/resources/queries/queries.dev.tsv";
    private static final String trecEvalResultPath = "PerformanceTest/src/main/resources/results/testResult.txt";
    private static final String fixed = "Q0";
    private static final String runid = "RUN-01";

    private static boolean saveResultsTrecEval(String topicId, BoundedPriorityQueue priorityQueue) {
        PriorityQueue<Results> results_queue = priorityQueue.reverseOrder();
        int i = 1;

        try (BufferedWriter statisticsBuffer = new BufferedWriter(new FileWriter(trecEvalResultPath, true))) {
            String resultsLine;

            while (results_queue.peek() != null) {
                Results results = results_queue.poll();
                resultsLine = topicId + "\t" + fixed + "\t" + results.getDoc_id()
                        + "\t" + i + "\t" + results.getScore() + "\t" + runid + "\n";
                statisticsBuffer.write(resultsLine);
                i++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public static void evaluateQueriesTest() throws IOException {

        Scanner myReader = new Scanner(new File(queriesPathTrain), StandardCharsets.UTF_8);

        System.out.println("Starting query performances evaluation..." + "\n");

        int num_queries = 0;
        long sum_elapsedTime = 0;

        String min_query = "", max_query = "";
        long min_elaps = 0, max_elaps = 0;
        String line;

        while (myReader.hasNextLine()) {

            line = myReader.nextLine();

            if (line.isBlank())
                continue;

            String[] row = line.split("\t");
            String query_id = row[0];
            String query_text = row[1];

            long startTime = System.nanoTime();
            BoundedPriorityQueue results = QueryProcess.submitQuery(query_text);
            System.out.println("Query processed: " + num_queries);
            long elapsedTime = (System.nanoTime() - startTime) / 1000000;

            sum_elapsedTime += elapsedTime;
            num_queries += 1;

            if (elapsedTime < min_elaps || num_queries == 1) {
                min_elaps = elapsedTime;
                min_query = query_text;
            } else if (elapsedTime > max_elaps) {
                max_elaps = elapsedTime;
                max_query = query_text;
            }

            if (Flags.isTrecEval() && results != null) {
                if (!saveResultsTrecEval(query_id, results))
                    System.out.println("Error encountered while writing trec_eval_results");
            }
        }

        System.out.println("STATISTICS FOR QUERIES, IN " + Flags.getQueryMode() + " MODE WITH " + Flags.getScoringFunction() + " AS SCORING FUNCTION: ");
        System.out.println("Average time elapsed: " + sum_elapsedTime / num_queries + " ms");
        System.out.println("Query with min time elapsed: " + min_query + "- " + min_elaps + "ms");
        System.out.println("Query with max time elapsed: " + max_query + "- " + max_elaps + "ms");
        System.out.println("Cache hit rate: " + GuavaCache.getInstance().getStats().hitRate() + ".");
        System.out.println("Eviction count: " + GuavaCache.getInstance().getStats().evictionCount() + ".");

        myReader.close();
    }
}
