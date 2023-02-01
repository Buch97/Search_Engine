package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.common.utils.Flags;
import it.unipi.dii.aide.mircv.querymanager.QueryProcess;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;


public class QueryProcessingMain {

    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length > 0) {
            if (args[0].equals("-c"))
                Flags.setQueryMode("c");
            if (args.length > 1) {
                if (args[1].equals("-bm25"))
                    Flags.setScoringFunction("bm25");
            }
            if (args.length > 2) {
                if (args[2].equals("-10"))
                    Flags.setK(10);
            }

            if (args.length > 3) {
                if (args[3].equals("-maxScore"))
                    Flags.setQueryAlgorithm("maxScore");
            }
        }

        // GuavaCache.preloadCache();
        // System.out.println(GuavaCache.invertedListLoadingCache.asMap());

        QueryProcess.startQueryProcessor();

        for (; ; ) {
            System.out.println("Please, submit your query! Otherwise digit \"!exit\" to stop the execution or \"!mode\" to change query type.");
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));
            String query = reader.readLine();
            if (Objects.equals(query, "!exit")) {
                QueryProcess.closeQueryProcessor();
            } else if (Objects.equals(query, "!mode")) {
                System.out.println("Digit \"0\" for disjunctive mode or \"1\" for conjunctive mode.");
                String input = reader.readLine();
                if (Objects.equals(input, "1"))
                    Flags.setQueryMode("c");
                else if (Objects.equals(input, "0")){
                    Flags.setQueryMode("d");
                } else {
                    System.out.println("Not valid input. Query mode must be 0 or 1.");
                }
            } else if (Objects.equals(query, "") || query.trim().length() == 0) {
                System.out.println("The query is empty.");
            } else
                QueryProcess.submitQuery(query);
        }
    }
}
