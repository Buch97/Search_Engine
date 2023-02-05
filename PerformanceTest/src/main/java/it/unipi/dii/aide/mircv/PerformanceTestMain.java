package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.common.utils.Flags;
import it.unipi.dii.aide.mircv.common.utils.Utils;
import it.unipi.dii.aide.mircv.evaluation.Evaluator;
import it.unipi.dii.aide.mircv.querymanager.QueryProcess;

import java.io.File;
import java.io.IOException;

public class PerformanceTestMain
{
    public static void main( String[] args ) throws IOException, InterruptedException {

        // Command line flag parsing
        if(args.length > 0){
            if(args[0].equals("-c"))
                Flags.setQueryMode("c");
            else if(args[0].equals("-d"))
                Flags.setQueryMode("d");
            if (args.length > 1) {
                if (args[1].equals("-bm25"))
                    Flags.setScoringFunction("bm25");
                else if (args[1].equals("-tdidf"))
                    Flags.setScoringFunction("tfidf");
            }
            if (args.length > 2){
                if (args[2].equals("-maxScore")) {
                    Flags.setQueryAlgorithm("maxScore");
                }
            }
            if (args.length > 3){
                if (args[3].equals("-treceval")) {
                    Flags.setTrecEval(true);
                    Flags.setK(100);
                }
            }
        }

        System.out.println("Mode selected: " + Flags.getQueryMode() + ".");
        System.out.println("Scoring function: " + Flags.getScoringFunction() + ".");
        System.out.println("Algorithm used: " + Flags.getQueryAlgorithm());
        System.out.println("TREC EVAL: " + Flags.getTrecEval() + ".");
        System.out.println("Test on " + Flags.getK() + " results.");

        // Missing directories creation
        Utils.createDir(new File("PerformanceTest/src/main/resources/queries"));
        Utils.createDir(new File("PerformanceTest/src/main/resources/results"));

        Flags.setEvaluation(true);
        QueryProcess.startQueryProcessor();
        Evaluator.evaluateQueriesTest();
        QueryProcess.closeQueryProcessor();
        Flags.setEvaluation(false);
    }
}
