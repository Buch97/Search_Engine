package it.unipi.dii.aide.mircv.common.utils;

public class Flags {

    private static boolean stopStem = true;     //default remove the stemming phase during the text processing

    private static boolean trecEval = false;    //default the program don't test the system in trecEval

    private static boolean debug = false;       //default goes the program goes in execute mode

    private static String queryMode = "d";      //default the program process the disjunctive query

    private static String scoringFunction = "tfIdf"; //default compute the tf-idf score

    private static int k = 20;                      //default retrieve the 20 top document from the query

    private static boolean evaluation = false;      //default the program don't test the system

    private static String queryAlgorithm = "daat";  //default process the query using daat algorithm

    public Flags() {
    }

    public static boolean isTrecEval() {
        return trecEval;
    }

    public static boolean getTrecEval() {
        return trecEval;
    }

    public static void setTrecEval(boolean bool) {
        trecEval = bool;
    }

    public static boolean isStopStem() {
        return stopStem;
    }

    public static void setStopStem(boolean stopStem) {
        Flags.stopStem = stopStem;
    }

    public static boolean isDebug() {
        return debug;
    }

    public static void setDebug(boolean debug) {
        Flags.debug = debug;
    }

    public static String getQueryMode() {
        return queryMode;
    }

    public static void setQueryMode(String queryMode) {
        Flags.queryMode = queryMode;
    }

    public static String getScoringFunction() {
        return scoringFunction;
    }

    public static void setScoringFunction(String scoringFunction) {
        Flags.scoringFunction = scoringFunction;
    }

    public static int getK() {
        return k;
    }

    public static void setK(int k) {
        Flags.k = k;
    }

    public static String getQueryAlgorithm() {
        return queryAlgorithm;
    }

    public static void setQueryAlgorithm(String queryAlgorithm) {
        Flags.queryAlgorithm = queryAlgorithm;
    }

    public static boolean isEvaluation() {
        return evaluation;
    }

    public static void setEvaluation(boolean evaluation) {
        Flags.evaluation = evaluation;
    }
}
