package it.unipi.dii.aide.mircv.common.utils;

public class Flags {

    private static boolean stopStem = true;

    private static boolean trecEval = false;

    private static boolean debug = false;

    private static String queryMode = "d";

    private static String scoringFunction = "tfIdf";

    private static int k = 20;

    private static boolean evaluation = false;

    public Flags() {
    }

    public static boolean isTrecEval(){
        return trecEval;
    }

    public static void setTrecEval(boolean bool){
        trecEval = bool;
    }
    public static boolean getTrecEval(){
        return trecEval;
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

    public static boolean isEvaluation() {
        return evaluation;
    }

    public static void setEvaluation(boolean evaluation) {
        Flags.evaluation = evaluation;
    }
}
