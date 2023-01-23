package it.unipi.dii.aide.mircv.common.utils;

public class Flags {

    private static boolean stopStem = true;

    private static boolean debug = false;

    private static String queryMode = "d";

    private static String scoringFunction = "tfIdf";

    private static int k = 20;

    public Flags() {
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
}
