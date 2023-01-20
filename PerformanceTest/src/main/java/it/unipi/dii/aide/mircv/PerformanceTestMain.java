package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.evaluation.Evaluator;
import it.unipi.dii.aide.mircv.querymanager.QueryProcess;

import java.io.IOException;

public class PerformanceTestMain
{
    public static void main( String[] args ) throws IOException {
        QueryProcess.startQueryProcessor();
        Evaluator.evaluateQueriesTest();
        QueryProcess.closeQueryProcessor();
    }
}
