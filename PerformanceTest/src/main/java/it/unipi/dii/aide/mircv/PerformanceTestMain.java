package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.common.utils.Flags;
import it.unipi.dii.aide.mircv.common.utils.Utils;
import it.unipi.dii.aide.mircv.evaluation.Evaluator;
import it.unipi.dii.aide.mircv.querymanager.QueryProcess;

import java.io.File;
import java.io.IOException;

public class PerformanceTestMain
{
    public static void main( String[] args ) throws IOException {

        if(args.length > 0){
            if(args[0].equals("-c"))
                Flags.setQueryMode("c");
        }

        Utils.createDir(new File("PerformanceTest/src/main/resources/queries"));
        Utils.createDir(new File("PerformanceTest/src/main/resources/results"));

        QueryProcess.startQueryProcessor();
        Evaluator.evaluateQueriesTest();
        QueryProcess.closeQueryProcessor();
    }
}
