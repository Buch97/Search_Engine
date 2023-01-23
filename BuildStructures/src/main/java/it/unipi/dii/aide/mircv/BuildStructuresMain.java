package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.algorithms.CreateBlocks;
import it.unipi.dii.aide.mircv.common.utils.Flags;
import it.unipi.dii.aide.mircv.common.utils.Utils;

import java.io.File;

public class BuildStructuresMain {

    public static void main(String[] args) {

        if (args.length > 0) {
            if (args[0].equals("-debug"))
                Flags.setDebug(true);
            if (args.length > 1) {
                if (args[1].equals("-noss"))
                    Flags.setStopStem(false);
            }
        }

        Utils.createDir(new File("resources/output"));
        Utils.createDir(new File("resources/stats"));
        Utils.createDir(new File("BuildStructures/src/main/resources/blocks"));

        CreateBlocks.buildDataStructures();

        System.out.println("Data structures build successfully.");
    }


}
