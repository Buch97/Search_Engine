package it.unipi.dii.aide.mircv;

import java.io.File;
import java.io.IOException;

import static it.unipi.dii.aide.mircv.algorithms.Spimi.buildDataStructures;

public class BuildStructuresMain {
    private static final String doc_id_path = "resources/output/inverted_index_doc_id_bin.dat";
    private static final String term_freq_path = "resources/output/inverted_index_term_frequency_bin.dat";

    public static void main(String[] args) throws IOException {

        File theDir = new File("resources/output");

        if (!theDir.exists()) {
            if (theDir.mkdirs())
                System.out.println("New directory 'resources/output' created");
        }

        theDir = new File("resources/stats");
        if (!theDir.exists()) {
            if (theDir.mkdirs())
                System.out.println("New directory 'resources/stats' created");
        }

        theDir = new File("BuildStructures/src/main/resources/blocks");
        if (!theDir.exists()) {
            if (theDir.mkdirs())
                System.out.println("New directory '/blocks' created");
        }

        buildDataStructures();

        System.out.println("Data structures build successfully.");
    }
}
