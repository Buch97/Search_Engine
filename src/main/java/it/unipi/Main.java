package it.unipi;

import it.unipi.builddatastructures.IndexConstruction;
import it.unipi.querymanager.QueryProcess;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;

public class Main {
    private static final String doc_id_path = "src/main/resources/output/inverted_index_doc_id_bin.dat";
    private static final String term_freq_path = "src/main/resources/output/inverted_index_term_frequency_bin.dat";
    public static int k = 20;
    public static int num_docs;
    public static int num_terms;
    public static DB db_document_index;
    public static DB db_lexicon;

    public static void main(String[] args) throws IOException {
        //semplice roba di utility per creare le directory in cui ci vanno salvati i files
        File theDir = new File("./src/main/resources/output");

        if (!theDir.exists()) {
            if (theDir.mkdirs())
                System.out.println("New directory '/output' created");
        }

        theDir = new File("./src/main/resources/intermediate_postings");
        if (!theDir.exists()) {
            if (theDir.mkdirs())
                System.out.println("New directory '/intermediate_postings' created");
        }

        //if (!(new File(doc_id_path).exists()) || !(new File(term_freq_path).exists()))
        IndexConstruction.buildDataStructures();

        db_lexicon = DBMaker.fileDB("./src/main/resources/output/lexicon_disk_based.db").closeOnJvmShutdown().make();
        db_document_index = DBMaker.fileDB("./src/main/resources/output/document_index.db").closeOnJvmShutdown().make();

        for (; ; ) {

            System.out.println("Please, submit your query! Otherwise digit \"!exit\" to stop the execution.");
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));
            String query = reader.readLine();
            if (Objects.equals(query, "!exit")) {
                db_lexicon.close();
                db_document_index.close();
                System.exit(0);
            }
            System.out.println("Your request: " + query);
            QueryProcess.parseQuery(query, k, db_lexicon, db_document_index);
        }
    }
}
