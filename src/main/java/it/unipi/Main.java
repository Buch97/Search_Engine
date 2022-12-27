package it.unipi;

import it.unipi.builddatastructures.IndexConstruction;
import it.unipi.utils.CollectionStatistics;
import it.unipi.utils.FileChannelInvIndex;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;

import static it.unipi.querymanager.QueryProcess.submitQuery;

public class Main {
    private static final String doc_id_path = "src/main/resources/output/inverted_index_doc_id_bin.dat";
    private static final String term_freq_path = "src/main/resources/output/inverted_index_term_frequency_bin.dat";
    public static int k = 20;
    public static DB db_document_index;
    private static final String mode = "READ";
    public static DB db_lexicon;

    public static void main(String[] args) throws IOException {
        //semplice roba di utility per creare le directory in cui ci vanno salvati i files
        CollectionStatistics.computeNumDocs();
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

        if (!(new File(doc_id_path).exists()) || !(new File(term_freq_path).exists()))
            IndexConstruction.buildDataStructures();

        db_lexicon = DBMaker.fileDB("./src/main/resources/output/lexicon_disk_based.db")
                .fileMmapEnable()
                .fileMmapPreclearDisable()
                .closeOnJvmShutdown()
                .readOnly()
                .make();
        db_document_index = DBMaker.fileDB("./src/main/resources/output/document_index.db")
                .fileMmapEnable()
                .fileMmapPreclearDisable()
                .closeOnJvmShutdown()
                .readOnly()
                .make();

        CollectionStatistics.computeAvgDocLen(db_document_index);

        FileChannelInvIndex.openFileChannels(mode);
        FileChannelInvIndex.MapFileChannel();

        for (;;) {
            System.out.println("Please, submit your query! Otherwise digit \"!exit\" to stop the execution.");
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));
            String query = reader.readLine();
            if (Objects.equals(query, "!exit")) {
                db_lexicon.close();
                db_document_index.close();
                FileChannelInvIndex.unmapBuffer();
                FileChannelInvIndex.closeFileChannels();
                System.exit(0);
            }
            else if (Objects.equals(query, "") || query.trim().length() == 0) {
                System.out.println("The query is empty.");
            }
            else submitQuery(reader, query);
        }
    }
}
