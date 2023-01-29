package it.unipi.dii.aide.mircv.common.utils;

import it.unipi.dii.aide.mircv.common.bean.InvertedList;
import it.unipi.dii.aide.mircv.common.bean.TermStats;
import it.unipi.dii.aide.mircv.common.compressor.Compression;
import it.unipi.dii.aide.mircv.common.utils.filechannel.FileChannelInvIndex;

import java.io.File;
import java.io.IOException;

public class Utils {

    public static void createDir(File theDir) {
        if (!theDir.exists()) {
            if (theDir.mkdirs())
                System.out.println("New directory 'resources/output' created");
        }
    }

    public static InvertedList retrievePostingLists(String term, TermStats termStats) throws IOException, InterruptedException {

        int size_doc_id_list = extractSize(termStats.getOffset_doc_id_start(), termStats.getOffset_doc_id_end());
        int size_term_freq_list = extractSize(termStats.getOffset_term_freq_start(), termStats.getOffset_term_freq_end());

        byte[] doc_id_buffer = new byte[size_doc_id_list];
        byte[] term_freq_buffer = new byte[size_term_freq_list];

        FileChannelInvIndex.readMappedFile(doc_id_buffer, term_freq_buffer, termStats.getOffset_doc_id_start(), termStats.getOffset_term_freq_start());
        Compression compression = new Compression(termStats.getDoc_frequency());
        /*if(!Flags.isEvaluation())
            System.out.println("Decompressing " + term);*/

        compression.decodePostingList(doc_id_buffer, term_freq_buffer);
        return new InvertedList(term, compression.getDecodedPostingList(), 0);
    }

    private static int extractSize(long start, long end) {
        return (int) (end - start);
    }

}
