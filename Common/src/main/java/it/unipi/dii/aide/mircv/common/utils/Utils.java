package it.unipi.dii.aide.mircv.common.utils;

import it.unipi.dii.aide.mircv.common.bean.InvertedList;
import it.unipi.dii.aide.mircv.common.bean.Posting;
import it.unipi.dii.aide.mircv.common.bean.TermStats;
import it.unipi.dii.aide.mircv.common.compressor.Compression;
import it.unipi.dii.aide.mircv.common.utils.filechannel.FileChannelInvIndex;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        compression.decodePostingList(doc_id_buffer, term_freq_buffer);
        return new InvertedList(term, compression.getDecodedPostingList(), 0);
    }

    public static Posting getIndexPostingbyId(InvertedList invertedList, int doc_id) {
        List<Posting> postingList = invertedList.getPostingArrayList();
        int postingListSize = postingList.size();

        for (int i = invertedList.getPos(); i < postingListSize; i++) {
            if (postingList.get(i).getDoc_id() >= doc_id) {
                return postingList.get(i);
            }
        }
        return null;
    }

    public static int getIndex(InvertedList invertedList, int doc_id) {
        List<Posting> postingList = invertedList.getPostingArrayList();
        int postingListSize = postingList.size();

        for (int i = invertedList.getPos(); i < postingListSize; i++) {
            if (postingList.get(i).getDoc_id() == doc_id) {
                return i;
            }
        }
        return 0;
    }

    private static int extractSize(long start, long end) {
        return (int) (end - start);
    }

}
