package it.unipi.dii.aide.mircv.common.utils;

import it.unipi.dii.aide.mircv.common.compressor.Compression;
import it.unipi.dii.aide.mircv.common.utils.filechannel.FileChannelInvIndex;
import it.unipi.dii.aide.mircv.common.bean.InvertedList;
import it.unipi.dii.aide.mircv.common.bean.Posting;
import it.unipi.dii.aide.mircv.common.bean.TermStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class Utils {

    public static InvertedList retrievePostingLists(String term, TermStats termStats) throws IOException {
        List<Posting> query_posting_list = new ArrayList<>();

        int size_doc_id_list = extractSize(termStats.getOffset_doc_id_start(), termStats.getOffset_doc_id_end());
        int size_term_freq_list = extractSize(termStats.getOffset_term_freq_start(), termStats.getOffset_term_freq_end());

        byte[] doc_id_buffer = new byte[size_doc_id_list];
        byte[] term_freq_buffer = new byte[size_term_freq_list];

        FileChannelInvIndex.readMappedFile(doc_id_buffer, term_freq_buffer, termStats.getOffset_doc_id_start(), termStats.getOffset_term_freq_start());
        Compression compression = new Compression();
        System.out.println("Decompressing " + term);

        for (int i = 0; i < termStats.getDoc_frequency(); i++) {
            int term_freq = compression.decodingUnaryList(BitSet.valueOf(term_freq_buffer));
            int doc_id = compression.decodingVariableByte(doc_id_buffer);
            query_posting_list.add(new Posting(doc_id, term_freq));
        }

        return new InvertedList(term, query_posting_list, 0);
    }



    private static int extractSize(long start, long end) {
        return (int) (end - start);
    }

    public static void printBitSet(BitSet bi, int size) {

        for (int i = 0; i < size; i++) {
            if (bi.get(i))
                System.out.print("1");
            else System.out.print("0");
        }
        System.out.println("\n");
    }
}
