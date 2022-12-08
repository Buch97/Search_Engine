package it.unipi.bean;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RafInvertedIndex {
    private static MappedByteBuffer index_doc_id;
    private static MappedByteBuffer index_term_freq;

    public RafInvertedIndex(String inv_index_docid, String inv_index_term_freq) throws IOException {
        RandomAccessFile raf_docid = new RandomAccessFile(inv_index_docid, "rw");
        FileChannel fileChannel_doc_id = raf_docid.getChannel();
        RandomAccessFile raf_term_freq = new RandomAccessFile(inv_index_term_freq, "rw");
        FileChannel fileChannel_term_freq = raf_term_freq.getChannel();

        index_doc_id = fileChannel_doc_id.map(FileChannel.MapMode.READ_WRITE, 0, fileChannel_doc_id.size());
        index_term_freq = fileChannel_term_freq.map(FileChannel.MapMode.READ_WRITE, 0, fileChannel_term_freq.size());
    }

    public static MappedByteBuffer getIndex_doc_id() {
        return index_doc_id;
    }

    public static MappedByteBuffer getIndex_term_freq() {
        return index_term_freq;
    }

    public static void setIndex_doc_id(MappedByteBuffer index_doc_id) {
        RafInvertedIndex.index_doc_id = index_doc_id;
    }

    public static void setIndex_term_freq(MappedByteBuffer index_term_freq) {
        RafInvertedIndex.index_term_freq = index_term_freq;
    }
}
