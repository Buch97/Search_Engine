package it.unipi.bean;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class RafInvertedIndex {
    //private static MappedByteBuffer index_doc_id;
    //private static MappedByteBuffer index_term_freq;

    public static FileChannel fileChannel_doc_id;
    public static FileChannel fileChannel_term_freq;

    public RafInvertedIndex(String inv_index_docid, String inv_index_term_freq) throws IOException {

        RandomAccessFile raf_docid = new RandomAccessFile(inv_index_docid, "rw");
        fileChannel_doc_id = raf_docid.getChannel();

        RandomAccessFile raf_term_freq = new RandomAccessFile(inv_index_term_freq, "rw");
        fileChannel_term_freq = raf_term_freq.getChannel();

        // index_doc_id = fileChannel_doc_id.map(FileChannel.MapMode.READ_WRITE, 0, fileChannel_doc_id.size());
        // index_term_freq = fileChannel_term_freq.map(FileChannel.MapMode.READ_WRITE, 0, fileChannel_term_freq.size());
    }

}
