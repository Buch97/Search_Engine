package it.unipi.bean;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileChannelInvIndex {
    //private static MappedByteBuffer index_doc_id;
    //private static MappedByteBuffer index_term_freq;

    public static FileChannel fileChannel_doc_id;
    public static FileChannel fileChannel_term_freq;

    public FileChannelInvIndex(String inv_index_docid, String inv_index_term_freq, String mode) throws IOException {

        File doc_id_bin = new File(inv_index_docid);
        doc_id_bin.createNewFile();
        Path path_doc_id = Paths.get(inv_index_docid);

        File term_freq_bin = new File(inv_index_term_freq);
        term_freq_bin.createNewFile();
        Path path_term_freq = Paths.get(inv_index_term_freq);

        if (mode == "APPEND"){
            fileChannel_doc_id = FileChannel.open(path_doc_id, StandardOpenOption.APPEND);
            fileChannel_term_freq = FileChannel.open(path_term_freq, StandardOpenOption.APPEND);
        }

        if (mode == "READ"){
            fileChannel_doc_id = FileChannel.open(path_doc_id, StandardOpenOption.READ);
            fileChannel_term_freq = FileChannel.open(path_term_freq, StandardOpenOption.READ);
        }

        //RandomAccessFile raf_term_freq = new RandomAccessFile(inv_index_term_freq, "rw");
        //fileChannel_term_freq = raf_term_freq.getChannel();

        // index_doc_id = fileChannel_doc_id.map(FileChannel.MapMode.READ_WRITE, 0, fileChannel_doc_id.size());
        // index_term_freq = fileChannel_term_freq.map(FileChannel.MapMode.READ_WRITE, 0, fileChannel_term_freq.size());
    }
}
