package it.unipi.bean;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class FileChannelInvIndex {

    private static final Path doc_id_path = Path.of("src/main/resources/output/inverted_index_doc_id_bin.dat");
    private static final Path term_freq_path = Path.of("src/main/resources/output/inverted_index_term_frequency_bin.dat");
    public static FileChannel fileChannel_doc_id;
    public static FileChannel fileChannel_term_freq;

    private FileChannelInvIndex() {
    }

    public static void openFileChannels(String access_mode) throws IOException {

        File doc_id_file = new File(String.valueOf(doc_id_path));
        File term_freq_file = new File(String.valueOf(term_freq_path));

        if (!doc_id_file.exists()) {
            if (doc_id_file.createNewFile())
                System.out.println("FILE " + doc_id_path + " CREATED");
        }
        if (!term_freq_file.exists()) {
            if (term_freq_file.createNewFile())
                System.out.println("FILE " + term_freq_path + " CREATED");
        }

        if (Objects.equals(access_mode, "APPEND")) {
            fileChannel_doc_id = FileChannel.open(doc_id_path, StandardOpenOption.APPEND);
            fileChannel_term_freq = FileChannel.open(term_freq_path, StandardOpenOption.APPEND);
        }

        if (Objects.equals(access_mode, "READ")) {
            fileChannel_doc_id = FileChannel.open(doc_id_path, StandardOpenOption.READ);
            fileChannel_term_freq = FileChannel.open(term_freq_path, StandardOpenOption.READ);
        }

    }

    public static void closeFileChannels() throws IOException {
        fileChannel_doc_id.close();
        fileChannel_term_freq.close();
    }
}
