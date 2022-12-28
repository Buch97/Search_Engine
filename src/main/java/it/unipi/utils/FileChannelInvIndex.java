package it.unipi.utils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class FileChannelInvIndex {

    private static final Path doc_id_path = Path.of("src/main/resources/output/inverted_index_doc_id_bin.dat");
    private static final Path term_freq_path = Path.of("src/main/resources/output/inverted_index_term_frequency_bin.dat");
    public static FileChannel fileChannel_doc_id;
    public static FileChannel fileChannel_term_freq;
    public static MappedByteBuffer MappedDocIdBuffer;
    public static MappedByteBuffer MappedTermFreqBuffer;

    private FileChannelInvIndex() {
    }

    public static void MapFileChannel() throws IOException {
        MappedTermFreqBuffer = fileChannel_term_freq.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel_term_freq.size());
        MappedDocIdBuffer = fileChannel_doc_id.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel_doc_id.size());
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

    public static void unmapBuffer() {
        MappedDocIdBuffer = null;
        MappedTermFreqBuffer = null;
        System.gc();
    }

    public static void readMappedFile(byte[] doc_id_buffer, byte[] term_freq_buffer, long offset_doc_id_start, long offset_term_freq_start) throws IOException {
        FileChannelInvIndex.MappedTermFreqBuffer.get((int) offset_term_freq_start, term_freq_buffer);
        FileChannelInvIndex.MappedDocIdBuffer.get((int) offset_doc_id_start, doc_id_buffer);
    }

    public static void read(ByteBuffer doc_id_buffer, ByteBuffer term_freq_buffer, long offset_doc_id_start, long offset_term_freq_start) throws IOException {
        FileChannelInvIndex.fileChannel_doc_id.read(doc_id_buffer, (int) offset_doc_id_start);
        FileChannelInvIndex.fileChannel_term_freq.read(term_freq_buffer, (int) offset_term_freq_start);
    }

    public static void write(byte[] doc_id_compressed, byte[] term_freq_compressed) throws IOException {
        FileChannelInvIndex.fileChannel_doc_id.write(ByteBuffer.wrap(doc_id_compressed));
        FileChannelInvIndex.fileChannel_term_freq.write(ByteBuffer.wrap(term_freq_compressed));
    }
}
