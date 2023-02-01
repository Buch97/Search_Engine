package it.unipi.dii.aide.mircv.common.inMemory;

import it.unipi.dii.aide.mircv.common.bean.DocumentIndexStats;
import it.unipi.dii.aide.mircv.common.bean.TermStats;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class AuxiliarStructureOnMemory {
    public static final int ENTRY_SIZE_LEXICON = 108;
    public static final int ENTRY_SIZE_DOCINDEX = 72;
    public static HashMap<String, TermStats> lexiconMemory = new HashMap<>();
    public static HashMap<Integer, DocumentIndexStats> documentIndexMemory = new HashMap<>();

    public static int loadLexicon(FileChannel channelLexicon) throws IOException {
        long offset = 0;
        MappedByteBuffer buffer;
        while (offset < channelLexicon.size()) {

            buffer = channelLexicon.map(FileChannel.MapMode.READ_ONLY, offset, ENTRY_SIZE_LEXICON);

            // Buffer not created
            if (buffer == null)
                return -1;

            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

            String[] encodedTerm = charBuffer.toString().split("\0");
            if (encodedTerm.length == 0)
                return 0;

            String term = encodedTerm[0];

            buffer = channelLexicon.map(FileChannel.MapMode.READ_WRITE, offset + 64, ENTRY_SIZE_LEXICON - 64);

            int doc_freq = buffer.getInt();
            int coll_freq = buffer.getInt();
            long offset_doc_id_start = buffer.getLong();
            long offset_term_freq_start = buffer.getLong();
            long offset_doc_id_end = buffer.getLong();
            long offset_term_freq_end = buffer.getLong();
            float termUpperBound = buffer.getFloat();

            lexiconMemory.put(term, new TermStats(doc_freq, coll_freq, offset_doc_id_start, offset_term_freq_start, offset_doc_id_end, offset_term_freq_end,termUpperBound));

            offset += ENTRY_SIZE_LEXICON;

        }
        return 0;
    }

    public static void loadDocumentIndex(FileChannel channelDocIndex) throws IOException {
        long offset = 0;
        MappedByteBuffer buffer;

        while (offset < channelDocIndex.size()) {

            buffer = channelDocIndex.map(FileChannel.MapMode.READ_ONLY, offset, ENTRY_SIZE_DOCINDEX);

            // Buffer not created
            if (buffer == null)
                return;

            int doc_id = buffer.getInt();
            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

            String[] encodedTerm = charBuffer.toString().split("\0");
            if (encodedTerm.length == 0)
                return;

            String doc_no = encodedTerm[0];
            buffer = channelDocIndex.map(FileChannel.MapMode.READ_WRITE, offset + 68, ENTRY_SIZE_DOCINDEX - 68);

            int doc_len = buffer.getInt();
            documentIndexMemory.put(doc_id, new DocumentIndexStats(doc_no, doc_len));
            offset += ENTRY_SIZE_DOCINDEX;
        }
    }
}
