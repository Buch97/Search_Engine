package it.unipi.dii.aide.mircv.common.inMemory;

import it.unipi.dii.aide.mircv.common.bean.TermStats;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class AuxiliarStructureOnMemory {
    public static final int ENTRY_SIZE_LEXICON = 108;      //size of a lexicon entry in Lexicon binary file
    public static final int ENTRY_SIZE_DOCINDEX = 72;      //size of a document index entry in Document Index binary file
    public static HashMap<String, TermStats> lexiconMemory = new HashMap<>();  //Entries lexicon loaded in memory with terms as keys

    //Load the Lexicon binary file in memory
    public static int loadLexicon(FileChannel channelLexicon) throws IOException {
        long offset = 0;
        MappedByteBuffer buffer;
        while (offset < channelLexicon.size()) {

            // instantiation of MappedByteBuffer for the entry
            buffer = channelLexicon.map(FileChannel.MapMode.READ_ONLY, offset, ENTRY_SIZE_LEXICON);

            // Buffer not created
            if (buffer == null)
                return -1;

            // Read from file into the charBuffer, then pass to the string
            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

            String[] encodedTerm = charBuffer.toString().split("\0");
            if (encodedTerm.length == 0)
                return 0;

            String term = encodedTerm[0];

            // Instantiate the buffer for reading other information
            buffer = channelLexicon.map(FileChannel.MapMode.READ_WRITE, offset + 64, ENTRY_SIZE_LEXICON - 64);

            int doc_freq = buffer.getInt();
            int coll_freq = buffer.getInt();
            long offset_doc_id_start = buffer.getLong();
            long offset_term_freq_start = buffer.getLong();
            long offset_doc_id_end = buffer.getLong();
            long offset_term_freq_end = buffer.getLong();
            float termUpperBound = buffer.getFloat();

            //
            lexiconMemory.put(term, new TermStats(doc_freq, coll_freq, offset_doc_id_start, offset_term_freq_start, offset_doc_id_end, offset_term_freq_end,termUpperBound));

            offset += ENTRY_SIZE_LEXICON;           //update the offset to the next entry

        }
        return 0;
    }

}
