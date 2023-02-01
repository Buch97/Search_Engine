package it.unipi.dii.aide.mircv.common.bean;


import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static it.unipi.dii.aide.mircv.common.inMemory.AuxiliarStructureOnMemory.ENTRY_SIZE_DOCINDEX;

public class DocumentIndexStats {
    String doc_no;
    int doc_len;

    public DocumentIndexStats(String doc_no, int doc_len) {
        this.doc_no = doc_no;
        this.doc_len = doc_len;
    }

    public String getDoc_no() {
        return doc_no;
    }

    public int getDoc_len() {
        return doc_len;
    }

    public long writeDocumentIndex(FileChannel channelDocIndex, long positionDocIndex, int doc_id) throws IOException {
        MappedByteBuffer docIndexBuffer = channelDocIndex.map(FileChannel.MapMode.READ_WRITE, positionDocIndex, ENTRY_SIZE_DOCINDEX);

        docIndexBuffer.putInt(doc_id);

        CharBuffer charBuffer = CharBuffer.allocate(64);

        //populate char buffer char by char
        for (int i = 0; i < doc_no.length(); i++)
            charBuffer.put(i, doc_no.charAt(i));

        // Write the term into file
        docIndexBuffer.put(StandardCharsets.UTF_8.encode(charBuffer));

        docIndexBuffer.putInt(doc_len);

        return positionDocIndex + ENTRY_SIZE_DOCINDEX;
    }

    @Override
    public String toString() {
        return "DocumentIndexStats{" +
                "doc_no='" + doc_no + '\'' +
                ", doc_len=" + doc_len +
                '}';
    }
}
