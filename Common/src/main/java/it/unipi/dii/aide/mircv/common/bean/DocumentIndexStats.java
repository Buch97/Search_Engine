package it.unipi.dii.aide.mircv.common.bean;


import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static it.unipi.dii.aide.mircv.common.inMemory.AuxiliarStructureOnMemory.ENTRY_SIZE_DOCINDEX;


public class DocumentIndexStats {
    String doc_no;                  //pid of a document
    int doc_len;                    //length of a documents in terms of number of terms


    //Constructor for the document index entry of a specific document
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


    //Write the document index entry of a doc_id on disk
    public long writeDocumentIndex(FileChannel channelDocIndex, long positionDocIndex, int doc_id) throws IOException {
        // instantiation of MappedByteBuffer for the entry
        MappedByteBuffer docIndexBuffer = channelDocIndex.map(FileChannel.MapMode.READ_WRITE, positionDocIndex, ENTRY_SIZE_DOCINDEX);

        docIndexBuffer.putInt(doc_id);    // Write the docid into file

        CharBuffer charBuffer = CharBuffer.allocate(64); // Create the CharBuffer with size 64

        //populate char buffer char by char
        for (int i = 0; i < doc_no.length(); i++)
            charBuffer.put(i, doc_no.charAt(i));

        // Write the docno into file
        docIndexBuffer.put(StandardCharsets.UTF_8.encode(charBuffer));

        docIndexBuffer.putInt(doc_len);  // Write the doclen into file

        return positionDocIndex + ENTRY_SIZE_DOCINDEX; // update memory offset
    }

    //Read the doc_len of a document index entry by doc_id as key from disk
    public static int readDocLen(MappedByteBuffer channelDocIndex, int doc_id) {
        long offset = (long) doc_id * ENTRY_SIZE_DOCINDEX + 68;
        return channelDocIndex.getInt((int) offset);
    }

    @Override
    public String toString() {
        return "DocumentIndexStats{" +
                "doc_no='" + doc_no + '\'' +
                ", doc_len=" + doc_len +
                '}';
    }
}
