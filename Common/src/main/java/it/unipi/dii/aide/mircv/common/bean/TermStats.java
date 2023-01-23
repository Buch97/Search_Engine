package it.unipi.dii.aide.mircv.common.bean;



import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static it.unipi.dii.aide.mircv.common.inMemory.AuxiliarStructureOnMemory.ENTRY_SIZE_LEXICON;

public class TermStats{
    String term;
    int doc_frequency;
    int coll_frequency;
    long offset_doc_id_start;
    long offset_term_freq_start;
    long offset_doc_id_end;
    long offset_term_freq_end;
    long actual_offset;


    public long getOffset_doc_id_end() {
        return offset_doc_id_end;
    }

    public void setOffset_doc_id_end(long offset_doc_id_end) {
        this.offset_doc_id_end = offset_doc_id_end;
    }

    public long getOffset_term_freq_end() {
        return offset_term_freq_end;
    }

    public void setOffset_term_freq_end(long offset_term_freq_end) {
        this.offset_term_freq_end = offset_term_freq_end;
    }

    public TermStats(String term, int doc_frequency, int coll_frequency, long offset_doc_id_start, long offset_term_freq_start, long offset_doc_id_end, long offset_term_freq_end) {
        this.term=term;
        this.doc_frequency = doc_frequency;
        this.coll_frequency = coll_frequency;
        this.offset_doc_id_start = offset_doc_id_start;
        this.offset_term_freq_start = offset_term_freq_start;
        this.offset_doc_id_end = offset_doc_id_end;
        this.offset_term_freq_end = offset_term_freq_end;
    }

    public TermStats(int doc_frequency, int coll_frequency, long offset_doc_id_start, long offset_term_freq_start, long offset_doc_id_end, long offset_term_freq_end){
        this.doc_frequency = doc_frequency;
        this.coll_frequency = coll_frequency;
        this.offset_doc_id_start = offset_doc_id_start;
        this.offset_term_freq_start = offset_term_freq_start;
        this.offset_doc_id_end = offset_doc_id_end;
        this.offset_term_freq_end = offset_term_freq_end;
    }

    public long writeTermStats(long positionLex, FileChannel channelLexicon) throws IOException {

        MappedByteBuffer bufferLexicon = channelLexicon.map(FileChannel.MapMode.READ_WRITE, positionLex, ENTRY_SIZE_LEXICON);

        CharBuffer charBuffer = CharBuffer.allocate(64);
        System.out.println(term+" "+term.length());

        //populate char buffer char by char
        for (int i = 0; i < term.length(); i++)
            charBuffer.put(i, term.charAt(i));

        // Write the term into file
        bufferLexicon.put(StandardCharsets.UTF_8.encode(charBuffer));

        System.out.println(doc_frequency);
        bufferLexicon.putInt(doc_frequency);
        bufferLexicon.putInt(coll_frequency);

        bufferLexicon.putLong(offset_doc_id_start);
        bufferLexicon.putLong(offset_term_freq_start);
        bufferLexicon.putLong(offset_doc_id_end);
        bufferLexicon.putLong(offset_term_freq_end);

        return positionLex+ENTRY_SIZE_LEXICON;

    }

    public int getDoc_frequency() {
        return doc_frequency;
    }

    public void setDoc_frequency(int doc_frequency) {
        this.doc_frequency = doc_frequency;
    }

    public int getColl_frequency() {
        return coll_frequency;
    }

    public void setColl_frequency(int coll_frequency) {
        this.coll_frequency = coll_frequency;
    }

    public long getOffset_doc_id_start() {
        return offset_doc_id_start;
    }

    public long getOffset_term_freq_start() {
        return offset_term_freq_start;
    }

    public void setOffset_doc_id_start(long offset_doc_id_start) {
        this.offset_doc_id_start = offset_doc_id_start;
    }

    public void setOffset_term_freq_start(long offset_term_freq_start) {
        this.offset_term_freq_start = offset_term_freq_start;
    }

    @Override
    public String toString() {
        return "TermStats{" +
                "doc_frequency=" + doc_frequency +
                ", coll_frequency=" + coll_frequency +
                ", offset_doc_id_start=" + offset_doc_id_start +
                ", offset_term_freq_start=" + offset_term_freq_start +
                ", offset_doc_id_end=" + offset_doc_id_end +
                ", offset_term_freq_end=" + offset_term_freq_end +
                ", actual_offset=" + actual_offset +
                '}';
    }
}

