package it.unipi.dii.aide.mircv.common.bean;

import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static it.unipi.dii.aide.mircv.common.inMemory.AuxiliarStructureOnMemory.ENTRY_SIZE_LEXICON;


// Entry of the lexicon for a term
public class TermStats {
    String term;                //Term to which refers the vocabulary entry
    int doc_frequency;          //Document frequency of the term
    int coll_frequency;         //Collection frequency of the term
    long offset_doc_id_start;   //Starting point of the term's posting list in the inverted index in bytes
    long offset_term_freq_start; //Starting point of the frequencies in the inverted index in bytes
    long offset_doc_id_end;
    long offset_term_freq_end;
    long actual_offset;
    float termUpperBound;       //maximum value of TFIDF for the term


    //Constructor for the lexicon entry of a term on disk
    public TermStats(String term, int doc_frequency, int coll_frequency, long offset_doc_id_start, long offset_term_freq_start, long offset_doc_id_end, long offset_term_freq_end,float maxTermFrequency) {
        this.term = term;
        this.doc_frequency = doc_frequency;
        this.coll_frequency = coll_frequency;
        this.offset_doc_id_start = offset_doc_id_start;
        this.offset_term_freq_start = offset_term_freq_start;
        this.offset_doc_id_end = offset_doc_id_end;
        this.offset_term_freq_end = offset_term_freq_end;
        this.termUpperBound = (float) (1+Math.log(maxTermFrequency)) * (float) Math.log((double) CollectionStatistics.num_docs/doc_frequency);

    }

    //Constructor for the lexicon entry values of a term as key on memory
    public TermStats(int doc_frequency, int coll_frequency, long offset_doc_id_start, long offset_term_freq_start, long offset_doc_id_end, long offset_term_freq_end,float termUpperBound) {
        this.doc_frequency = doc_frequency;
        this.coll_frequency = coll_frequency;
        this.offset_doc_id_start = offset_doc_id_start;
        this.offset_term_freq_start = offset_term_freq_start;
        this.offset_doc_id_end = offset_doc_id_end;
        this.offset_term_freq_end = offset_term_freq_end;
        this.termUpperBound = termUpperBound;
    }

    public long getOffset_doc_id_end() {
        return offset_doc_id_end;
    }

    public long getOffset_term_freq_end() {
        return offset_term_freq_end;
    }


    //Write the lexicon entry of a term on disk
    public long writeTermStats(long positionLex, FileChannel channelLexicon) throws IOException {

        //// instantiation of MappedByteBuffer
        MappedByteBuffer bufferLexicon = channelLexicon.map(FileChannel.MapMode.READ_WRITE, positionLex, ENTRY_SIZE_LEXICON);

        CharBuffer charBuffer = CharBuffer.allocate(64);

        //populate char buffer char by char
        for (int i = 0; i < term.length(); i++)
            charBuffer.put(i, term.charAt(i));

        // Write the term into file
        bufferLexicon.put(StandardCharsets.UTF_8.encode(charBuffer));

        bufferLexicon.putInt(doc_frequency);
        bufferLexicon.putInt(coll_frequency);

        bufferLexicon.putLong(offset_doc_id_start);
        bufferLexicon.putLong(offset_term_freq_start);
        bufferLexicon.putLong(offset_doc_id_end);
        bufferLexicon.putLong(offset_term_freq_end);
        bufferLexicon.putFloat(termUpperBound);

        return positionLex + ENTRY_SIZE_LEXICON;    //return position for which we have to start writing on file

    }

    public int getDoc_frequency() {
        return doc_frequency;
    }

    public long getOffset_doc_id_start() {
        return offset_doc_id_start;
    }

    public long getOffset_term_freq_start() {
        return offset_term_freq_start;
    }

    public float getTermUpperBound(){return termUpperBound;}

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

