package it.unipi;

import java.util.List;
import java.util.Map;

//• The inverted index is usually stored on disk, and in memory
//• Usually multiple inverted indexes, with a constant number of documents per index
//• In compressed form, even if in memory (never ever fully decompressed) ADVANCED TASK

public class Inverted_Index {
    Map<String, List<Posting>> inverted_index;

    public Inverted_Index(Map<String, List<Posting>> inverted_index) {
        this.inverted_index = inverted_index;
    }

    public Map<String, List<Posting>> getInverted_index() {
        return inverted_index;
    }

    public void setInverted_index(Map<String, List<Posting>> inverted_index) {
        this.inverted_index = inverted_index;
    }
}

