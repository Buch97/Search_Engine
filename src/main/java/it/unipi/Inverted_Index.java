package it.unipi;

import java.util.List;

//• The inverted index is usually stored on disk, and in memory
//• Usually multiple inverted indexes, with a constant number of documents per index
//• In compressed form, even if in memory (never ever fully decompressed) ADVANCED TASK

public class Inverted_Index {
    List<Posting> posting_list;

    public Inverted_Index(List<Posting> posting_list) {
        this.posting_list = posting_list;
    }

    public List<Posting> getPosting_list() {
        return posting_list;
    }

    public void setPosting_list(List<Posting> posting_list) {
        this.posting_list = posting_list;
    }
}
