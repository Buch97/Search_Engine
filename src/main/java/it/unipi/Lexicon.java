package it.unipi;

//• Lexicon (or vocabulary):
//  • Contains the term statistics across the whole collection, such as the document frequency, i.e., the number of documents in which the term
//    appears at least once
//  • Contains a lookup table from the index terms to the byte offset of the inverted lists in the inverted file
//• Contains one element for each distinct term
//• Dictionary, hash table, succinct data structure or disk-based data structure
//• Lookup based on term (the term is key)
//• Stores start of corresponding posting list in index
//• A file offset for a disk-based index or a pointer
//• Also stores length of list, maybe other items
//• Can get large in some cases

public class Lexicon {
    String term;
    int doc_frequency;
    Inverted_Index inverted_index;

    public Lexicon(String term, int doc_frequency, Inverted_Index inverted_index) {
        this.term = term;
        this.doc_frequency = doc_frequency;
        this.inverted_index = inverted_index;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public int getDoc_frequency() {
        return doc_frequency;
    }

    public void setDoc_frequency(int doc_frequency) {
        this.doc_frequency = doc_frequency;
    }

    public Inverted_Index getInverted_index() {
        return inverted_index;
    }

    public void setInverted_index(Inverted_Index inverted_index) {
        this.inverted_index = inverted_index;
    }
}
