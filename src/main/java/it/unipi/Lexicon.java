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

import java.util.Map;

public class Lexicon {
    Map<String, Term_Stats> lexicon;

    public Lexicon(Map<String, Term_Stats> lexicon) {
        this.lexicon = lexicon;
    }

    public Map<String, Term_Stats> getLexicon() {
        return lexicon;
    }

    public void setLexicon(Map<String, Term_Stats> lexicon) {
        this.lexicon = lexicon;
    }

    public void print(){
        for (String key: lexicon.keySet()) {
            int freq = lexicon.get(key).getDocument_frequency();
            //Inverted_Index inv_ind = lexicon.get(key).getInverted_index();
            System.out.println("TERM: " + key + "   DOC_FREQUENCY: " + freq);
        }
    }
}