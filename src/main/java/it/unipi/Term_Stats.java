package it.unipi;

import java.util.ArrayList;
import java.util.List;

public class Term_Stats {
    int document_frequency;
    ArrayList<Posting> postingList;

    public Term_Stats(int document_frequency, ArrayList<Posting> postingList) {
        this.document_frequency = document_frequency;
        this.postingList = postingList;
    }

    public int getDocument_frequency() {
        return document_frequency;
    }

    public void setDocument_frequency(int document_frequency) {
        this.document_frequency = document_frequency;
    }

    public ArrayList<Posting> getPostingList() {
        return postingList;
    }

    public void setPostingList(ArrayList<Posting> postingList) {
        this.postingList = postingList;
    }

}
