package it.unipi;

public class Term_Stats {
    int document_frequency;
    Inverted_Index inverted_index;

    public Term_Stats(int document_frequency, Inverted_Index inverted_index) {
        this.document_frequency = document_frequency;
        this.inverted_index = inverted_index;
    }

    public int getDocument_frequency() {
        return document_frequency;
    }

    public void setDocument_frequency(int document_frequency) {
        this.document_frequency = document_frequency;
    }

    public Term_Stats update_frequency(int document_frequency) {
        this.document_frequency = document_frequency + 1;
        return this;
    }

    public Inverted_Index getInverted_index() {
        return inverted_index;
    }

    public void setInverted_index(Inverted_Index inverted_index) {
        this.inverted_index = inverted_index;
    }
}
