package it.unipi.bean;

import java.util.ArrayList;
import java.util.List;

public class InvertedList {

    String term;
    int pos;
    List<Posting> postingArrayList;

    public InvertedList(String term, List<Posting> postingArrayList, int pos){
        this.term = term;
        this.postingArrayList = new ArrayList<>(postingArrayList);
        this.pos = pos;
    }

    public String getTerm() {
        return term;
    }

    public int getPos() {
        return pos;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public List<Posting> getPostingArrayList() {
        return postingArrayList;
    }

    public void setPostingArrayList(ArrayList<Posting> postingArrayList) {
        this.postingArrayList = postingArrayList;
    }

    @Override
    public String toString() {
        return "TermPositionBlock{" +
                "term='" + term + '\'' +
                ", pos=" + pos +
                ", postingArrayList=" + postingArrayList +
                '}';
    }
}
