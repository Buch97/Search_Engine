package it.unipi.bean;

import java.util.ArrayList;

public class TermPositionBlock {

    String term;
    int block_index;
    ArrayList<Posting> postingArrayList;

    public TermPositionBlock(String term, ArrayList<Posting> postingArrayList, int block_index){
        this.term = term;
        this.postingArrayList = new ArrayList<>(postingArrayList);
        this.block_index = block_index;
    }

    public String getTerm() {
        return term;
    }

    public int getBlock_index() {
        return block_index;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void setBlock_index(int block_index) {
        this.block_index = block_index;
    }

    public ArrayList<Posting> getPostingArrayList() {
        return postingArrayList;
    }

    public void setPostingArrayList(ArrayList<Posting> postingArrayList) {
        this.postingArrayList = postingArrayList;
    }

    @Override
    public String toString() {
        return "TermPositionBlock{" +
                "term='" + term + '\'' +
                ", block_index=" + block_index +
                ", postingArrayList=" + postingArrayList +
                '}';
    }
}
