package it.unipi.utils;

import it.unipi.bean.TermPositionBlock;

import java.util.Comparator;

public class TermPositionBlockComparator implements Comparator<TermPositionBlock> {

    @Override
    public int compare(TermPositionBlock o1, TermPositionBlock o2) {
        if (o1.getTerm().equals(o2.getTerm())) {
            return Integer.compare(o1.getBlock_index(), o2.getBlock_index());
        }
        else return o1.getTerm().compareTo(o2.getTerm());
    }
}
