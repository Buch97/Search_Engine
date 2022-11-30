package it.unipi.utils;

import it.unipi.bean.TermPositionBlock;

import java.util.Comparator;

public class TermPositionBlockComparator implements Comparator<TermPositionBlock> {

    @Override
    public int compare(TermPositionBlock o1, TermPositionBlock o2) {
        return o1.getTerm().compareTo(o2.getTerm());
    }
}
