package it.unipi.utils;

import it.unipi.bean.InvertedList;

import java.util.Comparator;

public class InvertedListComparator implements Comparator<InvertedList> {

    @Override
    public int compare(InvertedList o1, InvertedList o2) {
        if (o1.getTerm().equals(o2.getTerm())) {
            return Integer.compare(o1.getPos(), o2.getPos());
        }
        else return o1.getTerm().compareTo(o2.getTerm());
    }
}
