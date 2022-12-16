package it.unipi.utils;

import it.unipi.bean.InvertedList;
import it.unipi.bean.Results;

import java.util.Comparator;

public class ResultsComparator implements Comparator<Results> {

    @Override
    public int compare(Results o1, Results o2) {
        return Double.compare(o2.getScore(), o1.getScore());
    }
}
