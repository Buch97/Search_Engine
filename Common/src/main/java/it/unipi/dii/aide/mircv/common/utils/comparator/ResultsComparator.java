package it.unipi.dii.aide.mircv.common.utils.comparator;

import it.unipi.dii.aide.mircv.common.bean.Results;

import java.util.Comparator;

public class ResultsComparator implements Comparator<Results> {

    @Override
    public int compare(Results o1, Results o2) {
        return Double.compare(o1.getScore(), o2.getScore());
    }
}
