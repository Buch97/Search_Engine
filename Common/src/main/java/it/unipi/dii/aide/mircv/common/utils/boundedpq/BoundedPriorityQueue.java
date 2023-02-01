package it.unipi.dii.aide.mircv.common.utils.boundedpq;

import it.unipi.dii.aide.mircv.common.bean.Results;

import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;

public class BoundedPriorityQueue {

    Comparator<Results> comparator;
    int maxSize;
    PriorityQueue<Results> results;

    public BoundedPriorityQueue(Comparator<Results> comparator, int maxSize) {
        this.comparator = comparator;
        this.maxSize = maxSize;
        this.results = new PriorityQueue<>(maxSize, comparator);
    }

    public void add(int current_doc_id, double score) {
        //Check if the last element in the priorityQueue is lower then the element that i want to insert
        if(results.size() == maxSize){
            assert results.peek() != null;
            if(results.peek().getScore() < score){
                results.poll();
                results.add(new Results(current_doc_id, score));
            }
        }
        else{
            results.add(new Results(current_doc_id, score));
        }
    }

    public int getMaxSize(){
        return maxSize;
    }

    public PriorityQueue<Results> getResults(){
        return results;
    }

    public void printRankedResults() {
        System.out.println("Results: ");
        PriorityQueue<Results> reverseResults = new PriorityQueue<>(maxSize, Collections.reverseOrder(comparator));
        reverseResults.addAll(results);

        try {
            for (int i = 0; i < maxSize; i++) {
                Results result = reverseResults.poll();
                assert result != null;
                System.out.println((i + 1) + ". " + "DOC ID: " + result.getDoc_id() + " SCORE: " + result.getScore());
                if (reverseResults.size() == 0)
                    break;
            }
        } catch (NullPointerException e) {
            System.out.println("No results found for this query");
        }
    }
}
