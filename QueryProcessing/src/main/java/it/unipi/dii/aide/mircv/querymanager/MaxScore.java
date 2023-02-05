package it.unipi.dii.aide.mircv.querymanager;

import it.unipi.dii.aide.mircv.common.bean.DocumentIndexStats;
import it.unipi.dii.aide.mircv.common.bean.InvertedList;
import it.unipi.dii.aide.mircv.common.bean.Posting;
import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;
import it.unipi.dii.aide.mircv.common.utils.Flags;
import it.unipi.dii.aide.mircv.common.utils.boundedpq.BoundedPriorityQueue;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static it.unipi.dii.aide.mircv.common.inMemory.AuxiliarStructureOnMemory.lexiconMemory;
import static it.unipi.dii.aide.mircv.common.utils.Utils.getIndex;
import static it.unipi.dii.aide.mircv.common.utils.Utils.getIndexPostingbyId;
import static it.unipi.dii.aide.mircv.querymanager.QueryProcess.docIndexBuffer;

//Implements the max score algorithm
public class MaxScore {

    //Process using MaxScore algorithm a list of posting list of the query terms
    public static void maxScore(String[] queryTerms, ArrayList<InvertedList> L, BoundedPriorityQueue results, int k, String mode) throws IOException {
        HashMap<String, Float> termUpperBounds = new HashMap<>();
        double threshold = -1;          //Current threshold to enter the Priority queue of the results
        boolean conjunctive = false;

        for (String term : queryTerms) {
            if (lexiconMemory.get(term) != null)
                termUpperBounds.put(term, lexiconMemory.get(term).getTermUpperBound());
        }

        //Sort by increasing term upper bound posting lists to be scored
        L = (ArrayList<InvertedList>) L.stream().sorted(Comparator.comparingDouble(e -> termUpperBounds.get(e.getTerm())))
                .collect(Collectors.toList());


        HashMap<String, Integer> doc_freqs = new HashMap<>();

        for (InvertedList invertedList : L) {
            doc_freqs.put(invertedList.getTerm(), lexiconMemory.get(invertedList.getTerm()).getDoc_frequency());
        }

        if (Objects.equals(mode, "c"))
            conjunctive = true;


        int essentialIndex = -1;         //divide posting lists to be scored in essential and non-essential posting lists
        double documentUpperBound;
        boolean thresholdupdate = true;  //check if we must update the division in essential and non-essential posting lists

        while (true) {
            double nonEssentialTermUpperBound = 0;

            if (thresholdupdate) {
                essentialIndex = retrieveEssentialIndex(termUpperBounds, L, threshold);

                if (essentialIndex == -1)
                    break;
            }

            // search for minimum docid to be scored among essential posting lists
            int current_doc_id = min_doc_id(L, essentialIndex, conjunctive);

            if (current_doc_id == -1)
                break;

            if (conjunctive) {
                current_doc_id = nextGeqPostingLists(L, current_doc_id,essentialIndex);
                if (current_doc_id == -1)
                    break;
            }

            // process DAAT the essential posting lists for docToProcess
            double partialScore = computeEssentialList(L, essentialIndex, current_doc_id, doc_freqs);

            // sum the term upper bounds for all non-essential posting lists
            for (int i = 0; i < essentialIndex; i++) {
                if (L.get(i) != null)
                    nonEssentialTermUpperBound += termUpperBounds.get(L.get(i).getTerm());
            }

            documentUpperBound = partialScore + nonEssentialTermUpperBound;

            // check if non-essential posting lists must be processed or not
            if (documentUpperBound > threshold) {
                // process non-essential posting list partial skipping all documents up to current_doc_id
                double nonEssentialScores = computeNonEssentialList(L, essentialIndex, current_doc_id, doc_freqs);

                documentUpperBound = documentUpperBound - nonEssentialTermUpperBound + nonEssentialScores;

                // check if the document can enter the Priority queue
                if (documentUpperBound > threshold) {

                    if (results.getResults().size() == k) {  // is full
                        assert results.getResults().peek() != null;
                        threshold = results.getResults().peek().getScore();
                        if (threshold > documentUpperBound)
                            threshold = documentUpperBound;

                        thresholdupdate = true;
                    } else
                        thresholdupdate = false;

                    results.add(current_doc_id, documentUpperBound);

                }
            }
        }

    }

    //method to move the invertedList of postings to the given docid
    private static int nextGeqPostingLists(ArrayList<InvertedList> L, int current_doc_id,int essentialIndex) {
        int nextGEQ = current_doc_id;

        for (int i = essentialIndex; i < L.size(); i++) {  // i-th posting list

            InvertedList invertedList = L.get(i);
            ArrayList<Posting> postingList = (ArrayList<Posting>) invertedList.getPostingArrayList();

            // check if there are postings to iterate in the i-th posting list
            if (postingList != null && postingList.size() != L.get(i).getPos()) {
                Posting posting = postingList.get(invertedList.getPos());

                if (posting.getDoc_id() < nextGEQ) {
                    posting = getIndexPostingbyId(invertedList, nextGEQ);
                    if (posting == null)
                        return -1;
                }

                // check if in the current posting list is not present docidToProcess, but it is present a docid >
                if (posting.getDoc_id() > nextGEQ) {
                    // the current docid will be the candidate next docid to be processed
                    nextGEQ = posting.getDoc_id();
                    invertedList.setPos(getIndex(invertedList, nextGEQ));
                    i = -1;
                }
            } else {
                return -1;
            }
        }
        return nextGEQ;
    }

    private static double computeNonEssentialList(ArrayList<InvertedList> L, int essentialIndex, int current_doc_id, HashMap<String, Integer> doc_freqs) {
        double nonEssentialScore = 0;

        for (int i = 0; i < essentialIndex; i++) {
            InvertedList postingList = L.get(i);
            int index = binarySearch(postingList, current_doc_id);
            if (index != -1) {
                postingList.setPos(index);
                nonEssentialScore += getScore(current_doc_id, doc_freqs, postingList);
            }
        }
        return nonEssentialScore;
    }

    //search the current_doc_id using the iterative binary search algorithm
    private static int binarySearch(InvertedList postingList, int current_doc_id) {
        List<Posting> arrayList = postingList.getPostingArrayList();
        int l = postingList.getPos(), r = arrayList.size() - 1;
        // Checking element in whole array
        while (l <= r) {
            int m = l + (r - l) / 2;

            // Check if x is present at mid
            if (arrayList.get(m).getDoc_id() == current_doc_id)
                return m;

            // If x greater, ignore left half
            if (arrayList.get(m).getDoc_id() < current_doc_id)
                l = m + 1;

                // If x is smaller,
                // element is on left side
                // so ignore right half
            else
                r = m - 1;
        }

        // If we reach here,
        // element is not present
        return -1;
    }


    private static double computeEssentialList(ArrayList<InvertedList> L, int essentialIndex, int current_doc_id, HashMap<String, Integer> doc_freqs) {
        double score = 0;
        for (int i = essentialIndex; i < L.size(); i++) {
            score += getScore(current_doc_id, doc_freqs, L.get(i));
        }
        return score;
    }

    private static double getScore(int current_doc_id, HashMap<String, Integer> doc_freqs, InvertedList invertedList) {

        double score = 0;

        if (invertedList.getPos() < invertedList.getPostingArrayList().size()) {

            Posting posting = invertedList.getPostingArrayList().get(invertedList.getPos());
            int doc_id = posting.getDoc_id();
            int term_freq = posting.getTerm_frequency();

            if (current_doc_id == doc_id) {
                if (Objects.equals(Flags.getScoringFunction(), "bm25")) {
                    int doc_len = DocumentIndexStats.readDocLen(docIndexBuffer, doc_id);
                    score = Score.BM25Score(term_freq, doc_freqs.get(invertedList.getTerm()), doc_len);
                } else
                    score = Score.tfIdfScore(term_freq, doc_freqs.get(invertedList.getTerm()));

                invertedList.setPos(invertedList.getPos() + 1);
            }
        }
        return score;
    }


    private static int retrieveEssentialIndex(HashMap<String, Float> termUpperBounds, ArrayList<InvertedList> L, double threshold) {
        double sum = 0;

        for (int i = 0; i < L.size(); i++) {
            sum += termUpperBounds.get(L.get(i).getTerm());

            if (sum > threshold)
                return i;

        }
        return -1;
    }


    private static int min_doc_id(ArrayList<InvertedList> L, int essentialIndex, boolean conjunctive) {
        int min_doc_id;
        if (conjunctive)
            min_doc_id = -1;
        else
            min_doc_id = CollectionStatistics.num_docs;

        for (int i = essentialIndex; i < L.size(); i++) {
            InvertedList invertedList = L.get(i);
            if (invertedList.getPos() < invertedList.getPostingArrayList().size()) {
                if (conjunctive)
                    min_doc_id = Math.max(invertedList.getPostingArrayList().get(invertedList.getPos()).getDoc_id(), min_doc_id);
                else
                    min_doc_id = Math.min(invertedList.getPostingArrayList().get(invertedList.getPos()).getDoc_id(), min_doc_id);
            }


        }
        if (min_doc_id == -1 || min_doc_id == CollectionStatistics.num_docs)
            return -1;


        return min_doc_id;
    }
}
