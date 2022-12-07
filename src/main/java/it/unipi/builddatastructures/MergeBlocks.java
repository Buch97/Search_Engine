package it.unipi.builddatastructures;

import it.unipi.bean.Posting;
import it.unipi.bean.TermPositionBlock;
import it.unipi.bean.TermStats;
import it.unipi.utils.Compression;
import it.unipi.utils.TermPositionBlockComparator;
import org.mapdb.DB;
import org.mapdb.HTreeMap;

import java.io.*;
import java.util.*;

import static java.lang.Math.round;

public class MergeBlocks {

    private MergeBlocks (){}

    public static void mergeBlocks(DB db, int blockNumber) throws IOException {

        System.out.println("----------------------START MERGE PHASE----------------------");

        // Definition of comparator, implemented in Class TermPositionBlock
        Comparator<TermPositionBlock> comparator = new TermPositionBlockComparator();
        // Priority queue with size equal to the number of blocks
        PriorityQueue<TermPositionBlock> priorityQueue = new PriorityQueue<>(blockNumber, comparator);
        // List of terms added to the priority queue during the move forward phase
        List<BufferedReader> readerList = new ArrayList<>();

        // Definition of parameters that describe the term in the blocks
        long offset_doc_id = 0;
        long offset_term_freq = 0;
        int doc_frequency;
        int coll_frequency;
        long actual_offset_doc_id;
        long actual_offset_term_freq;
        int size = 1;

        // Disk based lexicon using the HTreeMap
        HTreeMap<String, TermStats> myMapLexicon = (HTreeMap<String, TermStats>) db.hashMap("lexicon").createOrOpen();

        // Open write buffers for lexicon and inverted index
        // BufferedWriter inv_ind_doc_id = new BufferedWriter(new FileWriter("./src/main/resources/output/inverted_index_doc_id.tsv"));
        // BufferedWriter inv_ind_term_frequency = new BufferedWriter(new FileWriter("./src/main/resources/output/inverted_index_term_frequency.tsv"));
        // From int to binary
        DataOutputStream inv_ind_doc_id_bin = new DataOutputStream(new BufferedOutputStream
                (new FileOutputStream("./src/main/resources/output/inverted_index_doc_id_bin.dat")));
        DataOutputStream inv_ind_term_frequency_bin = new DataOutputStream(new BufferedOutputStream
                (new FileOutputStream("./src/main/resources/output/inverted_index_term_frequency_bin.dat")));
        //BufferedWriter lexicon = new BufferedWriter(new FileWriter("./src/main/resources/output/lexicon.tsv"));
        //String header = "TERM" + "\t" + "DOC_FREQUENCY" + "\t" + "COLL_FREQUENCY" + "\t" + "BYTE_OFFSET_PL" + "\n";
        //lexicon.write(header);

        // array of buffered reader to read each block at the same time
        for (int i = 0; i < blockNumber; i++) {
            readerList.add(new BufferedReader(new FileReader("./src/main/resources/intermediate_postings/" +
                    "inverted_index" + i + ".tsv")));
        }

        // Open buffered readers, one for each block
        // First lines of each block are inserted in a priority queue
        // Sorted with a custom comparator
        openBufferedReaders(priorityQueue, readerList);

        // For loop is terminated when each bufferedReader reach the EOF
        while (priorityQueue.size() != 0) {

            // Peek first term
            String currentTerm = priorityQueue.peek().getTerm();
            //System.out.println("PROCESSING TERM " + currentTerm + "...");

            // Add to lexicon the current term
            //lexicon.write(currentTerm + "\t");
            doc_frequency = 0;
            coll_frequency = 0;
            actual_offset_doc_id = offset_doc_id;
            actual_offset_term_freq = offset_term_freq;

            // Definition of iterator to scan the priority queue
            Iterator<TermPositionBlock> value = priorityQueue.iterator();
            while (value.hasNext()) {

                // New object that has to be tested against the current term
                TermPositionBlock termPositionBlock = value.next();
                // Retrieve term and list of postings of the new objects
                String term = termPositionBlock.getTerm();
                ArrayList<Posting> postings = termPositionBlock.getPostingArrayList();

                // Compare new term with current term
                if (Objects.equals(term, currentTerm)) {
                    // If equals, then update parameters of the term
                    doc_frequency += postings.size();
                    size =  postings.size();
                    for (Posting posting : postings) {
                        coll_frequency += posting.getTerm_frequency();
                        // inv_ind_doc_id.append((char) posting.getDoc_id()).append(" ");
                        // inv_ind_term_frequency.append((char) posting.getTerm_frequency()).append(" ");
                        BitSet doc_id_compressed = Compression.gammaEncoding(posting.getDoc_id());
                        inv_ind_doc_id_bin.write(doc_id_compressed.toByteArray());

                        BitSet term_freq_compressed = Compression.unaryEncoding(posting.getTerm_frequency());
                        inv_ind_term_frequency_bin.write(term_freq_compressed.toByteArray());

                        offset_doc_id += doc_id_compressed.size()/8;
                        offset_term_freq += term_freq_compressed.size()/8;
                    }
                    inv_ind_doc_id_bin.flush();
                    inv_ind_term_frequency_bin.flush();
                }
            }

            // Build inverted index
            //inv_ind_doc_id.write("\n");
            //inv_ind_term_frequency.write("\n");

            // Build lexicon
            // offset += "\n".getBytes().length;
            //lexicon.write(doc_frequency + "\t" + coll_frequency + "\t" + actual_offset + "\n");
            myMapLexicon.put(currentTerm, new TermStats(doc_frequency, coll_frequency, actual_offset_doc_id, actual_offset_term_freq, size));

            // Reset the iterator
            value = priorityQueue.iterator();
            // List with new terms to add in the priority queue
            List<TermPositionBlock> itemsToAdd = new ArrayList<>();

            while (value.hasNext()) {
                moveForward(readerList, currentTerm, value, itemsToAdd);
            }
            priorityQueue.addAll(itemsToAdd);
        }

        for (BufferedReader reader : readerList)
            reader.close();

        inv_ind_term_frequency_bin.close();
        inv_ind_doc_id_bin.close();
        //lexicon.close();
        System.out.println("----------------------END MERGE PHASE----------------------");
    }
    private static void openBufferedReaders(PriorityQueue<TermPositionBlock> priorityQueue, List<BufferedReader> readerList) throws IOException {
        // For each reader read one line ad build an object TermPositionBlock
        // Add that object to priority queue
        for (BufferedReader reader : readerList) {
            // Skip first line of each block
            reader.readLine();
            String line = reader.readLine();
            if (line != null) {
                TermPositionBlock termPositionBlock = BuildTermPositionBlock(readerList, reader, line);
                priorityQueue.add(termPositionBlock);
            } else
                reader.close();
        }
    }
    private static void moveForward(List<BufferedReader> readerList, String currentTerm, Iterator<TermPositionBlock> value, List<TermPositionBlock> itemsToAdd) throws IOException {
        // Check each object in priority queue
        // Remove only terms equals to current term and read the next line in each of these blocks
        TermPositionBlock termPositionBlock = value.next();
        if ((termPositionBlock != null) && (Objects.equals(termPositionBlock.getTerm(), currentTerm))) {
            String nextRow = readerList.get(termPositionBlock.getBlock_index()).readLine();
            value.remove();

            if (nextRow != null) {
                itemsToAdd.add(BuildTermPositionBlock(readerList, readerList.get(termPositionBlock.getBlock_index()), nextRow));
            }
            else
                readerList.get(termPositionBlock.getBlock_index()).close();
        }
    }
    private static TermPositionBlock BuildTermPositionBlock(List<BufferedReader> readerList, BufferedReader reader, String line) {
        String term = line.split("\t")[0];
        String postingList = line.split("\t")[1];
        ArrayList<Posting> postingArrayList = new ArrayList<>();

        for (String posting : postingList.split(" ")) {
            int doc_id = Integer.parseInt(posting.split(":")[0]);
            int term_freq = Integer.parseInt(posting.split(":")[1]);
            postingArrayList.add(new Posting(doc_id, term_freq));
        }
        return new TermPositionBlock(term, postingArrayList, readerList.indexOf(reader));
    }
}