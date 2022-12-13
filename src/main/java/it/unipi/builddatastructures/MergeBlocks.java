package it.unipi.builddatastructures;

import it.unipi.bean.FileChannelInvIndex;
import it.unipi.bean.Posting;
import it.unipi.bean.TermPositionBlock;
import it.unipi.bean.TermStats;
import it.unipi.utils.Compression;
import it.unipi.utils.TermPositionBlockComparator;
import org.mapdb.DB;
import org.mapdb.HTreeMap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class MergeBlocks {

    private static final String mode = "APPEND";

    private MergeBlocks() {
    }

    public static void mergeBlocks(DB db, int blockNumber) throws IOException {

        System.out.println("----------------------START MERGE PHASE----------------------");

        FileChannelInvIndex.openFileChannels(mode);

        // Definition of comparator, implemented in Class TermPositionBlock
        Comparator<TermPositionBlock> comparator = new TermPositionBlockComparator();
        // Priority queue with size equal to the number of blocks
        PriorityQueue<TermPositionBlock> priorityQueue = new PriorityQueue<>(blockNumber, comparator);
        // List of terms added to the priority queue during the move forward phase
        List<BufferedReader> readerList = new ArrayList<>();

        // Definition of parameters that describe the term in the blocks
        long offset_doc_id_start;
        long offset_term_freq_start;
        long offset_doc_id_end = 0;
        long offset_term_freq_end = 0;
        int doc_frequency;
        int coll_frequency;

        // Disk based lexicon using the HTreeMap
        HTreeMap<String, TermStats> myMapLexicon = (HTreeMap<String, TermStats>) db.hashMap("lexicon").createOrOpen();

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

            // Add to lexicon the current term
            doc_frequency = 0;
            coll_frequency = 0;
            offset_doc_id_start = offset_doc_id_end;
            offset_term_freq_start = offset_term_freq_end;

            // Definition of iterator to scan the priority queue
            Iterator<TermPositionBlock> value = priorityQueue.iterator();
            Compression compression = new Compression();
            byte[] doc_id_compressed;
            byte[] term_freq_compressed;

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

                    for (Posting posting : postings) {

                        coll_frequency += posting.getTerm_frequency();

                        compression.gammaEncoding(posting.getDoc_id());
                        compression.unaryEncoding(posting.getTerm_frequency());
                    }
                }
            }

            doc_id_compressed = compression.getGammaBitSet().toByteArray();
            // 0	140:1 146:1 404:2 738:1 911:1
            term_freq_compressed = compression.getUnaryBitSet().toByteArray();

            FileChannelInvIndex.fileChannel_doc_id.write(ByteBuffer.wrap(doc_id_compressed));
            FileChannelInvIndex.fileChannel_term_freq.write(ByteBuffer.wrap(term_freq_compressed));

            offset_doc_id_end += Math.ceilDiv(compression.getPosGamma(), 8);
            offset_term_freq_end += Math.ceilDiv(compression.getPosUnary(), 8);

            // Build lexicon
            myMapLexicon.put(currentTerm, new TermStats(doc_frequency, coll_frequency, offset_doc_id_start, offset_term_freq_start, offset_doc_id_end, offset_term_freq_end));

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

        FileChannelInvIndex.closeFileChannels();

        System.out.println("----------------------END MERGE PHASE----------------------");
    }

    public static void mergeBlocksText(DB db, int blockNumber) throws IOException {

        System.out.println("----------------------START MERGE PHASE----------------------");

        // Definition of comparator, implemented in Class TermPositionBlock
        Comparator<TermPositionBlock> comparator = new TermPositionBlockComparator();
        // Priority queue with size equal to the number of blocks
        PriorityQueue<TermPositionBlock> priorityQueue = new PriorityQueue<>(blockNumber, comparator);
        // List of terms added to the priority queue during the move forward phase
        List<BufferedReader> readerList = new ArrayList<>();

        // Definition of parameters that describe the term in the blocks
        long actual_offset;
        long offset = 0;
        int doc_frequency;
        int coll_frequency;

        // Disk based lexicon using the HTreeMap
        HTreeMap<String, TermStats> myMapLexiconText = (HTreeMap<String, TermStats>) db.hashMap("lexiconText").createOrOpen();

        BufferedWriter inv_index = new BufferedWriter(new FileWriter("./src/main/resources/output/inv_index.tsv"));

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
            inv_index.append(currentTerm).append(" ");

            // Add to lexicon the current term
            //lexicon.write(currentTerm + "\t");
            doc_frequency = 0;
            coll_frequency = 0;
            actual_offset = offset;

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

                    for (Posting posting : postings) {

                        coll_frequency += posting.getTerm_frequency();

                        inv_index.write(posting.getDoc_id() + ":");
                        offset += (posting.getDoc_id() + ":").length();

                        inv_index.write(posting.getTerm_frequency() + " ");
                        offset += (posting.getTerm_frequency() + " ").length();

                        inv_index.flush();
                    }
                }
            }
            inv_index.newLine();
            inv_index.write("\n");

            // Build lexicon
            myMapLexiconText.put(currentTerm, new TermStats(doc_frequency, coll_frequency, actual_offset));

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

        inv_index.close();
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
            } else
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

    public static void printBitSet(BitSet bi, int size) {

        for (int i = 0; i < size; i++) {
            if (bi.get(i))
                System.out.print("1");
            else System.out.print("0");
        }
        System.out.println("\n");
    }
}

