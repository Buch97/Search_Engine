package it.unipi.builddatastructures;

import it.unipi.bean.Posting;
import it.unipi.bean.InvertedList;
import it.unipi.bean.TermStats;
import it.unipi.utils.Compression;
import it.unipi.utils.FileChannelInvIndex;
import it.unipi.utils.InvertedListComparator;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.*;
import java.util.*;

public class MergeBlocks {

    private static final String mode = "APPEND";
    public static DB db_lexicon;
    private MergeBlocks() {
    }
    public static void mergeBlocks(int blockNumber) throws IOException {

        System.out.println("----------------------START MERGE PHASE----------------------");

        FileChannelInvIndex.openFileChannels(mode);

        // Definition of comparator, implemented in Class TermPositionBlock
        Comparator<InvertedList> comparator = new InvertedListComparator();
        // Priority queue with size equal to the number of blocks
        PriorityQueue<InvertedList> priorityQueue = new PriorityQueue<>(blockNumber, comparator);
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
        db_lexicon = DBMaker.fileDB("./src/main/resources/output/lexicon_disk_based.db")
                .closeOnJvmShutdown()
                .checksumHeaderBypass()
                .make();

        HTreeMap<String, TermStats> myMapLexicon = (HTreeMap<String, TermStats>) db_lexicon
                .hashMap("lexicon")
                .createOrOpen();

        // array of buffered reader to read each block at the same time
        for (int i = 0; i < blockNumber; i++) {
            readerList.add(new BufferedReader(new FileReader("./src/main/resources/intermediate_postings/" +
                    "inverted_index" + i + ".tsv")));
        }

        // Open buffered readers, one for each block
        // First lines of each block are inserted in a priority queue
        // Sorted with a custom comparator
        openBufferedReaders(priorityQueue, readerList);

        // For loop is terminated when priority queue is empty
        while (priorityQueue.size() != 0) {

            // Set parameters to add in lexicon
            doc_frequency = 0;
            coll_frequency = 0;
            offset_doc_id_start = offset_doc_id_end;
            offset_term_freq_start = offset_term_freq_end;

            // Peek first term
            String currentTerm = priorityQueue.peek().getTerm();

            Compression compression = new Compression();

            while (Objects.equals(currentTerm, Objects.requireNonNull(priorityQueue.peek()).getTerm())) {

                int blockIndex = Objects.requireNonNull(priorityQueue.peek()).getPos();
                List<Posting> postings = Objects.requireNonNull(priorityQueue.peek()).getPostingArrayList();

                doc_frequency += postings.size();

                for (Posting posting : postings) {
                    coll_frequency += posting.getTerm_frequency();
                    compression.gammaEncoding(posting.getDoc_id());
                    //compression.encodingVariableByte(posting.getDoc_id());
                    compression.unaryEncoding(posting.getTerm_frequency());
                }
                updatePriorityQueue(priorityQueue, readerList.get(blockIndex), blockIndex);
                if (priorityQueue.size() == 0) break;
            }

            //byte[] doc_id_compressed = compression.getVariableByteBuffer().toByteArray();
            byte[] doc_id_compressed = compression.getGammaBitSet().toByteArray();
            byte[] term_freq_compressed = new byte[Math.ceilDivExact(compression.getPosUnary(), 8)];

            if (compression.getUnaryBitSet().toByteArray().length != 0) {
                System.arraycopy(compression.getUnaryBitSet().toByteArray(), 0, term_freq_compressed, 0, compression.getUnaryBitSet().toByteArray().length);
            }

            FileChannelInvIndex.write(doc_id_compressed, term_freq_compressed);

            offset_doc_id_end += doc_id_compressed.length;
            offset_term_freq_end += term_freq_compressed.length;
            // Build lexicon
            myMapLexicon.put(currentTerm, new TermStats(doc_frequency, coll_frequency, offset_doc_id_start, offset_term_freq_start, offset_doc_id_end, offset_term_freq_end));
        }

        for (BufferedReader reader : readerList)
            reader.close();

        db_lexicon.close();
        FileChannelInvIndex.closeFileChannels();

        System.out.println("----------------------END MERGE PHASE----------------------");
    }

    public static void mergeBlocksText(int blockNumber) throws IOException {

        System.out.println("----------------------START MERGE PHASE----------------------");

        // Definition of comparator, implemented in Class TermPositionBlock
        Comparator<InvertedList> comparator = new InvertedListComparator();
        // Priority queue with size equal to the number of blocks
        PriorityQueue<InvertedList> priorityQueue = new PriorityQueue<>(blockNumber, comparator);
        // List of terms added to the priority queue during the move forward phase
        List<BufferedReader> readerList = new ArrayList<>();

        // Definition of parameters that describe the term in the blocks
        long actual_offset;
        long offset = 0;
        int doc_frequency;
        int coll_frequency;

        db_lexicon = DBMaker.fileDB("./src/main/resources/output/lexicon_disk_based.db")
                .closeOnJvmShutdown()
                .checksumHeaderBypass()
                .make();

        // Disk based lexicon using the HTreeMap
        HTreeMap<String, TermStats> myMapLexiconText = (HTreeMap<String, TermStats>) db_lexicon.hashMap("lexiconText").createOrOpen();

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
        // For loop is terminated when priority queue is empty
        while (priorityQueue.size() != 0) {

            // Add to lexicon the current term
            //lexicon.write(currentTerm + "\t");
            doc_frequency = 0;
            coll_frequency = 0;
            actual_offset = offset;

            String currentTerm = priorityQueue.peek().getTerm();

            inv_index.write(currentTerm + "\t");
            while (Objects.equals(currentTerm, Objects.requireNonNull(priorityQueue.peek()).getTerm())) {

                int blockIndex = Objects.requireNonNull(priorityQueue.peek()).getPos();
                List<Posting> postings = Objects.requireNonNull(priorityQueue.peek()).getPostingArrayList();

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
                updatePriorityQueue(priorityQueue, readerList.get(blockIndex), blockIndex);
                if (priorityQueue.size() == 0) break;
            }
            inv_index.newLine();
            inv_index.write("\n");

            // Build lexicon
            myMapLexiconText.put(currentTerm, new TermStats(doc_frequency, coll_frequency, actual_offset));
        }

        for (BufferedReader reader : readerList)
            reader.close();

        inv_index.close();
        System.out.println("----------------------END MERGE PHASE----------------------");
    }

    private static void updatePriorityQueue(PriorityQueue<InvertedList> priorityQueue, BufferedReader block, int index) throws IOException {
        priorityQueue.poll();
        addNextTerm(block, priorityQueue, index);
    }

    private static void openBufferedReaders(PriorityQueue<InvertedList> priorityQueue, List<BufferedReader> readerList) throws IOException {
        // For each reader read one line ad build an object TermPositionBlock
        // Add that object to priority queue
        for (BufferedReader reader : readerList) {
            // Skip first line of each block
            reader.readLine();
            String line = reader.readLine();
            if (line != null) {
                InvertedList invertedList = BuildTermPositionBlock(line, readerList.indexOf(reader));
                priorityQueue.add(invertedList);
            } else
                reader.close();
        }
    }

    private static void addNextTerm(BufferedReader block, PriorityQueue<InvertedList> priorityQueue, int index) throws IOException {

        String nextRow = block.readLine();
        if (nextRow != null) {
            priorityQueue.add(BuildTermPositionBlock(nextRow, index));
        } else
            block.close();
    }

    private static InvertedList BuildTermPositionBlock(String line, int index) {

        String term = line.split("\t")[0];
        String postingList = line.split("\t")[1];
        ArrayList<Posting> postingArrayList = new ArrayList<>();

        for (String posting : postingList.split(" ")) {
            int doc_id = Integer.parseInt(posting.split(":")[0]);
            int term_freq = Integer.parseInt(posting.split(":")[1]);
            postingArrayList.add(new Posting(doc_id, term_freq));
        }
        return new InvertedList(term, postingArrayList, index);
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

