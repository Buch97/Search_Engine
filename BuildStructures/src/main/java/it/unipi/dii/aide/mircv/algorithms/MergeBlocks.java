package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.bean.InvertedList;
import it.unipi.dii.aide.mircv.common.bean.Posting;
import it.unipi.dii.aide.mircv.common.bean.TermStats;
import it.unipi.dii.aide.mircv.common.compressor.Compression;
import it.unipi.dii.aide.mircv.common.utils.comparator.InvertedListComparator;
import it.unipi.dii.aide.mircv.common.utils.filechannel.FileChannelInvIndex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;


public class MergeBlocks {

    private static final String mode = "APPEND";
    private static final String LEXICON = "resources/output/lexicon";

    private MergeBlocks() {
    }

    public static void mergeBlocks(int blockNumber) throws IOException {

        System.out.println("----------------------START MERGE PHASE----------------------");

        FileChannelInvIndex.openFileChannels(mode);

        // Definition of comparator, implemented in Class TermPositionBlock
        InvertedListComparator comparator = new InvertedListComparator();
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


        FileChannel lexicon = (FileChannel) Files.newByteChannel(Paths.get(LEXICON),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);


        // array of buffered reader to read each block at the same time
        for (int i = 0; i < blockNumber; i++) {
            readerList.add(new BufferedReader(new FileReader("BuildStructures/src/main/resources/blocks/" +
                    "block" + i + ".tsv")));
        }

        // Open buffered readers, one for each block
        // First lines of each block are inserted in a priority queue
        // Sorted with a custom comparator
        openBufferedReaders(priorityQueue, readerList);
        long positionLex = 0;
        float maxTermFrequency;
        float localTermFrequency;
        // For loop is terminated when priority queue is empty
        while (priorityQueue.size() != 0) {

            // Set parameters to add in lexicon
            maxTermFrequency=0;
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
                    localTermFrequency=posting.getTerm_frequency();
                    if(localTermFrequency > maxTermFrequency) maxTermFrequency = localTermFrequency;

                    coll_frequency += posting.getTerm_frequency();
                    //compression.gammaEncoding(posting.getDoc_id());
                    compression.encodingVariableByte(posting.getDoc_id());
                    compression.unaryEncoding(posting.getTerm_frequency());
                }
                updatePriorityQueue(priorityQueue, readerList.get(blockIndex), blockIndex);
                if (priorityQueue.size() == 0) break;
            }

            byte[] doc_id_compressed = compression.getVariableByteBuffer().toByteArray();
            //byte[] doc_id_compressed = compression.getGammaBitSet().toByteArray();
            byte[] term_freq_compressed = new byte[Math.ceilDivExact(compression.getPosUnary(), 8)];

            if (compression.getUnaryBitSet().toByteArray().length != 0) {
                System.arraycopy(compression.getUnaryBitSet().toByteArray(), 0, term_freq_compressed, 0, compression.getUnaryBitSet().toByteArray().length);
            }

            FileChannelInvIndex.write(doc_id_compressed, term_freq_compressed);

            offset_doc_id_end += doc_id_compressed.length;
            offset_term_freq_end += term_freq_compressed.length;
            // Build lexicon

            TermStats termStats = new TermStats(currentTerm, doc_frequency, coll_frequency, offset_doc_id_start, offset_term_freq_start, offset_doc_id_end, offset_term_freq_end,maxTermFrequency);
            positionLex = termStats.writeTermStats(positionLex, lexicon);
        }

        for (BufferedReader reader : readerList)
            reader.close();

        lexicon.close();
        FileChannelInvIndex.closeFileChannels();

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

}

