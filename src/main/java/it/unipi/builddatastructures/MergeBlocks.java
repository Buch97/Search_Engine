package it.unipi.builddatastructures;

import it.unipi.bean.Posting;
import it.unipi.bean.RafInvertedIndex;
import it.unipi.bean.TermPositionBlock;
import it.unipi.bean.TermStats;
import it.unipi.utils.Compression;
import it.unipi.utils.TermPositionBlockComparator;
import org.eclipse.collections.api.map.primitive.ImmutableObjectDoubleMap;
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

        new RafInvertedIndex("src/main/resources/output/inverted_index_doc_id_bin.dat",
                "src/main/resources/output/inverted_index_term_frequency_bin.dat", mode);

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

        // Open write buffers for lexicon and inverted index
        // BufferedWriter inv_ind_doc_id = new BufferedWriter(new FileWriter("./src/main/resources/output/inverted_index_doc_id.tsv"));
        // BufferedWriter inv_ind_term_frequency = new BufferedWriter(new FileWriter("./src/main/resources/output/inverted_index_term_frequency.tsv"));
        // From int to binary
        ObjectOutputStream inv_ind_doc_id_bin = new ObjectOutputStream(new BufferedOutputStream
                (new FileOutputStream("./src/main/resources/output/inverted_index_doc_id_bin.dat")));
        ObjectOutputStream inv_ind_term_frequency_bin = new ObjectOutputStream(new BufferedOutputStream
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
            offset_doc_id_start = offset_doc_id_end;
            offset_term_freq_start = offset_term_freq_end;

            // Definition of iterator to scan the priority queue
            Iterator<TermPositionBlock> value = priorityQueue.iterator();
            Compression compression = new Compression();
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

                    byte[] doc_id_compressed;
                    byte[] term_freq_compressed;

                    for (Posting posting : postings) {
                        coll_frequency += posting.getTerm_frequency();
                        // inv_ind_doc_id.append((char) posting.getDoc_id()).append(" ");
                        // inv_ind_term_frequency.append((char) posting.getTerm_frequency()).append(" ");
                        //byte[] doc_id_compressed = Compression.gammaEncoding(posting.getDoc_id()).toByteArray();
                        //inv_ind_doc_id_bin.write(doc_id_compressed.toByteArray());
                        compression.gammaEncoding(posting.getDoc_id());
                        compression.unaryEncoding(posting.getTerm_frequency());
                    }

                    doc_id_compressed = compression.getGammaBitSet().toByteArray();
                    //inv_ind_doc_id_bin.write(doc_id_compressed);
                    RafInvertedIndex.fileChannel_doc_id.write(ByteBuffer.wrap(doc_id_compressed));

                    term_freq_compressed = compression.getUnaryBitSet().toByteArray();
                    //inv_ind_term_frequency_bin.write(term_freq_compressed);
                    RafInvertedIndex.fileChannel_term_freq.write(ByteBuffer.wrap(term_freq_compressed));
                    // offset_doc_id_end += doc_id_compressed.length;
                    // offset_term_freq_end += term_freq_compressed.length;

                    inv_ind_doc_id_bin.flush();
                    inv_ind_term_frequency_bin.flush();
                }
            }

            offset_doc_id_end += Math.ceilDiv(compression.getPosGamma(), 8);
            offset_term_freq_end += Math.ceilDiv(compression.getPosUnary(), 8);

            // Build inverted index
            //inv_ind_doc_id.write("\n");
            //inv_ind_term_frequency.write("\n");

            // Build lexicon
            // offset += "\n".getBytes().length;
            //lexicon.write(doc_frequency + "\t" + coll_frequency + "\t" + actual_offset + "\n");
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

        inv_ind_term_frequency_bin.close();
        inv_ind_doc_id_bin.close();

        RafInvertedIndex.fileChannel_doc_id.close();
        RafInvertedIndex.fileChannel_term_freq.close();
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

        //StringBuilder s = new StringBuilder();ù
        for (int i = 0; i < size; i++) {
            if (bi.get(i))
                System.out.print("1");
            else System.out.print("0");
        }
        System.out.println("\n");
    }
}
