package it.unipi.build_data_structures;


import it.unipi.bean.Posting;
import it.unipi.bean.TermPositionBlock;
import it.unipi.bean.Term_Stats;
import it.unipi.bean.Token;
import it.unipi.utils.TermPositionBlockComparator;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.*;
import java.util.*;


public class Index_Construction {

    //dimensione per costruire i blocchi messa ora per prova a 3000 su una small collection
    public final static int SPIMI_TOKEN_STREAM_MAX_LIMIT = 3000;
    public final static List<Token> tokenStream = new ArrayList<>();
    public static int BLOCK_NUMBER = 0; //indice da usare per scrivere i file parziali dell'inverted index
    //public static HTreeMap<?, ?> myMapLexicon;

    public static void buildDataStructures(DB db) {
        try {
            File myObj = new File("./src/main/resources/collections/small_collection.tsv");

            Scanner myReader = new Scanner(myObj, "UTF-8");
            BufferedWriter writer_doc_index = new BufferedWriter(new FileWriter("./src/main/resources/output/document_index.tsv"));
            writer_doc_index.write("DOC_ID" + "\t" + "DOC_NO" + "\t" + "DOC_LEN" + "\n");

            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();

                //handling of malformed lines
                if (!data.contains("\t"))
                    continue;

                String[] row = data.split("\t");
                String doc_no = row[0];
                String text = row[1];

                //aggiungo il documento che sto processando al doc_index
                documentIndexAddition(doc_no, text, writer_doc_index);

                //faccio parsing/tokenization del documento
                parseDocumentBody(Integer.parseInt(doc_no), text);
            }

            writer_doc_index.close();
            myReader.close();

            mergeBlocks(db);
            //db.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void documentIndexAddition(String doc_no, String text, BufferedWriter writer) throws IOException {
        int doc_len = text.getBytes().length;
        writer.write(Integer.parseInt(doc_no) + "\t" + doc_no + "\t" + doc_len + "\n");
    }

    public static void parseDocumentBody(int doc_id, String text) {
        Tokenizer tokenizer = new Tokenizer(text);
        Map<String, Integer> results = tokenizer.tokenize();

        for (String token : results.keySet())
            tokenStream.add(new Token(token, doc_id, results.get(token)));

        //aggiungo token al mio stream fino a che non ho raggiunto il limite
        if (tokenStream.size() >= SPIMI_TOKEN_STREAM_MAX_LIMIT) {
            //sono pronto per creare l'inverted index relativo a questo blocco
            invertedIndexSPIMI();
            tokenStream.clear(); //pulisco lo stream
            BLOCK_NUMBER++;
        }
    }

    //guarda pseudocodice slide 59
    private static void invertedIndexSPIMI() {

        File output_file = new File("./src/main/resources/intermediate_postings/inverted_index" + BLOCK_NUMBER + ".tsv");
        // one dictionary for each block
        HashMap<String, ArrayList<Posting>> dictionary = new HashMap<>();
        ArrayList<Posting> postings_list;

        //while (Runtime.getRuntime().freeMemory() > 0) {
        for (Token token : Index_Construction.tokenStream) {
            if (!dictionary.containsKey(token.getTerm()))
                postings_list = addToDictionary(dictionary, token.getTerm());
            else
                postings_list = dictionary.get(token.getTerm());

            if (!postings_list.contains(null)) {
                int capacity = postings_list.size() * 2;
                postings_list.ensureCapacity(capacity); //aumenta la length dell arraylist
            }

            postings_list.add(new Posting(token.getDoc_id(), token.getFrequency()));
        }
        //}

        //faccio il sort del vocabolario per facilitare la successiva fase di merging
        TreeMap<String, ArrayList<Posting>> sorted_dictionary = new TreeMap<>(dictionary);

        try {
            //scrivo sul file
            FileWriter myWriter = new FileWriter(output_file);
            myWriter.write("TERM" + "\t" + "POSTING_LIST" + "\n");

            for (String term : sorted_dictionary.keySet()) {
                myWriter.write(term + "\t");

                for (Posting p : sorted_dictionary.get(term))
                    myWriter.write(p.getDoc_id() + ":" + p.getTerm_frequency() + " ");

                myWriter.write("\n");
            }
            myWriter.close();

        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

    }

    private static ArrayList<Posting> addToDictionary(Map<String, ArrayList<Posting>> vocabulary, String token) {
        int capacity = 1;
        ArrayList<Posting> postings_list = new ArrayList<>(capacity);
        vocabulary.put(token, postings_list);
        return postings_list;
    }

    // MERGE:
    // One buffered reader for each block
    // We take the first term of each block and put it into a priority queue, sorted alphabetically
    // Then we take the first object from the priority queue, in order to compare it with other term from other blocks
    // The buffer is moved forward only in blocks in which we have a term equal to the current term

    private static void mergeBlocks(DB db) throws IOException {

        Comparator<TermPositionBlock> comparator = new TermPositionBlockComparator();
        PriorityQueue<TermPositionBlock> priorityQueue = new PriorityQueue<>(BLOCK_NUMBER, comparator);
        List<BufferedReader> readerList = new ArrayList<>();

        long offset = 0;
        int doc_frequency;
        int coll_frequency;
        long actual_offset;

        HTreeMap<String, Term_Stats> myMapLexicon = (HTreeMap<String, Term_Stats>) db.hashMap("lexicon").createOrOpen();

        BufferedWriter inv_ind = new BufferedWriter(new FileWriter("./src/main/resources/output/inverted_index.tsv"));
        BufferedWriter lexicon = new BufferedWriter(new FileWriter("./src/main/resources/output/lexicon.tsv"));
        String header = "TERM" + "\t" + "DOC_FREQUENCY" + "\t" + "COLL_FREQUENCY" + "\t" + "BYTE_OFFSET_PL" + "\n";
        lexicon.write(header);

        // array of buffered reader to read each block at the same time
        for (int i = 0; i < BLOCK_NUMBER; i++) {
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
            lexicon.write(currentTerm + "\t");
            doc_frequency = 0;
            coll_frequency = 0;
            actual_offset = offset;

            Iterator<TermPositionBlock> value = priorityQueue.iterator();

            while (value.hasNext()) {

                TermPositionBlock termPositionBlock = value.next();
                String term = termPositionBlock.getTerm();
                ArrayList<Posting> postings = termPositionBlock.getPostingArrayList();

                if (Objects.equals(term, currentTerm)) {
                    doc_frequency += postings.size();
                    for (Posting posting : postings) {
                        coll_frequency += posting.getTerm_frequency();
                        inv_ind.append(posting.toString()).append(" ");
                    }
                    offset += postings.toString().getBytes().length;
                }
            }

            // Build inverted index
            inv_ind.write("\n");

            // Build lexicon
            offset += "\n".getBytes().length;
            lexicon.write(doc_frequency + "\t" + coll_frequency + "\t" + actual_offset + "\n");
            myMapLexicon.put(currentTerm, new Term_Stats(doc_frequency, coll_frequency, actual_offset));

            // Reset the iterator
            value = priorityQueue.iterator();
            // List with new terms to add in the priority queue
            List<TermPositionBlock> itemsToAdd = new ArrayList<>();

            while (value.hasNext()) {
                moveForward(priorityQueue, readerList, currentTerm, value, itemsToAdd);
            }
            priorityQueue.addAll(itemsToAdd);
        }

        for (BufferedReader reader : readerList)
            reader.close();

        inv_ind.close();
        lexicon.close();
    }

    private static void openBufferedReaders(PriorityQueue<TermPositionBlock> priorityQueue, List<BufferedReader> readerList) throws IOException {
        for (BufferedReader reader : readerList) {
            // Skip first line of each block
            reader.readLine();
            String line = reader.readLine();
            if (line != null) {
                TermPositionBlock termPositionBlock = BuildTermPositionBlock(priorityQueue, readerList, reader, line);
                priorityQueue.add(termPositionBlock);
            } else
                reader.close();
        }
    }

    private static void moveForward(PriorityQueue<TermPositionBlock> priorityQueue, List<BufferedReader> readerList, String currentTerm, Iterator<TermPositionBlock> value, List<TermPositionBlock> itemsToAdd) throws IOException {
        TermPositionBlock termPositionBlock = value.next();
        if ((termPositionBlock != null) && (Objects.equals(termPositionBlock.getTerm(), currentTerm))) {
            String nextRow = readerList.get(termPositionBlock.getBlock_index()).readLine();
            value.remove();

            if (nextRow != null) {
                itemsToAdd.add(BuildTermPositionBlock(priorityQueue, readerList, readerList.get(termPositionBlock.getBlock_index()), nextRow));
            }
            else
                readerList.get(termPositionBlock.getBlock_index()).close();
        }
    }

    private static TermPositionBlock BuildTermPositionBlock(PriorityQueue<TermPositionBlock> priorityQueue, List<BufferedReader> readerList, BufferedReader reader, String line) {
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