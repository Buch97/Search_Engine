package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.bean.DocumentIndexStats;
import it.unipi.dii.aide.mircv.common.bean.Posting;
import it.unipi.dii.aide.mircv.common.bean.Token;
import it.unipi.dii.aide.mircv.common.textProcessing.Tokenizer;
import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;
import it.unipi.dii.aide.mircv.common.utils.Flags;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

public class CreateBlocks {
    public final static List<Token> tokenStream = new ArrayList<>();
    private static final String DOCUMENT_INDEX = "resources/output/document_index";
    public static int BLOCK_NUMBER = 0;

    public static void buildDataStructures() {
        try {
            File myObj;

            //Pick the collection to use
            if (Flags.isDebug()) {
                myObj = new File("resources/collections/small_collection.tsv");
                System.out.println("Running in debug mode");
            } else {
                myObj = new File("resources/collections/collection.tsv");
                System.out.println("Running in execution mode");
            }

            Scanner myReader = new Scanner(myObj, StandardCharsets.UTF_8);

            //File channel allocation
            FileChannel document_index = (FileChannel) Files.newByteChannel(Paths.get(DOCUMENT_INDEX),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE);


            System.out.println("----------------------START GENERATING INVERTED INDEX BLOCKS----------------------");

            long positionDocIndex = 0;
            boolean lastLine = false;

            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();

                //Handling of malformed lines
                if (!data.contains("\t"))
                    continue;

                String[] row = data.split("\t");
                String doc_no = row[0];
                String text = row[1];

                if (!myReader.hasNextLine())
                    lastLine = true;

                //Parsing/tokenization of the document
                parseDocumentBody(Integer.parseInt(doc_no), text, lastLine);

                //Add document to the document index
                positionDocIndex = documentIndexAddition(doc_no, document_index, positionDocIndex);
            }

            CollectionStatistics.computeAvgDocLen();

            document_index.close();
            myReader.close();

            System.out.println("----------------------INVERTED INDEX BLOCKS READY----------------------");
            MergeBlocks.mergeBlocks(BLOCK_NUMBER);

        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void parseDocumentBody(int doc_id, String text, Boolean lastLine) throws IOException {
        //Space-based tokenization
        Tokenizer tokenizer = new Tokenizer(text);
        Map<String, Integer> results = tokenizer.tokenize();

        //Add token to a stream of tokens
        for (String token : results.keySet())
            tokenStream.add(new Token(token, doc_id, results.get(token)));

        // leave 20% of memory free
        long MEMORY_THRESHOLD = Runtime.getRuntime().totalMemory() * 20 / 100;
        // Add token to tokenStream until we reach a size threshold or if there are no more documents to process
        if (Runtime.getRuntime().freeMemory() < MEMORY_THRESHOLD || lastLine) {
            // Create the inverted index of the block
            invertedIndexSPIMI();
            // clear the stream of token
            tokenStream.clear();
            BLOCK_NUMBER++;
            System.gc();
        }
    }

    private static void invertedIndexSPIMI() {

        File output_file = new File("BuildStructures/src/main/resources/blocks/block" + BLOCK_NUMBER + ".tsv");

        //One dictionary for each block
        HashMap<String, ArrayList<Posting>> dictionary = new HashMap<>();

        for (Token token : CreateBlocks.tokenStream) {
            String term = token.getTerm();
            //If the entry for that term does not exist it is created
            if (!dictionary.containsKey(term)){
                addToDictionary(dictionary, token.getTerm());
            }
            //If the ArrayList is full we expand its size
            if (!dictionary.get(token.getTerm()).contains(null)) {
                int capacity = dictionary.get(token.getTerm()).size() * 2;
                dictionary.get(token.getTerm()).ensureCapacity(capacity);
            }
            //Add current posting to dictionary
            dictionary.get(token.getTerm()).add(new Posting(token.getDoc_id(), token.getFrequency()));
        }

        //Dictionary is sorted alphabetically by key
        dictionary = dictionary.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        try {
            //Dictionary is written inside a file which represents a block
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(output_file));
            bufferedWriter.write("TERM" + "\t" + "POSTING_LIST" + "\n");

            for (String term : dictionary.keySet()) {
                bufferedWriter.write(term + "\t");

                for (Posting p : dictionary.get(term))
                    bufferedWriter.write(p.getDoc_id() + ":" + p.getTerm_frequency() + " ");

                bufferedWriter.write("\n");
            }
            System.out.println("WRITTEN BLOCK " + BLOCK_NUMBER);
            bufferedWriter.close();

        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    private static void addToDictionary(Map<String, ArrayList<Posting>> vocabulary, String token) {
        int capacity = 1;
        ArrayList<Posting> postings_list = new ArrayList<>(capacity);
        vocabulary.put(token, postings_list);
    }

    //This function create a new entry in document index for the current processed document
    private static long documentIndexAddition(String doc_no, FileChannel document_index, long position) throws IOException {
        int doc_len = Tokenizer.doc_len;
        int doc_id = Integer.parseInt(doc_no);

        DocumentIndexStats documentIndexStats = new DocumentIndexStats(doc_no, doc_len);
        CollectionStatistics.setNum_docs();
        CollectionStatistics.setAvg_doc_len(doc_len);

        return documentIndexStats.writeDocumentIndex(document_index, position, doc_id);
    }

}