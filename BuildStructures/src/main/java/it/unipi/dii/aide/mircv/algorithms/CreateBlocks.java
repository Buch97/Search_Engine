package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.bean.DocumentIndexStats;
import it.unipi.dii.aide.mircv.common.bean.Posting;
import it.unipi.dii.aide.mircv.common.bean.Token;
import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;
import it.unipi.dii.aide.mircv.common.utils.Flags;
import it.unipi.dii.aide.mircv.common.utils.serializers.CustomSerializerDocumentIndexStats;
import it.unipi.dii.aide.mircv.common.textProcessing.Tokenizer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CreateBlocks {
    public static int SPIMI_TOKEN_STREAM_MAX_LIMIT;
    public final static List<Token> tokenStream = new ArrayList<>();
    public static DB db_document_index;
    public static int BLOCK_NUMBER = 0;


    public static void buildDataStructures() {
        try {
            File myObj;

            if(Flags.isDebug()) {
                myObj = new File("resources/collections/small_collection.tsv");
                SPIMI_TOKEN_STREAM_MAX_LIMIT = 3000;
                System.out.println("Running in debug mode");
            }
            else{
                    myObj = new File("resources/collections/collection.tsv");
                SPIMI_TOKEN_STREAM_MAX_LIMIT = 5000000;
                    System.out.println("Running in execution mode");
                }
            //BufferedWriter doc_index = new BufferedWriter(new FileWriter("./src/main/resources/output/doc_index.tsv"));

            Scanner myReader = new Scanner(myObj, StandardCharsets.UTF_8);
            db_document_index = DBMaker.fileDB("resources/output/document_index.db")
                    .closeOnJvmShutdown()
                    .checksumHeaderBypass()
                    .make();

            HTreeMap<Integer, DocumentIndexStats> document_index_map = db_document_index
                    .hashMap("document_index")
                    .keySerializer(Serializer.INTEGER)
                    .valueSerializer(new CustomSerializerDocumentIndexStats())
                    .createOrOpen();

            System.out.println("----------------------START GENERATING INVERTED INDEX BLOCKS----------------------");
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();

                // Handling of malformed lines
                if (!data.contains("\t"))
                    continue;

                String[] row = data.split("\t");
                String doc_no = row[0];
                String text = row[1];

                // Parsing/tokenization of the document
                parseDocumentBody(Integer.parseInt(doc_no), text);

                // Add document to the document index
                documentIndexAddition(doc_no, document_index_map);
                //documentIndexAdditionTextual(doc_no, doc_index);
            }

            CollectionStatistics.computeNumDocs();
            CollectionStatistics.computeAvgDocLen(document_index_map);

            db_document_index.close();
            myReader.close();
            //doc_index.close();
            System.out.println("----------------------INVERTED INDEX BLOCKS READY----------------------");
            MergeBlocks.mergeBlocks(BLOCK_NUMBER);
            //MergeBlocks.mergeBlocksText(BLOCK_NUMBER);

        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void parseDocumentBody(int doc_id, String text) throws IOException {
        Tokenizer tokenizer = new Tokenizer(text);
        Map<String, Integer> results = tokenizer.tokenize();

        for (String token : results.keySet())
            tokenStream.add(new Token(token, doc_id, results.get(token)));

        // Add token to tokenStream until we reach a size threshold
        if (tokenStream.size() >= SPIMI_TOKEN_STREAM_MAX_LIMIT) {
            // Create the inverted index of the block
            invertedIndexSPIMI();
            // clear the stream of token
            tokenStream.clear();
            BLOCK_NUMBER++;
        }
    }

    private static void invertedIndexSPIMI() {

        // Pseudocode at slide 59
        File output_file = new File("BuildStructures/src/main/resources/blocks/block" + BLOCK_NUMBER + ".tsv");

        // one dictionary for each block
        HashMap<String, ArrayList<Posting>> dictionary = new HashMap<>();
        ArrayList<Posting> postings_list;

        for (Token token : CreateBlocks.tokenStream) {
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

        TreeMap<String, ArrayList<Posting>> sorted_dictionary = new TreeMap<>(dictionary);

        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(output_file));
            bufferedWriter.write("TERM" + "\t" + "POSTING_LIST" + "\n");

            for (String term : sorted_dictionary.keySet()) {
                bufferedWriter.write(term + "\t");

                for (Posting p : sorted_dictionary.get(term))
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

    private static ArrayList<Posting> addToDictionary(Map<String, ArrayList<Posting>> vocabulary, String token) {
        int capacity = 1;
        ArrayList<Posting> postings_list = new ArrayList<>(capacity);
        vocabulary.put(token, postings_list);
        return postings_list;
    }

    private static void documentIndexAddition(String doc_no, HTreeMap<Integer, DocumentIndexStats> document_index_map) throws IOException {
        int doc_len = Tokenizer.doc_len;
        int doc_id = Integer.parseInt(doc_no);
        DocumentIndexStats documentIndexStats = new DocumentIndexStats(doc_no, doc_len);
        document_index_map.put(doc_id, documentIndexStats);
    }

    private static void documentIndexAdditionTextual(String doc_no, BufferedWriter doc_index) throws IOException {
        int doc_len = Tokenizer.doc_len;
        int doc_id = Integer.parseInt(doc_no);
        doc_index.write("DOC_ID: " + doc_id + " DOC_NO: " + doc_no + " DOC_LEN: " + doc_len + "\n");
    }
}