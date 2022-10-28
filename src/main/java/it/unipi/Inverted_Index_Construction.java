package it.unipi;


import java.io.*;
import java.util.*;


public class Inverted_Index_Construction {

    public static final String[] STOPWORDS = {"a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
            "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then",
            "there", "these", "they", "this", "to", "was", "will", "with"};
    public static final String[] PUNCTUATUION = {"'", ".", ":", ",", "!", "?", "(", ")", ";", "-",
            "_", "/", "\\", "[", "]", "{", "}"};
    public final static int SPIMI_TOKEN_STREAM_MAX_LIMIT = 3000;
    public final static List<Token> tokenStream = new ArrayList<>();
    public static Map<Integer, Doc_Stats> documents = new HashMap<>();
    public static Map<String, Term_Stats> vocabulary = new HashMap<>();
    public static int block_number = 0;
    public static File inverted_index = new File("C:\\Users\\pucci\\Desktop\\AIDE\\" +
            "Multimedia Information Retrieval and Computer Vision\\inverted_index.tsv");

    public static void main(String[] args) {
        try {
            File myObj = new File("C:\\Users\\pucci\\Desktop\\AIDE\\" +
                    "Multimedia Information Retrieval and Computer Vision\\small_collection.tsv");
            Scanner myReader = new Scanner(myObj, "UTF-8");

            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                String[] row = data.split("\t");
                String doc_no = row[0];
                String text = row[1];
                documentIndexMapping(doc_no, text);
                parseDocumentBody(Integer.parseInt(doc_no), text);
                mergeBlocks();
            }

            for (Token elem : tokenStream)
                System.out.println(elem.getTerm() + ":" + elem.getDoc_id() + ":" + elem.getFrequency());

            Document_Index_Hash document_index = new Document_Index_Hash(documents);
            Lexicon lexicon = new Lexicon(vocabulary);
            //lexicon.print();
            //document_index.print();
            myReader.close();

        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    private static void documentIndexMapping(String doc_no, String text) {
        int doc_len = text.getBytes().length;
        Doc_Stats doc = new Doc_Stats(doc_no, doc_len);
        documents.put(Integer.parseInt(doc_no), doc);
    }

    public static void parseDocumentBody(int doc_id, String text) {
        Tokenizer tokenizer = new Tokenizer(doc_id, text);
        Map<String, Integer> results = tokenizer.tokenize();
        for (String token : results.keySet())
            tokenStream.add(new Token(token, tokenizer.getDocId(), results.get(token)));

        if (tokenStream.size() >= SPIMI_TOKEN_STREAM_MAX_LIMIT) {
            // Call SPIMI once the block is full.
            invertedIndexSPIMI();
            tokenStream.clear();
        }
    }

    private static void invertedIndexSPIMI() {
        block_number++;
        File output_file = new File("C:\\Users\\pucci\\Desktop\\AIDE\\" +
                "Multimedia Information Retrieval and Computer Vision\\inverted_index" + block_number + ".tsv");
        TreeMap<String, Term_Stats> vocabulary = new TreeMap<>();
        ArrayList<Posting> postings_list;

        //while (Runtime.getRuntime().freeMemory() > 0) {
            for (Token token : Inverted_Index_Construction.tokenStream) {
                if (!vocabulary.containsKey(token.getTerm()))
                    postings_list = addToLexicon(vocabulary, token.getTerm());
                else
                    postings_list = vocabulary.get(token.getTerm()).getPostingList();

                if (!postings_list.isEmpty()) {
                    int capacity = postings_list.size() * 2;
                    postings_list.ensureCapacity(capacity);
                }

                postings_list.add(new Posting(token.getDoc_id(), token.getFrequency()));
            }
        //}

        try {
            FileWriter myWriter = new FileWriter(output_file);

            for (String term : vocabulary.keySet()) {
                myWriter.write(term);

                for (Posting p : vocabulary.get(term).getPostingList())
                    myWriter.write("\t" + p.getDoc_id() + ":" + p.getTerm_frequency());

                myWriter.write("\n");
            }
            myWriter.close();

        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

    }

    private static ArrayList<Posting> addToLexicon(Map<String, Term_Stats> vocabulary, String token) {
        int capacity = 1;
        ArrayList<Posting> postings_list = new ArrayList<>(capacity);
        Term_Stats term_stats = new Term_Stats(0, postings_list);
        vocabulary.put(token, term_stats);
        return postings_list;
    }

    private static void mergeBlocks() throws FileNotFoundException {
        final List<BufferedReader> readerList = new ArrayList<BufferedReader>();

        for(int i = 0 ; i < block_number ; i++){
            readerList.add(new BufferedReader(new FileReader("C:\\Users\\pucci\\Desktop\\AIDE\\" +
                    "Multimedia Information Retrieval and Computer Vision\\inverted_index" + i + ".tsv")));



        }

    }

}