package it.unipi;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;


public class App
{
    public static final String[] STOPWORDS = {"a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
            "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then",
            "there", "these","they", "this", "to", "was", "will", "with"};
    public static final String[] PUNCTUATUION = {"'", ".", ":", ",", "!", "?", "(", ")", ";", "-", "_", "/", "\\", "[", "]",
         "{", "}"};

    public static void main( String[] args )
    {
        try {
            File myObj = new File("C:\\Users\\pucci\\Desktop\\AIDE\\Multimedia Information Retrieval and Computer Vision\\prova.tsv");
            Scanner myReader = new Scanner(myObj, "UTF-8");
            //List<Doc_Stats> docs = new ArrayList<>();
            HashMap<Integer, Doc_Stats> documents = new HashMap<>();
            HashMap<String, Term_Stats> vocabulary = new HashMap<>();
            int tab_length = "\t".getBytes().length;

            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                String[] row = data.split("\t");
                String doc_no = row[0];
                String text = row[1];
                lexicon_construction(text, vocabulary);
                //System.out.println(text);
                int doc_len = text.getBytes().length;
                Doc_Stats doc = new Doc_Stats(doc_no,doc_len);
                documents.put(Integer.parseInt(doc_no),doc);
                //docs.add(doc);
            }

            //Document_Index document_index = new Document_Index(docs);
            Document_Index_Hash document_index = new Document_Index_Hash(documents);
            Lexicon lexicon = new Lexicon(vocabulary);
            lexicon.print();
            //document_index.print();
            myReader.close();

        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    private static void lexicon_construction(String text, HashMap<String, Term_Stats> lexicon) {
        text = text.replaceAll("[.,;:\\-?!'_<>(){}\\[\\]\"/^*+&£@€°|%$=#]","");
        text = text.toLowerCase(Locale.ROOT);
        String[] tokens = text.split(" ");
        List<String> tokenized_list = new ArrayList<>(Arrays.asList(tokens));
        Inverted_Index inverted_index = new Inverted_Index(null);

        for (String elem : STOPWORDS)
            tokenized_list.removeIf(i -> Objects.equals(i, elem));
        tokenized_list.removeIf(i -> Objects.equals(i, ""));
        System.out.println(tokenized_list);

        int count = 1;

        for (String word : tokenized_list) {
            if (!lexicon.containsKey(word)) {
                //System.out.println("primo " + word);
                Term_Stats term_stats = new Term_Stats(1, inverted_index);
                lexicon.put(word, term_stats);
            }
            // qui devo aumentare la frequenza di uno a meno che il termine non compaia due volte nel SOLITO documento
            else if ((lexicon.containsKey(word)) && (count != 0)){ // aggiorno la doc_freq 1 volta sola
                //System.out.println("secondo " + word);
                count --;
                int freq = lexicon.get(word).getDocument_frequency();
                lexicon.put(word, lexicon.get(word).update_frequency(freq));
            }
        }
    }
}


//problemi: nel lexicon ci sono diversi terminbi composti da un carattere solo,
// ci sono caratteri strani, non conta bene la frequenza
