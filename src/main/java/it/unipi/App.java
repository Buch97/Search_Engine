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
                text = text.replaceAll("[.,;:\\-?!'_<>(){}\\[\\]\"/^*+&£@€°|%$=#]","");
                text = text.toLowerCase(Locale.ROOT);
                String[] tokens = text.split(" ");
                lexicon_construction(tokens, vocabulary);
                inverted_index_updating(tokens);
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


    private static void lexicon_construction(String[] tokens, HashMap<String, Term_Stats> lexicon) {

        List<String> tokenized_list = new ArrayList<>(Arrays.asList(tokens));
        Inverted_Index inverted_index = new Inverted_Index(null);
        ArrayList<String> doc_vocabulary = new ArrayList<>();

        //tolgo le stopwords
        for (String elem : STOPWORDS)
            tokenized_list.removeIf(i -> Objects.equals(i, elem));

        //tolgo stringa vuota dovuta allo split quando ce piu di uno spazio tra le parole
        tokenized_list.removeIf(i -> Objects.equals(i, ""));
        System.out.println(tokenized_list);

        //mi faccio una lista senza duplicati (serve per calcolare bene la doc frequency per il lexicon)
        for (String element : tokenized_list) {
            if (!doc_vocabulary.contains(element)) {
                doc_vocabulary.add(element);
            }
        }

        //aggiungo i termini al mio lexicon e se ci sono gia aggiorno la doc frequency
        for (String word : doc_vocabulary) {
            if (!lexicon.containsKey(word)) {
                Term_Stats term_stats = new Term_Stats(1, inverted_index);
                lexicon.put(word, term_stats);
            }
            else{
                int freq = lexicon.get(word).getDocument_frequency();
                lexicon.put(word, lexicon.get(word).update_frequency(freq));
            }
        }

    }

    private static void inverted_index_updating(String[] tokens) {
        List<String> tokenized_list = new ArrayList<>(Arrays.asList(tokens));
        Map<String,Integer> term_frequency = new HashMap<>();

        //serve per creare la posting list (conto la frequenza dei termini e lo metto in un map)
        for (String i : tokenized_list) {
            Integer j = term_frequency.get(i);
            term_frequency.put(i, (j == null) ? 1 : j + 1);
        }

        //serve solo per stampare
        for (String key: term_frequency.keySet()) {
            int freq = term_frequency.get(key);
            System.out.println("TERM: " + key + "  FREQ: " + freq);
        }
    }
}


//problemi: nel lexicon ci sono diversi termini composti da un carattere solo,
// ci sono caratteri strani, non conta bene la frequenza
