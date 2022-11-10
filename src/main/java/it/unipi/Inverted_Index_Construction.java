package it.unipi;


import java.io.*;
import java.util.*;


public class Inverted_Index_Construction {

    public final static int SPIMI_TOKEN_STREAM_MAX_LIMIT = 3000;
    public final static List<Token> tokenStream = new ArrayList<>();
    public static Map<Integer, Doc_Stats> documents = new HashMap<>();
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
            }

            Document_Index document_index = new Document_Index(documents);
            document_index.save_to_file();
            myReader.close();
            mergeBlocks();
            lexiconConstruction();


        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
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
            invertedIndexSPIMI();
            tokenStream.clear();
        }
    }

    private static void invertedIndexSPIMI() {
        block_number++;
        File output_file = new File("C:\\Users\\pucci\\Desktop\\AIDE\\" +
                "Multimedia Information Retrieval and Computer Vision\\inverted_index" + block_number + ".tsv");
        TreeMap<String, ArrayList<Posting>> vocabulary = new TreeMap<>();
        ArrayList<Posting> postings_list;

        //while (Runtime.getRuntime().freeMemory() > 0) {
            for (Token token : Inverted_Index_Construction.tokenStream) {
                if (!vocabulary.containsKey(token.getTerm()))
                    postings_list = addToLexicon(vocabulary, token.getTerm());
                else
                    postings_list = vocabulary.get(token.getTerm());

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
                myWriter.write(term + "\t");

                for (Posting p : vocabulary.get(term))
                    myWriter.write(p.getDoc_id() + ":" + p.getTerm_frequency() + " ");

                myWriter.write("\n");
            }
            myWriter.close();

        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

    }

    private static ArrayList<Posting> addToLexicon(Map<String, ArrayList<Posting>> vocabulary, String token) {
        int capacity = 1;
        ArrayList<Posting> postings_list = new ArrayList<>(capacity);
        vocabulary.put(token, postings_list);
        return postings_list;
    }

    // apro tutti i file in parallelo, ad ogni iterazione devo prendere il termine alfabeticamente minore fra tutti
    // andare a vedere in tutti gli altri file se contengono quel termine e concatenare le posting lists
    // parto leggendo la prima riga di tutti i file, prendo il termine minore e faccio la procedura, avanzo di una riga
    // solo sui file da cui ho letto il termine corrente

    private static void mergeBlocks() throws IOException {
        ArrayList<String> orderedLines = new ArrayList<>();
        List<BufferedReader> readerList = new ArrayList<>();
        BufferedWriter output = new BufferedWriter(new FileWriter("C:\\Users\\pucci\\Desktop\\AIDE\\" +
                "Multimedia Information Retrieval and Computer Vision\\final_inverted_index.tsv"));

        for(int i = 1 ; i <= block_number ; i++){
            readerList.add(new BufferedReader(new FileReader("C:\\Users\\pucci\\Desktop\\AIDE\\" +
                    "Multimedia Information Retrieval and Computer Vision\\inverted_index" + i + ".tsv")));
        }

        ArrayList<String> currentReadedLines = new ArrayList<>();
        //metto la prima riga di ogni file dentro due arraylist: orderedLines e currentReadedLines (mi serve per associare uno
        // specifico reader alla posting da lui letta. Perche orderedLines viene poi ordinato e perdo traccia di questa
        // informazione perche l indice nell arrayList non corrisponde piu allo specifico reader)
        for (BufferedReader reader : readerList) {
            String line = reader.readLine();
            if (line != null) {
                currentReadedLines.add(line);
                orderedLines.add(line);
            }
            else
                reader.close();
        }

        // condizione di uscita dal loop infinito (quando tutti i reader sono arrivati a EOF e quindi l'arraylist avrà
        // size 0 perche non ci viene inserito nessun elemento)
        while (orderedLines.size() != 0) {

            //ordino l'array e prendo il primo elemento (che sarà il minore alfabeticamente)
            Collections.sort(orderedLines);
            String currentTerm = orderedLines.get(0).split("\t")[0];
            //System.out.println("MINORE: " + currentTerm);

            /*for (String elem : orderedLines)
                System.out.println("orderedLines " + elem);*/

            output.write(currentTerm + "\t");

            for (String row : orderedLines) {
                String term = row.split("\t")[0];
                String posting = row.split("\t")[1];

                if (Objects.equals(term, currentTerm))
                    output.append(posting);
            }

            output.write("\n");

            //rimuovo le righe appena processate dal mio array di appoggio
            orderedLines.removeIf(elem -> elem.split("\t")[0].equals(currentTerm));

            //guardo in quali file ho letto il currentTerm e solo in quelli avanzo il reader e metto la riga nuova che leggo nel mio orderedLines
            for (String elem : currentReadedLines) {
                if ((elem != null) && (Objects.equals(elem.split("\t")[0], currentTerm))) {
                    int fileIndex = currentReadedLines.indexOf(elem);
                    String nextRow = readerList.get(fileIndex).readLine();
                    currentReadedLines.set(fileIndex, nextRow);
                    if (nextRow != null)
                        orderedLines.add(nextRow);
                    else
                        readerList.get(fileIndex).close();
                }
            }

            /*for (String elem : currentReadedLines)
                System.out.println("***Lines***: " + elem);

            for (String elem : orderedLines)
                System.out.println("***orderedLines***: " + elem);*/

        }

        output.close();

    }

    private static void lexiconConstruction() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\Users\\pucci\\Desktop\\AIDE\\" +
                "Multimedia Information Retrieval and Computer Vision\\lexicon.tsv"));
        BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\pucci\\Desktop\\AIDE\\" +
                "Multimedia Information Retrieval and Computer Vision\\final_inverted_index.tsv"));

        String line = reader.readLine();
        long offset = 0;
        while (line != null){
            String term = line.split("\t")[0];
            int doc_frequency = line.split("\t")[1].split(" ").length;
            writer.write(term + "\t" + "DOC_FREQUENCY: " + doc_frequency + "\t" + "BYTE_OFFSET: " + offset + "\n");
            offset += line.getBytes().length;
            line = reader.readLine();

        }
        writer.close();
        reader.close();
    }

}