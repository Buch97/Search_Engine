package it.unipi;


import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.*;
import java.util.*;


public class Index_Construction {

    //dimensione per costruire i blocchi messa ora per prova a 3000 su una small collection
    public final static int SPIMI_TOKEN_STREAM_MAX_LIMIT = 3000;
    public final static List<Token> tokenStream = new ArrayList<>();
    public static int block_number = 0; //indice da usare per scrivere i file parziali dell'inverted index
    public static HTreeMap myMap;

    public static void buildDataStructures() {
        try {
            File myObj = new File("./src/main/resources/collections/small_collection.tsv");

            //semplice roba di utility per creare le directory in cui ci vanno salvati i files
            File theDir = new File("./src/main/resources/output");

            if (!theDir.exists()){
                if(theDir.mkdirs())
                    System.out.println("New directory '/output' created");
            }

            theDir = new File("./src/main/resources/intermediate_postings");
            if (!theDir.exists()){
                if(theDir.mkdirs())
                    System.out.println("New directory '/intermediate_postings' created");
            }

            Scanner myReader = new Scanner(myObj, "UTF-8");
            BufferedWriter writer_doc_index = new BufferedWriter(new FileWriter("./src/main/resources/output/document_index.tsv"));
            writer_doc_index.write("DOC_ID" + "\t" + "DOC_NO" + "\t" + "DOC_LEN" + "\n");

            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                System.out.println(data);

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
            //faccio il merge di tutte le posting intermedie e mi costruisco inv_index e lexicon parallelamente
            //DB db = DBMaker.fileDB("./src/main/resources/output/lexicon_disk_based.db").make();
            //myMap = db.hashMap("lexicon").createOrOpen();

            mergeBlocks();
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
            block_number++;
        }
    }

    //guarda pseudocodice slide 59
    private static void invertedIndexSPIMI() {

        File output_file = new File("./src/main/resources/intermediate_postings/inverted_index" + block_number + ".tsv");
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

    //MERGE:
    // apro tutti i file in parallelo, ad ogni iterazione devo prendere il termine alfabeticamente minore fra tutti
    // andare a vedere in tutti gli altri file se contengono quel termine e concatenare le posting lists
    // parto leggendo la prima riga di tutti i file, prendo il termine minore e faccio la procedura, avanzo di una riga
    // SOLO sui file da cui ho letto il termine corrente

    private static void mergeBlocks() throws IOException {
        ArrayList<String> orderedLines = new ArrayList<>();
        ArrayList<String> currentReadedLines = new ArrayList<>();
        List<BufferedReader> readerList = new ArrayList<>();
        long offset = 0;
        int doc_frequency;
        int coll_frequency;
        long actual_offset;

        BufferedWriter inv_ind = new BufferedWriter(new FileWriter("./src/main/resources/output/inverted_index.tsv"));

        BufferedWriter lexicon = new BufferedWriter(new FileWriter("./src/main/resources/output/lexicon.tsv"));
        String header = "TERM" + "\t" + "DOC_FREQUENCY" + "\t" + "COLL_FREQUENCY" + "\t" + "BYTE_OFFSET_PL" + "\n";
        lexicon.write(header);

        //mi creo l'array di buffer di lettura cosi che scorro tutti i file in parallelo
        for(int i = 0 ; i <= block_number ; i++){
            readerList.add(new BufferedReader(new FileReader("./src/main/resources/intermediate_postings/inverted_index" + i + ".tsv")));
        }

        //metto la prima riga di ogni file dentro due arraylist: orderedLines (mi serve per calcolare il termine alfabeticamente minore)
        // e currentReadedLines (mi serve per associare uno specifico reader alla posting da lui letta.
        // Perche orderedLines viene poi ordinato e perdo traccia di questa
        // informazione perche l'indice nell arrayList non corrisponde piu allo specifico reader)
        for (BufferedReader reader : readerList) {
            //salto la prima riga che contiene lo header del file
            reader.readLine();
            String line = reader.readLine();
            if (line != null) {
                currentReadedLines.add(line);
                orderedLines.add(line);
            }
            else
                reader.close();
        }

        // condizione di uscita dal loop infinito (quando tutti i reader sono arrivati a EOF e quindi l'arraylist avrà
        // size 0 perche non contiene piu nessun elemento)

        while (orderedLines.size() != 0) {

            //ordino l'array e prendo il primo elemento (che sarà il minore alfabeticamente)
            Collections.sort(orderedLines);
            String currentTerm = orderedLines.get(0).split("\t")[0];

            lexicon.write(currentTerm + "\t");
            doc_frequency = 0;
            coll_frequency = 0;
            actual_offset = offset;

            for (String row : orderedLines) {
                String term = row.split("\t")[0];
                String posting = row.split("\t")[1];

                if (Objects.equals(term, currentTerm)){
                    doc_frequency += posting.split(" ").length;
                    for(String post : posting.split(" ")){
                        coll_frequency += Integer.parseInt(post.split(":")[1]);
                    }
                    inv_ind.append(posting);
                    offset += posting.getBytes().length;
                }
            }

            inv_ind.write("\n");
            offset += "\n".getBytes().length;
            lexicon.write(doc_frequency + "\t" + coll_frequency + "\t" + actual_offset + "\n");
            //myMap.put(currentTerm, new Term_Stats(doc_frequency, coll_frequency, actual_offset));

            //rimuovo le righe appena processate dal mio array di appoggio
            //orderedLines conterrà sempre N termini da confrontare tra di loro per prendere il minore
            orderedLines.removeIf(elem -> elem.split("\t")[0].equals(currentTerm));

            //guardo in quali file ho letto il currentTerm e solo in quelli avanzo il reader e metto la riga nuova che leggo nel mio orderedLines
            for (String elem : currentReadedLines) {
                if ((elem != null) && (Objects.equals(elem.split("\t")[0], currentTerm))) {
                    int fileIndex = currentReadedLines.indexOf(elem);
                    String nextRow = readerList.get(fileIndex).readLine();
                    currentReadedLines.set(fileIndex, nextRow);
                    if (nextRow != null)
                        //ci metto dentro i termini nuovi dei soli buffer che ho avanzato
                        orderedLines.add(nextRow);
                    else
                        readerList.get(fileIndex).close();
                }
            }
        }

        for (BufferedReader reader : readerList)
            reader.close();

        inv_ind.close();
        lexicon.close();
    }

}