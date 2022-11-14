package it.unipi;


import java.io.*;
import java.util.*;


public class Inverted_Index_Construction {

    //dimensione per costruire i blocchi messa ora per prova a 3000 su una small collection
    public final static int SPIMI_TOKEN_STREAM_MAX_LIMIT = 5000000;
    public final static List<Token> tokenStream = new ArrayList<>();
    public static int block_number = 0; //indice da usare per scrivere i file parziali dell'inverted index
    public static File inverted_index = new File("./src/main/resources/output/inverted_index.tsv");

    public static void main(String[] args) {
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
            //faccio il merge di tutte le posting intermedie
            mergeBlocks();
            //dall'inverted index finale mi leggo riga per riga e costruisco il vocabolario finale
            //lo faccio qua in fondo perche nel vocabolario ci va la doc frequency che è la length della posting list, che conosco solo dopo aver finito il merge
            lexiconConstruction();


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
        Tokenizer tokenizer = new Tokenizer(doc_id, text);
        Map<String, Integer> results = tokenizer.tokenize();

        for (String token : results.keySet())
            tokenStream.add(new Token(token, tokenizer.getDocId(), results.get(token)));

        //aggiungo token al mio stream fino a che non ho raggiunto il limite
        if (tokenStream.size() >= SPIMI_TOKEN_STREAM_MAX_LIMIT) {
            //sono pronto per creare l'inverted index relativo a questo blocco
            invertedIndexSPIMI();
            tokenStream.clear(); //pulisco lo stream
        }
    }

    //guarda pseudocodice slide 59
    private static void invertedIndexSPIMI() {
        block_number++;
        File output_file = new File("./src/main/resources/intermediate_postings/inverted_index" + block_number + ".tsv");
        //tree map e non map perche le inserisce gia ordinate alfabeticamente
        HashMap<String, ArrayList<Posting>> vocabulary = new HashMap<>();
        ArrayList<Posting> postings_list;

        //while (Runtime.getRuntime().freeMemory() > 0) {
            for (Token token : Inverted_Index_Construction.tokenStream) {
                if (!vocabulary.containsKey(token.getTerm()))
                    postings_list = addToLexicon(vocabulary, token.getTerm());
                else
                    postings_list = vocabulary.get(token.getTerm());

                if (!postings_list.isEmpty()) {
                    int capacity = postings_list.size() * 2;
                    postings_list.ensureCapacity(capacity); //aumenta la length dell arraylist
                }

                postings_list.add(new Posting(token.getDoc_id(), token.getFrequency()));
            }
        //}

        TreeMap<String, ArrayList<Posting>> sorted_vocabulary = new TreeMap<>(vocabulary);

        try {
            //scrivo sul file
            FileWriter myWriter = new FileWriter(output_file);
            myWriter.write("TERM" + "\t" + "POSTING_LIST" + "\n");

            for (String term : sorted_vocabulary.keySet()) {
                myWriter.write(term + "\t");

                for (Posting p : sorted_vocabulary.get(term))
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
    // SOLO sui file da cui ho letto il termine corrente

    private static void mergeBlocks() throws IOException {
        ArrayList<String> orderedLines = new ArrayList<>();
        List<BufferedReader> readerList = new ArrayList<>();
        BufferedWriter output = new BufferedWriter(new FileWriter("./src/main/resources/output/final_inverted_index.tsv"));
        output.write("TERM" + "\t" + "POSTING_LIST" + "\n");

        //mi creo l'array di buffer di lettura cosi che scorro tutti i file in parallelo
        for(int i = 1 ; i <= block_number ; i++){
            readerList.add(new BufferedReader(new FileReader("./src/main/resources/intermediate_postings/inverted_index" + i + ".tsv")));
        }

        ArrayList<String> currentReadedLines = new ArrayList<>();
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

            output.write(currentTerm + "\t");

            for (String row : orderedLines) {
                String term = row.split("\t")[0];
                String posting = row.split("\t")[1];

                if (Objects.equals(term, currentTerm))
                    output.append(posting);
            }

            output.write("\n");

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

        output.close();

    }

    private static void lexiconConstruction() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("./src/main/resources/output/lexicon.tsv"));
        BufferedReader reader = new BufferedReader(new FileReader("./src/main/resources/output/final_inverted_index.tsv"));

        writer.write("TERM" + "\t" + "DOC_FREQUENCY" + "\t" + "BYTE_OFFSET_PL" + "\n");
        String line = reader.readLine();
        reader.readLine();
        long offset = 0;
        while (line != null){
            String term = line.split("\t")[0];
            int doc_frequency = line.split("\t")[1].split(" ").length;
            writer.write(term + "\t" + doc_frequency + "\t" + offset + "\n");
            offset += line.getBytes().length;
            line = reader.readLine();

        }
        writer.close();
        reader.close();
    }

}