package it.unipi;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;


public class App
{
    public static void main( String[] args )
    {
        try {
            File myObj = new File("C:\\Users\\pucci\\Desktop\\AIDE\\Multimedia Information Retrieval and Computer Vision\\small_collection.tsv");
            Scanner myReader = new Scanner(myObj);
            //List<Doc_Stats> docs = new ArrayList<>();
            HashMap<Integer, Doc_Stats> documents = new HashMap<>();
            int tab_length = "\t".getBytes().length;

            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                String doc_no = data.substring(0,data.indexOf("\t"));
                int doc_len = data.getBytes().length - tab_length - doc_no.getBytes().length;
                Doc_Stats doc = new Doc_Stats(doc_no,doc_len);
                documents.put(Integer.parseInt(doc_no),doc);
                //docs.add(doc);
            }

            //Document_Index document_index = new Document_Index(docs);
            Document_Index_Hash doc_ind = new Document_Index_Hash(documents);
            //document_index.print();
            doc_ind.print();
            myReader.close();

        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}
