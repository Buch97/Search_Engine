package it.unipi;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class App
{
    public static void main( String[] args )
    {
        try {
            File myObj = new File("C:\\Users\\pucci\\Desktop\\AIDE\\Multimedia Information Retrieval and Computer Vision\\small_collection.tsv");
            Scanner myReader = new Scanner(myObj);
            List<Doc_Stats> docs = new ArrayList<>();
            int tab_length = "\t".getBytes().length;

            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                int doc_id = Integer.parseInt(data.substring(0,data.indexOf("\t")));
                int doc_len = data.getBytes().length - tab_length - Integer.toString(doc_id).getBytes().length;
                Doc_Stats doc = new Doc_Stats(doc_id,doc_len);
                docs.add(doc);
            }

            Document_Index document_index = new Document_Index(docs);
            document_index.print();
            myReader.close();

        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}
