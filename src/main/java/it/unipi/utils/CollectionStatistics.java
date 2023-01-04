package it.unipi.utils;

import it.unipi.bean.DocumentIndexStats;
import it.unipi.utils.serializers.CustomSerializerDocumentIndexStats;
import org.mapdb.DB;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.*;
import java.util.Map;

public class CollectionStatistics {
    public static int num_docs = 0;
    public static double avg_doc_len = 0;

    public CollectionStatistics() {
    }

    public static void computeAvgDocLen(DB db_document_index) throws IOException {
        HTreeMap<Integer, DocumentIndexStats> document_index_map = db_document_index
                .hashMap("document_index")
                .keySerializer(Serializer.INTEGER)
                .valueSerializer(new CustomSerializerDocumentIndexStats())
                .createOrOpen();
        long sum = 0;

        for (Map.Entry<Integer, DocumentIndexStats> entry : document_index_map.entrySet()) {
            sum += entry.getValue().getDoc_len();
        }
        avg_doc_len = (double) (sum / num_docs);

        if (num_docs != 0 && avg_doc_len != 0)
            writeOnFile();
    }

    public static void computeNumDocs() throws IOException {
        int rows = 0;
        BufferedReader collection = new BufferedReader(new FileReader("./src/main/resources/collections/collection.tsv"));
        while (collection.readLine() != null) rows++;
        num_docs = rows;

        if (num_docs != 0 && avg_doc_len != 0)
            writeOnFile();
    }

    private static void writeOnFile() throws IOException {
        File file = new File("src/main/resources/Stats/stats.txt");
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));

        try {
            bufferedWriter.append(String.valueOf(avg_doc_len)).append(" ");
            bufferedWriter.append(String.valueOf(num_docs));
        } catch (Exception e) {
            e.printStackTrace();
        }
        bufferedWriter.close();
    }

    public static void setParameters() {
        File file = new File("src/main/resources/Stats/stats.txt");
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
            String line = bufferedReader.readLine();
            avg_doc_len = Double.parseDouble(line.split(" ")[0]);
            num_docs = Integer.parseInt(line.split(" ")[1]);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
