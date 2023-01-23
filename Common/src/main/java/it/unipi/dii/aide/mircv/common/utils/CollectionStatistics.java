package it.unipi.dii.aide.mircv.common.utils;

import it.unipi.dii.aide.mircv.common.bean.DocumentIndexStats;
import org.mapdb.HTreeMap;

import java.io.*;
import java.util.Map;

public class CollectionStatistics {
    public static int num_docs = 0;
    public static double avg_doc_len = 0;

    public CollectionStatistics() {
    }

    public static void computeAvgDocLen(HTreeMap<Integer, DocumentIndexStats> documentIndexMap) throws IOException {
        long sum = 0;

        for (Map.Entry<Integer, DocumentIndexStats> entry : documentIndexMap.entrySet()) {
            sum += entry.getValue().getDoc_len();
        }

        avg_doc_len = (double) (sum / num_docs);
        writeOnFile();
    }

    public static void computeNumDocs() throws IOException {
        int rows = 0;
        BufferedReader collection;

        if(Flags.isDebug())
            collection = new BufferedReader(new FileReader("resources/collections/small_collection.tsv"));
        else
            collection = new BufferedReader(new FileReader("resources/collections/collection.tsv"));

        while (collection.readLine() != null) rows++;
        num_docs = rows;
    }

    private static void writeOnFile() throws IOException {
        File file = new File("resources/stats/stats.txt");
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
        File file = new File("resources/stats/stats.txt");
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
            String line = bufferedReader.readLine();
            avg_doc_len = Double.parseDouble(line.split(" ")[0]);
            num_docs = Integer.parseInt(line.split(" ")[1]);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
