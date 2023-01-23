package it.unipi.dii.aide.mircv.common.utils;


import java.io.*;

public class CollectionStatistics {
    private static final String stats = "resources/stats/stats.txt";
    public static int num_docs = 0;
    public static double avg_doc_len = 0;

    public CollectionStatistics() {
    }

    public static void setNum_docs() {
        num_docs++;
    }

    public static void setAvg_doc_len(int doc_len) {
        avg_doc_len += doc_len;
    }

    public static void computeAvgDocLen() {
        avg_doc_len /= num_docs;
        writeOnFile();
    }


    private static void writeOnFile() {

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(stats))) {
            bufferedWriter.append(String.valueOf(avg_doc_len)).append(" ");
            bufferedWriter.append(String.valueOf(num_docs));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void setParameters() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(stats))) {
            String line = bufferedReader.readLine();
            avg_doc_len = Double.parseDouble(line.split(" ")[0]);
            num_docs = Integer.parseInt(line.split(" ")[1]);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
